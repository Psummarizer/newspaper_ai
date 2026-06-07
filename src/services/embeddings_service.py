"""
EmbeddingsService — pipeline 3-stage profesional de filtrado.

Stage 1 (este módulo): embeddings + cosine. Recall alto, threshold bajo.
Stage 2 (orchestrator): LLM YES/NO estricto por artículo. Precision alta.
Stage 3 (orchestrator): LLM rules (subtopics, exclusiones del usuario).

Estrategia:
  - OpenAI text-embedding-3-small ($0.02/1M tokens, 1536 dim).
    Mejor separación que Mistral embed para topics generalistas.
  - Topic expansion: una llamada LLM (Mistral free) por topic+context expande
    el topic con vocabulario semántico antes del embedding. Mejora separación.
  - Cache persistente: embedding por noticia almacenado en topics.json.
  - Cost tracking: log diario en GCS (`embeddings_costs.json`).
  - Fail-open: si la API falla, el pipeline continúa (LLM filter actúa solo).
"""

import os
import math
import json
import logging
from datetime import datetime
from typing import List, Optional

from openai import AsyncOpenAI
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Coste oficial OpenAI (revisar OpenAI pricing periódicamente):
# text-embedding-3-small: $0.02 / 1M input tokens.
COST_PER_1M_TOKENS_USD = 0.02
EMBED_MODEL = "text-embedding-3-small"
EMBED_DIM = 1536  # dimensión nativa de text-embedding-3-small


class EmbeddingsService:
    """Singleton para embeddings + cost tracking."""

    _instance: Optional["EmbeddingsService"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_done = False
        return cls._instance

    def __init__(self):
        if self._init_done:
            return
        load_dotenv()
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = EMBED_MODEL
        self.client: Optional[AsyncOpenAI] = None
        if self.api_key:
            try:
                self.client = AsyncOpenAI(api_key=self.api_key)
            except Exception as e:
                logger.warning(f"EmbeddingsService init falló: {e}")
                self.client = None
        else:
            logger.warning("OPENAI_API_KEY no encontrada — embeddings desactivados (fail-open)")
        # Cache memoria del run
        self._memory_cache: dict = {}
        # Cost accumulator del run actual
        self._run_tokens = 0
        self._run_cost_usd = 0.0
        self._init_done = True

    @property
    def is_available(self) -> bool:
        return self.client is not None

    @staticmethod
    def cosine(a: List[float], b: List[float]) -> float:
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(y * y for y in b))
        if na == 0 or nb == 0:
            return 0.0
        return dot / (na * nb)

    async def embed_text(self, text: str) -> Optional[List[float]]:
        if not self.is_available or not text:
            return None
        text = text.strip()[:8000]
        if not text:
            return None
        cache_key = hash(text)
        if cache_key in self._memory_cache:
            return self._memory_cache[cache_key]
        try:
            response = await self.client.embeddings.create(
                model=self.model, input=[text],
            )
            vec = response.data[0].embedding
            tokens = getattr(response.usage, "total_tokens", 0) or 0
            self._track_cost(tokens)
            self._memory_cache[cache_key] = vec
            return vec
        except Exception as e:
            logger.warning(f"embed_text falló: {e}")
            return None

    async def embed_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        if not self.is_available or not texts:
            return [None] * len(texts)
        clean = [(i, (t or "").strip()[:8000]) for i, t in enumerate(texts)]
        non_empty = [(i, t) for i, t in clean if t]
        if not non_empty:
            return [None] * len(texts)

        results: List[Optional[List[float]]] = [None] * len(texts)
        to_fetch_idx: List[int] = []
        to_fetch_text: List[str] = []
        for i, t in non_empty:
            cache_key = hash(t)
            if cache_key in self._memory_cache:
                results[i] = self._memory_cache[cache_key]
            else:
                to_fetch_idx.append(i)
                to_fetch_text.append(t)

        if not to_fetch_text:
            return results

        # OpenAI text-embedding-3-small acepta hasta 2048 inputs/llamada.
        # Usamos batch de 100 para no superar 8192 tokens/input total.
        BATCH = 100
        for start in range(0, len(to_fetch_text), BATCH):
            batch_text = to_fetch_text[start:start + BATCH]
            batch_idx = to_fetch_idx[start:start + BATCH]
            try:
                response = await self.client.embeddings.create(
                    model=self.model, input=batch_text,
                )
                tokens = getattr(response.usage, "total_tokens", 0) or 0
                self._track_cost(tokens)
                for j, item in enumerate(response.data):
                    vec = item.embedding
                    orig_i = batch_idx[j]
                    results[orig_i] = vec
                    self._memory_cache[hash(batch_text[j])] = vec
            except Exception as e:
                logger.warning(f"embed_batch falló (batch {start}): {e}")
        return results

    def _track_cost(self, tokens: int) -> None:
        if tokens <= 0:
            return
        self._run_tokens += tokens
        self._run_cost_usd += tokens * COST_PER_1M_TOKENS_USD / 1_000_000

    def get_run_stats(self) -> dict:
        return {
            "tokens": self._run_tokens,
            "cost_usd": round(self._run_cost_usd, 6),
            "model": self.model,
        }

    def reset_run_stats(self) -> None:
        self._run_tokens = 0
        self._run_cost_usd = 0.0

    async def filter_by_similarity(
        self, topic_text: str, articles: List[dict],
        threshold: float = 0.20, log_label: str = "",
    ) -> tuple:
        """Filtra artículos por similaridad coseno con el topic (Stage 1).

        Threshold default 0.20 — topics amplios (deporte, geopolitica) tienen
        rangos dispersos 0.20-0.30. El Stage 2 LLM YES/NO descarta lo no relevante.

        Returns:
            (kept, dropped). Si embeddings desactivados → fail-open.
        """
        if not self.is_available or not articles:
            return articles, []

        topic_vec = await self.embed_text(topic_text)
        if topic_vec is None:
            return articles, []

        # Embeddings de artículos (cacheados en `embedding` del dict)
        idx_need: List[int] = []
        text_need: List[str] = []
        for i, art in enumerate(articles):
            emb = art.get("embedding")
            # Solo re-calcular si no hay embedding O si la dim no coincide con el modelo
            if not isinstance(emb, list) or len(emb) != EMBED_DIM:
                title = (art.get("titulo", "") or "").strip()
                resumen = (art.get("resumen", "") or "").strip()[:500]
                text = f"{title}. {resumen}".strip()
                if text:
                    idx_need.append(i)
                    text_need.append(text)

        if text_need:
            new_embs = await self.embed_batch(text_need)
            for k, vec in enumerate(new_embs):
                if vec is not None:
                    articles[idx_need[k]]["embedding"] = vec

        kept, dropped = [], []
        sims = []
        for art in articles:
            emb = art.get("embedding")
            if not emb or len(emb) != EMBED_DIM:
                kept.append(art)  # fail-open
                continue
            sim = self.cosine(topic_vec, emb)
            sims.append(sim)
            art["_sim_score"] = round(sim, 4)
            if sim >= threshold:
                kept.append(art)
            else:
                dropped.append(art)

        if sims:
            avg = sum(sims) / len(sims)
            logger.info(
                f"   🧠 Stage 1 embed [{log_label}]: {len(kept)}/{len(articles)} pasaron "
                f"(thr={threshold}, avg_sim={avg:.3f}, max={max(sims):.3f}, min={min(sims):.3f})"
            )
        return kept, dropped


async def expand_topic_with_llm(topic: str, context: str, processor) -> str:
    """Expande topic+context con vocabulario semántico para mejorar separación
    en embeddings. UNA llamada Mistral free, cacheada cross-runs.

    Ejemplo:
      Input: ("Roman archeology", "new discoveries and restorations")
      Output: "Roman archeology, new discoveries and restorations.
               archaeology excavations ruins ancient civilizations Pompeii
               artifacts Greek Roman restoration temples necropolis epigraphy
               antiquity"
    """
    base = (topic or "").strip()
    if context:
        base = f"{base}. {context.strip()}"
    if not base:
        return ""

    prompt = f"""Genera una expansión semántica del topic para mejorar embeddings.

TOPIC + CONTEXTO: "{base}"

Devuelve un PÁRRAFO de 30-50 palabras con:
- El topic original SIN modificar al inicio.
- Después: vocabulario semánticamente cercano:
  · Sinónimos y términos técnicos del dominio.
  · Conceptos relacionados (no entidades específicas como nombres de personas
    o empresas que pueden estar desactualizadas).
  · Ámbitos geográficos/temporales si aplica.
- Cubre los matices del CONTEXTO si los hay.
- ⚠️ NO añadas nombres concretos de jugadores, entrenadores, CEOs o
  personajes — estos cambian (fichajes, dimisiones) y el embedding queda
  desactualizado. Usa términos genéricos como "plantilla", "entrenador",
  "primer equipo", "directiva".

EJEMPLOS:
TOPIC: "Roman archeology" CTX: "new discoveries"
→ "Roman archeology, new discoveries. Excavations ruins ancient civilizations
   Pompeii artifacts archaeologists temples necropolis epigraphy antiquity
   restoration sites Mediterranean classical Greek Roman."

TOPIC: "Real Madrid" CTX: "Solo fútbol masculino"
→ "Real Madrid, fútbol masculino. La Liga Champions League Bernabéu primera
   plantilla entrenador presidente fichaje traspaso resultado liga partido
   canterano filial Castilla, sin baloncesto sin femenino."

Devuelve SOLO el párrafo expandido, sin comillas ni explicación."""

    try:
        response = await processor.client.chat.completions.create(
            model=processor.model_fast,
            messages=[{"role": "user", "content": prompt}],
        )
        expanded = (response.choices[0].message.content or "").strip()
        # Sanity check: si la respuesta es absurdamente corta, devolver original
        if len(expanded) < len(base) // 2:
            return base
        return expanded
    except Exception as e:
        logger.warning(f"Topic expansion falló: {e}")
        return base


async def llm_strict_yes_no_filter(
    topic: str, context: str, articles: List[dict], processor, batch_size: int = 20,
    subtopic_rules: List[dict] = None,
) -> List[dict]:
    """Stage 2: LLM YES/NO estricto. Verifica que cada artículo:
       1. Trata DIRECTA Y SUSTANCIALMENTE sobre el topic.
       2. Cumple AL MENOS UNO de los subtopics si están definidos.
       3. NO es obsoleto (eventos futuros anunciados que YA pasaron).

    subtopic_rules: [{"name":"tenis","rule":"preferir Alcaraz/Jódar"}, ...]
    Llamadas Mistral free en batch (20 por call). Devuelve solo los SI.
    """
    if not articles:
        return articles

    from datetime import datetime as _dt
    now_str = _dt.now().strftime("%A, %d de %B de %Y, %H:%M (zona Madrid)")
    base = topic + (f". {context}" if context else "")

    # Stage 2: pasa SUBTOPICS POSITIVOS (entidades preferidas) + reglas de
    # exclusión al prompt. El LLM puede entonces:
    #   - Reconocer que "Jódar" es entidad preferida del usuario (válido)
    #   - Aplicar exclusiones explícitas ("solo masculino" excluye femenino)
    subtopics_block = ""
    positive_subs: List[str] = []
    exclusion_rules: List[str] = []
    if subtopic_rules:
        for sr in subtopic_rules:
            name = sr.get("name", "")
            rule = (sr.get("rule", "") or "")
            rule_lower = rule.lower()
            # Nombre del subtopic + cualquier "preferir X" → entidad positiva
            entity_line = name
            if rule and "preferir" in rule_lower:
                entity_line = f"{name} ({rule})"
            positive_subs.append(entity_line)
            # Detectar exclusiones explícitas
            if rule and any(k in rule_lower for k in ["solo ", "sólo ", "no ", "sin ", "excluir", "salvo "]):
                exclusion_rules.append(f"subtopic '{name}': {rule}")
        # Detección de topic NICHO: ≥5 subtopics, todos son entidades sin
        # regla de exclusión (ej: "Canton Network", "JPM Kinexys", "Onyx HSBC"
        # para Institutional blockchain networks). En ese caso, una noticia
        # del dominio (DLT permissioned, tokenización, settlement on-chain)
        # es legítima aunque no nombre la plataforma concreta. Sin este
        # ajuste, el LLM rechazaba ~90% de artículos válidos por no nombrar
        # exactamente "Kinexys" o "Canton". Caso real 2026-05-28: alex con
        # 11→1 (Institutional blockchain) y 4→1 (Tokenización).
        _niche_mode = len(positive_subs) >= 5 and not exclusion_rules
        if positive_subs:
            if _niche_mode:
                subtopics_block = (
                    "\nENTIDADES DE INTERÉS PRIORITARIO (NO obligatorias):\n"
                    + "\n".join(f"  · {p}" for p in positive_subs)
                    + "\nEstas entidades concretas le interesan especialmente al usuario,\n"
                    + "PERO el topic es un dominio amplio. Cualquier noticia DEL DOMINIO\n"
                    + "del topic es SI aunque no nombre estas entidades específicas.\n"
                    + "Solo rechaza si la noticia está CLARAMENTE fuera del dominio.\n"
                )
            else:
                subtopics_block = (
                    "\nENTIDADES PREFERIDAS del usuario dentro del topic:\n"
                    + "\n".join(f"  · {p}" for p in positive_subs)
                    + "\nNoticias que mencionen estas entidades = SI (son lo que el usuario quiere).\n"
                    + "Noticias del topic general SIN mencionar estas entidades = SI también si"
                      " son del mismo dominio (no descartes por no mencionar la entidad concreta).\n"
                )
        if exclusion_rules:
            subtopics_block += (
                "\nREGLAS DE EXCLUSIÓN EXPLÍCITAS del usuario (OBLIGATORIAS):\n"
                + "\n".join(f"  · {r}" for r in exclusion_rules)
                + "\nUna noticia que viole alguna de estas reglas = NO.\n"
            )

        # --- ADJACENCIA DE DISCIPLINAS (estricta) ---
        # Cuando el topic es genérico (ej "deporte") pero los subtopics nombran
        # disciplinas/equipos concretos (F1, MotoGP, Tenis, Lakers), debemos
        # rechazar (a) disciplinas adyacentes que comparten apellidos comunes
        # y (b) contenido lifestyle/personal del personaje sin contexto deportivo.
        # Casos reales (fallos observados):
        #   - subtopic="F1"     → "Alonso pasea con Porsche 918 en Mónaco" NO
        #   - subtopic="Lakers" → "Wembanyama lleva a los Spurs a las Finales NBA" NO
        # IMPORTANTE: si el usuario tiene MÚLTIPLES subtopics adyacentes (ej
        # tenis Y pádel), el "forbidden" de tenis debe excluir pádel — el
        # usuario ya pidió ambos explícitamente.
        #
        # Cada entrada: (forbidden_set, valid_examples, what_we_want)
        # `what_we_want` = lo que SI cuenta como el subtopic (filtra lifestyle/gossip).
        _discipline_adjacency = {
            "f1":          ({"rally", "dakar", "wec", "indycar", "motogp", "fórmula e", "formula e", "nascar", "sbk",
                             "lifestyle del piloto", "colección de coches", "vida personal"},
                            "Verstappen, Hamilton, Leclerc, Norris, Russell, Sainz Jr, Alonso, Piastri, Tsunoda",
                            "Gran Premio, parrilla, clasificación, podio, ingeniería, fichajes/rookies del equipo F1, FIA, sanciones, polémicas en pista"),
            "formula 1":   ({"rally", "dakar", "wec", "indycar", "motogp", "fórmula e", "formula e", "nascar", "sbk",
                             "lifestyle del piloto", "colección de coches", "vida personal"},
                            "Verstappen, Hamilton, Leclerc, Norris, Russell, Sainz Jr, Alonso, Piastri, Tsunoda",
                            "Gran Premio, parrilla, clasificación, podio, ingeniería, fichajes/rookies del equipo F1, FIA, sanciones, polémicas en pista"),
            "motogp":      ({"f1", "formula 1", "fórmula 1", "sbk", "rally", "dakar", "bsb",
                             "lifestyle del piloto", "colección de motos", "vida personal"},
                            "Márquez, Bagnaia, Quartararo, Acosta, Bezzecchi, Martín",
                            "Gran Premio, parrilla MotoGP, clasificación, podio, fichajes, FIM, lesiones de carrera"),
            "tenis":       ({"pádel", "padel", "tenis de mesa", "frontón", "frontenis", "badminton", "bádminton",
                             "vida personal del tenista", "moda/celebrity"},
                            "Alcaraz, Sinner, Djokovic, Nadal, Swiatek, Sabalenka",
                            "torneo ATP/WTA, partido, set, ranking, retirada, lesión deportiva, sanción"),
            "pádel":       ({"tenis", "badminton", "bádminton", "tenis de mesa",
                             "vida personal del jugador", "celebrity"},
                            "Coello, Tapia, Galán, Lebrón",
                            "Premier Padel, WPT, partido, ranking, equipos, sanción"),
            "padel":       ({"tenis", "badminton", "bádminton", "tenis de mesa",
                             "vida personal del jugador", "celebrity"},
                            "Coello, Tapia, Galán, Lebrón",
                            "Premier Padel, WPT, partido, ranking, equipos, sanción"),
            "nba":         ({"ncaa", "euroliga", "acb", "wnba", "baloncesto femenino",
                             "vida personal", "moda/celebrity"},
                            "LeBron, Curry, Doncic, Tatum, Jokic, Antetokounmpo",
                            "playoffs, finals, partido, traspaso, draft NBA, lesión, sanción"),
            # Lakers = SOLO Lakers. Otras franquicias NBA NO valen, aunque sean NBA.
            # Excepción: noticia de NBA general (rules, draft, commissioner) que afecta a Lakers.
            "lakers":      ({"otras franquicias NBA (Spurs, Knicks, Celtics, Warriors, Heat, Nuggets, Thunder, etc.) sin mencionar a Lakers",
                             "ncaa", "euroliga", "acb", "wnba", "baloncesto femenino",
                             "vida personal", "moda/celebrity"},
                            "LeBron, Reaves, Doncic (LAL), Hachimura",
                            "Lakers playoffs, partido de Lakers, traspaso Lakers, lesión jugador Lakers"),
            # Real Madrid = SOLO Real Madrid (primer equipo masculino).
            # Champions League sin mencionar Real Madrid NO vale.
            "real madrid": ({"otros equipos LaLiga (Atlético, Barcelona, Sevilla, Valencia, Athletic, etc.) sin mencionar Real Madrid",
                             "selecciones nacionales sin conexión al club",
                             "baloncesto femenino", "fútbol femenino",
                             "filial/Castilla", "cantera/juvenil",
                             "vida personal", "moda/celebrity"},
                            "Bellingham, Vinicius, Modric, Mbappé, Camavinga, Tchouameni, Rodrygo, Valverde",
                            "partido Real Madrid, traspaso/fichaje Real Madrid, lesión jugador Real Madrid, LaLiga (con Real Madrid), Champions (con Real Madrid)"),
        }
        # Set de disciplinas que el usuario YA tiene como subtopic — nunca
        # las metemos en el "forbidden" de otro subtopic.
        _user_disciplines = {
            s.get("name", "").lower().strip() for s in subtopic_rules
        }
        # Aliases para que "F1" en user_disciplines también cubra "formula 1"
        _alias_map = {
            "f1": ["formula 1", "fórmula 1"],
            "formula 1": ["f1", "fórmula 1"],
            "fórmula 1": ["f1", "formula 1"],
            "pádel": ["padel"],
            "padel": ["pádel"],
        }
        _user_disciplines_expanded = set(_user_disciplines)
        for d in list(_user_disciplines):
            _user_disciplines_expanded.update(_alias_map.get(d, []))

        adjacency_lines: list = []
        for sub_name in (s.get("name", "") for s in subtopic_rules):
            key = sub_name.lower().strip()
            if key in _discipline_adjacency:
                forbidden_set, examples, what_we_want = _discipline_adjacency[key]
                # Filtrar disciplinas que el propio usuario tiene como subtopic —
                # no podemos rechazarlas si el usuario las pidió. EXCEPCIÓN: si el
                # forbidden no es una disciplina sino lifestyle/personal, siempre
                # se rechaza (esos no se piden como subtopic).
                forbidden_filtered = sorted(
                    f for f in forbidden_set
                    if f.lower() not in _user_disciplines_expanded
                )
                adjacency_lines.append(
                    f"  · subtopic '{sub_name}':\n"
                    f"      ✅ SI a: {what_we_want}.\n"
                    f"      ✅ Jugadores/equipos relevantes: {examples}.\n"
                    f"      ❌ NO a: {'; '.join(forbidden_filtered)}."
                )
        if adjacency_lines:
            subtopics_block += (
                "\nDISCIPLINAS ADYACENTES — CRITERIO POR SUBTOPIC:\n"
                + "\n".join(adjacency_lines)
                + "\nORDEN DE EVALUACIÓN (aplica en este orden — no saltes pasos):\n"
                + "  1. ¿El TITULAR o RESUMEN del artículo MENCIONA EXPLÍCITAMENTE el subtopic\n"
                + "     (nombre del club/disciplina/atleta del subtopic)? → SI.\n"
                + "     Ejemplos:\n"
                + "       'Real Madrid 2 - Granada 0' (con subtopic=Real Madrid) → SI.\n"
                + "       'Horarios del Gran Premio de Mónaco F1 2026' (con subtopic=F1) → SI.\n"
                + "       'Alcaraz pasa a cuartos en Roland Garros' (con subtopic=tenis) → SI.\n"
                + "  2. ¿Es noticia del dominio amplio del subtopic, aunque no nombre la entidad\n"
                + "     concreta (ej: 'Cambios en el reglamento ATP' para subtopic=tenis;\n"
                + "     'FIA sanciona a Red Bull' para subtopic=F1)? → SI.\n"
                + "  3. ¿Es de una disciplina LISTADA como ❌ NO arriba (cross-leak claro)? → NO.\n"
                + "  4. ¿Es lifestyle/celebrity/vida personal del atleta SIN contexto deportivo\n"
                + "     (colección de coches, moda, fiestas, vida sentimental)? → NO.\n"
                + "  5. Ante la duda → SI. Mejor incluir y dejar que el selector final filtre,\n"
                + "     que perder cobertura por sobre-restricción.\n"
            )

    kept = []
    for start in range(0, len(articles), batch_size):
        batch = articles[start:start + batch_size]
        items = "\n".join(
            f"ID {i}: {(a.get('titulo','') or '')[:120]} | {(a.get('resumen','') or '')[:180]}"
            for i, a in enumerate(batch)
        )
        prompt = f"""FECHA Y HORA ACTUAL: {now_str}

Verificación de pertenencia al TOPIC + vigencia temporal + reglas de exclusión.

TOPIC: "{base}"
{subtopics_block}
Artículos:
{items}

Para cada artículo responde SI o NO. Reglas en orden:

1. ✅ ¿Trata SUSTANCIALMENTE sobre el TOPIC general? Por defecto INCLUIR.
   - TOPIC "deporte" → SI a CUALQUIER deporte (fútbol, baloncesto, tenis, F1,
     MotoGP, padel, ciclismo, atletismo, etc.).
   - TOPIC "IA" → SI a cualquier noticia de inteligencia artificial.
   - TOPIC "fontaneria monetaria" → SI a bancos centrales, tipos, repo, QE.

2. ⚠️ DISCIPLINAS ADYACENTES (si el bloque "DISCIPLINAS ADYACENTES" aparece arriba):
   Aplícalas TAL CUAL. Sin mención explícita de la disciplina del subtopic,
   un artículo de disciplina adyacente = NO, aunque comparta apellido común.

3. ⚠️ REGLAS DE EXCLUSIÓN del usuario (si las hay arriba) son OBLIGATORIAS.
   Ej: "solo masculino" → excluye femenino EXPLÍCITO (Liga F, WTA).
   Ej: "no X" / "sin Y" → excluye Y.
   "preferir X" NO es exclusión (NUNCA descarta por preferencia).

4. ⏰ VIGENCIA TEMPORAL: si la noticia anuncia un evento FUTURO que YA HA
   OCURRIDO según la fecha actual → NO (obsoleto).
   Ej: hoy 11-may, noticia "Real Madrid afronta clásico del sábado 10-may" → NO.

5. 🚫 RECHAZAR si CLARAMENTE no es del topic.
   Ej: TOPIC "fontaneria monetaria" + "Deuda Comunidad Madrid" → NO (regional, no monetaria).
   Ej: TOPIC "startups" + "ESG empresarial" → NO (ESG no es startups).
   Ej: TOPIC "Roman archeology" + "ransomware" → NO.

ANTE LA DUDA → SI **salvo** disciplinas adyacentes a un topic específico (regla 2):
ahí preferimos rechazar para evitar cross-leak entre F1/rally, NBA/NCAA, etc.

JSON only: {{"results": ["SI","NO","SI","NO",...]}}"""
        # Stage 2 usa gpt-5-nano (OpenAI) en lugar de Mistral. Razón:
        # mistral-small rechazaba ~90% de artículos válidos en topics nicho
        # (caso 2026-05-28: alex con Institutional blockchain / Tokenización).
        # gpt-5-nano es 10x más barato que Gemini Flash y mejor en semántica.
        # Fallback a Mistral via call_quality_llm si OpenAI no está disponible
        # o falla (evita romper el pipeline si OPENAI_API_KEY falta).
        try:
            from src.utils.openai_nano import call_openai_nano, OpenAINanoTracker
            if not OpenAINanoTracker().is_available:
                raise RuntimeError("openai_unavailable")
            response = await call_openai_nano(
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                label="stage2_yes_no",
            )
            data = json.loads(response.choices[0].message.content)
            results = data.get("results", [])
            for i, art in enumerate(batch):
                verdict = (results[i] if i < len(results) else "SI").strip().upper()
                if verdict.startswith("S"):
                    kept.append(art)
                else:
                    logger.info(f"      🚫 Stage 2 LLM-strict: descarta '{(art.get('titulo','') or '')[:60]}'")
        except Exception as e:
            logger.warning(f"Stage 2 gpt-5-nano falló (batch {start}): {e}. Fallback a Mistral.")
            try:
                from src.utils.llm_quality import call_quality_llm
                response = await call_quality_llm(
                    processor,
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                    label="stage2_yes_no_fallback",
                )
                data = json.loads(response.choices[0].message.content)
                results = data.get("results", [])
                for i, art in enumerate(batch):
                    verdict = (results[i] if i < len(results) else "SI").strip().upper()
                    if verdict.startswith("S"):
                        kept.append(art)
                    else:
                        logger.info(f"      🚫 Stage 2 LLM-strict (fallback): descarta '{(art.get('titulo','') or '')[:60]}'")
            except Exception as e2:
                logger.warning(f"Stage 2 fallback Mistral también falló (batch {start}): {e2}")
                kept.extend(batch)  # fail-open: deja pasar el batch entero
    return kept
