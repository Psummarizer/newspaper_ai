"""
Auto-discover RSS feeds for topics with low coverage.

Pipeline por topic:
  1. LLM (quality) sugiere hasta 10 feeds RSS reales con metadata.
  2. Para cada candidato: HTTP GET + feedparser + check de frescura (≥1 entry
     <14 días) + check de schema mínimo.
  3. Relevance check con LLM fast: ¿los 5 últimos titulares tratan del topic?
  4. Dedup vs sources.json existente (por rss_url y por dominio).
  5. Append a sources.json en GCS + audit log en GCS.

Rate-limit: skip topic discovered exitosamente <24h atrás (evita martillear
LLM si el problema no son las fuentes sino el filtro de relevancia).

Uso CLI:
    python scripts/auto_discover_rss.py --topic freight "gold & silver"
    python scripts/auto_discover_rss.py --topic freight --force   # ignora rate-limit

Uso programático:
    from scripts.auto_discover_rss import RSSAutoDiscoverer
    summary = await RSSAutoDiscoverer().discover(["freight"])
"""
import os
import sys
import json
import logging
import asyncio
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import feedparser

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

# NOTA CREDENCIALES (fix 2026-06-07): NO sobrescribir GOOGLE_APPLICATION_CREDENTIALS
# con FIREBASE_CREDENTIALS_JSON. Ese service-account (firebase-adminsdk@podsummarizer-1)
# NO tiene acceso al bucket `newsletter-ai-data` (vive en pod-summarizer-ai-agent), así
# que fijarlo rompía la conexión GCS del discoverer → `is_connected()`=False → discovery
# abortaba en producción (log GCS vacío crónico). En Cloud Run el GCSService usa ADC vía
# metadata server (el SA del Job sí tiene acceso). Para uso CLI local: ejecutar antes
# `gcloud auth application-default login` (ADC). Además, al invocarse desde la ingesta se
# le pasa el GCSService ya conectado (ver __init__), evitando crear uno nuevo.

from src.services.gcs_service import GCSService
from src.services.llm_factory import LLMFactory
from src.utils.constants import CATEGORIES_LIST

logger = logging.getLogger(__name__)

RATE_LIMIT_HOURS = 24
LOG_FILENAME = "rss_autoadd_log.json"
SOURCES_FILENAME = "sources.json"
MAX_CANDIDATES_PER_TOPIC = 10
MIN_ENTRIES_REQUIRED = 3
ENTRY_FRESHNESS_DAYS = 14
HTTP_TIMEOUT = 12
MAX_TOTAL_RUNTIME_SECONDS = 240  # safety cap when invoked from ingest


class RSSAutoDiscoverer:
    def __init__(self, max_runtime_seconds: int = MAX_TOTAL_RUNTIME_SECONDS,
                 gcs_service: "GCSService" = None):
        # Reutiliza el GCSService ya conectado del caller (la ingesta) si se pasa.
        # Crear uno nuevo es frágil: depende del estado de credenciales del proceso
        # (ver NOTA CREDENCIALES arriba). El de la ingesta usa ADC y está conectado.
        self.gcs = gcs_service or GCSService()
        self.client_q, self.model_q = LLMFactory.get_client("quality")
        self.client_f, self.model_f = LLMFactory.get_client("fast")
        self.max_runtime_seconds = max_runtime_seconds

    async def discover(self, topics: List[str], force: bool = False) -> Dict:
        """Discovery completa para una lista de topics. Devuelve summary."""
        if not self.gcs.is_connected():
            logger.error("GCS no conectado, abortando discovery")
            return {"error": "gcs_not_connected"}

        raw_sources = self.gcs.get_json_file(SOURCES_FILENAME)
        if not isinstance(raw_sources, list):
            logger.error(f"sources.json no es lista (tipo={type(raw_sources).__name__})")
            return {"error": "sources_bad_shape"}

        log = self._load_log()
        existing_urls = {(s.get("rss_url") or "").lower().strip() for s in raw_sources}
        existing_domains_by_cat: Dict[Tuple[str, str], int] = {}
        for s in raw_sources:
            key = ((s.get("domain") or "").lower().strip(), s.get("category") or "")
            existing_domains_by_cat[key] = existing_domains_by_cat.get(key, 0) + 1

        summary = {
            "discovered": 0, "added": 0, "validated": 0,
            "skipped_rate_limit": 0, "skipped_dup": 0, "skipped_invalid": 0,
            "skipped_irrelevant": 0, "per_topic": {},
        }
        now = datetime.now()
        deadline = now + timedelta(seconds=self.max_runtime_seconds)

        for topic in topics:
            if datetime.now() > deadline:
                logger.warning(f"Discovery deadline alcanzado, saltando topics restantes")
                break

            # Rate limit
            last_ok = log.get(topic, {}).get("last_success")
            if not force and last_ok:
                try:
                    last_dt = datetime.fromisoformat(last_ok)
                    if (now - last_dt).total_seconds() < RATE_LIMIT_HOURS * 3600:
                        summary["skipped_rate_limit"] += 1
                        logger.info(f"[skip] {topic!r}: discovered hace <{RATE_LIMIT_HOURS}h")
                        continue
                except Exception:
                    pass

            log.setdefault(topic, {})["last_attempt"] = now.isoformat()

            try:
                candidates = await self._suggest_candidates(topic)
                summary["discovered"] += len(candidates)
                logger.info(f"[{topic!r}] LLM sugirió {len(candidates)} candidatos")

                added: List[Dict] = []
                for cand in candidates:
                    rss_url = (cand.get("rss_url") or "").strip()
                    domain = (cand.get("domain") or "").lower().strip()
                    category = cand.get("category") or ""
                    if not rss_url:
                        continue
                    if rss_url.lower() in existing_urls:
                        summary["skipped_dup"] += 1
                        continue
                    if category not in CATEGORIES_LIST:
                        # Reasigna a una categoría válida o salta
                        cand["category"] = self._coerce_category(category)
                        category = cand["category"]
                    if existing_domains_by_cat.get((domain, category), 0) >= 2:
                        # Ya hay ≥2 feeds de este dominio/categoría; no inflar
                        summary["skipped_dup"] += 1
                        continue

                    titles = await self._validate_feed(rss_url)
                    if titles is None:
                        summary["skipped_invalid"] += 1
                        continue
                    summary["validated"] += 1

                    if not await self._check_relevance(topic, titles):
                        summary["skipped_irrelevant"] += 1
                        continue

                    cand["is_active"] = True
                    # Normaliza campos opcionales
                    if not cand.get("base_url") and domain:
                        cand["base_url"] = f"https://{domain}"
                    if not cand.get("language"):
                        cand["language"] = "en"
                    if not cand.get("country"):
                        cand["country"] = ""
                    raw_sources.append(cand)
                    existing_urls.add(rss_url.lower())
                    existing_domains_by_cat[(domain, category)] = (
                        existing_domains_by_cat.get((domain, category), 0) + 1
                    )
                    added.append(cand)

                summary["added"] += len(added)
                summary["per_topic"][topic] = [
                    {"name": c.get("name"), "rss_url": c.get("rss_url"), "category": c.get("category")}
                    for c in added
                ]
                if added:
                    log[topic]["last_success"] = now.isoformat()
                    log[topic]["last_added"] = [c.get("rss_url") for c in added]
                    logger.info(f"[{topic!r}] +{len(added)} fuentes añadidas")
                else:
                    log[topic]["last_empty"] = now.isoformat()
                    logger.info(f"[{topic!r}] sin fuentes nuevas válidas")
            except Exception as e:
                logger.error(f"Error en discovery de {topic!r}: {e}")
                log[topic]["last_error"] = str(e)[:200]

        # Persist (solo si hubo cambios reales)
        if summary["added"] > 0:
            if self.gcs.upload_sources(raw_sources):
                logger.info(f"sources.json actualizado en GCS: +{summary['added']} fuentes")
            else:
                logger.error("Fallo subiendo sources.json a GCS")
                summary["upload_failed"] = True
        self._save_log(log)
        return summary

    async def _suggest_candidates(self, topic: str) -> List[Dict]:
        cats_str = ", ".join(CATEGORIES_LIST)
        prompt = f"""Sugiere hasta {MAX_CANDIDATES_PER_TOPIC} feeds RSS REALES y consolidados sobre el topic: "{topic}".

REGLAS CRÍTICAS:
- URLs RSS que SABES que existen. NO inventes paths. Si no estás seguro de la URL exacta, omite ese feed.
- Prioriza medios profesionales conocidos (Reuters, Bloomberg, FT, WSJ, The Economist, El País, El Confidencial, Expansión, El Mundo, La Vanguardia, RTVE, Le Monde, Süddeutsche, Politico, Axios, NYT, Guardian, BBC, AP, Forbes, etc).
- Para topics nicho (commodities, regulación específica, deportes minoritarios) busca medios especializados conocidos (FreightWaves, JOC, Lloyd's List, Bloomberg Commodities, Kitco, OilPrice, Coindesk, The Block, Decrypt, etc).
- Mezcla idiomas según el topic (60% inglés, 40% español por defecto).
- NO sugieras blogs personales, sustack pequeños, ni feeds que no estés CASI SEGURO existen.

CATEGORÍAS PERMITIDAS (elige la más exacta para cada feed):
{cats_str}

Responde JSON estricto:
{{"feeds": [
  {{"name": "Reuters Commodities", "domain": "reuters.com",
    "rss_url": "https://www.reuters.com/markets/commodities/rss",
    "base_url": "https://www.reuters.com", "language": "en", "country": "US",
    "category": "Economía y Finanzas"}}
]}}
"""
        try:
            response = await self.client_q.chat.completions.create(
                model=self.model_q,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            feeds = result.get("feeds", []) or []
            return feeds[:MAX_CANDIDATES_PER_TOPIC]
        except Exception as e:
            logger.error(f"_suggest_candidates({topic!r}): {e}")
            return []

    async def _validate_feed(self, url: str) -> Optional[List[str]]:
        """Returns first 5 titles if feed parses + has ≥1 fresh entry; else None."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                    headers={"User-Agent": "Mozilla/5.0 (BriefingNews/1.0)"},
                    allow_redirects=True,
                ) as resp:
                    if resp.status != 200:
                        logger.debug(f"  {url} → HTTP {resp.status}")
                        return None
                    content = await resp.read()
        except Exception as e:
            logger.debug(f"  {url} → fetch error: {type(e).__name__}")
            return None

        try:
            parsed = feedparser.parse(content)
        except Exception as e:
            logger.debug(f"  {url} → parse error: {e}")
            return None

        entries = parsed.entries or []
        if len(entries) < MIN_ENTRIES_REQUIRED:
            logger.debug(f"  {url} → solo {len(entries)} entries (min {MIN_ENTRIES_REQUIRED})")
            return None

        from time import mktime
        cutoff = datetime.now() - timedelta(days=ENTRY_FRESHNESS_DAYS)
        fresh = 0
        titles: List[str] = []
        for e in entries[:10]:
            t = e.get("title")
            if t:
                titles.append(t)
            ps = e.get("published_parsed") or e.get("updated_parsed")
            if ps:
                try:
                    if datetime.fromtimestamp(mktime(ps)) >= cutoff:
                        fresh += 1
                except Exception:
                    pass

        if fresh < 1:
            logger.debug(f"  {url} → feed estancado (sin entries <{ENTRY_FRESHNESS_DAYS}d)")
            return None

        return titles[:5]

    async def _check_relevance(self, topic: str, titles: List[str]) -> bool:
        if not titles:
            return False
        titles_text = "\n".join(f"- {t}" for t in titles[:5])
        prompt = f"""¿La MAYORÍA (≥3 de 5) de estos titulares tratan principalmente sobre el topic "{topic}"?
Tangencias temáticas NO cuentan; debe ser cobertura directa del topic.

Titulares:
{titles_text}

Responde JSON: {{"relevant": true|false, "matches": <int>, "reason": "breve"}}
"""
        try:
            response = await self.client_f.chat.completions.create(
                model=self.model_f,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            result = json.loads(response.choices[0].message.content)
            return bool(result.get("relevant", False))
        except Exception as e:
            logger.debug(f"_check_relevance({topic!r}): {e}")
            return True  # fail-open

    def _coerce_category(self, raw: str) -> str:
        """Mapea categorías no estándar a la lista válida."""
        if not raw:
            return "Sociedad"
        lower = raw.lower()
        for cat in CATEGORIES_LIST:
            if cat.lower() == lower or cat.lower() in lower or lower in cat.lower():
                return cat
        return "Sociedad"

    def _load_log(self) -> Dict:
        data = self.gcs.get_json_file(LOG_FILENAME)
        return data if isinstance(data, dict) else {}

    def _save_log(self, log: Dict):
        self.gcs.save_json_file(LOG_FILENAME, log)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", nargs="+", required=True, help="Topic(s) a investigar")
    parser.add_argument("--force", action="store_true", help="Ignora el rate-limit de 24h")
    args = parser.parse_args()

    discoverer = RSSAutoDiscoverer()
    summary = asyncio.run(discoverer.discover(args.topic, force=args.force))
    print(json.dumps(summary, indent=2, ensure_ascii=False))
