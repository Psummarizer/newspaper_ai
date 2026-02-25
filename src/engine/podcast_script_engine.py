import json
import logging
import asyncio
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from openai import AsyncOpenAI
import datetime

logger = logging.getLogger(__name__)

@dataclass
class NewsItem:
    index: int
    topic: str
    title: str
    content: str
    source: str
    # Phase 1 Scores
    impact_score: int = 0
    debate_score: int = 0
    curiosity_score: int = 0
    emotional_score: int = 0
    narrative_weight: int = 0

class PodcastScriptEngine:
    """
    Motor de guion de podcast v3.
    Arquitectura simplificada en 2 fases:
      1. Scoring + selección inteligente de las mejores noticias
      2. Generación completa del guion en UNA SOLA llamada LLM
    """
    
    def __init__(self, openai_client: AsyncOpenAI, model: str = "gpt-5-nano"):
        self.client = openai_client
        self.model = model

    async def generate_script(self, items: List[Dict]) -> str:
        """
        Main pipeline: scoring → selection → single-call full script generation.
        """
        logger.info("🚀 Starting Podcast Script Engine v3 (2-Phase)")
        
        # 0. Pre-process to dataclass
        news_items = []
        for i, item in enumerate(items):
            content = item.get("resumen", "")
            if len(content) < 50: content = item.get("noticia", "")[:500]
            
            news_items.append(NewsItem(
                index=i+1,
                topic=item.get("topic", "General"),
                title=item.get("titulo", ""),
                content=content,
                source=item.get("source_name", "")
            ))
            
        if not news_items:
            return ""

        # Phase 1: Scoring + Selection
        logger.info("📊 Phase 1: Scoring & Selection")
        news_items = await self._phase1_score_and_select(news_items)
        logger.info(f"   Selected {len(news_items)} news items")
        
        # Phase 2: Full Script Generation (SINGLE LLM call)
        logger.info("🎬 Phase 2: Full Script Generation (single call)")
        script = await self._phase2_generate_full_script(news_items)
        
        # Validate duration
        word_count = len(script.split())
        est_minutes = word_count / 150  # Conservative speaking rate
        logger.info(f"   ✅ Script generated: {word_count} words (~{est_minutes:.1f} min)")
        
        return script

    async def _phase1_score_and_select(self, items: List[NewsItem]) -> List[NewsItem]:
        """
        Scores all items and selects the top 12 using round-robin by topic.
        """
        # Score all items in batch
        items_text = ""
        for item in items:
            items_text += f"ID {item.index} | TOPIC: {item.topic} | TITLE: {item.title}\nSUMMARY: {item.content[:300]}\n\n"
            
        prompt = f"""
        ANALYZE these news items for a podcast script.
        For each item (ID), rate 0-5 on these criteria:
        - impact (Relevance to listener's life)
        - debate (Controversial potential)
        - curiosity (Surprising/fun facts)
        - emotional (Human interest/drama)
        
        Return JSON format:
        [
            {{ "id": 1, "impact": 4, "debate": 2, "curiosity": 5, "emotional": 1 }},
            ...
        ]
        
        ITEMS:
        {items_text}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            content = response.choices[0].message.content
            data = json.loads(content)
            scores_list = data.get("scores", [])
            if isinstance(data, list): scores_list = data
            elif isinstance(data, dict):
                 for k,v in data.items():
                     if isinstance(v, list): 
                         scores_list = v
                         break
            
            scores_map = {int(s.get("id", 0)): s for s in scores_list}
            
            for item in items:
                s = scores_map.get(item.index, {})
                item.impact_score = s.get("impact", 2)
                item.debate_score = s.get("debate", 2)
                item.curiosity_score = s.get("curiosity", 2)
                item.emotional_score = s.get("emotional", 2)
                item.narrative_weight = item.impact_score + item.debate_score + item.curiosity_score + item.emotional_score
                
        except Exception as e:
            logger.error(f"Scoring Error: {e}")
            for item in items:
                item.narrative_weight = 5
        
        # Select top 12 via round-robin
        selected = self._select_top_items(items, max_items=12)
        
        # Re-index
        for idx, item in enumerate(selected):
            item.index = idx + 1
            
        return selected

    def _select_top_items(self, items: List[NewsItem], max_items: int = 12) -> List[NewsItem]:
        if len(items) <= max_items:
            return items
            
        topics = {}
        for item in items:
            if item.topic not in topics:
                topics[item.topic] = []
            topics[item.topic].append(item)
            
        for topic in topics:
            topics[topic].sort(key=lambda x: x.narrative_weight, reverse=True)
            
        selected = []
        while len(selected) < max_items and any(topics.values()):
            current_round = []
            for topic, topic_items in topics.items():
                if topic_items:
                    current_round.append(topic_items[0])
            
            current_round.sort(key=lambda x: x.narrative_weight, reverse=True)
            
            for candidate in current_round:
                if len(selected) < max_items:
                    selected.append(candidate)
                    topics[candidate.topic].pop(0)
                else:
                    break
                    
        selected.sort(key=lambda x: x.index)
        return selected

    async def _phase2_generate_full_script(self, items: List[NewsItem]) -> str:
        """
        Generates the ENTIRE podcast script in ONE LLM call.
        This is the core innovation: by generating everything at once,
        the conversation flows naturally with real context and transitions.
        """
        # Build news cards
        news_cards = ""
        for item in items:
            news_cards += f"""
---
NOTICIA {item.index}: {item.title}
Tema: {item.topic} | Fuente: {item.source}
Resumen: {item.content[:400]}
Peso narrativo: {item.narrative_weight}/20
---
"""
        
        num_items = len(items)
        # Target: 150 words/min spoken Spanish. 7-15 min = 1050-2250 words.
        # With 12 items: aim for ~1800 words total (~12 min).
        # With fewer items: give each more space.
        target_words = min(2200, max(1100, num_items * 150))
        target_minutes = target_words // 150
        
        system_prompt = f"""Eres un guionista de podcast de noticias en ESPAÑOL DE ESPAÑA (castellano peninsular neutro, NUNCA uses expresiones latinoamericanas).

TU TAREA: Escribe el guion COMPLETO de un episodio de podcast de noticias. El podcast tiene dos presentadores:

ÁLVARO — Presentador principal. Curioso, directo, con humor inteligente. Le gusta lanzar el dato fuerte, a veces exagera para provocar reacción. Habla como un tío normal de 35 años con cultura general alta.

ELVIRA — Co-presentadora analista. Inteligente, rápida, un punto irónica. Aporta contexto propio, corrige cuando hace falta, añade anécdotas históricas o datos que el oyente no espera. Habla como una periodista experimentada de 38 años.

═══════════════════════════════════════
ESTRUCTURA POR NOTICIA (OBLIGATORIA)
═══════════════════════════════════════

Cada noticia tiene 3 fases:

FASE 1 — CONTEXTO (primeros 2-3 turnos):
Un presentador introduce la noticia con los hechos clave. El otro puede añadir un dato complementario o matizar. NADIE interrumpe todavía: primero hay que entender qué pasó.

FASE 2 — ANÁLISIS (siguientes 3-4 turnos):
Los dos desarrollan implicaciones, consecuencias, contexto histórico, comparaciones. Cada uno aporta su propia perspectiva. Las intervenciones son de 1-2 frases, alternadas, fluidas.

FASE 3 — DEBATE / INTERPELACIÓN (últimos 2-3 turnos):
Aquí se pican, se preguntan cosas concretas, discrepan o sacan una conclusión inesperada. Una pregunta concreta ("¿tú lo harías?"), un reto ("eso me parece una exageración porque..."), o una reflexión personal.

Total por noticia: mínimo 8 intercambios.

═══════════════════════════════════════
REGLAS DE FORMATO
═══════════════════════════════════════

1. SOLO líneas de diálogo en este formato exacto:
   ÁLVARO: texto
   ELVIRA: texto
   Nada fuera de este formato. Sin acotaciones, sin (risas), sin [pausa].

2. LONGITUD DE TURNO según fase:
   - Fase 1 (Contexto): hasta 3-4 frases por turno. Hay que contar la historia bien.
   - Fase 2 (Análisis): hasta 2-3 frases. Fluido pero alternado.
   - Fase 3 (Debate): máximo 1-2 frases. Aquí sí queremos ritmo rápido y chispa.

3. Las interrupciones son ESPORÁDICAS — como mucho 1 por noticia y solo en la Fase 3. NO son la norma. Y NUNCA uses "—Espera, eso no es exactamente así" repetido: cada interrupción debe ser diferente y surgir del contenido.

4. Prohibido el formato entrevista puro: los dos dan datos, los dos opinan.

═══════════════════════════════════════
REGLAS DE CONTENIDO
═══════════════════════════════════════

1. INTRO: Máximo 4 líneas. Saludo rápido, energía alta, adelanto de la primera noticia.

2. NOTICIAS: No solo repitan el resumen. Fase 1 para los hechos, Fase 2 para el análisis profundo (implicaciones, anécdotas, comparaciones), Fase 3 para la chispa personal.

3. TRANSICIONES: PROHIBIDO "pasamos a la siguiente". Atar con un chiste, un contraste o una conexión temática.

4. VARIEDAD TONAL: Humor, indignación, asombro según el contenido. No todas las noticias se tratan igual.

5. CIERRE: Máximo 4 líneas. Despedida cálida. Última línea OBLIGATORIA: "Hasta aquí tus noticias diarias. Esperamos que te haya gustado."

═══════════════════════════════════════
PROHIBICIONES
═══════════════════════════════════════
- NUNCA: "café", "taza", "humeante"
- NUNCA: empezar dos noticias consecutivas igual
- NUNCA: "a ver", "bueno", "claro" más de 3 veces en todo el guion
- NUNCA: "¿qué opinas?", "cuéntanos más", "interesante punto", "muy buen apunte"
- NUNCA: mencionar que estáis en un estudio o que tenéis guion
- NUNCA: interrumpir en la Fase 1 de una noticia — deja que la historia se cuente primero

EXTENSIÓN OBJETIVO: Aproximadamente {target_words} palabras (~{target_minutes} minutos de audio). HAY {num_items} noticias, así que dedica unas {target_words // num_items} palabras a cada una de media."""

        user_prompt = f"""Genera el guion completo del episodio de hoy con estas {num_items} noticias:

{news_cards}

Recuerda: SOLO líneas "ÁLVARO: ..." y "ELVIRA: ...", estructura narrativa (contexto → análisis → debate), {target_words} palabras aproximadamente."""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=1.0
            )
            script = response.choices[0].message.content.strip()
            
            # Clean up: remove any lines that don't start with ÁLVARO: or ELVIRA:
            cleaned_lines = []
            for line in script.split("\n"):
                stripped = line.strip()
                if stripped.startswith("ÁLVARO:") or stripped.startswith("ELVIRA:") or stripped.startswith("Álvaro:") or stripped.startswith("Elvira:"):
                    # Normalize casing
                    if stripped.startswith("Álvaro:"):
                        stripped = "ÁLVARO:" + stripped[7:]
                    elif stripped.startswith("Elvira:"):
                        stripped = "ELVIRA:" + stripped[7:]
                    cleaned_lines.append(stripped)
                elif not stripped:
                    cleaned_lines.append("")  # Keep blank lines for readability
                    
            return "\n".join(cleaned_lines)
            
        except Exception as e:
            logger.error(f"Full Script Generation Error: {e}")
            return ""
