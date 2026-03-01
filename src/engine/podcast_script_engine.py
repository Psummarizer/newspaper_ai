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
    Motor de guion de podcast v4 — Arquitectura Modular.
    Genera segmentos independientes por noticia (reutilizables),
    y luego intro + transiciones + cierre por separado.
    """
    
    def __init__(self, openai_client: AsyncOpenAI, model: str = "gpt-5-nano", language: str = "es"):
        self.client = openai_client
        self.model = model
        self.language = language.lower().strip()
        # Resolve language display name for prompts
        try:
            from src.services.llm_factory import LLMFactory
            lang_cfg = LLMFactory.get_language_config(self.language)
            self.lang_name = lang_cfg.get("name", "Spanish")
        except Exception:
            self.lang_name = "Spanish"

    # ═══════════════════════════════════════════════════════════
    # PIPELINE PRINCIPAL
    # ═══════════════════════════════════════════════════════════

    async def generate_script(self, items: List[Dict]) -> Dict:
        """
        Pipeline modular:
          1. Scoring + Selección (máx 10)
          2. Generación de guion por noticia (paralelo)
          3. Generación de intro + transiciones + cierre
        
        Returns dict:
        {
            "segments": [{"index": 1, "title": "...", "script": "ÁLVARO: ..."}],
            "intro_script": "ÁLVARO: ...",
            "transitions": ["ÁLVARO: ...", ...],
            "outro_script": "ÁLVARO: ...",
            "full_script": "..."  # Script concatenado para TTS directo
        }
        """
        logger.info("🚀 Starting Podcast Script Engine v4 (Modular)")
        
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
            return {"segments": [], "intro_script": "", "transitions": [], "outro_script": "", "full_script": ""}

        # Phase 1: Scoring + Selection (máx 10)
        logger.info("📊 Phase 1: Scoring & Selection")
        news_items = await self._phase1_score_and_select(news_items)
        logger.info(f"   Selected {len(news_items)} news items")
        
        # Phase 2: Generate segment scripts IN PARALLEL
        logger.info("🎬 Phase 2: Generating individual segment scripts...")
        segments = await self._phase2_generate_segments(news_items)
        logger.info(f"   ✅ Generated {len(segments)} segment scripts")
        
        # Phase 3: Generate intro + transitions + outro
        logger.info("🎤 Phase 3: Generating intro, transitions & outro...")
        titles = [s["title"] for s in segments]
        glue = await self._phase3_generate_glue(titles)
        
        # Assemble full script for direct TTS
        full_script = self._assemble_full_script(segments, glue)
        
        word_count = len(full_script.split())
        est_minutes = word_count / 150
        logger.info(f"   ✅ Full script assembled: {word_count} words (~{est_minutes:.1f} min)")

        return {
            "segments": segments,
            "intro_script": glue.get("intro", ""),
            "transitions": glue.get("transitions", []),
            "outro_script": glue.get("outro", ""),
            "full_script": full_script
        }

    # ═══════════════════════════════════════════════════════════
    # PHASE 1: SCORING + SELECTION
    # ═══════════════════════════════════════════════════════════

    async def _phase1_score_and_select(self, items: List[NewsItem]) -> List[NewsItem]:
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
        
        # Select top 10 via round-robin
        selected = self._select_top_items(items, max_items=10)
        
        # Re-index
        for idx, item in enumerate(selected):
            item.index = idx + 1
            
        return selected

    def _select_top_items(self, items: List[NewsItem], max_items: int = 10) -> List[NewsItem]:
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

    # ═══════════════════════════════════════════════════════════
    # PHASE 2: SEGMENT SCRIPTS (ONE PER NEWS ITEM)
    # ═══════════════════════════════════════════════════════════

    async def _phase2_generate_segments(self, items: List[NewsItem]) -> List[Dict]:
        """Generate segment scripts in parallel, one per news item."""
        tasks = [self._generate_single_segment(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        segments = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error generating segment {i+1}: {result}")
                continue
            if result:
                segments.append(result)
        
        return segments

    async def _generate_single_segment(self, item: NewsItem) -> Optional[Dict]:
        """Generate ~1 min dialogue script for a single news item."""
        
        is_spanish = self.language == "es"
        host1_label = "ÁLVARO" if is_spanish else "HOST1"
        host2_label = "ELVIRA" if is_spanish else "HOST2"
        
        if is_spanish:
            host1_desc = "ÁLVARO — Presentador principal. Curioso, directo, con humor inteligente. Habla como un tío normal de 35 años con cultura general alta."
            host2_desc = "ELVIRA — Co-presentadora analista. Inteligente, rápida, un punto irónica. Aporta contexto propio, corrige cuando hace falta."
        else:
            host1_desc = f"HOST1 — Main presenter. Curious, direct, with intelligent humor. Natural, conversational tone for a {self.lang_name}-speaking audience."
            host2_desc = f"HOST2 — Co-presenter/analyst. Smart, sharp, slightly ironic. Adds context and corrects when needed."

        system_prompt = f"""You are a news podcast scriptwriter. Write ENTIRELY IN {self.lang_name.upper()} — every single word, no exceptions.

{host1_desc}
{host2_desc}

Write the script for ONE SEGMENT of ~1 minute about the news item provided. Two presenters.

SEGMENT STRUCTURE (3 mandatory phases):

PHASE 1 — CONTEXT (2-3 turns):
One presenter introduces the news with key facts. The other may add a detail. Understand what happened first. 2-3 sentences per turn.

PHASE 2 — ANALYSIS (2-3 turns):
Implications, consequences, historical context. Each presenter contributes a perspective. 1-2 sentences per turn.

PHASE 3 — DEBATE (1-2 turns):
They challenge each other, ask something specific, disagree, or reach an unexpected conclusion. 1 sentence per turn.

RULES:
- ONLY lines: {host1_label}: text / {host2_label}: text
- No stage directions, no (laughter), no [pause]
- Total: 120-160 words (~1 minute of audio)
- Do NOT include intro or farewell — it is an isolated segment
- NEVER: 'coffee', 'cup', 'what do you think?', 'tell us more'
- NEVER: mention you are in a studio or following a script
- WRITE IN {self.lang_name.upper()} ONLY"""

        user_prompt = f"""Generate the segment about this news item:

TITLE: {item.title}
TOPIC: {item.topic} | SOURCE: {item.source}
SUMMARY: {item.content[:500]}

Remember: ~150 words, structure context → analysis → debate, ONLY "{host1_label}:" and "{host2_label}:", ALL IN {self.lang_name.upper()}."""

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=1.0
            )
            script = response.choices[0].message.content
            if not script:
                logger.warning(f"Empty response for segment {item.index}")
                return None
            script = script.strip()
            
            # Clean: keep only dialogue lines
            cleaned = self._clean_script(script)
            
            word_count = len(cleaned.split())
            logger.info(f"   📰 Segment {item.index} '{item.title[:40]}...': {word_count} words")
            
            return {
                "index": item.index,
                "title": item.title,
                "topic": item.topic,
                "script": cleaned
            }
            
        except Exception as e:
            logger.error(f"Segment Generation Error for '{item.title}': {e}")
            return None

    # ═══════════════════════════════════════════════════════════
    # PHASE 3: INTRO + TRANSITIONS + OUTRO
    # ═══════════════════════════════════════════════════════════

    async def _phase3_generate_glue(self, titles: List[str]) -> Dict:
        """Generate intro, transitions between segments, and outro."""
        
        num = len(titles)
        titles_text = "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles)])

        # Build explicit pairs for transitions
        pairs_text = ""
        for i in range(num - 1):
            pairs_text += f"Transition {i+1}: From \u00ab{titles[i]}\u00bb \u2192 To \u00ab{titles[i+1]}\u00bb\n"

        is_spanish = self.language == "es"
        host1_label = "\u00c1LVARO" if is_spanish else "HOST1"
        host2_label = "ELVIRA" if is_spanish else "HOST2"

        system_prompt = f"""You are a news podcast scriptwriter. Write ENTIRELY IN {self.lang_name.upper()} — every single word, no exceptions.
Two presenters: {host1_label} and {host2_label}.

Your task: generate the CONNECTOR PIECES of the podcast (intro, transitions, outro).

═══════════════════════════════════════
INTRO
═══════════════════════════════════════
4-6 dialogue lines. Energetic greeting + attractively previews today's news without revealing content yet.

═══════════════════════════════════════
TRANSITIONS — CRITICAL RULES
═══════════════════════════════════════
Each transition is a BRIDGE of 3-5 dialogue lines between the news that just ended and the next one.

MANDATORY in each transition:
1. CLOSE of the previous topic: one last reflection, punchline or joke that wraps up the first topic.
2. HOOK entry: mention the title or topic of the NEXT news in a way that makes the listener want to know more. Do not start the next segment without announcing it.
3. VARIED STYLE: use a different technique in each transition:
   - Thematic contrast ("From X we move to something completely different...")
   - Rhetorical question connecting both topics
   - Curious anecdote or fact that bridges them
   - Humor or irony before the switch
   - One presenter interrupts the other to announce the next topic

FORBIDDEN:
- "Moving on to the next story"
- Starting the new topic directly without announcing it
- Summarising the news that was just covered
- Repeating the same transition style twice

═══════════════════════════════════════
OUTRO
═══════════════════════════════════════
3-4 lines. Warm sign-off. Last line MANDATORY: the equivalent in {self.lang_name} of "That's all for today's news. We hope you enjoyed it."

GENERAL RULES:
- ONLY lines: {host1_label}: text / {host2_label}: text
- No stage directions, no (laughter), no [pause]
- Write IN {self.lang_name.upper()} ONLY

JSON FORMAT:
{{
    "intro": "{host1_label}: ...\\n{host2_label}: ...\\n...",
    "transitions": [
        "{host1_label}: <close topic 1> <hook topic 2>\\n{host2_label}: ...\\n...",
        ...
    ],
    "outro": "{host1_label}: ...\\n{host2_label}: <sign-off in {self.lang_name}>"
}}"""

        user_prompt = f"""Generate the connector pieces for a podcast with these {num} news items:

NEWS ORDER:
{titles_text}

TRANSITIONS NEEDED (from → to):
{pairs_text}
I need exactly {num - 1} transitions, in the order shown.

Remember: each transition MUST close the previous topic AND announce the next one by name before it starts. ALL IN {self.lang_name.upper()}.

Respond with valid JSON only."""


        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=1.0
            )
            content = response.choices[0].message.content
            if not content:
                logger.warning("Empty response for glue generation")
                return self._fallback_glue(titles)
            
            data = json.loads(content)
            
            # Clean all scripts
            data["intro"] = self._clean_script(data.get("intro", ""))
            data["outro"] = self._clean_script(data.get("outro", ""))
            data["transitions"] = [self._clean_script(t) for t in data.get("transitions", [])]
            
            # Ensure we have enough transitions
            while len(data["transitions"]) < num - 1:
                data["transitions"].append("")
                
            return data
            
        except Exception as e:
            logger.error(f"Glue Generation Error: {e}")
            return self._fallback_glue(titles)

    def _fallback_glue(self, titles: List[str]) -> Dict:
        """Fallback minimal glue if LLM fails."""
        return {
            "intro": "ÁLVARO: ¡Hola! Bienvenidos a vuestras noticias diarias.\nELVIRA: Tenemos mucho que contar hoy, así que vamos al lío.",
            "transitions": ["" for _ in range(len(titles) - 1)],
            "outro": "ÁLVARO: Y eso es todo por hoy.\nELVIRA: Hasta aquí tus noticias diarias. Esperamos que te haya gustado."
        }

    # ═══════════════════════════════════════════════════════════
    # ASSEMBLY
    # ═══════════════════════════════════════════════════════════

    def _assemble_full_script(self, segments: List[Dict], glue: Dict) -> str:
        """Assemble the full script from segments and glue pieces."""
        parts = []
        
        # Intro
        if glue.get("intro"):
            parts.append(glue["intro"])
        
        # Segments with transitions
        transitions = glue.get("transitions", [])
        for i, segment in enumerate(segments):
            # Transition before this segment (except the first one)
            if i > 0 and i - 1 < len(transitions) and transitions[i - 1]:
                parts.append(transitions[i - 1])
            
            parts.append(segment["script"])
        
        # Outro
        if glue.get("outro"):
            parts.append(glue["outro"])
        
        return "\n\n".join(parts)

    # ═══════════════════════════════════════════════════════════
    # UTILITIES
    # ═══════════════════════════════════════════════════════════

    def _clean_script(self, script: str) -> str:
        """Keep only ÁLVARO:/ELVIRA: dialogue lines."""
        if not script:
            return ""
        cleaned_lines = []
        for line in script.split("\n"):
            stripped = line.strip()
            if stripped.startswith("ÁLVARO:") or stripped.startswith("ELVIRA:") or stripped.startswith("Álvaro:") or stripped.startswith("Elvira:"):
                if stripped.startswith("Álvaro:"):
                    stripped = "ÁLVARO:" + stripped[7:]
                elif stripped.startswith("Elvira:"):
                    stripped = "ELVIRA:" + stripped[7:]
                cleaned_lines.append(stripped)
        return "\n".join(cleaned_lines)
