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
    def __init__(self, openai_client: AsyncOpenAI, model: str = "gpt-5-nano"):
        self.client = openai_client
        self.model = model

    async def generate_script(self, items: List[Dict]) -> str:
        """
        Main pipeline execution.
        items: List of dicts with keys: topic, title, resumen (content), source (optional)
        """
        logger.info("🚀 Starting 5-Phase Podcast Script Engine")
        
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

        # Phase 1: Scoring
        logger.info("📊 Phase 1: Narrative Scoring")
        news_items = await self._phase1_scoring(news_items)
        
        # Phase 2: Blueprint
        logger.info("📐 Phase 2: Episode Blueprint")
        blueprint = await self._phase2_blueprint(news_items)
        logger.info(f"   Blueprint target: {blueprint.get('estimated_target_minutes')}m, Tone: {blueprint.get('tone_profile')}")

        # Phase 3: Block Generation
        logger.info("🧱 Phase 3: Block Generation")
        raw_script = await self._phase3_generate_blocks(news_items, blueprint)
        
        # Phase 4: Duration Control
        logger.info("⏱️ Phase 4: Duration Control")
        adjusted_script = await self._phase4_duration_control(raw_script, news_items, blueprint)
        
        # Phase 5: Refinement
        logger.info("✨ Phase 5: Naturalness Refinement")
        final_script = await self._phase5_refinement(adjusted_script)
        
        return final_script

    async def _phase1_scoring(self, items: List[NewsItem]) -> List[NewsItem]:
        """
        Analyzes news items to assign narrative scores.
        """
        # We can do this in batch or individually. Batch is faster/cheaper.
        items_text = ""
        for item in items:
            items_text += f"ID {item.index} | TOPIC: {item.topic} | TITLE: {item.title}\nSUMMARY: {item.content[:300]}\n\n"
            
        prompt = f"""
        ANALYZE these news items for a podcast script.
        For each item (ID), rate 0-5 on these criteria:
        - impact_score (Relevance to listener's life)
        - debate_score (Controversial potential)
        - curiosity_score (Surprising/fun facts)
        - emotional_score (Human interest/drama)
        
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
            # Handle if root is the list or under a key
            if isinstance(data, list): scores_list = data
            elif isinstance(data, dict):
                 # Try to find the list in values
                 for k,v in data.items():
                     if isinstance(v, list): 
                         scores_list = v
                         break
            
            # Map back to items
            scores_map = {int(s.get("id", 0)): s for s in scores_list}
            
            for item in items:
                s = scores_map.get(item.index, {})
                item.impact_score = s.get("impact", 2)
                item.debate_score = s.get("debate", 2)
                item.curiosity_score = s.get("curiosity", 2)
                item.emotional_score = s.get("emotional", 2)
                # Calculate weight
                item.narrative_weight = item.impact_score + item.debate_score + item.curiosity_score + item.emotional_score
                
        except Exception as e:
            logger.error(f"Phase 1 Error: {e}")
            # Fallback defaults
            for item in items:
                item.narrative_weight = 5
                
        return items

    async def _phase2_blueprint(self, items: List[NewsItem]) -> Dict:
        """
        Creates the structural plan for the episode.
        """
        # Identify Anchor (Max weight)
        if not items: return {}
        
        sorted_by_weight = sorted(items, key=lambda x: x.narrative_weight, reverse=True)
        anchor = sorted_by_weight[0]
        
        total_minutes = max(7, min(15, 5 + len(items))) # Rough estimate
        
        # Construct simplified representation for LLM
        items_summary = []
        for item in items:
            items_summary.append({
                "index": item.index,
                "topic": item.topic,
                "title": item.title,
                "scores": {
                    "impact": item.impact_score,
                    "debate": item.debate_score
                }
            })
            
        prompt = f"""
        Design a Podcast Episode Blueprint.
        Target Length: {total_minutes} minutes.
        Total Items: {len(items)}.
        
        Anchor Story (Most Important): ID {anchor.index} ({anchor.title})
        
        ITEMS:
        {json.dumps(items_summary, indent=2)}
        
        REQUIREMENTS:
        - Determine 'tone_profile' (dynamic, reflexive, mixed) based on scores.
        - Create a 'news_plan' array. FOR EACH ITEM (in original order 1..N):
            - depth_level: "light" (mention <45s), "medium" (standard ~60s), "deep" (anchor/debate ~90s+)
            - interruption_intensity: "low", "medium", "high"
            - humor_allowed: boolean
            - debate_expected: boolean
        
        Output JSON:
        {{
            "estimated_target_minutes": {total_minutes},
            "tone_profile": "...",
            "anchor_news_index": {anchor.index},
            "news_plan": [ ... ]
        }}
        """
        
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logger.error(f"Phase 2 Error: {e}")
            # Fallback blueprint
            return {
                "estimated_target_minutes": total_minutes,
                "tone_profile": "mixed",
                "anchor_news_index": anchor.index,
                "news_plan": [{ "index": i.index, "depth_level": "medium" } for i in items]
            }

    async def _get_news_plan(self, index: int, blueprint: Dict) -> Dict:
        for p in blueprint.get("news_plan", []):
            if p.get("index") == index:
                return p
        return {"depth_level": "medium", "interruption_intensity": "medium"}

    async def _phase3_generate_blocks(self, items: List[NewsItem], blueprint: Dict) -> str:
        """
        Generates script block by block.
        """
        script_blocks = []
        context_buffer = "Inicio del programa."
        
        # 1. INTRO
        logger.info("   Writing Intro...")
        intro_prompt = f"""
        Write the INTRO for a daily news podcast.
        Hosts: Álvaro (Energetic, curious) and Elvira (Expert analyst).
        
        Tone: {blueprint.get('tone_profile')}.
        First News Teaser: {items[0].title}.
        
        Structure:
        1. High energy greeting (No "Welcome to podcast name", just greeting).
        2. Very brief banter (10s).
        3. Transition to first topic.
        
        Output ONLY dialogue:
        ÁLVARO: ...
        ELVIRA: ...
        """
        intro = await self._call_llm(intro_prompt)
        script_blocks.append(intro)
        context_buffer = intro[-500:] # Keep last chars for context
        
        # 2. NEWS LOOP
        for i, item in enumerate(items):
            logger.info(f"   Writing News {item.index}: {item.title[:30]}...")
            plan = await self._get_news_plan(item.index, blueprint)
            
            is_first = (i == 0)
            is_last = (i == len(items) - 1)
            next_topic = items[i+1].topic if not is_last else "Clausura"
            
            block_prompt = f"""
            Generate generic dialogue for News Item {item.index}.
            Topic: {item.topic}
            Title: {item.title}
            Content: {item.content}
            Source: {item.source}
            
            PLAN:
            - Depth: {plan.get('depth_level')}
            - Debate: {plan.get('debate_expected')}
            - Humor: {plan.get('humor_allowed')}
            
            CONTEXT (Previous conversation):
            {context_buffer}
            
            INSTRUCTIONS:
            - Álvaro introduces/questions. Elvira explains/analyzes.
            - IF 'deep', Elvira must give context/history/implications.
            - IF 'debate', Álvaro must challenge Elvira.
            - NO explicit transitions like "Now let's talk about". Use semantic bridges if possible.
            - Connect to next topic ({next_topic}) at the end ONLY if natural.
            
            Output ONLY dialogue.
            """
            block = await self._call_llm(block_prompt)
            script_blocks.append(block)
            context_buffer = block[-500:]
            
        # 3. OUTRO
        logger.info("   Writing Outro...")
        outro_prompt = f"""
        Write the OUTRO.
        Context: {context_buffer}
        
        Instructions:
        - Brief recap or final thought.
        - Warm goodbye.
        - REQUIRED FINAL PHRASE: "Hasta aquí tus noticias diarias. Esperamos que te haya gustado."
        """
        outro = await self._call_llm(outro_prompt)
        script_blocks.append(outro)
        
        return "\n\n".join(script_blocks)

    async def _phase4_duration_control(self, script: str, items: List[NewsItem], blueprint: Dict) -> str:
        """
        Checks word count and expands/contracts if necessary.
        """
        word_count = len(script.split())
        est_minutes = word_count / 160  # Avg speaking rate
        
        logger.info(f"   Estimated duration: {est_minutes:.1f} min ({word_count} words)")
        
        if est_minutes < 7:
            logger.info("   ⚠️ Too short. Expanding anchor news...")
            # Expand logic: Find anchor news block and inject more debate
            anchor_idx = blueprint.get("anchor_news_index", 1)
            # (Simplification: For now just append a 'Blue Room' bonus debate segment before outro)
            # Ideally we would locate the specific block, but regexing speakers is tricky.
            # We will generate a "Deep Dive" segment to insert before Outro.
            
            anchor_item = next((x for x in items if x.index == anchor_idx), items[0])
            
            expansion_prompt = f"""
            The podcast is too short. Generate an extended 'Deep Dive' conversation about:
            {anchor_item.title}
            
            Focus on: Future implications, ethical dilemma, or historical parallel.
            Make it a 1-minute intense dialogue between Álvaro and Elvira.
            
            Output ONLY dialogue.
            """
            expansion = await self._call_llm(expansion_prompt)
            
            # Insert before the last block (Outro)
            blocks = script.split("\n\n")
            if len(blocks) > 1:
                blocks.insert(-1, expansion)
                return "\n\n".join(blocks)
                
        elif est_minutes > 15:
            logger.info("   ⚠️ Too long. (Compact logic placeholder - skipping for safety)")
            # Determining which part to cut is risky without parsing. 
            # For v1, we just warn.
            pass
            
        return script

    async def _phase5_refinement(self, script: str) -> str:
        """
        Polishes the script for naturalness.
        """
        # Process in chunks to avoid context limits if script is huge, 
        # but 15 min script is ~2400 words, fits in GPT-4o window easily.
        
        prompt = f"""
        Refine this podcast script to make it sound EXTREMELY NATURAL.
        
        Goals:
        - Add micro-interruptions ("espera", "pero...", "ajá").
        - Add realistic hesitations or enthusiasm.
        - Ensure Álvaro sounds energetic/sarcastic and Elvira smart/calm.
        - Remove robotic connectors ("Por otro lado", "En conclusión").
        - FIX: Ensure strict dialogue format:
          ÁLVARO: ...
          ELVIRA: ...
          
        SCRIPT:
        {script}
        """
        
        return await self._call_llm(prompt, temperature=0.7)

    async def _call_llm(self, prompt: str, temperature: float = 1.0) -> str:
        try:
            # Check if model supports temperature (heuristics or just force 1.0)
            # optimizations for gpt-5-nano often require temp=1
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                     {"role": "system", "content": "You are an expert Spanish scriptwriter for radio. Spanish Peninsular Neutro. NO Latin American terms."},
                     {"role": "user", "content": prompt}
                ],
                temperature=1.0 # Forced to 1.0 as per API requirements for this model
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"LLM Call Error: {e}")
            return ""
