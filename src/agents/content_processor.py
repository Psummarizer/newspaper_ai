import logging
import json
import re
from typing import List, Dict
from src.services.llm_factory import LLMFactory


def _extract_json(text: str) -> dict:
    """Extract JSON from LLM response that may contain markdown code blocks."""
    text = re.sub(r'^```json\s*', '', text.strip())
    text = re.sub(r'^```\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()
    return json.loads(text)


class ContentProcessorAgent:
    def __init__(self, mock_mode: bool = False):
        self.logger = logging.getLogger(__name__)
        self.client, self.model_fast = LLMFactory.get_client("fast")
        _, self.model_quality = LLMFactory.get_client("quality")
        self.mock_mode = mock_mode

    async def select_front_page_stories(self, all_articles: List[Dict], language: str = "es") -> List[Dict]:
        """Selecciona las 3-7 noticias más importantes para la portada."""
        if not all_articles:
            return []

        if self.mock_mode:
            self.logger.info("📰 [MOCK] Seleccionando portada mock...")
            return [{"headline": art.get('title', '')[:50], "summary": f"Resumen mock {i+1}.",
                     "category": art.get('category', 'General'), "emoji": "📰",
                     "original_url": art.get('url')} for i, art in enumerate(all_articles[:5])]

        self.logger.info(f"📰 Seleccionando noticias de portada entre {len(all_articles)} candidatos...")

        articles_input = ""
        for i, art in enumerate(all_articles):
            snippet = art.get('content', '')[:200].replace("\n", " ")
            img_tag = "[IMG]" if art.get('image_url') else ""
            articles_input += f"ID {i}: {img_tag}[{art.get('category')}] {art.get('title')} | {snippet}\n"

        try:
            lang_cfg = LLMFactory.get_language_config(language)
            lang_name = lang_cfg.get("name", "Spanish")
        except Exception:
            lang_name = "Spanish"

        system_prompt = f"""
        You are the Editor-in-Chief of 'Daily Briefing AI'.
        YOUR JOB: SELECT the best stories for the FRONT PAGE.

        ⚠️ CRITICAL LANGUAGE RULE: Write EVERY SINGLE WORD in {lang_name.upper()}.
        This means: headline, summary, and category — all in {lang_name}. No exceptions.

        INPUT: {len(all_articles)} candidate news items.
        OUTPUT: JSON with the 3 to 7 most important/impactful stories.

        SELECTION CRITERIA:
        1. TODAY'S NEWS FIRST: Strongly prefer news about events that happened TODAY.
        2. Variety of topics (Politics, Tech, Sports, Economy...).
        3. High impact and relevance (big news with real consequences).
        4. NO DUPLICATES: If multiple stories cover the SAME event, choose only the best one.
        5. DISCARD previews if results exist, promotional content, tangential filler.
        6. FEATURED IMAGE: The FIRST story becomes the cover story. STRONGLY prefer [IMG] stories first.

        OUTPUT JSON FORMAT:
        {{
            "selected_stories": [
                {{
                    "original_id": 0,
                    "headline": "Impactful headline in {lang_name} (Max 5 words).",
                    "summary": "Direct summary text in {lang_name}.",
                    "category": "Category name in {lang_name}",
                    "emoji": "🏛️"
                }}
            ]
        }}

        SUMMARIES:
        - FEATURED (1st): 1-2 complete sentences, max 30 words total. Must end with a period. No mid-sentence cuts.
        - REST: 1 complete sentence, 10-15 words. Must end with a period.
        - ALL IN {lang_name.upper()}.
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model_quality,
                messages=[
                    {"role": "user", "content": system_prompt},
                    {"role": "user", "content": articles_input}
                ]
            )

            result = _extract_json(response.choices[0].message.content)
            selected = result.get("selected_stories", [])

            final_selection = []
            for item in selected:
                idx = item.get("original_id")
                if idx is not None and 0 <= idx < len(all_articles):
                    original = all_articles[idx]
                    final_selection.append({
                        "headline": item.get("headline"),
                        "summary": item.get("summary"),
                        "category": item.get("category"),
                        "emoji": item.get("emoji"),
                        "original_url": original.get("url"),
                        "image_url": original.get("image_url"),
                    })

            self.logger.info(f"   ✅ Portada generada con {len(final_selection)} noticias.")
            return final_selection

        except Exception as e:
            self.logger.error(f"❌ Error seleccionando portada: {e}")
            return [{"headline": art.get("title", art.get("titulo", ""))[:80],
                     "summary": art.get("content", art.get("resumen", ""))[:100],
                     "category": art.get("category", ""), "emoji": "📰",
                     "original_url": art.get("url"), "image_url": art.get("image_url"),
                     } for art in all_articles[:5]]
