import os
import logging
import json
import re
import asyncio
from typing import Dict, Any, List
from urllib.parse import urlparse
from src.services.llm_factory import LLMFactory

def _extract_json(text: str) -> dict:
    """Extract JSON from LLM response that may contain markdown code blocks."""
    # Remove markdown code blocks
    text = re.sub(r'^```json\s*', '', text.strip())
    text = re.sub(r'^```\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    text = text.strip()
    return json.loads(text)

class ContentProcessorAgent:
    def __init__(self, mock_mode: bool = False):
        self.logger = logging.getLogger(__name__)
        # Cargamos el cliente y los modelos activos (fast y quality) del Factory
        self.client, self.model_fast = LLMFactory.get_client("fast")
        _, self.model_quality = LLMFactory.get_client("quality")
        self.mock_mode = mock_mode

    # ---------------------------------------------------------
    # PASO 1: FILTRADO INTELIGENTE (Selector)
    # ---------------------------------------------------------
    async def filter_relevant_articles(self, topic: str, articles: List[Dict]) -> List[Dict]:
        if not articles: return []
        
        # MOCK MODE
        if self.mock_mode:
            self.logger.info(f"🔎 [MOCK] Saltando filtro IA para '{topic}'. Pasando candidatos directos.")
            return articles[:3]

        self.logger.info(f"🔎 Filtrando relevancia para '{topic}' entre {len(articles)} noticias...")
        
        # 1. First Pass: Length Filter 
        # Bajamos el límite para permitir que descripciones vacías que solo tienen el link HTML pasen al LLM
        # y así el LLM pueda activar la flag 'needs_scraping'.
        candidates_checked = []
        for art in articles:
            content_text = art.get('content') or ""
            content_len = len(content_text.split())
            if content_len > 3 or "http" in content_text: # Allow if there is at least a link or >3 words
                # Update art content to be safe
                art['content'] = content_text
                candidates_checked.append(art)
            else:
                self.logger.debug(f"📉 Descartando artículo muy corto ({content_len} palabras): {art.get('title')}")
                
        if not candidates_checked:
            self.logger.warning(f"⚠️ Ningún artículo supera el filtro mínimo para '{topic}'.")
            return []
            
        # Optimization: Limit to top 40 candidates to avoid huge prompts
        articles = candidates_checked[:40]

        articles_input = ""
        for i, art in enumerate(articles):
            snippet = art.get('content', '')[:300].replace("\n", " ")
            articles_input += f"ID {i}: TÍTULO: {art.get('title')} | SNIPPET: {snippet}\n"

        system_prompt = f"""
        Eres un Analista de Inteligencia Estricto.
        Misión: Identificar qué noticias están **DIRECTAMENTE** relacionadas con el tema: "{topic}".
        
        CRITERIO DE RELEVANCIA Y CALIDAD:
        1. **Relevancia Temática:
            - La noticia debe tratar SUSTANCIALMENTE sobre "{topic}".
            - Si el tema es "Real Madrid", descarta "Fútbol General".
        
        2. **Impacto e Importancia**:
            - Prioriza: Grandes avances, cambios regulatorios, fusiones/adquisiciones clave, resultados científicos.
            - Descarta: Anécdotas menores, rumores sin base, declaraciones irrelevantes.

        3. **Carácter INFORMATIVO (CRÍTICO)**:
            - SOLO admite noticias puramente informativas, análisis o reportajes periodísticos.
            - **DESCARTA INMEDIATAMENTE**:
                - Contenido publicitario, publirreportajes ("advertorials") o notas de prensa de marcas.
                - Artículos que parezcan querer vender un producto o servicio.
                - Contenido ambiguo que mezcla información con promoción comercial clara.
                - Clickbait obvio o contenido de muy baja calidad.

        4. **Nivel Tecnológico vs Gadgets (CRÍTICO para Tech/IA/Cloud)**:
            - Si el topic es técnico (IA, Cloud, Quantum, Tecnología):
                - **BUSCAMOS**: Avances en investigación, impacto industrial, infraestructura, regulación, modelos fundamentales, estrategia empresarial.
                - **DESCARTAR**: Reviews de productos de consumo (móviles, laptops, relojes, proyectores, smart home), "mejores gadgets", o actualizaciones menores de apps de consumidor final.
                - **REGLA**: Si es un "gadget" o hardware de consumo, FUERA.
                - Ejemplo: "Nueva arquitectura de chips Blackwell" -> SI. "Review del nuevo proyector IA" -> NO.

        Tu trabajo es filtrar ruido y spam. Solo deja pasar información de valor sustancial para el usuario.
        
        5. **Detección de Texto Vacío (NUEVO)**:
            - A veces el SNIPPET no contiene una noticia, sino solo la repetición del Titulo, nombres de medios, palabras como "Ver más", o simplemente una lista de enlaces vacíos.
            - Si la noticia es MUY relevante por su TÍTULO pero el SNIPPET no aporta contexto suficiente porque parece vacío o un error del RSS, marca "needs_scraping": true.
        
        SALIDA JSON: {{ "valid_items": [{{"id": 0, "needs_scraping": false}}, {{"id": 2, "needs_scraping": true}}] }}
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model_fast,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": articles_input}
                ],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            
            # Retro-compatibility check in case LLM fails the format
            if "valid_ids" in result and not "valid_items" in result:
                valid_items = [{"id": i, "needs_scraping": False} for i in result["valid_ids"]]
            else:
                valid_items = result.get("valid_items", [])
            
            filtered_articles = []
            
            from src.services.scraper_service import ScraperService
            from datetime import datetime, timedelta, timezone
            scraper = ScraperService()
            min_date = datetime.now(timezone.utc) - timedelta(days=2) # 48h aprox
            
            for item in valid_items:
                idx = item.get("id")
                needs_scraping = item.get("needs_scraping", False)
                
                if idx is not None and idx < len(articles):
                    art = dict(articles[idx]) # copy
                    
                    if needs_scraping:
                        url = art.get('url')
                        self.logger.info(f"   🕸️ El LLM solicitó scraping para '{art.get('title')}' ({url})...")
                        if url:
                            scraped_data = scraper.scrape_and_validate(url, min_date)
                            if scraped_data and scraped_data.get('content'):
                                art['content'] = scraped_data['content']
                                self.logger.info("      ✅ Text scrapeado con éxito.")
                            else:
                                self.logger.warning("      ⚠️ Scraping falló o texto rechazado, se quedará con el snippet original.")
                                
                    filtered_articles.append(art)
                    
            self.logger.info(f"   ✅ Seleccionadas {len(filtered_articles)} noticias relevantes para '{topic}'.")
            
            # Merge similar articles and limit to max 3 per topic
            merged = self._merge_articles(topic, filtered_articles)
            limited = merged[:3]
            self.logger.info(f"   📦 Después de fusionar y limitar, {len(limited)} artículos para '{topic}'.")
            return limited
        except Exception as e:
            self.logger.error(f"❌ Error en filtro {topic}: {e}")
            return []

    # ---------------------------------------------------------
    # Helper to merge similar articles into a single cohesive article per topic
    def _merge_articles(self, topic: str, articles: List[Dict]) -> List[Dict]:
        """Group articles by domain and combine their contents."""
        groups = {}
        for art in articles:
            url = art.get('url', '')
            domain = urlparse(url).netloc if url else ''
            groups.setdefault(domain, []).append(art)
        merged = []
        for domain, group in groups.items():
            if len(group) == 1:
                merged.append(group[0])
                continue
            combined_title = f"{topic} - {domain}" if domain else topic
            combined_content = " ".join([a.get('content', '') for a in group])
            merged.append({
                "title": combined_title,
                "content": combined_content,
                "url": group[0].get('url'),
                "category": group[0].get('category'),
                "image_url": group[0].get('image_url') # Preserve image
            })
        return merged

    # PASO 2: REDACCIÓN POR CATEGORÍA
    # ---------------------------------------------------------
    async def write_category_section(self, category_name: str, articles: List[Dict], language: str = "es") -> str:
        if not articles:
            return ""
        
        if self.mock_mode:
            return self._generate_mock_content(category_name, language)
        
        self.logger.info(f"✍️  Redactando sección '{category_name}' con {len(articles)} noticias (GPT-5-nano)...")

        context_text = ""
        for i, art in enumerate(articles):
            content_clean = art.get('content', '')[:6000].replace("\n", " ") 
            context_text += f"-- NOTICIA {i+1} --\nID: {i}\nTÍTULO: {art.get('title')}\nCONTENIDO: {content_clean}\nLINK: {art.get('url')}\nIMAGEN: {art.get('image_url')}\n\n"

        # Detección de necesidad de tono divulgativo
        is_divulgative = any(k in category_name for k in ["Tecnología", "Digital", "Ciencia", "Investigación"])
        divulgative_instruction = ""
        if is_divulgative:
             divulgative_instruction = "7. **TONO DIVULGATIVO (OBLIGATORIO)**: Estás escribiendo para un público general curioso, no para expertos. Evita tecnicismos innecesarios o EXPLÍCALOS de forma sencilla. Haz el texto accesible y didáctico."

        system_prompt = f"""
        Eres el Editor Jefe de una Newsletter Premium. Vas a redactar la sección: "{category_name}".
        Idioma de salida STRICTO: {language} (Español Peninsular de España).

        OBJETIVO:
        Sintetizar las noticias proporcionadas en un conjunto de artículos PROFUNDOS y cohesivos.
        
        REGLAS DE ORO (CRÍTICAS):
        1. **IDIOMA PENINSULAR**: 
           - Escribe EXCLUSIVAMENTE en Español de España (Castellano Neutro). 
           - **PROHIBIDO** usar términos latinoamericanos.
           - MAL: "Costo", "Computadora", "Celular", "Video" (sin tilde), "Chequear", "Renta".
           - BIEN: "Coste", "Ordenador", "Móvil", "Vídeo", "Comprobar", "Alquiler".
        2. **PROFUNDIDAD Y ESTRUCTURA**:
           - Escribe AL MENOS 2 PÁRRAFOS separados. Usa punto y aparte.
           - NO escribas un bloque de texto gigante. Separa ideas.
        3. **NEGRITAS**: Debes incluir AL MENOS 2 FRASES en <b>negrita</b> por noticia.
           - Úsalas para resaltar la idea central y una consecuencia/dato clave.
           - SIEMPRE frases completas (Sujeto + Verbo).
        4. **ESTILO**: Justifica el texto narrativamente. Tono periodístico serio y profesional.
        5. **FUENTES**: Incluye todas las fuentes originales.
        6. **IMAGEN**: Si la noticia tiene imagen, inclúyela DEBAJO del título usando <img src="..."> con estilo centrado y limitado (max-width:240px, max-height:160px).
        {divulgative_instruction}
        8. **CONTEXTO EXPLÍCITO (CRÍTICO)**:
           - NUNCA asumas contexto.
           - Nombrar SIEMPRE la ciudad, país, empresa o persona específica.
           - MAL: "La ciudad aprobó...", "La compañía lanzó..."
           - BIEN: "Madrid aprobó...", "Google lanzó..."
           - Si la noticia habla del tiempo en "la región", especifica QUÉ región.
        
        FORMATO HTML (ESTRICTO) POR NOTICIA:
        <div class="news-item" category="{category_name}">
            <h3>EMOJI + TÍTULO DESCRIPTIVO Y ATERRIZADO (Sujeto + Acción)</h3>
            <div style="margin-bottom: 12px; text-align: center;">
                 <img src="URL_IMAGEN" alt="Título" style="max-width: 240px; max-height: 160px; width: auto; height: auto; object-fit: cover; border-radius: 8px; display: inline-block;">
            </div>
            <p>
               Primer párrafo fuerte explicando el QUÉ y el POR QUÉ. (+100 palabras)
            </p>
            <p>
               Segundo párrafo detallando CÓMO, CUÁNDO y CONSECUENCIAS. (+100 palabras)
            </p>
            <p class="sources">
               Fuentes: <a href="URL1">Medio 1</a> | ...
            </p>
        </div>
        """

        try:
            response = await self.client.chat.completions.create(
                model=self.model_quality, 
                messages=[
                    {"role": "user", "content": system_prompt},
                    {"role": "user", "content": context_text}
                ]
            )
            
            content = response.choices[0].message.content or ""
            # self.logger.info(f"DEBUG LLM RESPONSE for {category_name}: {len(content)} chars.")
            
            content = content.strip()
            content = content.replace("```html", "").replace("```", "")
            
            # Post-process: convertir markdown a HTML
            content = self._markdown_to_html(content)
            
            return content
            
        except Exception as e:
            self.logger.error(f"❌ Error redactando categoría {category_name}: {e}")
            return f"<p>Error generando sección {category_name}.</p>"
    
    def _markdown_to_html(self, text: str) -> str:
        """Convierte markdown residual a HTML limpio."""
        import re
        
        # Convertir **bold** a <b>bold</b>
        text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
        
        # Convertir *italic* a <i>italic</i> (solo si no es parte de **)
        text = re.sub(r'(?<!\*)\*([^*]+)\*(?!\*)', r'<i>\1</i>', text)
        
        # Convertir __bold__ a <b>bold</b>
        text = re.sub(r'__(.+?)__', r'<b>\1</b>', text)
        
        # Asegurar saltos de párrafo: doble newline -> </p><p>
        text = re.sub(r'\n\n+', '</p>\n<p>', text)
        
        # Saltos simples dentro de párrafos -> <br>
        text = re.sub(r'(?<!</p>)\n(?!<)', '<br>\n', text)
        
        return text


    def _generate_mock_content(self, category_name: str, language: str) -> str:
        """Genera contenido ficticio para pruebas de diseño sin coste"""
        self.logger.info(f"🎨 [MOCK] Generando contenido falso para {category_name}...")
        return f"""
        <div class="news-item" category="{category_name}">
            <h3>🚀 TÍTULO EJEMPLO SIN ETIQUETAS</h3>
            <p>
                Este es un texto de prueba del <b>Modo Mock para validar el diseño justificado</b> sin gastar dinero.
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
                Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. 
                Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. 
                Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
            </p>
            <p>
                <b>Aquí probamos una frase completa en negrita para verificar que el estilo CSS funciona correctamente en móvil y escritorio.</b>
                Es fundamental que el texto se vea denso, como un libro, con guiones al final de línea (hyphens) para evitar ríos blancos.
                Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium.
            </p>
            <p class="sources">
                Fuente: <a href="https://example.com">Mock Source Daily</a>
            </p>
        </div>
        """

    # ---------------------------------------------------------
    # PASO 3: SELECCIÓN DE PORTADA (FRONT PAGE)
    # ---------------------------------------------------------
    async def select_front_page_stories(self, all_articles: List[Dict], language: str = "es") -> List[Dict]:
        """Selecciona las 3-7 noticias más importantes para la portada."""
        if not all_articles:
            return []

        if self.mock_mode:
            # Mock simple: coger las primeras 5
            self.logger.info("📰 [MOCK] Seleccionando portada mock...")
            mock_selection = []
            for i, art in enumerate(all_articles[:5]):
                mock_selection.append({
                    "headline": art.get('title', 'Sin título')[:50],
                    "summary": f"Resumen mock de la noticia {i+1} para la portada.",
                    "category": art.get('category', 'General'),
                    "emoji": "📰",
                    "original_url": art.get('url')
                })
            return mock_selection

        self.logger.info(f"📰 Seleccionando noticias de portada entre {len(all_articles)} candidatos...")

        # Preparar input para LLM
        articles_input = ""
        for i, art in enumerate(all_articles):
            snippet = art.get('content', '')[:200].replace("\n", " ")
            img_tag = "[IMG]" if art.get('image_url') else ""
            articles_input += f"ID {i}: {img_tag}[{art.get('category')}] {art.get('title')} | {snippet}\n"

        # Resolve language name for the prompt
        try:
            from src.services.llm_factory import LLMFactory
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
        1. TODAY'S NEWS FIRST: Strongly prefer news about events that happened TODAY. Results/outcomes over previews/predictions.
        2. Variety of topics (Politics, Tech, Sports, Economy...).
        3. High impact and relevance (big news with real consequences).
        4. NO DUPLICATES (CRITICAL):
           - If multiple stories cover the SAME event, choose only the most complete one.
           - Never include 2 stories that cover basically the same thing.
        5. DISCARD pre-event previews/lineups if post-event results exist, promotional content, and tangential lifestyle filler.
        6. FEATURED IMAGE: The FIRST story in the array becomes the featured/cover story. STRONGLY prefer a story marked with [IMG] as the first item (it has a photo).

        OUTPUT JSON FORMAT:
        {{
            "selected_stories": [
                {{
                    "original_id": 0,
                    "headline": "Impactful headline in {lang_name} (Max 5 words).",
                    "summary": "Direct summary text, no prefix, written in {lang_name}.",
                    "category": "Category name in {lang_name}",
                    "emoji": "🏛️"
                }},
                ...
            ]
        }}
        
        IMPORTANT NOTES ON SUMMARIES:
        - **FEATURED (1st story in array)**: Exactly 28 words. No label prefix, just the text.
        - **REST**: 10-15 words. No label prefix, just the text.
        - **STYLE**: Complete sentences with a hook.
        - ALL IN {lang_name.upper()}.
        - NO IMAGES.
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
                        "image_url": original.get("image_url"),  # portada del email
                    })
            
            self.logger.info(f"   ✅ Portada generada con {len(final_selection)} noticias.")
            return final_selection

        except Exception as e:
            self.logger.error(f"❌ Error seleccionando portada: {e}")
            # Fallback: use first 5 articles directly
            fallback = []
            for art in all_articles[:5]:
                fallback.append({
                    "headline": art.get("title", art.get("titulo", ""))[:80],
                    "summary": art.get("content", art.get("resumen", ""))[:100],
                    "category": art.get("category", ""),
                    "emoji": "📰",
                    "original_url": art.get("url"),
                    "image_url": art.get("image_url"),
                })
            self.logger.info(f"   ⚠️ Portada fallback con {len(fallback)} noticias.")
            return fallback
