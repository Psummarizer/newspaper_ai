import os
import logging
import json
import re
from typing import Dict, Any, List
from openai import AsyncOpenAI
from src.services.image_service import ImageService

class ContentProcessorAgent:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.image_service = ImageService()
        self.used_images = set()

    # ---------------------------------------------------------
    # PASO 1: FILTRADO INTELIGENTE (Selector)
    # ---------------------------------------------------------
    async def filter_relevant_articles(self, topic: str, articles: List[Dict]) -> List[Dict]:
        if not articles: return []

        self.logger.info(f"🔎 Filtrando relevancia para '{topic}' entre {len(articles)} noticias...")

        articles_input = ""
        for i, art in enumerate(articles):
            # Aumentamos el límite a 6000 caracteres
            content_rich = art.get('content', '')[:6000].replace("\\n", " ")
            articles_input += f"ID {i}: TÍTULO: {art.get('title')} | TEXTO: {content_rich}\\n\\n"

        system_prompt = f"""
        Eres un Analista de Inteligencia experto.
        Tu misión es FILTRAR noticias para un informe sobre: "{topic}".
        INSTRUCCIONES:
        1. Analiza cada noticia proporcionada.
        2. Decide si es está relacionado con "{topic}".
           - Si habla explícitamente del tema: SI.
           - Si esta relacionado con "{topic}": SI.
           - Si no tiene nada que ver: NO.
        3. Si esta relacionada pero es una oferta o publicidad para comprar algo entonces no sirve
        3. Responde ÚNICAMENTE con un JSON que contenga una lista de los IDs numéricos de las noticias válidas.

        Formato de salida esperado:
        {{ "valid_ids": [0, 3, 5] }}
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": articles_input}
                ],

                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            valid_ids = result.get("valid_ids", [])
            filtered_articles = [articles[i] for i in valid_ids if i < len(articles)]
            self.logger.info(f"   ✅ Seleccionadas {len(filtered_articles)} noticias relevantes.")
            return filtered_articles

        except Exception as e:
            self.logger.error(f"❌ Error en el filtrado de noticias: {e}")
            return []

    # ---------------------------------------------------------
    # PASO 2: REDACCIÓN CON MARCADORES DE IMAGEN (MEJORADO)
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # MÉTODO: INYECTOR DE IMÁGENES ULTRA SIMPLE (MODIFICADO)
    # ---------------------------------------------------------
    async def _inject_images_smart(self, text: str, topic: str) -> str:
        """
        Busca [[IMG_CONTEXT: ...]] y sustituye por <img> real
        """
        import re

        # Verificar que hay marcadores
        if "[[IMG_CONTEXT:" not in text:
            self.logger.warning("📸 No hay marcadores IMG_CONTEXT en el texto")
            return text

        # Contar cuántos hay
        count = text.count("[[IMG_CONTEXT:")
        self.logger.info(f"📸 Detectados {count} marcadores IMG_CONTEXT")

        # Procesar uno por uno de forma manual
        processed = 0
        while "[[IMG_CONTEXT:" in text:
            # Encontrar el inicio del marcador
            start = text.find("[[IMG_CONTEXT:")
            if start == -1:
                break

            # Encontrar el final ]]
            end = text.find("]]", start)
            if end == -1:
                self.logger.error("Marcador sin cerrar")
                break

            # Extraer el marcador completo y el contexto
            full_marker = text[start:end + 2]
            context = text[start + 14:end].strip()  # 14 es len("[[IMG_CONTEXT:")

            self.logger.info(f"   📝 Procesando marcador {processed + 1}: '{context[:60]}...'")

            # Generar keywords simples (máximo 3 palabras clave)
            keywords = await self._generate_simple_keywords(context, topic)

            # Buscar imagen
            image_url = None
            attempts = 0
            max_attempts = 10  # Máximo de intentos para evitar loops infinitos

            for keyword in keywords:
                self.logger.info(f"      🔍 Buscando: '{keyword}'")

                # Intentar obtener varias imágenes hasta encontrar una no usada
                while attempts < max_attempts:
                    candidate_url = await self.image_service.get_relevant_image(keyword)
                    attempts += 1

                    if candidate_url and candidate_url not in self.used_images:
                        image_url = candidate_url
                        self.used_images.add(image_url)  # Marcar como usada
                        self.logger.info(f"      ✅ Imagen nueva encontrada")
                        break
                    elif candidate_url:
                        self.logger.info(f"      ⚠️ Imagen ya usada, buscando otra...")

                if image_url:
                    break

            # Si no encuentra, usar el topic general
            if not image_url and attempts < max_attempts:
                self.logger.warning(f"      ⚠️ No se encontró imagen única, intentando con topic: '{topic}'")

                while attempts < max_attempts:
                    candidate_url = await self.image_service.get_relevant_image(topic)
                    attempts += 1

                    if candidate_url and candidate_url not in self.used_images:
                        image_url = candidate_url
                        self.used_images.add(image_url)
                        break

            # Crear HTML de imagen o vacío
            if image_url:
                img_html = f"""<div style="margin: 20px 0;">
    <img src="{image_url}" alt="{context}" style="width: 100%; border-radius: 8px; display: block; max-height: 400px; object-fit: cover;">
    </div>"""
            else:
                self.logger.warning(f"      ❌ No se pudo encontrar imagen única después de {attempts} intentos")
                img_html = ""

            # Reemplazar el marcador
            text = text.replace(full_marker, img_html, 1)
            processed += 1

        self.logger.info(f"📸 Procesados {processed} marcadores")
        self.logger.info(f"📸 Total de imágenes únicas usadas: {len(self.used_images)}")

        # Verificar que no quedó ninguno
        if "[[IMG_CONTEXT:" in text:
            self.logger.error("⚠️ Quedaron marcadores sin procesar")
            text = text.replace("[[IMG_CONTEXT:", "").replace("]]", "")

        return text

    # [RESTO DEL CÓDIGO SE MANTIENE EXACTAMENTE IGUAL]

    async def _generate_simple_keywords(self, context: str, topic: str) -> List[str]:
        """
        Genera keywords específicas priorizando nombres propios y contexto
        """
        try:
            prompt = f"""Extract 4-6 SPECIFIC keywords for news image search.
    Topic context: {topic}

    PRIORITY ORDER:
    1. Person names (politicians, athletes, celebrities) - FIRST
    2. Specific events/places (stadiums, cities, institutions)
    3. General concepts (only if no specific entities)

    Rules:
    - If there are PERSON NAMES, include them (e.g., "Pedro Sánchez", "Mbappé")
    - Use 2-4 words per keyword
    - Mix specific + general (e.g., "Pedro Sánchez government", "Real Madrid football")
    - English keywords work better for international image databases
    - NO generic words like "news", "article", "event"

    Text: {context[:5000]}

    Output format (one per line):
    Pedro Sánchez Spain
    Spanish government meeting
    political summit Madrid"""

            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000
            )

            keywords_text = response.choices[0].message.content.strip()
            keywords = [k.strip() for k in keywords_text.split('\n') if k.strip()][:6]

            # Si no hay keywords, usar el topic como fallback
            if not keywords:
                keywords = [topic]
                
            self.logger.info(f"🔑 Keywords generadas: {keywords}")
            return keywords

        except Exception as e:
            self.logger.error(f"Error generando keywords: {e}")
            # Fallback: extraer nombres propios con regex simple
            import re
            names = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b', context)
            if names:
                return names[:3] + [topic]
            return [topic]


    # ---------------------------------------------------------
    # PROCESO PRINCIPAL (SIN CAMBIOS)
    # ---------------------------------------------------------
    async def process(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        articles = payload.get("articles", [])
        topic = payload.get("topic", "General")
        language = payload.get("language", "es")

        if not articles:
            return None

        # Extraer nombres de fuentes únicas
        source_names = list(set([art.get('source_name', '') or art.get('source', '') for art in articles if art.get('source_name') or art.get('source')]))
        
        articles_text = ""
        for i, art in enumerate(articles):
            content_clean = art.get('content', '')[:7500].replace("\\\\n", " ")
            articles_text += f"--- NOTICIA {i+1} ---\\\\nTÍTULO: {art.get('title')}\\\\nCONTENIDO: {content_clean}\\\\n\\\\n"

        # TU PROMPT EXACTO SIN CAMBIOS
        system_prompt = f"""
        Eres un Editor Jefe. Vas a escribir una sección de Newsletter sobre: "{topic}".
        Idioma de salida: {language}.
        Las noticias que recibes YA HAN SIDO FILTRADAS y son relevantes.

        TU TAREA:
        Crear un resumen ejecutivo periodístico de alto nivel fusionando estas noticias. Cada resumen que generes tienes que poner 3/4 frases con negritas de más de 10 palabras del contenido más relevante. Y cada resumen tiene que tener entre 400-500 palabras

        INSTRUCCIONES DE IMÁGENES (MUY IMPORTANTE):
        - Por cada noticia distinta que cubras, DEBES incluir una imagen.
        - NO pongas la etiqueta <img> tú mismo.
        - EN SU LUGAR, usa este marcador exacto justo DEBAJO del <h3>:
        [[IMG_CONTEXT: contexto_breve_de_la_noticia]]
        - Ejemplo:
        <h3>⚽ Real Madrid gana la Champions</h3>
        [[IMG_CONTEXT: Real Madrid celebra victoria Champions League estadio]]

        REGLAS DE FORMATO:
        1. Usa etiquetas <h3> para los subtítulos. Tienen que estar en "{language}". Que no haya más de tres subtitulos por cada "{topic}".
        2. Empieza CADA <h3> con un ICONO/EMOJI relacionado.
        3. Los <h3> tienes que traducirlos a "{language}"
        3. Usa etiquetas <p> para los párrafos.
        4. NO uses <html>, <body>, ni <div>. Solo el contenido HTML interior.
        5. Sé periodístico y objetivo.
        6. Longitud de cada subtema: Entre 400 y 500 palabras (CADA subtema, NO EL TOTAL DEL TOPIC, CADA subtema TIENE QUE TENER ESA LONGITUD).. Tinen que estar en "{language}"
        7. Si pones varios subtemas que sean relativamente variados. Es decir si todos hablan de una geografía especifa o un tema especifico juntemoslos.
        8. Evita poner contenido que pueda hacer publicidad como por ejemplo donde se va a poder ver un contenido
        9. NEGRITAS: Usa <b> para resaltar las partes mas importantes del articulo y que sean FRASES COMPLETAS las negritas, no palabras sueltas. Ejemplo correcto: <b>El gobierno ha aprobado una nueva ley que entrará en vigor en 2025</b>. Ejemplo incorrecto: La <b>nueva</b> ley.
        10. Tiene que haber 3/4 frases en negrita por articulo. Frases completas de más de 10 palabras por artículo que escribas.

        ESTRUCTURA DE CADA NOTICIA:
        <h3>EMOJI TÍTULO</h3>
        [[IMG_CONTEXT: descripción visual específica de esta noticia]]
        <p>Resumen de la noticia con <b>datos clave</b>.</p>
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": articles_text}
                ],

            )

            draft_html = response.choices[0].message.content.strip()

             # --- LIMPIEZA Y FORMATO ROBUSTO ---
            # 1. Eliminar bloques de código
            draft_html = draft_html.replace("```html", "").replace("```", "").strip()

            # 2. Corregir Negritas Markdown (**texto** -> <b>texto</b>)
            import re
            draft_html = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', draft_html)

            # 3. Forzar Párrafos si no hay ninguno
            # Si detectamos que hay dobles saltos de línea pero NO hay etiquetas <p>, envolvemos.
            if "<p>" not in draft_html:
                # Separar por dobles saltos de línea
                blocks = re.split(r'\n\s*\n', draft_html)
                new_html = ""
                for block in blocks:
                    block = block.strip()
                    if not block: continue
                    
                    # Si el bloque ya es un encabezado, lo dejamos
                    if block.startswith("<h") or block.startswith("<ul>") or block.startswith("<li>"):
                        new_html += f"{block}\n"
                    else:
                        new_html += f"<p>{block}</p>\n"
                draft_html = new_html

            # 4. Añadir atribución de fuentes
            if source_names:
                sources_text = ", ".join(source_names[:5])  # Limitar a 5 fuentes
                if len(source_names) > 5:
                    sources_text += f" y {len(source_names) - 5} más"
                draft_html += f"""
                <p style="margin-top: 20px; padding-top: 15px; border-top: 1px solid #38444D; font-size: 12px; color: #8899A6;">
                    📰 <b style="color: #1DA1F2;">Fuentes:</b> {sources_text}
                </p>
                """

            # Inyectar imágenes
            final_html_with_images = await self._inject_images_smart(draft_html, topic)

            return {"topic": topic, "html_content": final_html_with_images}

        except Exception as e:
            self.logger.error(f"❌ Error redacción: {e}")
            return None


    # ---------------------------------------------------------
    # PASO 3 (FINAL): ARQUITECTO
    # ---------------------------------------------------------
    async def structure_final_newsletter(self, raw_articles_html: str, user_language: str = "es") -> str:
        self.logger.info("🏗️  Arquitecto AI: Reestructurando...")

        system_prompt = f"""
        Eres el Director Editorial de una Newsletter Premium.
        Idioma de salida: {user_language}.

        TU OBJETIVO:
        Organizar una lista desordenada de artículos en SECCIONES MAESTRAS siguiendo este ORDEN EXACTO.

        ESTRATEGIA DE AGRUPACIÓN (USAR ESTOS CAJONES EN ESTE ORDEN):
        Si no hay noticias para un cajón, simplemente no lo crees.

        1. 🏛️ ACTUALIDAD POLÍTICA Y GOBIERNO
           - Política doméstica, Gobierno, Partidos, Justicia, Tribunales, Leyes.
           - OJO: Todo lo que pase en el país del usuario va aquí, NO en Internacional.

        2. 🌍 ACTUALIDAD INTERNACIONAL
           - Conflictos bélicos extranjeros, Geopolítica, UE, OTAN, Elecciones en otros países.

        3. 💰 ECONOMÍA Y MERCADOS
           - Bolsa, Empresas, Resultados financieros, Inflación, Empleo, Vivienda.

        4. 💻 TECNOLOGÍA Y DIGITAL
           - Inteligencia Artificial, Software, Criptomonedas, Gadgets, Startups, Internet, Ciberseguridad.

        5. 🔬 CIENCIA E INVESTIGACIÓN
           - Espacio, Medicina, Salud, Biología, Física, Descubrimientos científicos, Medio Ambiente.

        6. ⚽ DEPORTES
           - Fútbol, Motor, Tenis, Baloncesto, Olimpiadas, esports, Formula 1, MotoGP, Motor otros.

        7. 🎭 SOCIEDAD Y CULTURA
           - Cine, Música, Literatura, Sucesos virales, Curiosidades, Estilo de vida, Celebrities.

        REGLAS CRÍTICAS:
        1. UNIFICACIÓN: No separes noticias del mismo tema. Agrúpalas en el cajón que mejor encaje.
        2. ORDEN: Respeta rigurosamente el orden del 1 al 7. No hace falta que esten todos, si hay alguno que no tiene algo no hace falta que lo pongas.
        3. INTEGRIDAD: MUEVE los bloques <h3>...</h3><p>...</p> tal cual están. NO reescribas el contenido.
        4. LIMPIEZA: Elimina noticias duplicadas si las detectas.
        5. FORMATO: **RESPETA LAS ETIQUETAS <b> QUE YA EXISTEN EN EL TEXTO**. No las borres.
        6. DUPLICADOS: Elimina noticias repetidas.

        Salida: HTML limpio con <div class="section"><h2>TÍTULO</h2> ... </div>
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-5-nano",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": raw_articles_html}
                ],

            )

            final_html = response.choices[0].message.content.strip()
            final_html = final_html.replace("```html", "").replace("```", "").strip()
            return final_html

        except Exception as e:
            self.logger.error(f"❌ Error en Arquitectura Final: {e}")
            return raw_articles_html
