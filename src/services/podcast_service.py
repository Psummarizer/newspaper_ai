"""
News Podcast Service
====================
Genera podcasts de noticias con dos voces (diálogo) usando Edge TTS.
Sube a Castos para distribución RSS.
"""

import asyncio
import logging
import json
import os
import tempfile
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from openai import AsyncOpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Voces Edge TTS para español
VOICES = {
    "ÁLVARO": "es-ES-AlvaroNeural",
    "ELVIRA": "es-ES-ElviraNeural"
}

class NewsPodcastService:
    """Servicio para generar podcasts de noticias con diálogo."""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5-nano"
        self.temp_dir = Path(tempfile.gettempdir()) / "news_podcast"
        self.temp_dir.mkdir(exist_ok=True)
        
        # Load TTS Configuration
        config_path = Path(__file__).parent.parent / "config" / "tts_config.json"
        self.tts_config = {}
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self.tts_config = json.load(f)
        except Exception as e:
            logger.warning(f"No tts_config.json found, defaulting to Edge TTS: {e}")
            
        self.provider = self.tts_config.get("provider", "edge")
        
        # --- PATH CONFIGURATION ---
        # Locate ffmpeg in sibling project
        self.project_root = Path(__file__).resolve().parent.parent.parent
        self.ffmpeg_path = self.project_root.parent / "podsummarizer_weekly_newsletter" / "src" / "tools" / "ffmpeg" / "bin" / "ffmpeg.exe"
        if not self.ffmpeg_path.exists():
            logger.warning(f"FFmpeg not found at {self.ffmpeg_path}. Check path.")
            self.ffmpeg_path = "ffmpeg" # Fallback to PATH
        else:
            logger.info(f"FFmpeg found: {self.ffmpeg_path}")

        self.google_client = None
        
        if self.provider == "google":
            try:
                from google.cloud import texttospeech
                self.google_client = texttospeech.TextToSpeechClient()
                logger.info("✅ Cliente Google Cloud TTS inicializado.")
            except Exception as e:
                logger.error(f"❌ Error inicializando Google TTS (falta librería o credenciales): {e}")
                self.provider = "edge" # Fallback

    async def generate_for_topics(self, user_id: str, topics_news: Dict[str, List[Dict]]) -> Optional[str]:
        """
        Genera un podcast completo a partir de un mapa de topics -> noticias.
        
        Args:
            user_id: ID del usuario (para personalización)
            topics_news: Dict { "Tecnología": [noticia1, noticia2], ... }
            
        Returns:
            Ruta al archivo de audio generado (.mp3) o None si falla.
        """
        if not topics_news:
            logger.warning("No hay noticias para generar podcast.")
            return None
            
        # 1. Preparar lista de items para el Engine
        engine_items = []
        for topic, news_list in topics_news.items():
            for n in news_list:
                engine_items.append({
                    "topic": topic,
                    "titulo": n.get('titulo', 'Sin título'),
                    "resumen": n.get('resumen', '') or n.get('noticia', '')[:800],
                    "source_name": n.get('fuente', 'Desconocido')
                })
        
        logger.info(f"📝 Generando guión PREMIUM (5-Fases) para {len(engine_items)} noticias...")
        
        try:
            # Importar Engine aquí para evitar ciclos si los hubiera
            from src.engine.podcast_script_engine import PodcastScriptEngine
            
            engine = PodcastScriptEngine(self.client, self.model)
            script = await engine.generate_script(engine_items)
            
            if not script:
                 logger.error("❌ El Engine devolvió un guión vacío.")
                 return None
                 
            logger.info("✅ Guión generado con éxito por PodcastScriptEngine.")
            
        except Exception as e:
            logger.error(f"❌ Error crítico en PodcastScriptEngine: {e}")
            return None

        # 3. Generar Audio
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"podcast_{user_id}_{timestamp}.mp3"
        output_path = str(self.temp_dir / output_filename)
        
        success = await self._generate_audio(script, output_path)
        
        if success:
            return output_path
        else:
            return None

    async def _generate_audio(self, script: str, output_path: str) -> bool:
        """
        Genera el audio del podcast usando configuración HÍBRIDA (Edge + Google).
        """
        # Parsear el guión en segmentos
        segments = []
        lines = script.strip().split("\n")
        
        # Determinar voces según proveedor
        if self.provider == "google":
            voices_map = self.tts_config.get("google_voices", {
                # "Host 1": "es-ES-Neural2-F", # Male
                # "Host 2": "es-ES-Neural2-H"  # Female
                # "Host 1": "es-ES-Chirp3-HD-Algenib", # Male
                # "Host 2": "es-ES-Chirp3-HD-Gacrux"  # Female
                "Host 1": "es-ES-Chirp-HD-D", # Male
                "Host 2": "es-ES-Chirp-HD-F"  # Female
            })
        else:
             voices_map = self.tts_config.get("edge_voices", {
                 "Host 1": "es-ES-AlvaroNeural",
                 "Host 2": "es-ES-XimenaNeural"
             })

        logger.info(f"🎙️ Generando audio con proveedor: {self.provider.upper()}. Voces: {voices_map}")

        for line in lines:
            line = line.strip()
            if not line: continue
            
            voice = None
            text = None
            role = None
            
            if line.startswith("ÁLVARO:"):
                role = "Host 1"
                voice = voices_map.get("Host 1")
                text = line.replace("ÁLVARO:", "").strip()
            elif line.startswith("ELVIRA:"):
                role = "Host 2"
                voice = voices_map.get("Host 2")
                text = line.replace("ELVIRA:", "").strip()
            
            if voice and text:
                segments.append((role, voice, text))
        
        if not segments:
            logger.error("No se encontraron segmentos válidos en el guión")
            return False
        
        logger.info(f"🎤 Generando {len(segments)} segmentos de audio...")
        
        # Generar audio para cada segmento
        audio_files = []
        
        # 1. INYECTAR INTRO MUSICAL (Si existe)
        root_dir = Path(__file__).parent.parent.parent
        intro_path = root_dir / "assets" / "intro.mp3"
        if intro_path.exists():
            logger.info(f"🎵 Añadiendo intro musical: {intro_path}")
            audio_files.append(str(intro_path))
        
        for i, (role, voice, text) in enumerate(segments):
            # Use .wav for better intermediate quality, especially if using Linear16
            temp_path = self.temp_dir / f"segment_{i}.wav"
            success = False
            
            if self.provider == "google":
                # Configuración específica para Elvira (Host 2) en Google
                # AUMENTO DE ANTUSIASMO V2: Pitch +3.5st, Rate 1.18x
                pitch = 3.5 if role == "Host 2" else 0.0
                rate = 1.18 if role == "Host 2" else 1.25
                # Force 44.1kHz to match intro music and avoid playback speed issues (Vader effect)
                success = await self._generate_segment_google(text, voice, temp_path, pitch=pitch, rate=rate, sample_rate=44100)
            else:
                success = await self._generate_segment_edge(text, voice, temp_path)
                
            if success and temp_path.exists():
                audio_files.append(str(temp_path))
                logger.debug(f"  ✅ Segmento {i+1} ({role}) generado")
            else:
                logger.warning(f"⚠️ Fallo generando segmento {i} ({voice})")

        if not audio_files:
            return False

        # Concatenar todos los segmentos
        return self._concatenate_audio(audio_files, output_path)

    async def _generate_segment_edge(self, text: str, voice: str, output_path: Path) -> bool:
        try:
            import edge_tts
            communicate = edge_tts.Communicate(text, voice, rate="+25%")
            await communicate.save(str(output_path))
            return True
        except Exception as e:
            logger.error(f"Error EdgeTTS: {e}")
            return False

    async def _generate_segment_google(self, text: str, voice_name: str, output_path: Path, pitch: float = 0.0, rate: float = 1.10, sample_rate: int = 44100) -> bool:
        try:
            from google.cloud import texttospeech
            
            if not self.google_client:
                 self.google_client = texttospeech.TextToSpeechClient()
            
            synthesis_input = texttospeech.SynthesisInput(text=text)
            
            lang_code = "-".join(voice_name.split("-")[:2]) # "es-ES"
            
            voice_params = texttospeech.VoiceSelectionParams(
                language_code=lang_code,
                name=voice_name
            )

            # Chirp voices do not support pitch/rate/sample_rate in AudioConfig
            is_chirp = "Chirp" in voice_name
            
            if is_chirp:
                 # Use LINEAR16 (WAV) for highest quality from Chirp
                 # Attempting to use speaking_rate (1.18) to add energy
                 audio_config = texttospeech.AudioConfig(
                    audio_encoding=texttospeech.AudioEncoding.LINEAR16,
                    speaking_rate=1.0
                )
            else:
                audio_config = texttospeech.AudioConfig(
                    audio_encoding=texttospeech.AudioEncoding.MP3,
                    speaking_rate=rate,
                    pitch=pitch,
                    sample_rate_hertz=sample_rate
                )

            response = self.google_client.synthesize_speech(
                input=synthesis_input, voice=voice_params, audio_config=audio_config
            )

            with open(output_path, "wb") as out:
                out.write(response.audio_content)
                
            return True
        except Exception as e:
            logger.error(f"Error GoogleTTS: {e}")
            return False

    def _concatenate_audio(self, audio_files: List[str], output_path: str) -> bool:        
        if not audio_files:
            logger.error("No se generó ningún segmento de audio")
            return False
        
        # Concatenar todos los segmentos
        logger.info("🔗 Concatenando segmentos...")
        
        # Intentar con FFmpeg (Robust concatenation + Re-encode to fix sample rates)
        ffmpeg_success = False
        try:
            # PRE-PROCESAMIENTO: Convertir todo a WAV uniforme (44100Hz, 16bit, mono/stereo)
            # Esto evita fallos al mezclar MP3 (intro) con WAV (voces) en concat demuxer
            processed_files = []
            for i, input_file in enumerate(audio_files):
                path_obj = Path(input_file)
                # Si es mp3 o queremos asegurar formato, convertimos a wav temporal
                # Intro es mp3, voces pueden ser wav o mp3. Unificamos todo.
                temp_wav = self.temp_dir / f"concat_part_{i}.wav"
                
                # FFMPEG para normalizar a WAV PCM 16bit 44100Hz
                cmd_convert = [
                    str(self.ffmpeg_path), "-y",
                    "-i", str(path_obj),
                    "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2", # Forzar stereo para evitar error mezcla mono/stereo
                    str(temp_wav)
                ]
                subprocess.run(cmd_convert, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                processed_files.append(str(temp_wav))

            # Crear lista para ffmpeg concat demuxer con los ficheros procesados
            list_path = self.temp_dir / "concat_list.txt"
            with open(list_path, "w", encoding="utf-8") as f:
                for audio_file in processed_files:
                    safe_path = Path(audio_file).absolute().as_posix()
                    f.write(f"file '{safe_path}'\n")
            
            # Ejecutar FFmpeg Final (WAVs -> MP3 High Quality)
            cmd = [
                str(self.ffmpeg_path), "-f", "concat", "-safe", "0",
                "-i", str(list_path),
                "-c:a", "libmp3lame", "-b:a", "192k", "-ar", "44100",
                "-y", str(output_path)
            ]
            
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
                ffmpeg_success = True
                logger.info(f"✅ Audio concatenado (FFmpeg): {output_path}")
            
            # Limpiar lista
            try: os.remove(list_path)
            except: pass
            
        except Exception as e:
            logger.warning(f"⚠️ FFmpeg falló o no instalado ({e}). Usando concatenación binaria (Riesgo de corrupción).")
        
        if not ffmpeg_success:
            try:
                with open(output_path, 'wb') as outfile:
                    for audio_file in audio_files:
                        with open(audio_file, 'rb') as infile:
                            outfile.write(infile.read())
                
                logger.info(f"✅ Audio concatenado (Binario): {output_path}")
            except Exception as e:
                logger.error(f"Error concatenando audio: {e}")
                return False
        # Limpiar archivos temporales
        for audio_file in audio_files:
            try:
                # NO borrar la intro original
                if str(intro_path) in str(audio_file):
                    continue
                os.remove(audio_file)
            except:
                pass
        
        return True
    
    def cleanup(self):
        """Limpia archivos temporales."""
        try:
            import shutil
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
        except:
            pass
    
    async def upload_to_castos(self, user_id: str, audio_path: str, episode_title: str = None) -> Optional[str]:
        """
        Sube el podcast a Castos y devuelve la URL del feed RSS privado.
        
        Usa CastosUploader que incluye RPA via Selenium para obtener el feed privado con UUID.
        
        Args:
            user_id: ID único del usuario (usado para crear/obtener su podcast privado)
            audio_path: Ruta al archivo de audio
            episode_title: Título del episodio (opcional)
        
        Returns:
            URL del feed RSS privado (con uuid=...) o None si falla
        """
        from src.services.castos_hosting import CastosUploader
        from pathlib import Path
        
        CASTOS_API_TOKEN = os.getenv("CASTOS_API_TOKEN")
        if not CASTOS_API_TOKEN:
            logger.error("CASTOS_API_TOKEN no configurado")
            return None
        
        # Título del podcast del usuario
        podcast_title = f"Briefing Diario - {user_id}"
        
        try:
            # 1. Obtener o crear podcast privado para el usuario
            logger.info(f"🔍 Buscando/creando podcast para usuario {user_id}...")
            
            castos_manager = CastosUploader()
            podcast_id, private_feed_url = castos_manager.get_or_create_podcast_id_by_title(
                podcast_title_target=podcast_title,
                market_for_language="es",
                private=True
            )
            
            if not podcast_id:
                logger.error(f"No se pudo obtener/crear podcast para {user_id}")
                return None
            
            logger.info(f"   ✅ Podcast ID: {podcast_id}")
            if private_feed_url:
                logger.info(f"   🔒 Feed privado: {private_feed_url}")
            
            # 2. Subir episodio
            if not episode_title:
                episode_title = f"Briefing {datetime.now().strftime('%d/%m/%Y')}"
            
            logger.info(f"📤 Subiendo episodio: {episode_title}")
            
            episode_uploader = CastosUploader(podcast_id=podcast_id)
            upload_result = episode_uploader.upload_episode(
                podcast_name=podcast_title,
                episode_title=episode_title,
                episode_description=f"Tu resumen de noticias del {datetime.now().strftime('%d de %B de %Y')}",
                audio_file_path=audio_path,
                market="es"
            )
            
            if upload_result:
                share_url, direct_url = upload_result
                logger.info(f"   ✅ Episodio subido: {share_url}")
            else:
                logger.warning("   ⚠️ Upload falló pero podcast creado")
            
            # 3. Retornar feed privado
            if private_feed_url:
                logger.info(f"   🔗 Feed RSS privado: {private_feed_url}")
                return private_feed_url
            
            return None
            
        except Exception as e:
            logger.error(f"Error subiendo a Castos: {e}")


# Script de prueba
async def test_podcast():
    """Genera un podcast de prueba."""
    service = NewsPodcastService()
    
    # Noticias de prueba
    test_news = {
        "Inteligencia Artificial": [
            {
                "titulo": "🤖 OpenAI lanza GPT-5 con capacidades multimodales avanzadas",
                "resumen": "La nueva versión del modelo de lenguaje incluye comprensión de video en tiempo real y razonamiento avanzado."
            },
            {
                "titulo": "💼 Microsoft invierte 10 mil millones en infraestructura de IA",
                "resumen": "La inversión se destinará a nuevos centros de datos especializados en cargas de trabajo de inteligencia artificial."
            }
        ],
        "Economía": [
            {
                "titulo": "📈 El BCE mantiene los tipos de interés estables",
                "resumen": "El Banco Central Europeo decide no modificar las tasas en su última reunión, citando la estabilidad de la inflación."
            }
        ]
    }
    
    result = await service.generate_for_topics("test_user", test_news)
    
    if result:
        print(f"\n✅ Podcast generado: {result}")
        print(f"   Tamaño: {os.path.getsize(result) / 1024:.1f} KB")
    else:
        print("\n❌ Fallo en la generación")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_podcast())
