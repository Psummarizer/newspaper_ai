import os
import json
import logging
from pathlib import Path
from openai import AsyncOpenAI
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class LLMFactory:
    _config = None
    _clients = {}

    @classmethod
    def _load_config(cls):
        if cls._config is None:
            # Usar resolve() asegura que se tome la ruta absoluta independientemente de desde dónde se ejecute el script
            config_path = Path(__file__).resolve().parent.parent / "config" / "model_config.json"
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    cls._config = json.load(f)
            except Exception as e:
                logger.error(f"Error loading model_config.json: {e}")
                # Fallback simple defaults
                cls._config = {
                    "active_llm_provider": "openai",
                    "llm_providers": {
                        "openai": {
                            "fast_model": "gpt-5-nano",
                            "quality_model": "gpt-4o-mini",
                            "base_url": None
                        }
                    }
                }
        return cls._config

    @classmethod
    def get_client(cls, task_type="fast"):
        """
        Devuelve el cliente configurado y el nombre del modelo.
        task_type: "fast" o "quality"
        """
        config = cls._load_config()
        provider = config.get("active_llm_provider", "openai")
        provider_config = config.get("llm_providers", {}).get(provider, {})
        
        # Obtener el modelo según el task_type
        model_key = f"{task_type}_model"
        model_name = provider_config.get(model_key)
        
        # Si no se encuentra el modelo específico, intentar hardcoded fallbacks
        if not model_name:
            if provider == "openai":
                model_name = "gpt-5-nano" if task_type == "fast" else "gpt-4o-mini"
            elif provider == "gemini":
                model_name = "gemini-2.5-flash" if task_type == "fast" else "gemini-2.5-pro"
                
        # Singleton client per provider
        if provider not in cls._clients:
            load_dotenv()
            
            if provider == "openai":
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key:
                    logger.warning("OPENAI_API_KEY no encontrada en entorno")
                cls._clients[provider] = AsyncOpenAI(api_key=api_key)
                
            elif provider == "gemini":
                api_key = os.getenv("GEMINI_API_KEY")
                if not api_key:
                    logger.warning("GEMINI_API_KEY no encontrada en entorno")
                base_url = provider_config.get("base_url", "https://generativelanguage.googleapis.com/v1beta/openai/")
                cls._clients[provider] = AsyncOpenAI(
                    api_key=api_key,
                    base_url=base_url
                )
            else:
                raise ValueError(f"Proveedor LLM no soportado: {provider}")
                
        return cls._clients[provider], model_name

    @classmethod
    def get_tts_config(cls, language: str = "es"):
        """
        Devuelve la configuración de TTS para el idioma indicado.
        Retorna: { "provider": str, "voices": { "Host 1": ..., "Host 2": ... } }
        """
        config = cls._load_config()
        provider = config.get("active_tts_provider", "google")
        lang = language.lower().strip()

        voices_by_lang = config.get("tts_voices_by_language", {})

        # Intentar el idioma exacto, luego fallback a "es"
        lang_voices = voices_by_lang.get(lang) or voices_by_lang.get("es", {})

        # Voces para el proveedor activo dentro de ese idioma
        voices = lang_voices.get(provider) or lang_voices.get("google", {})

        return {
            "provider": provider,
            "voices": voices,
            # Devolver también el mapa completo por si el caller necesita cambiar de proveedor
            "all_voices": lang_voices
        }

    @classmethod
    def get_language_config(cls, language: str = "es") -> dict:
        """Devuelve nombre, RTL y locale para el idioma dado."""
        config = cls._load_config()
        lang = language.lower().strip()
        return config.get("language_config", {}).get(lang, {
            "name": "Spanish", "rtl": False, "locale": "es-ES"
        })
