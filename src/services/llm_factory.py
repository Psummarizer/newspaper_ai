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
    def _get_or_create_client(cls, provider: str, key_suffix: str = "") -> "AsyncOpenAI":
        """Creates or returns a cached AsyncOpenAI client for the given provider.
        key_suffix: "" for primary key, "2" for secondary fallback key (e.g. MISTRAL_API_KEY2)
        """
        cache_key = f"{provider}{key_suffix}"
        if cache_key not in cls._clients:
            load_dotenv()
            config = cls._load_config()
            provider_config = config.get("llm_providers", {}).get(provider, {})

            # Map provider name → env var key
            env_keys = {
                "openai": "OPENAI_API_KEY",
                "gemini": "GEMINI_API_KEY",
                "groq": "GROQ_API_KEY",
                "mistral": "MISTRAL_API_KEY",
            }
            base_env_key = env_keys.get(provider, f"{provider.upper()}_API_KEY")
            env_key = base_env_key + key_suffix
            api_key = os.getenv(env_key)
            if not api_key:
                logger.warning(f"{env_key} no encontrada en entorno")

            base_url = provider_config.get("base_url")
            if base_url:
                cls._clients[cache_key] = AsyncOpenAI(api_key=api_key, base_url=base_url)
            else:
                cls._clients[cache_key] = AsyncOpenAI(api_key=api_key)

        return cls._clients[cache_key]

    @classmethod
    def get_fallback_client(cls, provider: str):
        """Returns (client, model_name) using secondary API key (e.g. MISTRAL_API_KEY2).
        Used when primary key hits rate limits (429).
        Falls back to 'gemini' quality provider if no secondary key exists.
        """
        config = cls._load_config()
        provider_config = config.get("llm_providers", {}).get(provider, {})
        model_name = provider_config.get("fast_model") or provider_config.get("quality_model", "mistral-small-latest")

        # Check if a secondary key exists
        env_keys = {"openai": "OPENAI_API_KEY", "gemini": "GEMINI_API_KEY",
                    "groq": "GROQ_API_KEY", "mistral": "MISTRAL_API_KEY"}
        base_env_key = env_keys.get(provider, f"{provider.upper()}_API_KEY")
        secondary_key = os.getenv(base_env_key + "2")

        if secondary_key:
            logger.info(f"🔄 Usando clave secundaria {base_env_key}2 como fallback")
            client = cls._get_or_create_client(provider, key_suffix="2")
            return client, model_name
        else:
            # Fall back to gemini quality provider
            logger.warning(f"No hay clave secundaria para {provider}, usando Gemini como fallback")
            return cls.get_client("quality")

    @classmethod
    def get_client(cls, task_type="fast"):
        """
        Devuelve el cliente configurado y el nombre del modelo.
        task_type: "fast" o "quality"

        Supports per-task provider routing via 'task_provider_routing' in config.
        Example: {"fast": "mistral", "quality": "gemini"} routes cheap tasks
        to Mistral and expensive tasks to Gemini.
        """
        config = cls._load_config()

        # Per-task routing: allows different providers for fast vs quality
        routing = config.get("task_provider_routing", {})
        provider = routing.get(task_type) or config.get("active_llm_provider", "openai")

        provider_config = config.get("llm_providers", {}).get(provider, {})
        model_key = f"{task_type}_model"
        model_name = provider_config.get(model_key)

        if not model_name:
            fallbacks = {
                "openai": {"fast": "gpt-5-nano", "quality": "gpt-4o-mini"},
                "gemini": {"fast": "gemini-2.5-flash", "quality": "gemini-2.5-pro"},
                "groq": {"fast": "gemma2-9b-it", "quality": "llama-3.3-70b-versatile"},
                "mistral": {"fast": "mistral-small-latest", "quality": "mistral-small-latest"},
            }
            model_name = fallbacks.get(provider, {}).get(task_type, "mistral-small-latest")

        client = cls._get_or_create_client(provider)
        return client, model_name

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
