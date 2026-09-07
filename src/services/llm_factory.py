import os
import re
import json
import time
import asyncio
import logging
from pathlib import Path
from openai import AsyncOpenAI
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


# --- Circuit breaker de proveedores ---------------------------------------
# Motivo (incidencia 2026-09-03 → 2026-09-06): las dos claves de Mistral
# agotaron la cuota del free-tier y devolvían 429 en CADA llamada. El código
# reintentaba con sleeps de 10/30/60s por lote, el job de ingesta reventaba el
# task-timeout de 3600s y topics.json se quedaba sin noticias → briefing vacío.
# Con el breaker, el primer proveedor que agota cuota se marca caído para el
# resto del run y las llamadas van directas al siguiente proveedor sano.
PROVIDER_DOWN_COOLDOWN_S = 30 * 60

# Bloqueo DURO: el proveedor responde 429 con `x-ratelimit-limit-req-minute: 0`,
# es decir el cupo asignado a la cuenta es cero — no es un pico de trafico ni
# tokens consumidos, sino acceso deshabilitado a nivel de workspace (verificacion
# pendiente, pago fallido, cuenta fuera del tier gratuito). Esperar no lo arregla,
# asi que se aparta al proveedor durante todo el run en lugar de reintentarlo cada
# media hora. Diagnosticado el 06/09/2026 con las dos claves de Mistral a la vez.
PROVIDER_HARD_BLOCK_COOLDOWN_S = 24 * 3600

# Orden de failover cuando el proveedor primario se cae. Se filtra en runtime
# por clave presente en el entorno.
# OpenAI va ANTES que Gemini a propósito: gpt-5-nano cuesta $0.05/1M input
# frente a $0.30/1M de Gemini Flash, y el proyecto de Gemini tiene billing
# habilitado (los excesos del free tier se cobran en silencio, ver la cabecera
# de src/utils/llm_quality.py). Gemini queda como penúltimo recurso.
FALLBACK_CHAIN = ["mistral", "openai", "gemini", "groq"]

_QUOTA_PATTERNS = (
    "429", "rate_limited", "rate limit", "resource_exhausted",
    "quota", "insufficient_quota", "too many requests",
)


def is_quota_error(exc: Exception) -> bool:
    """True si el error es de cuota/rate-limit (y por tanto reintentable en otro proveedor)."""
    s = str(exc).lower()
    return any(p in s for p in _QUOTA_PATTERNS)


def is_hard_block(exc: Exception) -> bool:
    """True si el 429 trae un limite asignado de 0 req/min (cuenta deshabilitada).

    Se distingue del rate-limit normal porque no se recupera con el tiempo: hay
    que arreglar la cuenta en el panel del proveedor.
    """
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None) or {}
    for name in ("x-ratelimit-limit-req-minute", "x-ratelimit-limit-requests"):
        raw = headers.get(name)
        if raw is None:
            continue
        try:
            if int(str(raw).strip()) == 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


class LLMFactory:
    _config = None
    _clients = {}
    _failover_clients = {}
    # {provider_key: epoch_ts_hasta_el_que_está_caído}
    _down_until = {}

    # --- Salud de proveedores ---------------------------------------------
    @classmethod
    def mark_down(cls, provider_key: str, cooldown_s: int = PROVIDER_DOWN_COOLDOWN_S):
        """Marca un proveedor (o clave concreta, ej. 'mistral2') como agotado."""
        if not cls.is_down(provider_key):
            logger.warning(
                f"🚫 Proveedor '{provider_key}' marcado como CAÍDO por cuota "
                f"({cooldown_s}s). Las llamadas irán al siguiente del chain."
            )
        cls._down_until[provider_key] = time.time() + cooldown_s

    @classmethod
    def is_down(cls, provider_key: str) -> bool:
        until = cls._down_until.get(provider_key, 0)
        if until and time.time() >= until:
            del cls._down_until[provider_key]
            return False
        return bool(until)

    @classmethod
    def reset_health(cls):
        cls._down_until = {}

    @classmethod
    def health_report(cls) -> dict:
        now = time.time()
        return {k: max(0, int(v - now)) for k, v in cls._down_until.items()}

    # --- Config -----------------------------------------------------------
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

    @staticmethod
    def _env_key_for(provider: str) -> str:
        env_keys = {
            "openai": "OPENAI_API_KEY",
            "gemini": "GEMINI_API_KEY",
            "groq": "GROQ_API_KEY",
            "mistral": "MISTRAL_API_KEY",
        }
        return env_keys.get(provider, f"{provider.upper()}_API_KEY")

    @classmethod
    def has_key(cls, provider: str, key_suffix: str = "") -> bool:
        load_dotenv()
        return bool(os.getenv(cls._env_key_for(provider) + key_suffix))

    @classmethod
    def _model_for(cls, provider: str, task_type: str) -> str:
        config = cls._load_config()
        provider_config = config.get("llm_providers", {}).get(provider, {})
        model_name = provider_config.get(f"{task_type}_model")
        if not model_name:
            fallbacks = {
                "openai": {"fast": "gpt-5-nano", "quality": "gpt-4o-mini"},
                "gemini": {"fast": "gemini-2.5-flash", "quality": "gemini-2.5-flash"},
                "groq": {"fast": "gemma2-9b-it", "quality": "llama-3.3-70b-versatile"},
                "mistral": {"fast": "mistral-small-latest", "quality": "mistral-small-latest"},
            }
            model_name = fallbacks.get(provider, {}).get(task_type, "mistral-small-latest")
        return model_name

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

            env_key = cls._env_key_for(provider) + key_suffix
            api_key = os.getenv(env_key)
            if not api_key:
                logger.warning(f"{env_key} no encontrada en entorno")

            base_url = provider_config.get("base_url")
            if base_url:
                cls._clients[cache_key] = AsyncOpenAI(api_key=api_key, base_url=base_url)
            else:
                cls._clients[cache_key] = AsyncOpenAI(api_key=api_key)

        return cls._clients[cache_key]

    # --- Chain de failover -------------------------------------------------
    @classmethod
    def build_chain(cls, task_type: str, primary: str) -> list:
        """Devuelve [(provider_key, provider, model, client), ...] ordenado:
        primario → clave secundaria del primario → resto de proveedores con clave.
        """
        chain = []
        seen = set()

        def _add(provider: str, suffix: str = ""):
            key = f"{provider}{suffix}"
            if key in seen or not cls.has_key(provider, suffix):
                return
            seen.add(key)
            chain.append((key, provider, cls._model_for(provider, task_type),
                          cls._get_or_create_client(provider, suffix)))

        _add(primary)
        _add(primary, "2")  # clave secundaria del mismo proveedor (ej. MISTRAL_API_KEY2)
        for prov in FALLBACK_CHAIN:
            if prov != primary:
                _add(prov)
        return chain

    @classmethod
    def get_fallback_client(cls, provider: str, task_type: str = "fast"):
        """Devuelve (client, model) del primer proveedor SANO distinto al que falló.

        Antes esto devolvía `get_client("quality")`, que con la config actual
        (quality → mistral) reenviaba al MISMO proveedor agotado: el fallback a
        Gemini documentado en CLAUDE.md nunca llegaba a ejecutarse.
        """
        for key, prov, model, client in cls.build_chain(task_type, provider):
            if key == provider or cls.is_down(key):
                continue
            logger.info(f"🔄 Fallback LLM: {provider} → {key} ({model})")
            return client, model
        # Nada sano: devolver el primario y que el caller gestione el error.
        logger.error(f"❌ Sin proveedor de fallback sano para '{provider}'")
        return cls._get_or_create_client(provider), cls._model_for(provider, task_type)

    @classmethod
    def get_client(cls, task_type="fast"):
        """
        Devuelve el cliente configurado y el nombre del modelo.
        task_type: "fast" o "quality"

        El cliente devuelto es un FailoverClient: expone la misma API que
        AsyncOpenAI (`.chat.completions.create(...)`) pero si el proveedor
        primario agota cuota (429) reintenta automáticamente con el siguiente
        proveedor del chain, traduciendo el nombre de modelo y los kwargs
        incompatibles. Los call-sites existentes no necesitan cambios.
        """
        config = cls._load_config()

        # Per-task routing: allows different providers for fast vs quality
        routing = config.get("task_provider_routing", {})
        provider = routing.get(task_type) or config.get("active_llm_provider", "openai")

        chain = cls.build_chain(task_type, provider)
        if not chain:
            # Sin ninguna clave: comportamiento antiguo (fallará al llamar).
            return cls._get_or_create_client(provider), cls._model_for(provider, task_type)

        # Cachear por firma del chain: si "fast" y "quality" resuelven al mismo
        # proveedor y modelo, devolvemos LA MISMA instancia. Antes de existir
        # FailoverClient, `_get_or_create_client` ya cacheaba por proveedor y
        # hay código que compara identidad (`client_quality is not client`
        # en _redact_batch) para decidir su ruta de fallback.
        sig = tuple((k, m) for k, _p, m, _c in chain)
        if sig not in cls._failover_clients:
            cls._failover_clients[sig] = FailoverClient(task_type, chain)
        return cls._failover_clients[sig], chain[0][2]

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


# --- Cliente con failover automático entre proveedores ---------------------

def _adapt_kwargs(provider: str, model: str, kwargs: dict) -> dict:
    """Normaliza kwargs a las peculiaridades de cada proveedor."""
    kw = dict(kwargs)
    if provider == "gemini" and kw.get("max_tokens") is not None:
        # Gemini 2.5 gasta el presupuesto en thinking tokens antes de emitir
        # texto: con un max_tokens pequeño devuelve content vacío (None).
        kw["max_tokens"] = max(int(kw["max_tokens"]), 512)
    if provider == "openai" and re.match(r"^(gpt-5|o[134])", model or ""):
        # Los modelos de razonamiento de OpenAI rechazan max_tokens y
        # temperature != 1 (400 Bad Request, no es error de cuota).
        if "max_tokens" in kw:
            kw["max_completion_tokens"] = kw.pop("max_tokens")
        kw.pop("temperature", None)
        kw.pop("top_p", None)
        # Igual que Gemini: los reasoning tokens se comen un presupuesto
        # pequeño y la respuesta llega vacía. Suelo alto + esfuerzo bajo.
        if kw.get("max_completion_tokens") is not None:
            kw["max_completion_tokens"] = max(int(kw["max_completion_tokens"]), 4096)
        kw.setdefault("reasoning_effort", "low")
    return kw


class _Completions:
    def __init__(self, owner: "FailoverClient"):
        self._owner = owner

    async def create(self, *, model=None, **kwargs):
        return await self._owner._create(**kwargs)


class _Chat:
    def __init__(self, owner: "FailoverClient"):
        self.completions = _Completions(owner)


class FailoverClient:
    """Proxy de AsyncOpenAI que recorre un chain de proveedores ante errores de cuota.

    - `model=` que pase el call-site se IGNORA: cada proveedor usa el suyo
      (resuelto desde model_config.json para el mismo task_type).
    - Un 429 aislado se reintenta una vez en el mismo proveedor (burst);
      si vuelve a fallar, el proveedor se marca caído y se salta al siguiente.
    - Errores que NO son de cuota se propagan tal cual (bug real, no failover).
    - Atributos no-chat (`.audio`, `.embeddings`, ...) se delegan al primario.
    """

    BURST_RETRY_SLEEP_S = 2

    def __init__(self, task_type: str, chain: list):
        self.task_type = task_type
        self._chain = chain
        self.chat = _Chat(self)

    @property
    def primary_model(self) -> str:
        return self._chain[0][2] if self._chain else ""

    @property
    def primary_provider(self) -> str:
        return self._chain[0][1] if self._chain else ""

    def __getattr__(self, item):
        # `.audio`, `.embeddings`, `.models`, ... → cliente primario directo.
        if not self.__dict__.get("_chain"):
            raise AttributeError(item)
        return getattr(self.__dict__["_chain"][0][3], item)

    async def _create(self, **kwargs):
        last_exc = None
        tried = []
        for key, provider, model, client in self._chain:
            if LLMFactory.is_down(key):
                continue
            kw = _adapt_kwargs(provider, model, kwargs)
            for attempt in (0, 1):
                try:
                    resp = await client.chat.completions.create(model=model, **kw)
                    if tried:
                        logger.info(f"✅ LLM resuelto vía fallback '{key}' tras {tried}")
                    return resp
                except Exception as e:
                    if not is_quota_error(e):
                        raise
                    last_exc = e
                    if is_hard_block(e):
                        # Cupo asignado = 0: la cuenta esta deshabilitada, no hay
                        # nada que esperar. Fuera del chain sin gastar el reintento.
                        logger.error(
                            f"⛔ '{key}' devuelve limite 0 req/min: acceso deshabilitado "
                            f"a nivel de cuenta, no es un pico de trafico. Revisa el panel "
                            f"del proveedor (verificacion / pago / tier)."
                        )
                        LLMFactory.mark_down(key, PROVIDER_HARD_BLOCK_COOLDOWN_S)
                        tried.append(key)
                        break
                    if attempt == 0:
                        # Puede ser un pico puntual: un reintento corto y barato.
                        await asyncio.sleep(self.BURST_RETRY_SLEEP_S)
                        continue
                    LLMFactory.mark_down(key)
                    tried.append(key)
        if last_exc:
            raise last_exc
        raise RuntimeError("FailoverClient: no hay proveedores LLM disponibles")
