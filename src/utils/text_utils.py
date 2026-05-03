def is_obvious_icon_url(img_url: str) -> bool:
    """Pre-check rápido (sin red) para descartar URLs que SON claramente
    iconos/logos/thumbnails por su patrón. Conservador: solo rechaza si hay
    evidencia explícita en la URL.

    Patrones rechazados:
      - Sufijos de tamaño pequeño: _64.png, _120.jpg, -48x48.png, etc.
      - Paths de iconos: /icons/, /logos/, /favicon, /avatar
      - Resizers con dimensiones <200px: resize/120, w=64, /600x31/, /crop/0x53/
      - Formatos casi siempre icono: .svg, .ico
      - Hosts conocidos de iconos: a.fsdn.com/sd/topics/, abs.twimg.com
    """
    if not img_url or not img_url.startswith("http"):
        return True  # vacío o no-http = inválido
    import re as _re
    u = img_url.lower().split("?")[0]
    if u.endswith(".svg") or u.endswith(".ico"):
        return True
    # Tamaños pequeños explícitos en path
    if _re.search(r'[_\-/](16|24|32|48|64|72|96|120|128|150)\.(png|jpg|jpeg|webp|gif)$', u):
        return True
    if _re.search(r'-\d{1,3}x\d{1,3}\.(png|jpg|jpeg|webp|gif)$', u):  # ej: -48x48.png
        # Solo rechazar si AMBAS dims < 200
        m = _re.search(r'-(\d{1,3})x(\d{1,3})\.', u)
        if m and int(m.group(1)) < 200 and int(m.group(2)) < 200:
            return True
    # Resizers/crops con dimensiones pequeñas
    if _re.search(r'/resize/\d{1,3}(?!\d)', u):  # resize/120 pero no resize/1200
        m = _re.search(r'/resize/(\d+)', u)
        if m and int(m.group(1)) < 250:
            return True
    if _re.search(r'/crop/\d{1,3}x\d{1,3}/', u):  # crop/155x100
        m = _re.search(r'/crop/(\d+)x(\d+)/', u)
        if m and (int(m.group(1)) < 200 or int(m.group(2)) < 100):
            return True
    if _re.search(r'/\d{2,4}x\d{1,3}/', u):  # 600x31 (banner thin)
        m = _re.search(r'/(\d+)x(\d+)/', u)
        if m and int(m.group(2)) < 100:  # altura muy baja
            return True
    # Paths típicos de iconos/logos
    icon_paths = ['/icons/', '/icon/', '/logos/', '/logo/', '/favicon',
                  '/avatar', '/emoji/', '/sprite', '/share-image',
                  'a.fsdn.com/sd/topics/', 'abs.twimg.com', 'pbs.twimg.com',
                  'static.xx.', 'og-default', 'placeholder', 'default-image']
    if any(p in u for p in icon_paths):
        return True
    # Pequeños tamaños como query params (legacy fallback — pre-check sin parsear querystring)
    qlower = img_url.lower()
    if 'w=64' in qlower or 'h=64' in qlower or 'size=small' in qlower:
        return True
    return False


async def validate_image_size(img_url: str) -> bool:
    """Valida que la imagen sea una foto real (no icono/logo).

    Política FAIL-OPEN: en caso de duda → ACEPTAR. Mejor mostrar la imagen
    real del artículo que la imagen de categoría por defecto.

    Pasos:
      1. Pre-check por URL (is_obvious_icon_url) → rápido, sin red, determinista.
      2. Si no es obviamente icono, intenta validar dimensiones con descarga
         parcial. Solo rechaza si Pillow LEE las dims y son < 200x150.
      3. Cualquier error de red, decodificación o ambigüedad → ACEPTAR.
    """
    # Paso 1: pre-check rápido por patrones de URL
    if is_obvious_icon_url(img_url):
        return False
    # Paso 2: intentar leer dims reales — fail-OPEN si no podemos
    try:
        import aiohttp
        from PIL import Image
        from io import BytesIO
        # User-Agent realista: algunos CDN bloquean "Mozilla/5.0" genérico.
        # Referer vacío: así validamos como lo verá el cliente de email (Gmail
        # no envía referer al descargar imágenes desde un correo).
        headers = {
            "Range": "bytes=0-32767",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(img_url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=4)) as resp:
                # 4xx/5xx = RECHAZO duro (Gmail tampoco podrá descargarla).
                # Antes: fail-OPEN -> guardábamos URLs 400 Bad Request (hotlink
                # protection), el cliente de email las mostraba rotas.
                if 400 <= resp.status < 600:
                    return False
                if resp.status not in (200, 206):
                    return True  # 1xx/3xx raro → fail-OPEN
                data = await resp.content.read(32768)
        try:
            img = Image.open(BytesIO(data))
            w, h = img.size
            if w < 200 or h < 150:
                return False
            return True
        except Exception:
            # Pillow no puede leer headers en 32KB (imagen grande / formato raro) → fail-OPEN
            return True
    except Exception:
        # Timeout, DNS, SSL → fail-OPEN
        return True


def sanitize_user_context(text: str, max_chars: int = 300) -> str:
    """Sanitiza el contexto de Firestore que el usuario escribe para sus topics.

    Defensa en profundidad — el contexto va a:
      - 5+ prompts LLM (filtro, dedup, parser de subtopics, redacción, selección)
      - Logs y eventualmente HTML del email
      - Posibles BD/almacenamiento en GCS

    Amenazas:
      1. Prompt injection: usuario intenta secuestrar el LLM
         ("ignore previous instructions", "system: …", "</prompt> ahora eres …")
      2. HTML/JS injection: si el contexto se pinta en el email
         (<script>, <img onerror=…>, javascript:)
      3. Tamaño excesivo: gasto Mistral / DoS
      4. Caracteres de control que rompan JSON/logs

    Política: FAIL-OPEN — preferimos mantener intención del usuario.
    Solo strippeamos lo claramente malicioso. No validamos contenido editorial.
    """
    if not text or not isinstance(text, str):
        return ""

    import re as _re
    s = text

    # 1. Strip caracteres de control (excepto \n, \t, \r) y null bytes
    s = _re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)

    # 2. Eliminar etiquetas HTML/script (cualquier <...>)
    #    No queremos <script>, <img onerror=…>, ni siquiera <b> aquí.
    s = _re.sub(r'<[^>]*>', '', s)

    # 3. Neutralizar patrones de prompt injection — los reemplazamos por
    #    [redacted] para no dar pistas al atacante de qué se filtra
    _PI_PATTERNS = [
        # English
        r'(?i)\bignore\s+(?:all\s+|any\s+|the\s+|previous\s+|prior\s+|the\s+above\s+)*(?:instructions?|prompts?|rules?|system|messages?)\b',
        r'(?i)\bdisregard\s+(?:all\s+|previous\s+|prior\s+|the\s+)*(?:instructions?|prompts?|rules?)\b',
        r'(?i)\bforget\s+(?:all\s+|everything|previous|the\s+above|what)\b',
        r'(?i)\b(?:you\s+are\s+now|act\s+as|pretend\s+to\s+be|roleplay\s+as)\b',
        r'(?i)\bjailbreak\b',
        r'(?i)\bnew\s+(?:instructions?|task|rules?|system\s+prompt)\b',
        # Spanish
        r'(?i)\bignor[ae]r?\s+(?:todas?\s+|las\s+|cualquier\s+|las\s+anteriores\s+)?(?:instrucciones?|reglas?|el\s+sistema)\b',
        r'(?i)\bolvid[ae](?:te)?\s+(?:todo|todas?|las\s+|cualquier|lo\s+anterior)\b',
        r'(?i)\b(?:act[uú]a\s+como|comp[oó]rtate\s+como|haz\s+de|finge\s+ser)\b',
        r'(?i)\beres\s+ahora\s+(?:un|una)\b',
        r'(?i)\bnuev[oa]s?\s+(?:instrucciones?|reglas?|tareas?|prompt)\b',
        # Tags / markup
        r'(?i)<\s*\|?\s*(?:system|assistant|user|im_start|im_end)\s*\|?\s*>',
        r'(?i)\[(?:system|assistant|user)\]\s*:',
        r'(?i)###\s*(?:system|instructions?|prompt)',
        r'(?i)```\s*(?:system|instructions?|prompt)',
        # URL schemes peligrosas
        r'javascript\s*:',
        r'data\s*:\s*text/html',
        r'vbscript\s*:',
    ]
    for pat in _PI_PATTERNS:
        s = _re.sub(pat, '[redacted]', s)

    # 4. Comprimir espacios en blanco
    s = _re.sub(r'\s+', ' ', s).strip()

    # 5. Truncar a max_chars (cap duro)
    if len(s) > max_chars:
        s = s[:max_chars].rstrip()

    return s


def truncate_to_sentence(text: str, max_chars: int = 220) -> str:
    """Truncate text to the last complete sentence within max_chars.
    Always returns text ending in sentence-final punctuation."""
    if not text:
        return ""
    if len(text) <= max_chars:
        if text[-1] not in '.!?':
            return text + "."
        return text

    truncated = text[:max_chars]
    cut = None
    for sep in ['. ', '! ', '? ', '.', '!', '?']:
        pos = truncated.rfind(sep)
        if pos > 30:
            cut = pos + 1
            break

    if cut:
        result = truncated[:cut].rstrip()
    else:
        space_pos = truncated.rfind(' ')
        if space_pos > 30:
            result = truncated[:space_pos].rstrip() + "."
        else:
            result = truncated.rstrip() + "."

    return result
