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
        async with aiohttp.ClientSession() as session:
            headers = {"Range": "bytes=0-32767", "User-Agent": "Mozilla/5.0"}
            async with session.get(img_url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status not in (200, 206):
                    return True  # CDN raro → fail-OPEN
                data = await resp.content.read(32768)
        try:
            img = Image.open(BytesIO(data))
            w, h = img.size
            # Solo rechazo categórico si dims confirmadas < 200x150
            if w < 200 or h < 150:
                return False
            return True
        except Exception:
            # Pillow no puede leer headers en 32KB (imagen grande / formato raro) → fail-OPEN
            return True
    except Exception:
        # Timeout, DNS, SSL → fail-OPEN
        return True


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
