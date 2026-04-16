async def validate_image_size(img_url: str) -> bool:
    """Valida que la imagen sea una foto real (no icono/logo/emoji) midiendo
    dimensiones reales vía descarga parcial de los primeros 16KB (suficiente
    para que Pillow lea los headers JPEG/PNG/WebP).

    Criterios de rechazo:
      - URL vacía, no http, .svg o .ico
      - Content-Length < 10KB
      - Dimensiones < 300x200
      - Ratio casi cuadrado (1:1) con lado menor < 500px → probable logo/avatar

    Uso: compartido entre ingest (filtra al guardar) y orchestrator (filtra
    URLs de cache pre-v0.67 que aún contengan iconos).
    """
    if not img_url or not img_url.startswith("http"):
        return False
    lower = img_url.lower().split("?")[0]
    if lower.endswith(".svg") or lower.endswith(".ico"):
        return False
    try:
        import aiohttp
        from PIL import Image
        from io import BytesIO
        async with aiohttp.ClientSession() as session:
            headers = {"Range": "bytes=0-16383", "User-Agent": "Mozilla/5.0"}
            async with session.get(img_url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=6)) as resp:
                if resp.status not in (200, 206):
                    return False
                content_range = resp.headers.get("Content-Range", "")
                if content_range:
                    try:
                        total_bytes = int(content_range.split("/")[-1])
                        if total_bytes < 10_000:
                            return False
                    except Exception:
                        pass
                data = await resp.content.read(16384)
        try:
            img = Image.open(BytesIO(data))
            w, h = img.size
            if w < 300 or h < 200:
                return False
            ratio = max(w, h) / min(w, h)
            if ratio < 1.15 and min(w, h) < 500:
                return False
            return True
        except Exception:
            return False
    except Exception:
        return False


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
