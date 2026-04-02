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
