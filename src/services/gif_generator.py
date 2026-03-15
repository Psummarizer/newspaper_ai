"""
GIF Generator - Animated email header & market ticker
======================================================
Generates animated GIFs for the newsletter email:
- Header: always-the-same rotating news animation
- Ticker: scrolling market prices with green/red arrows

Uses Pillow for image generation and GCS for hosting.
"""
import io
import os
import logging
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# Colors matching the email dark theme
BG_DARK = (21, 32, 43)        # #15202B
ACCENT = (29, 161, 242)       # #1DA1F2
WHITE = (255, 255, 255)
GRAY = (136, 153, 166)        # #8899A6
GREEN = (0, 200, 83)
RED = (255, 69, 58)
GOLD = (255, 215, 0)

HEADER_WIDTH = 600
HEADER_HEIGHT = 80
TICKER_HEIGHT = 36


def _get_font(size: int, bold: bool = False):
    """Get a font, falling back to default if custom fonts unavailable."""
    try:
        # Try common system fonts
        for name in ("arialbd.ttf" if bold else "arial.ttf",
                     "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf",
                     "LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"):
            try:
                return ImageFont.truetype(name, size)
            except OSError:
                continue
    except Exception:
        pass
    return ImageFont.load_default()


def generate_header_gif() -> bytes:
    """
    Generate a permanent animated header GIF.
    Rotating news icons slide across a dark background with 'Briefing Daily' text.
    Returns GIF bytes.
    """
    frames = []
    num_frames = 20
    duration_per_frame = 100  # ms

    font_title = _get_font(26, bold=True)
    font_sub = _get_font(11)

    # Icons that rotate across the header (text-based since we can't use real icons)
    icons = ["📰", "🌍", "📊", "⚡", "💹", "🏛️", "🔬", "⚽"]

    for f in range(num_frames):
        img = Image.new("RGB", (HEADER_WIDTH, HEADER_HEIGHT), BG_DARK)
        draw = ImageDraw.Draw(img)

        # Accent line at bottom
        draw.rectangle([(0, HEADER_HEIGHT - 3), (HEADER_WIDTH, HEADER_HEIGHT)], fill=ACCENT)

        # Sliding accent highlight (subtle glow effect)
        glow_x = int((f / num_frames) * (HEADER_WIDTH + 200)) - 100
        for i in range(60):
            alpha = max(0, 30 - abs(i - 30))
            color = (21 + alpha, 32 + alpha * 2, 43 + alpha * 3)
            draw.line([(glow_x + i, 0), (glow_x + i, HEADER_HEIGHT - 3)], fill=color)

        # Title text - centered
        title = "Briefing"
        title_bbox = draw.textbbox((0, 0), title, font=font_title)
        title_w = title_bbox[2] - title_bbox[0]

        daily = " Daily"
        daily_bbox = draw.textbbox((0, 0), daily, font=font_title)
        daily_w = daily_bbox[2] - daily_bbox[0]

        total_w = title_w + daily_w
        x_start = (HEADER_WIDTH - total_w) // 2
        y_text = 15

        draw.text((x_start, y_text), title, fill=WHITE, font=font_title)
        draw.text((x_start + title_w, y_text), daily, fill=ACCENT, font=font_title)

        # Subtitle
        sub = "AI Curated News"
        sub_bbox = draw.textbbox((0, 0), sub, font=font_sub)
        sub_w = sub_bbox[2] - sub_bbox[0]
        draw.text(((HEADER_WIDTH - sub_w) // 2, y_text + 35), sub, fill=GRAY, font=font_sub)

        # Small dots animation (subtle)
        for d in range(3):
            dot_phase = (f + d * 7) % num_frames
            dot_alpha = int(255 * (1 - abs(dot_phase - num_frames / 2) / (num_frames / 2)))
            dot_color = (ACCENT[0], ACCENT[1], min(255, ACCENT[2] + dot_alpha // 4))
            dot_x = 20 + d * 12
            dot_y = HEADER_HEIGHT - 12
            draw.ellipse([(dot_x, dot_y), (dot_x + 4, dot_y + 4)], fill=dot_color)

        # Right side dots
        for d in range(3):
            dot_phase = (f + d * 7 + 10) % num_frames
            dot_alpha = int(255 * (1 - abs(dot_phase - num_frames / 2) / (num_frames / 2)))
            dot_color = (ACCENT[0], ACCENT[1], min(255, ACCENT[2] + dot_alpha // 4))
            dot_x = HEADER_WIDTH - 52 + d * 12
            dot_y = HEADER_HEIGHT - 12
            draw.ellipse([(dot_x, dot_y), (dot_x + 4, dot_y + 4)], fill=dot_color)

        frames.append(img)

    # Save as GIF
    buf = io.BytesIO()
    frames[0].save(
        buf,
        format="GIF",
        save_all=True,
        append_images=frames[1:],
        duration=duration_per_frame,
        loop=0,
        optimize=True,
    )
    buf.seek(0)
    return buf.getvalue()


def generate_ticker_gif(prices: list) -> bytes:
    """
    Generate an animated scrolling ticker GIF with market prices.
    prices: list of {name, price, change_pct}
    Returns GIF bytes.
    """
    if not prices:
        return b""

    frames = []
    num_frames = 300  # Many frames for very slow, smooth scroll
    duration_per_frame = 80  # ms per frame (~24s full cycle)

    font = _get_font(13, bold=True)
    font_sm = _get_font(11)

    # Build ticker text segments
    segments = []
    for p in prices:
        name = p["name"]
        price = p["price"]
        change = p["change_pct"]
        arrow = "▲" if change >= 0 else "▼"
        sign = "+" if change >= 0 else ""
        segments.append({
            "text": f"  {name}  ${price:,.2f}  {arrow}{sign}{change:.1f}%  ",
            "color": GREEN if change >= 0 else RED,
        })

    # Measure total width of all segments
    dummy_img = Image.new("RGB", (1, 1))
    dummy_draw = ImageDraw.Draw(dummy_img)
    segment_widths = []
    for seg in segments:
        bbox = dummy_draw.textbbox((0, 0), seg["text"], font=font)
        segment_widths.append(bbox[2] - bbox[0])

    separator = "  │  "
    sep_bbox = dummy_draw.textbbox((0, 0), separator, font=font)
    sep_w = sep_bbox[2] - sep_bbox[0]

    total_text_width = sum(segment_widths) + sep_w * len(segments)

    # Generate scrolling frames
    scroll_speed = total_text_width / num_frames

    for f in range(num_frames):
        img = Image.new("RGB", (HEADER_WIDTH, TICKER_HEIGHT), BG_DARK)
        draw = ImageDraw.Draw(img)

        # Top/bottom accent lines
        draw.line([(0, 0), (HEADER_WIDTH, 0)], fill=(56, 68, 77))  # #38444D
        draw.line([(0, TICKER_HEIGHT - 1), (HEADER_WIDTH, TICKER_HEIGHT - 1)], fill=(56, 68, 77))

        offset = -int(f * scroll_speed)
        y = (TICKER_HEIGHT - 14) // 2

        # Draw twice for seamless loop
        for repeat in range(3):
            x = offset + repeat * total_text_width
            for i, seg in enumerate(segments):
                if x > HEADER_WIDTH:
                    break
                if x + segment_widths[i] > 0:
                    # Name part in white, price/change in color
                    parts = seg["text"].split("$")
                    if len(parts) == 2:
                        name_part = parts[0]
                        price_part = "$" + parts[1]
                        name_bbox = draw.textbbox((0, 0), name_part, font=font)
                        name_w = name_bbox[2] - name_bbox[0]
                        draw.text((x, y), name_part, fill=WHITE, font=font)
                        draw.text((x + name_w, y), price_part, fill=seg["color"], font=font)
                    else:
                        draw.text((x, y), seg["text"], fill=seg["color"], font=font)

                x += segment_widths[i]

                # Separator
                if x + sep_w > 0 and x < HEADER_WIDTH:
                    draw.text((x, y), separator, fill=GRAY, font=font)
                x += sep_w

        frames.append(img)

    buf = io.BytesIO()
    frames[0].save(
        buf,
        format="GIF",
        save_all=True,
        append_images=frames[1:],
        duration=duration_per_frame,
        loop=0,
        optimize=True,
    )
    buf.seek(0)
    return buf.getvalue()


def upload_gif_to_gcs(gif_bytes: bytes, blob_name: str) -> str:
    """Upload GIF bytes to GCS and return public URL."""
    try:
        from google.cloud import storage
        bucket_name = os.getenv("GCS_BUCKET_NAME", "newsletter-ai-data")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(gif_bytes, content_type="image/gif")
        blob.make_public()
        logger.info(f"GIF uploaded: {blob.public_url}")
        return blob.public_url
    except Exception as e:
        logger.error(f"Failed to upload GIF to GCS: {e}")
        return ""


# Cache the header URL so we only generate once
_header_gif_url = None

def get_header_gif_url() -> str:
    """Get or create the permanent header GIF. Cached in GCS."""
    global _header_gif_url
    if _header_gif_url:
        return _header_gif_url

    blob_name = "assets/briefing_header_v1.gif"

    # Check if already exists in GCS
    try:
        from google.cloud import storage
        bucket_name = os.getenv("GCS_BUCKET_NAME", "newsletter-ai-data")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            blob.make_public()
            _header_gif_url = blob.public_url
            logger.info(f"Header GIF exists: {_header_gif_url}")
            return _header_gif_url
    except Exception:
        pass

    # Generate and upload
    gif_bytes = generate_header_gif()
    url = upload_gif_to_gcs(gif_bytes, blob_name)
    _header_gif_url = url
    return url


def get_ticker_gif_url(prices: list) -> str:
    """Generate and upload a ticker GIF with current prices."""
    if not prices:
        return ""
    gif_bytes = generate_ticker_gif(prices)
    if not gif_bytes:
        return ""
    from datetime import datetime
    date_str = datetime.now().strftime("%Y%m%d")
    blob_name = f"assets/ticker_{date_str}.gif"
    return upload_gif_to_gcs(gif_bytes, blob_name)
