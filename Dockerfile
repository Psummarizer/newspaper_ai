# Usar imagen base ligera de Python
FROM python:3.11-slim

# Evitar que Python genere archivos .pyc y activar logs directos
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Madrid

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias Python primero (mejor cache de Docker layers)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Chromium, Driver, FFmpeg y tzdata (necesario para que TZ=Europe/Madrid funcione)
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    ffmpeg \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Configurar variables de entorno para Selenium
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Copiar archivo de entorno
COPY .env .

# Copiar el código fuente con estructura
COPY src/ src/
COPY scripts/ scripts/
COPY data/ data/
COPY assets/ assets/

# Exponer puerto (solo usado en modo Service)
EXPOSE 8080

# Entrypoint dual: ver src/entrypoint.py. El modo lo decide JOB_MODE:
#   - sin JOB_MODE / JOB_MODE=service → arranca FastAPI/uvicorn (Cloud Run Service)
#   - JOB_MODE=ingest                 → ejecuta pipeline de ingesta (Cloud Run Job)
#   - JOB_MODE=send                   → ejecuta generación de briefings (Cloud Run Job)
CMD ["python", "-m", "src.entrypoint"]
