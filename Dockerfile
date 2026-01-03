# Usar imagen base ligera de Python
FROM python:3.11-slim

# Evitar que Python genere archivos .pyc y activar logs directos
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar archivo de entorno
COPY .env .

# Copiar el código fuente con estructura
COPY src/ src/
COPY scripts/ scripts/
COPY data/ data/
# Copiar main.py a la raíz o asegurar path
# En este caso, main.py está en src/main.py, pero el WORKDIR es /app
# Docker espera ejecutar desde ahí.

# Exponer puerto (necesario para Cloud Run aunque sea un job, por health checks)
EXPOSE 8080

# Comando de inicio: Ejecutar el script principal
CMD ["python", "src/main.py"]
