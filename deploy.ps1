# Script de Despliegue para Newsletter AI
# Uso: ./deploy.ps1

# --- CONFIGURACION ---
$PROJECT_ID = "pod-summarizer-ai-agent"
$PROJECT_NUM = "838077446910"
$REGION = "europe-west1"
$SERVICE_NAME = "newsletter-bot"
$IMAGE_REPO_NAME = "newsletter-repo"
$IMAGE_TAG = "$REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_REPO_NAME/$SERVICE_NAME"

# --- 1. ACTIVAR APIs NECESARIAS ---
Write-Host "[INFO] Activando APIs (run, scheduler, artifacts)..."
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com --project=$PROJECT_ID

# --- 2. CONFIGURAR ARTIFACT REGISTRY ---
Write-Host "[INFO] Configurando Artifact Registry..."
# Crear repo si no existe
gcloud artifacts repositories describe $IMAGE_REPO_NAME --location=$REGION --project=$PROJECT_ID 2>$null
if ($LASTEXITCODE -ne 0) {
    gcloud artifacts repositories create $IMAGE_REPO_NAME --repository-format=docker --location=$REGION --description="Newsletter AI Images" --project=$PROJECT_ID
}
gcloud auth configure-docker $REGION-docker.pkg.dev --quiet

# --- 3. BUILD & PUSH ---
Write-Host "[INFO] Construyendo imagen Docker..."
# NOTA: Ahora copiamos el .env dentro de la imagen (Usuario confirmo que quiere esto para evitar costes de Secret Manager)
docker build -t "${IMAGE_TAG}:latest" .
if ($LASTEXITCODE -ne 0) { Write-Error "Fallo en docker build"; exit 1 }

Write-Host "[INFO] Subiendo imagen..."
docker push "${IMAGE_TAG}:latest"
if ($LASTEXITCODE -ne 0) { Write-Error "Fallo en docker push"; exit 1 }

# --- 4. DEPLOY CLOUD RUN ---
Write-Host "[INFO] Desplegando en Cloud Run..."
# Se han eliminado las banderas --secrets y --set-env-vars porque el usuario usa un .env embebido en la imagen.
gcloud run deploy $SERVICE_NAME `
    --image "${IMAGE_TAG}:latest" `
    --platform managed `
    --region $REGION `
    --memory 8Gi `
    --cpu 4 `
    --timeout 3600 `
    --service-account "$PROJECT_NUM-compute@developer.gserviceaccount.com" `
    --allow-unauthenticated `
    --project=$PROJECT_ID

if ($LASTEXITCODE -ne 0) { Write-Error "Fallo en deploy Cloud Run"; exit 1 }

# --- 5. CLOUD SCHEDULER ---
Write-Host "[INFO] Configurando Schedulers..."
$SERVICE_URL = gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format="value(status.url)" --project=$PROJECT_ID

if (-not $SERVICE_URL) {
    Write-Error "No se pudo obtener la URL del servicio. Revisa el despliegue."
    exit 1
}

# 5.1 Ingesta cada 12 horas (8:00 AM y 8:00 PM Madrid)
Write-Host "   -> Configurando 'newsletter-ingest-job' (8AM y 8PM)..."
gcloud scheduler jobs create http newsletter-ingest-job `
    --schedule="30 5,20 * * *" `
    --uri="$SERVICE_URL/ingest" `
    --http-method=POST `
    --location=$REGION `
    --project=$PROJECT_ID `
    --time-zone="Europe/Madrid" `
    --quiet 2>$null

if ($LASTEXITCODE -ne 0) {
    gcloud scheduler jobs update http newsletter-ingest-job `
        --schedule="30 5,20 * * *" `
        --uri="$SERVICE_URL/ingest" `
        --http-method=POST `
        --location=$REGION `
        --project=$PROJECT_ID `
        --time-zone="Europe/Madrid"
}

# 5.2 Generación Diaria (Daily 8:30 AM Madrid)
Write-Host "   -> Configurando 'newsletter-send-job' (Daily 8:30 AM)..."
gcloud scheduler jobs create http newsletter-send-job `
    --schedule="45 6 * * *" `
    --uri="$SERVICE_URL/send-newsletter" `
    --http-method=POST `
    --location=$REGION `
    --project=$PROJECT_ID `
    --time-zone="Europe/Madrid" `
    --quiet 2>$null

if ($LASTEXITCODE -ne 0) {
    gcloud scheduler jobs update http newsletter-send-job `
        --schedule="45 6 * * *" `
        --uri="$SERVICE_URL/send-newsletter" `
        --http-method=POST `
        --location=$REGION `
        --project=$PROJECT_ID `
        --time-zone="Europe/Madrid"
}

Write-Host "[OK] DESPLIEGUE FINALIZADO. URL: $SERVICE_URL"
Write-Host "Jobs Activos: newsletter-ingest-job (8AM y 8PM), newsletter-send-job (08:30 AM)"
