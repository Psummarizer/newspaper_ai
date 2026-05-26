# Script de Despliegue para Newsletter AI
# Uso: ./deploy.ps1
#
# Arquitectura:
#   - Cloud Run SERVICE (`newsletter-bot`)  → endpoints HTTP de testing manual.
#   - Cloud Run JOB    (`newsletter-ingest-job`) → ingesta RSS programada (twice/day).
#   - Cloud Run JOB    (`newsletter-send-job`)   → envío diario de briefings.
#
# Razón: los batch (ingesta 25-30 min, envío 35-45 min) exceden el
# attemptDeadline máximo de Cloud Scheduler→HTTP (180s). Cloud Run Jobs
# permite task-timeout hasta 24h y se invoca directamente desde Scheduler
# sin depender de un endpoint HTTP que mantenga la request abierta.

# --- CONFIGURACION ---
$PROJECT_ID = "pod-summarizer-ai-agent"
$PROJECT_NUM = "838077446910"
$REGION = "europe-west1"
$SERVICE_NAME = "newsletter-bot"
$INGEST_JOB_NAME = "newsletter-ingest-job"
$SEND_JOB_NAME   = "newsletter-send-job"
$IMAGE_REPO_NAME = "newsletter-repo"
$IMAGE_TAG = "$REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_REPO_NAME/$SERVICE_NAME"
$SERVICE_ACCOUNT = "$PROJECT_NUM-compute@developer.gserviceaccount.com"

# --- 1. ACTIVAR APIs NECESARIAS ---
Write-Host "[INFO] Activando APIs (run, scheduler, artifacts, cloudbuild)..."
gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudscheduler.googleapis.com cloudbuild.googleapis.com --project=$PROJECT_ID

# --- 2. CONFIGURAR ARTIFACT REGISTRY ---
Write-Host "[INFO] Configurando Artifact Registry..."
gcloud artifacts repositories describe $IMAGE_REPO_NAME --location=$REGION --project=$PROJECT_ID 2>$null
if ($LASTEXITCODE -ne 0) {
    gcloud artifacts repositories create $IMAGE_REPO_NAME --repository-format=docker --location=$REGION --description="Newsletter AI Images" --project=$PROJECT_ID
}
gcloud auth configure-docker $REGION-docker.pkg.dev --quiet

# --- 3. BUILD & PUSH (Cloud Build) ---
Write-Host "[INFO] Construyendo y subiendo imagen via Cloud Build..."
gcloud builds submit --tag "${IMAGE_TAG}:latest" --project=$PROJECT_ID --region=$REGION
if ($LASTEXITCODE -ne 0) { Write-Error "Fallo en Cloud Build (build+push)"; exit 1 }

# --- 4a. DEPLOY CLOUD RUN SERVICE (endpoints HTTP de testing) ---
Write-Host "[INFO] Desplegando Cloud Run Service (testing endpoints)..."
gcloud run deploy $SERVICE_NAME `
    --image "${IMAGE_TAG}:latest" `
    --platform managed `
    --region $REGION `
    --memory 8Gi `
    --cpu 4 `
    --timeout 3600 `
    --service-account $SERVICE_ACCOUNT `
    --allow-unauthenticated `
    --project=$PROJECT_ID
if ($LASTEXITCODE -ne 0) { Write-Error "Fallo en deploy Cloud Run Service"; exit 1 }

# --- 4b. DEPLOY CLOUD RUN JOBS (batch ingesta + envío) ---
# Misma imagen, distinto JOB_MODE en variable de entorno.
# task-timeout=3600s cubre con holgura los 25-45 min reales de cada batch.

function Deploy-CloudRunJob {
    param(
        [string]$JobName,
        [string]$JobMode
    )
    Write-Host "[INFO] Configurando Cloud Run Job '$JobName' (JOB_MODE=$JobMode)..."
    gcloud run jobs describe $JobName --region=$REGION --project=$PROJECT_ID 2>$null
    if ($LASTEXITCODE -eq 0) {
        gcloud run jobs update $JobName `
            --image "${IMAGE_TAG}:latest" `
            --region $REGION `
            --memory 8Gi `
            --cpu 4 `
            --task-timeout 3600s `
            --max-retries 1 `
            --service-account $SERVICE_ACCOUNT `
            --set-env-vars "JOB_MODE=$JobMode" `
            --project=$PROJECT_ID
    } else {
        gcloud run jobs create $JobName `
            --image "${IMAGE_TAG}:latest" `
            --region $REGION `
            --memory 8Gi `
            --cpu 4 `
            --task-timeout 3600s `
            --max-retries 1 `
            --service-account $SERVICE_ACCOUNT `
            --set-env-vars "JOB_MODE=$JobMode" `
            --project=$PROJECT_ID
    }
    if ($LASTEXITCODE -ne 0) { Write-Error "Fallo al desplegar Cloud Run Job '$JobName'"; exit 1 }
}

Deploy-CloudRunJob -JobName $INGEST_JOB_NAME -JobMode "ingest"
Deploy-CloudRunJob -JobName $SEND_JOB_NAME   -JobMode "send"

# --- 5. CLOUD SCHEDULER → Cloud Run Jobs ---
# Los schedulers ya NO apuntan a endpoints HTTP del Service. Ahora invocan
# Cloud Run Jobs directamente vía su API, que tolera ejecuciones largas.
Write-Host "[INFO] Configurando Cloud Scheduler para invocar Jobs..."

$JOB_API_BASE = "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs"

function Set-SchedulerJob {
    param(
        [string]$SchedulerName,
        [string]$Schedule,
        [string]$JobName,
        [string]$Description
    )
    $uri = "$JOB_API_BASE/${JobName}:run"
    Write-Host "   -> $Description ('$Schedule')..."
    gcloud scheduler jobs describe $SchedulerName --location=$REGION --project=$PROJECT_ID 2>$null
    if ($LASTEXITCODE -eq 0) {
        gcloud scheduler jobs update http $SchedulerName `
            --schedule=$Schedule `
            --uri=$uri `
            --http-method=POST `
            --oauth-service-account-email=$SERVICE_ACCOUNT `
            --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" `
            --attempt-deadline=180s `
            --location=$REGION `
            --project=$PROJECT_ID `
            --time-zone="Europe/Madrid"
    } else {
        gcloud scheduler jobs create http $SchedulerName `
            --schedule=$Schedule `
            --uri=$uri `
            --http-method=POST `
            --oauth-service-account-email=$SERVICE_ACCOUNT `
            --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" `
            --attempt-deadline=180s `
            --location=$REGION `
            --project=$PROJECT_ID `
            --time-zone="Europe/Madrid"
    }
    if ($LASTEXITCODE -ne 0) { Write-Error "Fallo configurando scheduler '$SchedulerName'"; exit 1 }
}

# Nota sobre attempt-deadline=180s: el scheduler solo dispara el Job (POST a
# Run API que devuelve 200 inmediatamente con el execution_name). El Job
# corre por su cuenta hasta 24h sin afectar al scheduler. 180s es suficiente
# margen para la llamada de disparo.

# 5.1 Ingesta cada 12 horas (6:30 AM y 8:30 PM Madrid)
Set-SchedulerJob `
    -SchedulerName "newsletter-ingest-job" `
    -Schedule "30 6,20 * * *" `
    -JobName $INGEST_JOB_NAME `
    -Description "Ingesta horaria (6:30 AM y 8:30 PM Madrid)"

# 5.2 Envío diario (7:15 AM Madrid)
Set-SchedulerJob `
    -SchedulerName "newsletter-send-job" `
    -Schedule "15 7 * * *" `
    -JobName $SEND_JOB_NAME `
    -Description "Envío diario (7:15 AM Madrid)"

# --- 6. RESUMEN ---
$SERVICE_URL = gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format="value(status.url)" --project=$PROJECT_ID
Write-Host ""
Write-Host "[OK] DESPLIEGUE FINALIZADO"
Write-Host "  Service URL (testing): $SERVICE_URL"
Write-Host "  Jobs activos:"
Write-Host "    - $INGEST_JOB_NAME (modo=ingest, schedule=30 6,20 Madrid)"
Write-Host "    - $SEND_JOB_NAME   (modo=send,   schedule=15 7 Madrid)"
Write-Host ""
Write-Host "Ejecutar Job manualmente:"
Write-Host "  gcloud run jobs execute $SEND_JOB_NAME --region $REGION --project $PROJECT_ID"
