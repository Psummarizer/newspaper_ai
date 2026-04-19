---
name: deploy-newsletter
description: Deploy newspaper_ai to Google Cloud Run via Cloud Build (no Docker needed). Use when the user says "deploy", "despliega", "sube a producción" or similar.
---

Deploy the newsletter-bot service to Cloud Run using Cloud Build (no local Docker required).

Project config:
- PROJECT_ID: pod-summarizer-ai-agent
- REGION: europe-west1
- SERVICE: newsletter-bot
- IMAGE: europe-west1-docker.pkg.dev/pod-summarizer-ai-agent/newsletter-repo/newsletter-bot:latest
- Working dir: d:/proyectos/Briefing_news/newspaper_ai

**IMPORTANT: Always ask for user confirmation before executing Step 2 (deploy). The build can run automatically, but deployment to production requires explicit approval.**

Steps to execute in order:

1. **Build & push** via Cloud Build (runs in GCP, no local Docker needed):
```bash
cd "d:/proyectos/Briefing_news/newspaper_ai" && gcloud builds submit \
  --tag "europe-west1-docker.pkg.dev/pod-summarizer-ai-agent/newsletter-repo/newsletter-bot:latest" \
  --project=pod-summarizer-ai-agent \
  --region=europe-west1
```

2. **Deploy** to Cloud Run:
```bash
gcloud run deploy newsletter-bot \
  --image "europe-west1-docker.pkg.dev/pod-summarizer-ai-agent/newsletter-repo/newsletter-bot:latest" \
  --platform managed \
  --region europe-west1 \
  --memory 8Gi \
  --cpu 4 \
  --timeout 3600 \
  --service-account "838077446910-compute@developer.gserviceaccount.com" \
  --allow-unauthenticated \
  --project=pod-summarizer-ai-agent
```

3. Confirm the revision is serving 100% traffic and report the service URL.

After deploy, offer to test with:
- `/send-test` → test email to psummarizer@gmail.com
- `/send-newsletter` → send to all active subscribers
