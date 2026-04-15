```bash
gcloud functions deploy stripe-fetch-charges \
  --runtime python310 \
  --trigger-http \
  --entry-point fetch_stripe_charges \
  --region australia-southeast1 \
  --memory 512MB \
  --allow-unauthenticated \
  --set-env-vars GCP_PROJECT_ID=your-project-id
```
