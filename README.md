## Time-Off Tracking (Deel -> Okta)

Cloud Function to sync an Okta group with users on long-term leave from Deel.

### Function Entry Point
- `time_off_tracking` in `cloud_function/main.py`

### Secret Manager (required)
Create secrets in project `it-automation-training`:
- `DEEL_API_TOKEN_SECRET` -> stores Deel API token
- `OKTA_API_TOKEN_SECRET` -> stores Okta SSWS token
- `OKTA_GROUP_ID_SECRET` -> stores Okta group ID

### Environment Variables (set on deploy)
- `DEEL_API_BASE_URL` (optional; default `https://api.letsdeel.com/rest/v2`)
- `DEEL_PAGE_SIZE` (optional; default `100`)
- `DEEL_START_DATE` (optional; default uses today at 00:00Z)
- `OKTA_ORG_URL` (required; e.g. `https://deel.okta.com`)
- `DEEL_API_TOKEN_SECRET` (required; secret name)
- `OKTA_API_TOKEN_SECRET` (required; secret name)
- `OKTA_GROUP_ID_SECRET` (required; secret name)
- `LONG_TERM_MIN_DAYS` (optional; default `30`)
- `LONG_TERM_MIN_AMOUNT` (optional; default `30`)

### Deploy (Gen 2)
```bash
gcloud functions deploy time-off-tracking \
  --gen2 --runtime=python311 --region=europe-west1 \
  --entry-point=time_off_tracking --trigger-http \
  --allow-unauthenticated \
  --source=cloud_function \
  --set-env-vars=OKTA_ORG_URL=https://deel.okta.com,DEEL_API_TOKEN_SECRET=deel-api-token,OKTA_API_TOKEN_SECRET=okta-api-token,OKTA_GROUP_ID_SECRET=okta-group-id,LONG_TERM_MIN_DAYS=30,LONG_TERM_MIN_AMOUNT=30
```

### Cloud Scheduler (daily trigger)
```bash
gcloud scheduler jobs create http time-off-tracking-daily \
  --schedule="0 9 * * *" \
  --time-zone="UTC" \
  --http-method=GET \
  --uri="https://europe-west1-it-automation-training.cloudfunctions.net/time-off-tracking" \
  --oidc-service-account-email="YOUR_SA@it-automation-training.iam.gserviceaccount.com"
```
