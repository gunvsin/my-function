```python
import os
import json
import stripe
from google.cloud import bigquery
from google.cloud import secretmanager

# Initialize Clients
sm_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

def stripe_webhook_handler(request):
    """Cloud Function to receive Stripe Webhook events."""
    
    # 1. Retrieve Secrets from Secret Manager
    project_id = os.environ.get("GCP_PROJECT_ID")
    
    # We need TWO secrets now: your API Key and your Webhook Signing Secret
    api_key_path = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
    wh_secret_path = f"projects/{project_id}/secrets/STRIPE_WEBHOOK_SECRET/versions/latest"
    
    stripe.api_key = sm_client.access_secret_version(request={"name": api_key_path}).payload.data.decode("UTF-8")
    endpoint_secret = sm_client.access_secret_version(request={"name": wh_secret_path}).payload.data.decode("UTF-8")

    # 2. Verify the Webhook Signature
    payload = request.get_data(as_text=False)
    sig_header = request.headers.get('Stripe-Signature')

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, endpoint_secret)
    except ValueError as e:
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError as e:
        return "Invalid signature", 400

    # 3. Handle the event type
    # We only care about 'charge.succeeded' based on your previous requirements
    if event['type'] == 'charge.succeeded':
        charge = event['data']['object']
        
        # Prepare data for BigQuery
        rows_to_insert = [{
            "id": charge['id'],
            "stripe_created": charge['created'],
            "data": charge,
            "metadata": {
                "source_name": "Stripe_Webhook",
                "processing_timestamp": "auto", # BigQuery can handle current_timestamp
                "batch_uuid": event['id']
            }
        }]
        
        # Insert into BigQuery (Streaming Insert)
        table_id = f"{project_id}.stripe_dataset.stripe_table"
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            print(f"BigQuery Insert Errors: {errors}")
            return "BigQuery Error", 500

    return "Success", 200
```
