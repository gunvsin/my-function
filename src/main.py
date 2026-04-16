import os
import json
import stripe
import datetime
import functions_framework
from google.cloud import bigquery
from google.cloud import secretmanager

# --- GLOBAL INITIALIZATION (Reused across function invocations) ---
# This reduces latency and prevents "Cold Start" overhead
sm_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

# Global variables to cache secrets in memory
STRIPE_KEYS = {"api_key": None, "webhook_secret": None}

def get_secrets(project_id):
    """Retrieves secrets once per container lifecycle to optimize performance."""
    if not STRIPE_KEYS["api_key"] or not STRIPE_KEYS["webhook_secret"]:
        print("Fetching secrets from Secret Manager...")
        
        # 1. Fetch Stripe API Key
        name_api = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        res_api = sm_client.access_secret_version(request={"name": name_api})
        STRIPE_KEYS["api_key"] = res_api.payload.data.decode("UTF-8")
        
        # 2. Fetch Webhook Signing Secret
        name_wh = f"projects/{project_id}/secrets/STRIPE_WEBHOOK_SECRET/versions/latest"
        res_wh = sm_client.access_secret_version(request={"name": name_wh})
        STRIPE_KEYS["webhook_secret"] = res_wh.payload.data.decode("UTF-8")
        
    return STRIPE_KEYS["api_key"], STRIPE_KEYS["webhook_secret"]

@functions_framework.http
def stripe_webhook_handler(request):
    """
    Master Event Handler: Receives any Stripe Event and streams it to BigQuery.
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = "stripe_dataset"
    table_id = "stripe_events"  # The Master Table
    
    # 1. Initialize Stripe
    api_key, webhook_secret = get_secrets(project_id)
    stripe.api_key = api_key

    # 2. Verify Webhook Signature
    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except ValueError as e:
        print(f"Invalid payload: {e}")
        return "Invalid payload", 400
    except stripe.error.SignatureVerificationError as e:
        print(f"Invalid signature: {e}")
        return "Invalid signature", 400

    # 3. Extract Core Data
    # Note: 'event' is the wrapper. 'event.data.object' is the actual Charge, Customer, etc.
    event_type = event['type']
    stripe_object = event['data']['object']
    
    # 4. Construct Data Engineering Payload
    # Using the high-precision UTC timestamp and source metadata requested
    row_to_insert = {
        "id": event['id'],               # Unique Event ID from Stripe
        "object_id": stripe_object.get('id'), # The ID of the charge/customer
        "event_type": event_type,
        "stripe_created": event['created'],
        "data": stripe_object,           # Full JSON object
        "metadata": {
            "source_name": "Stripe_Webhook_Master",
            "processing_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "batch_uuid": event.get('request', {}).get('id', 'manual_or_system')
        }
    }

    # 5. Streaming Insert to BigQuery
    # Webhooks must respond fast (<10s). bq_client.insert_rows_json is the fastest method.
    full_table_path = f"{project_id}.{dataset_id}.{table_id}"
    
    errors = bq_client.insert_rows_json(full_table_path, [row_to_insert])

    if errors:
        print(f"BigQuery Streaming Error: {errors}")
        # We return 500 so Stripe knows to retry the webhook later
        return f"BigQuery Error: {errors}", 500

    print(f"Successfully ingested event {event['id']} ({event_type})")
    return "Event Received", 200
