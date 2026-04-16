import os
import json
import stripe
import datetime
import functions_framework
from google.cloud import bigquery
from google.cloud import secretmanager

# --- GLOBAL INITIALIZATION ---
# Initializing clients outside the handler allows for "Warm Start" reuse,
# significantly reducing latency and execution cost.
sm_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

# Local cache for secrets to prevent redundant API calls to Secret Manager
runtime_cache = {
    "stripe_api_key": None,
    "webhook_secret": None
}

def load_secrets(project_id):
    """Retrieves and caches secrets from Secret Manager."""
    if not runtime_cache["stripe_api_key"] or not runtime_cache["webhook_secret"]:
        print("Initial lookup: Fetching secrets from Secret Manager...")
        
        # Access Stripe API Key
        api_key_name = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        api_response = sm_client.access_secret_version(request={"name": api_key_name})
        runtime_cache["stripe_api_key"] = api_response.payload.data.decode("UTF-8")
        
        # Access Webhook Signing Secret
        wh_secret_name = f"projects/{project_id}/secrets/STRIPE_WEBHOOK_SECRET/versions/latest"
        wh_response = sm_client.access_secret_version(request={"name": wh_secret_name})
        runtime_cache["webhook_secret"] = wh_response.payload.data.decode("UTF-8")
        
    return runtime_cache["stripe_api_key"], runtime_cache["webhook_secret"]

@functions_framework.http
def stripe_webhook_handler(request):
    """
    Expert-level Webhook Handler for Stripe Master Events.
    Captures all event types into a single partitioned BigQuery table.
    """
    # 1. Environment Configuration
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = "stripe_dataset"
    table_id = "stripe_events"
    
    if not project_id:
        print("CRITICAL: GCP_PROJECT_ID environment variable is missing.")
        return "Internal Configuration Error", 500

    # 2. Secret Retrieval
    try:
        api_key, webhook_secret = load_secrets(project_id)
        stripe.api_key = api_key
    except Exception as e:
        print(f"Secret Manager Error: {str(e)}")
        return "Internal Security Error", 500

    # 3. Webhook Signature Verification
    # This ensures the request is legitimately from Stripe
    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except ValueError as e:
        # Invalid payload
        return "Invalid Payload", 400
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        print(f"Signature Verification Failed: {str(e)}")
        return "Invalid Signature", 400

    # 4. Data Extraction
    event_type = event['type']
    stripe_object = event['data']['object']
    
    # 5. Construct BigQuery Row
    # Note: processing_timestamp is TOP-LEVEL to allow BigQuery Partitioning.
    row_to_insert = {
        "id": event['id'],               # Unique ID for the Stripe Event
        "object_id": stripe_object.get('id'), # ID of the actual Charge/Customer/etc.
        "event_type": event_type,
        "stripe_created": event['created'],
        "data": stripe_object,           # Full JSON object for downstream parsing
        "processing_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "metadata": {
            "source_name": "Stripe_Webhook_Master",
            "batch_uuid": event.get('request', {}).get('id', 'N/A') # Stripe Request ID
        }
    }

    # 6. Streaming Insert to BigQuery
    # Using insert_rows_json for near real-time ingestion
    table_full_path = f"{project_id}.{dataset_id}.{table_id}"
    
    errors = bq_client.insert_rows_json(table_full_path, [row_to_insert])

    if errors:
        # Returning a non-200 code triggers Stripe's automatic retry logic
        print(f"BigQuery Insert Error for event {event['id']}: {errors}")
        return f"Data Ingestion Error: {errors}", 500

    print(f"Successfully ingested {event_type} event: {event['id']}")
    return "OK", 200
