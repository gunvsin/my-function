
```python
import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# Retrieve Project ID from environment
PROJECT_ID = os.environ.get("GCP_PROJECT")

def get_stripe_api_key():
    client = secretmanager.SecretManagerServiceClient()
    # Ensure the secret name "STRIPE_API_KEY" matches what you created in Secret Manager
    name = f"projects/{PROJECT_ID}/secrets/STRIPE_API_KEY/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@functions_framework.http
def fetch_stripe_charges(request):
    """
    HTTP Cloud Function entry point.
    """
    try:
        # Initialize clients
        stripe.api_key = get_stripe_api_key()
        bq_client = bigquery.Client()

        # Calculate 24h window
        now = datetime.datetime.now(datetime.UTC)
        start_time = int((now - datetime.timedelta(hours=24)).timestamp())

        charges_to_insert = []
        
        # Stripe auto-pagination
        charges = stripe.Charge.list(created={"gte": start_time}, limit=100)

        for charge in charges.auto_paging_iter():
            row = charge.to_dict_recursive()
            
            # Add the required metadata field
            # We use 'ingestion_metadata' to avoid overwriting Stripe's own 'metadata'
            row['ingestion_metadata'] = {
                'source_name': 'Stripe_Primary',
                'utc_timestamp': datetime.datetime.now(datetime.UTC).isoformat(),
                'uuid': str(uuid.uuid4())
            }
            charges_to_insert.append(row)

        if charges_to_insert:
            # Table ID format: project_id.dataset_id.table_id
            table_id = f"{PROJECT_ID}.stripe_dataset.stripe_table" 
            errors = bq_client.insert_rows_json(table_id, charges_to_insert)
            
            if errors:
                return f"BigQuery Insert Errors: {errors}", 500
            return f"Success: Inserted {len(charges_to_insert)} records.", 200
        
        return "No new charges found.", 200

    except Exception as e:
        print(f"Error: {e}")
        return str(e), 500
