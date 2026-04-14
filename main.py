```python
import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# Config
PROJECT_ID = os.environ.get("GCP_PROJECT")
SECRET_ID = "STRIPE_API_KEY"
# We recommend a dedicated table for Charges for better query performance
DATASET_ID = "stripe_dataset"
TABLE_ID = "stripe_charges"
STAGING_TABLE_ID = f"{TABLE_ID}_staging"

def get_stripe_api_key():
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@functions_framework.http
def fetch_stripe_charges(request):
    try:
        stripe.api_key = get_stripe_api_key()
        bq_client = bigquery.Client()

        # 1. Fetch data from Stripe
        now = datetime.datetime.now(datetime.UTC)
        twenty_four_hours_ago = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_list = []
        charges = stripe.Charge.list(created={"gte": twenty_four_hours_ago}, limit=100)

        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            
            # Prepare row for BigQuery
            row = {
                "id": charge_dict['id'], # Stripe Charge ID (e.g., ch_123)
                "stripe_created": charge_dict['created'], # Unix timestamp
                "data": charge_dict, # The full snapshot as JSON
                "ingestion_metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": str(uuid.uuid4())
                }
            }
            charges_list.append(row)

        if not charges_list:
            return "No charges found.", 200

        # 2. Define Table Paths
        full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        staging_table_path = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"

        # 3. Load to Staging Table (Overwrite staging each time)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        load_job = bq_client.load_table_from_json(charges_list, staging_table_path, job_config=job_config)
        load_job.result() 

        # 4. Perform MERGE Statement (Deduplication Logic)
        # This updates the record if the ID exists, or inserts if it's new.
        merge_query = f"""
        MERGE `{full_table_path}` T
        USING `{staging_table_path}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET 
            T.data = S.data, 
            T.stripe_created = S.stripe_created,
            T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN
          INSERT (id, stripe_created, data, ingestion_metadata)
          VALUES (id, stripe_created, data, ingestion_metadata)
        """
        
        query_job = bq_client.query(merge_query)
        query_job.result()

        return f"Successfully processed {len(charges_list)} charges with MERGE.", 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}", 500
```
