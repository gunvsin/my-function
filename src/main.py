``python
import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT")
SECRET_ID = "STRIPE_API_KEY"
DATASET_ID = "stripe_dataset"
TABLE_ID = "stripe_charges"
STAGING_TABLE_ID = f"{TABLE_ID}_staging"

# Define the BigQuery Schema explicitly to avoid "RECORD has no schema" errors
SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("stripe_created", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("data", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_metadata", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("source_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processing_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("batch_uuid", "STRING", mode="NULLABLE"),
    ]),
]

def get_stripe_api_key():
    """Fetches the secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@functions_framework.http
def fetch_stripe_charges(request):
    """HTTP Cloud Function to fetch Stripe charges and upsert into BigQuery."""
    try:
        # 1. Setup Clients
        stripe.api_key = get_stripe_api_key()
        bq_client = bigquery.Client()

        # 2. Fetch Charges from last 24 hours
        now = datetime.datetime.now(datetime.UTC)
        start_timestamp = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_list = []
        # auto_paging_iter handles Stripe's pagination automatically
        charges = stripe.Charge.list(created={"gte": start_timestamp}, limit=100)

        batch_uuid = str(uuid.uuid4())

        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            
            row = {
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict, # This goes into the JSON column
                "ingestion_metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": batch_uuid
                }
            }
            charges_list.append(row)

        if not charges_list:
            return "No charges found in the last 24 hours.", 200

        # 3. Define Table References
        master_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        staging_table_path = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}"

        # 4. Load Data to Staging Table
        # WRITE_TRUNCATE ensures the staging table is cleaned every time we run
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition="WRITE_TRUNCATE",
        )
        
        load_job = bq_client.load_table_from_json(
            charges_list, 
            staging_table_path, 
            job_config=job_config
        )
        load_job.result() # Wait for upload to finish

        # 5. MERGE Staging into Master (Deduplication Logic)
        # This updates the row if the ID exists, or inserts it if it's new
        merge_query = f"""
        MERGE `{master_table_path}` T
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

        return f"Processed {len(charges_list)} charges successfully.", 200

    except Exception as e:
        print(f"Error encountered: {str(e)}")
        return f"Internal Error: {str(e)}", 500
```

---
