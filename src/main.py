```python
import os
import uuid
import datetime
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# 1st Gen functions use the 'request' parameter for HTTP triggers
def fetch_stripe_charges(request):
    """
    GCP 1st Gen Cloud Function to fetch Stripe charges from the last 24h
    and upsert them into BigQuery.
    """
    
    # --- CONFIGURATION ---
    # It is better practice to use Environment Variables for these
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
    DATASET_ID = "stripe_dataset"
    TABLE_ID = "stripe_charges"
    SECRET_ID = "STRIPE_API_KEY"
    
    try:
        # 1. INITIALIZE CLIENTS
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=PROJECT_ID)

        # 2. SECRET RETRIEVAL
        secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_path})
        stripe.api_key = response.payload.data.decode("UTF-8")

        # 3. TIME WINDOW (Last 24 Hours)
        now = datetime.datetime.now(datetime.timezone.utc)
        start_ts = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_uuid = str(uuid.uuid4())
        
        # 4. FETCH DATA FROM STRIPE
        # Using auto_paging_iter to handle more than 100 records automatically
        charges = stripe.Charge.list(created={"gte": start_ts}, limit=100)
        
        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            
            # Construct the record with the requested metadata
            charges_data.append({
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict, # The full JSON payload
                "metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now, # BigQuery client converts this to TIMESTAMP
                    "batch_uuid": batch_uuid
                }
            })

        if not charges_data:
            return "No charges found in the last 24 hours.", 200

        # 5. BIGQUERY INFRASTRUCTURE
        master_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        staging_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}_staging"

        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("stripe_created", "INTEGER"),
            bigquery.SchemaField("data", "JSON"),
            bigquery.SchemaField("metadata", "RECORD", fields=[
                bigquery.SchemaField("source_name", "STRING"),
                bigquery.SchemaField("processing_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("batch_uuid", "STRING"),
            ]),
        ]

        # 6. LOAD TO STAGING & MERGE (Upsert)
        # WRITE_TRUNCATE ensures the staging table is clean for this batch
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        
        # Load batch into staging
        load_job = bq_client.load_table_from_json(charges_data, staging_table, job_config=job_config)
        load_job.result() 

        # Merge SQL to prevent duplicate records if the function runs multiple times
        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN 
            UPDATE SET T.data = S.data, T.metadata = S.metadata
        WHEN NOT MATCHED THEN 
            INSERT (id, stripe_created, data, metadata)
            VALUES (S.id, S.stripe_created, S.data, S.metadata)
        """
        
        query_job = bq_client.query(merge_sql)
        query_job.result()

        return f"Successfully processed {len(charges_data)} records.", 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Internal Server Error: {str(e)}", 500
```
