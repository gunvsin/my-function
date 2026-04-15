import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# Initialize clients OUTSIDE the function (Global Scope)
# This allows the container to reuse connections across multiple calls
sm_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client()

@functions_framework.http
def fetch_stripe_charges(request):
    """
    HTTP Cloud Function (2nd Gen / Cloud Run)
    """
    # 1. Configuration from Environment Variables
    # (Set these in the Google Cloud Console)
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    DATASET_ID = "stripe_dataset"
    TABLE_ID = "stripe_charges"
    SECRET_ID = "STRIPE_API_KEY"

    if not PROJECT_ID:
        return "Internal Error: GCP_PROJECT_ID environment variable not set.", 500

    try:
        # 2. Secret Retrieval
        secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_path})
        stripe.api_key = response.payload.data.decode("UTF-8")

        # 3. Time Window (Last 24 Hours)
        now = datetime.datetime.now(datetime.timezone.utc)
        start_ts = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_uuid = str(uuid.uuid4())
        
        # 4. Fetch Stripe Data
        charges = stripe.Charge.list(created={"gte": start_ts}, limit=100)
        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            charges_data.append({
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict,
                "metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": batch_uuid
                }
            })

        if not charges_data:
            return "Incremental Sync: 0 records found.", 200

        # 5. BigQuery Operations
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

        # Load into Staging
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        load_job = bq_client.load_table_from_json(charges_data, staging_table, job_config=job_config)
        load_job.result() # Wait for load to finish

        # Merge to Master
        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET T.data = S.data, T.metadata = S.metadata
        WHEN NOT MATCHED THEN INSERT (id, stripe_created, data, metadata)
        VALUES (S.id, S.stripe_created, S.data, S.metadata)
        """
        bq_client.query(merge_sql).result()

        return f"Processed {len(charges_data)} records.", 200

    except Exception as e:
        # Logs to Cloud Logging
        print(f"PIPELINE FAILURE: {str(e)}")
        return f"Error: {str(e)}", 500
