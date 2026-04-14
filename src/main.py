import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# DO NOT fetch secrets here at the top level.
# Only define constants.
DATASET_ID = "stripe_dataset"
TABLE_ID = "stripe_charges"

@functions_framework.http
def fetch_stripe_charges(request):
    """Entry point: logic is now fully encapsulated to prevent startup crashes."""
    try:
        # 1. Fetch Project ID
        # In Cloud Functions, this is often provided as 'GOOGLE_CLOUD_PROJECT'
        project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        
        # 2. Initialize Clients inside the handler
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 3. Fetch Secret
        secret_name = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_name})
        stripe.api_key = response.payload.data.decode("UTF-8")

        # 4. Stripe Logic
        now = datetime.datetime.now(datetime.UTC)
        start_time = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_list = []
        charges = stripe.Charge.list(created={"gte": start_time}, limit=100)

        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            charges_list.append({
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict,
                "ingestion_metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": str(uuid.uuid4())
                }
            })

        if not charges_list:
            return "No charges found.", 200

        # 5. BigQuery Operations
        master_table = f"{project_id}.{DATASET_ID}.{TABLE_ID}"
        staging_table = f"{project_id}.{DATASET_ID}.{TABLE_ID}_staging"

        # Explicit Schema (Prevents "RECORD has no schema" error)
        SCHEMA = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("stripe_created", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("data", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("ingestion_metadata", "RECORD", mode="NULLABLE", fields=[
                bigquery.SchemaField("source_name", "STRING"),
                bigquery.SchemaField("processing_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("batch_uuid", "STRING"),
            ]),
        ]

        job_config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE")
        load_job = bq_client.load_table_from_json(charges_list, staging_table, job_config=job_config)
        load_job.result()

        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET T.data = S.data, T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN INSERT ROW
        """
        bq_client.query(merge_sql).result()

        return f"Successfully synced {len(charges_list)} charges.", 200

    except Exception as e:
        # This will now appear in your logs instead of crashing the container
        print(f"RUNTIME ERROR: {str(e)}")
        return f"Error: {str(e)}", 500
