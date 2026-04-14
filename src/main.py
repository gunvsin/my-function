import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery

# We define the schema here so BigQuery knows the structure
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

@functions_framework.http
def fetch_stripe_charges(request):
    try:
        # 1. Get Project ID from environment
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        
        # 2. Initialize Clients INSIDE the function to prevent startup crashes
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 3. Access Secret
        secret_path = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_path})
        stripe.api_key = response.payload.data.decode("UTF-8")

        # 4. Fetch Stripe Data
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
            return "No new charges found.", 200

        # 5. Table Configuration
        dataset_id = "stripe_dataset"
        table_id = "stripe_charges"
        full_table_path = f"{project_id}.{dataset_id}.{table_id}"
        staging_table_path = f"{project_id}.{dataset_id}.{table_id}_staging"

        # 6. Load to Staging (This auto-creates the table if it doesn't exist)
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition="WRITE_TRUNCATE",
        )
        
        load_job = bq_client.load_table_from_json(charges_list, staging_table_path, job_config=job_config)
        load_job.result()

        # 7. Ensure Master Table exists (Merge fails if Master is missing)
        try:
            bq_client.get_table(full_table_path)
        except:
            # Create master table with same schema if missing
            table = bigquery.Table(full_table_path, schema=SCHEMA)
            bq_client.create_table(table)

        # 8. MERGE Logic
        merge_query = f"""
        MERGE `{full_table_path}` T
        USING `{staging_table_path}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET T.data = S.data, T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN
          INSERT (id, stripe_created, data, ingestion_metadata)
          VALUES (id, stripe_created, data, ingestion_metadata)
        """
        bq_client.query(merge_query).result()

        return f"Success: Synced {len(charges_list)} charges.", 200

    except Exception as e:
        # This print statement will now show up in Logs Explorer
        print(f"Error in execution: {str(e)}")
        return f"Error: {str(e)}", 500
