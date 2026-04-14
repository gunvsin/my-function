import os
import functions_framework

# Schema is defined as a simple list; it doesn't trigger API calls, so it's safe at top level.
TABLE_SCHEMA = [
    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "stripe_created", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "data", "type": "JSON", "mode": "NULLABLE"},
    {"name": "ingestion_metadata", "type": "RECORD", "mode": "NULLABLE", "fields": [
        {"name": "source_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "batch_uuid", "type": "STRING", "mode": "NULLABLE"},
    ]},
]

@functions_framework.http
def fetch_stripe_charges(request):
    """
    Data Engineering Pipeline: Stripe -> BigQuery
    Pattern: Delayed Imports to prevent Port 8080 Timeout.
    """
    print("Startup: Container is healthy. Beginning execution...")

    # 1. DELAYED IMPORTS (The Fix for Revision 16)
    try:
        import uuid
        import datetime
        import stripe
        from google.cloud import secretmanager
        from google.cloud import bigquery
    except ImportError as e:
        print(f"CRITICAL: Library loading failed: {str(e)}")
        return f"Import Error: {str(e)}", 500

    try:
        # 2. Context Setup
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        dataset_id = "stripe_dataset"
        table_name = "stripe_charges"
        
        # 3. Client Initialization
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 4. API Key Retrieval
        secret_name = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        secret_payload = sm_client.access_secret_version(request={"name": secret_name})
        stripe.api_key = secret_payload.payload.data.decode("UTF-8")

        # 5. Incremental Fetch (Last 24 Hours)
        now = datetime.datetime.now(datetime.UTC)
        start_time = int((now - datetime.timedelta(hours=24)).timestamp())
        
        batch_uuid = str(uuid.uuid4())
        charges_data = []

        print(f"Syncing charges created since: {start_time}")
        charges = stripe.Charge.list(created={"gte": start_time}, limit=100)

        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            charges_data.append({
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict,
                "ingestion_metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": batch_uuid
                }
            })

        if not charges_data:
            return "Incremental sync completed: 0 new records.", 200

        # 6. BigQuery Upsert (Merge Strategy)
        master_table = f"{project_id}.{dataset_id}.{table_name}"
        staging_table = f"{project_id}.{dataset_id}.{table_name}_staging"

        # Load to staging
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField.from_api_repr(f) for f in TABLE_SCHEMA],
            write_disposition="WRITE_TRUNCATE",
        )
        
        load_job = bq_client.load_table_from_json(charges_data, staging_table, job_config=job_config)
        load_job.result() # Wait for staging load

        # MERGE Statement: Deduplication & Snapshot handling
        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET T.data = S.data, T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN
          INSERT (id, stripe_created, data, ingestion_metadata)
          VALUES (id, stripe_created, data, ingestion_metadata)
        """
        bq_client.query(merge_sql).result()

        return f"Successfully processed {len(charges_data)} records.", 200

    except Exception as e:
        print(f"PIPELINE ERROR: {str(e)}")
        return f"Error: {str(e)}", 500
