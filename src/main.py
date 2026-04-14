import os
import functions_framework

@functions_framework.http
def fetch_stripe_charges(request):
    """
    REVISION 32: Senior Developer Reset.
    Zero-Global imports to ensure Port 8080 connectivity.
    """
    print("Container Startup Successful: Listening on Port 8080.")

    try:
        # 1. DELAYED IMPORTS
        # We load these ONLY when the function is triggered.
        import uuid
        import datetime
        import stripe
        from google.cloud import secretmanager
        from google.cloud import bigquery
        print("Libraries loaded successfully.")

        # 2. CONFIGURATION
        PROJECT_ID = "project-babb1b90-331a-4e2e-a1b"
        DATASET_ID = "stripe_dataset"
        TABLE_NAME = "stripe_charges"
        
        # 3. CLIENT INITIALIZATION
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=PROJECT_ID)

        # 4. GET STRIPE KEY
        secret_path = f"projects/{PROJECT_ID}/secrets/STRIPE_API_KEY/versions/latest"
        secret_response = sm_client.access_secret_version(request={"name": secret_path})
        stripe.api_key = secret_response.payload.data.decode("UTF-8")

        # 5. FETCH DATA (Last 24 Hours)
        now = datetime.datetime.now(datetime.UTC)
        start_ts = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_batch = []
        batch_uuid = str(uuid.uuid4())
        
        print(f"Syncing Stripe charges since: {start_ts}")
        charges = stripe.Charge.list(created={"gte": start_ts}, limit=100)

        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            charges_batch.append({
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict,
                "ingestion_metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": batch_uuid
                }
            })

        if not charges_batch:
            return "No new charges found in the last 24h.", 200

        # 6. BIGQUERY UPSERT (Merge Strategy)
        master_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
        staging_table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}_staging"

        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("stripe_created", "INTEGER"),
            bigquery.SchemaField("data", "JSON"),
            bigquery.SchemaField("ingestion_metadata", "RECORD", fields=[
                bigquery.SchemaField("source_name", "STRING"),
                bigquery.SchemaField("processing_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("batch_uuid", "STRING"),
            ]),
        ]

        # Load to staging
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        bq_client.load_table_from_json(charges_batch, staging_table, job_config=load_config).result()

        # Merge staging to master
        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET T.data = S.data, T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN INSERT (id, stripe_created, data, ingestion_metadata)
        VALUES (id, stripe_created, data, ingestion_metadata)
        """
        bq_client.query(merge_sql).result()

        return f"Pipeline Success: {len(charges_batch)} charges synced.", 200

    except Exception as e:
        # This will now appear in your Logs Explorer as a readable error
        print(f"PIPELINE ERROR: {str(e)}")
        return f"Internal Error: {str(e)}", 500
