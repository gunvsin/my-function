import functions_framework

@functions_framework.http
def fetch_stripe_charges(request):
    """
    DATA ENGINEERED STARTUP: 
    We perform all imports INSIDE the function to bypass the 8080 startup timeout.
    """
    print("Container successfully started. Listening on Port 8080.")
    
    try:
        # 1. Delayed Imports
        import os
        import uuid
        import datetime
        import stripe
        from google.cloud import secretmanager
        from google.cloud import bigquery
        print("Dependency libraries loaded successfully.")

        # 2. Setup Context
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        dataset_id = "stripe_dataset"
        table_name = "stripe_charges"

        # 3. Client Initialization
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 4. Stripe API Key from Secret Manager
        secret_path = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        secret_payload = sm_client.access_secret_version(request={"name": secret_path})
        stripe.api_key = secret_payload.payload.data.decode("UTF-8")

        # 5. Data Retrieval
        now = datetime.datetime.now(datetime.UTC)
        start_time = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_uuid = str(uuid.uuid4())
        
        # Auto-paging handles high volume
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
            return "Sync complete: 0 charges found.", 200

        # 6. BigQuery Loading (Staging -> Merge)
        master_table = f"{project_id}.{dataset_id}.{table_name}"
        staging_table = f"{project_id}.{dataset_id}.{table_name}_staging"

        # Define Schema inside to ensure it uses the imported bigquery module
        SCHEMA = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("stripe_created", "INTEGER"),
            bigquery.SchemaField("data", "JSON"),
            bigquery.SchemaField("ingestion_metadata", "RECORD", fields=[
                bigquery.SchemaField("source_name", "STRING"),
                bigquery.SchemaField("processing_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("batch_uuid", "STRING"),
            ]),
        ]

        load_config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE")
        load_job = bq_client.load_table_from_json(charges_data, staging_table, job_config=load_config)
        load_job.result() 

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

        return f"Pipeline Success: {len(charges_data)} records processed.", 200

    except Exception as e:
        # This will now appear in your Logs Explorer as an actual error message
        print(f"PIPELINE CRASH: {str(e)}")
        return f"Internal Error: {str(e)}", 500
