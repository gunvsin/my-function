import functions_framework
import os

@functions_framework.http
def fetch_stripe_charges(request):
    """
    STRICT DATA ENGINEERING RESET.
    This version starts the server FIRST, then imports.
    """
    print("Health Check: Container is UP and listening on Port 8080.")

    try:
        # 1. DELAYED IMPORTS (Inside the function to prevent startup crash)
        import uuid
        import datetime
        import stripe
        from google.cloud import secretmanager
        from google.cloud import bigquery
        print("Libraries loaded successfully.")

        # 2. CONFIGURATION
        project_id = "project-babb1b90-331a-4e2e-a1b"
        dataset_id = "stripe_dataset"
        table_id = "stripe_charges"

        # 3. INITIALIZE CLIENTS
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 4. SECRET RETRIEVAL
        secret_path = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_path})
        stripe.api_key = response.payload.data.decode("UTF-8")

        # 5. FETCH STRIPE DATA
        now = datetime.datetime.now(datetime.UTC)
        start_ts = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_uuid = str(uuid.uuid4())
        
        # Stripe Auto-pagination
        charges = stripe.Charge.list(created={"gte": start_ts}, limit=100)
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
            return "Incremental Sync: 0 records found.", 200

        # 6. BIGQUERY UPSERT (Merge)
        master_table = f"{project_id}.{dataset_id}.{table_id}"
        staging_table = f"{project_id}.{dataset_id}.{table_id}_staging"

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

        # Load Staging
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        bq_client.load_table_from_json(charges_data, staging_table, job_config=load_config).result()

        # Merge Staging to Master
        merge_sql = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET T.data = S.data, T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN INSERT (id, stripe_created, data, ingestion_metadata)
        VALUES (id, stripe_created, data, ingestion_metadata)
        """
        bq_client.query(merge_sql).result()

        return f"Successfully processed {len(charges_data)} records.", 200

    except Exception as e:
        print(f"PIPELINE FAILURE: {str(e)}")
        return f"Pipeline Error: {str(e)}", 500
