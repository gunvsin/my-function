```python
import os
import functions_framework

@functions_framework.http
def fetch_stripe_charges(request):
    """
    Final Engineered Solution: Stripe to BigQuery.
    Bypasses Port 8080 timeout by delaying all heavy library loads.
    """
    print("Container Check-in: Listening on Port 8080. Logic starting...")

    try:
        # 1. Delayed Library Loading (Crucial for Cold Start success)
        import uuid
        import datetime
        import stripe
        from google.cloud import secretmanager
        from google.cloud import bigquery
        
        # 2. Project Context
        # Fallback to hardcoded ID to prevent env-var related startup crashes
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        
        # 3. Client Initialization
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 4. API Key Access
        secret_name = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        secret_version = sm_client.access_secret_version(request={"name": secret_name})
        stripe.api_key = secret_version.payload.data.decode("UTF-8")

        # 5. Incremental Data Fetch
        now = datetime.datetime.now(datetime.UTC)
        start_time = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_id = str(uuid.uuid4())
        
        print(f"Fetching Stripe charges since Unix: {start_time}")
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
                    "batch_uuid": batch_id
                }
            })

        if not charges_data:
            return "Sync Success: No new records to process.", 200

        # 6. BigQuery Upsert (Staging-to-Master Merge)
        dataset_id = "stripe_dataset"
        master_table = f"{project_id}.{dataset_id}.stripe_charges"
        staging_table = f"{project_id}.{dataset_id}.stripe_charges_staging"

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

        # Load to staging table
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        bq_client.load_table_from_json(charges_data, staging_table, job_config=load_config).result()

        # Merge to master table (Handles snapshots/deduplication)
        merge_query = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET T.data = S.data, T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN INSERT (id, stripe_created, data, ingestion_metadata)
        VALUES (id, stripe_created, data, ingestion_metadata)
        """
        bq_client.query(merge_query).result()

        return f"Successfully synced {len(charges_data)} records.", 200

    except Exception as e:
        print(f"PIPELINE FAILURE: {str(e)}")
        return f"Internal Error: {str(e)}", 500
