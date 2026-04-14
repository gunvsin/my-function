import os
import functions_framework

@functions_framework.http
def fetch_stripe_charges(request):
    """
    Stripe to BigQuery Ingestion Service.
    Delayed imports are used to ensure the container starts in <1 second.
    """
    print("Function triggered. Container is active on Port 8080.")

    try:
        # Delayed Imports
        import uuid
        import datetime
        import stripe
        from google.cloud import secretmanager
        from google.cloud import bigquery

        # Configuration
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        dataset_id = "stripe_dataset"
        table_name = "stripe_charges"

        # Initialize Clients
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 1. Retrieve API Key
        secret_name = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_name})
        stripe.api_key = response.payload.data.decode("UTF-8")

        # 2. Fetch last 24 hours of Charges
        now = datetime.datetime.now(datetime.UTC)
        start_ts = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_id = str(uuid.uuid4())
        
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
                    "batch_uuid": batch_id
                }
            })

        if not charges_data:
            return "Sync success: No new charges found.", 200

        # 3. BigQuery Upsert (Merge Strategy)
        master_table = f"{project_id}.{dataset_id}.{table_name}"
        staging_table = f"{project_id}.{dataset_id}.{table_name}_staging"

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

        # Overwrite staging
        load_config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE")
        bq_client.load_table_from_json(charges_data, staging_table, job_config=load_config).result()

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

        return f"Successfully processed {len(charges_data)} records.", 200

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return f"Pipeline Error: {str(e)}", 500
