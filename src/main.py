import os
import uuid
import datetime
import functions_framework
import stripe
from google.cloud import secretmanager
from google.cloud import bigquery
import os
import functions_framework
from google.cloud import secretmanager
from google.cloud import bigquery
import stripe

The fact that you are on **Revision 16** with the same error indicates that the Python process is failing during the **`import`** phase—even before it reaches your function logic. 

As a Google Cloud Engineer, I suspect that **Python 3.13** (which your logs show the builder is choosing) is incompatible with the specific versions of `grpcio` or `protobuf` required by the Google Cloud libraries. 

Here is the **Emergency Recovery Plan** to get this deployed.

### 1. Force the Runtime to Python 3.11
Python 3.13 is very new. Cloud Functions 2nd Gen is most stable on **3.11**. 

**Action:** Update your `cloudbuild.yaml` file. Ensure the `gcloud functions deploy` command specifically includes this flag:
`--runtime=python311`

---

### 2. The "Atomic" `main.py` (Diagnostic Mode)
We are going to move the **imports** inside the function. This is a technical "trick": it allows the container to start up and listen on Port 8080 **without loading any libraries first**. If there is a library error, you will finally see the real error message in the logs instead of a timeout.

**Replace your `main.py` with this exact diagnostic code:**

```python
import functions_framework
import os

@functions_framework.http
def fetch_stripe_charges(request):
    # We move imports INSIDE to prevent the "Port 8080" startup crash
    try:
        import stripe
        import uuid
        import datetime
        from google.cloud import secretmanager
        from google.cloud import bigquery
        print("Libraries imported successfully inside the function.")
    except ImportError as e:
        print(f"IMPORT ERROR: {str(e)}")
        return f"Library Import Failed: {str(e)}", 500

    try:
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        
        # Test if we can at least talk to Secret Manager
        sm_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        
        # This is a 'Dry Run' - just checking connectivity
        print(f"Connectivity test for: {secret_path}")
        
        return "Container started and libraries loaded. You are now safe to add the full logic back.", 200

    except Exception as e:
        print(f"RUNTIME ERROR: {str(e)}")
        return f"Infrastructure OK, but logic failed: {str(e)}", 500
@functions_framework.http
def fetch_stripe_charges(request):
    # This minimal version helps us confirm if the clients can even load
    try:
        project_id = os.environ.get("GCP_PROJECT") or "project-babb1b90-331a-4e2e-a1b"
        
        print(f"Starting check for Project: {project_id}")
        
        # Test Secret Manager Client
        sm_client = secretmanager.SecretManagerServiceClient()
        print("Secret Manager Client initialized.")
        
        # Test BigQuery Client
        bq_client = bigquery.Client(project=project_id)
        print("BigQuery Client initialized.")
        
        # If we got here, the "Port 8080" error should be gone.
        return "Infrastructure Check Passed. You can now add your Stripe logic back.", 200

    except Exception as e:
        print(f"DEBUG ERROR: {str(e)}")
        return f"Caught Error: {str(e)}", 500
# We define the schema here as a static object. 
# This is safe to keep global as it doesn't require API calls.
TABLE_SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("stripe_created", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("data", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("ingestion_metadata", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("source_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processing_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("batch_uuid", "STRING", mode="NULLABLE"),
    ]),
]

@functions_framework.http
def fetch_stripe_charges(request):
    """
    HTTP Cloud Function Entry Point.
    Clients are initialized inside to prevent startup crashes.
    """
    # 1. Immediate confirmation log (visible in Logs Explorer)
    print("Cloud Function: stripe-fetch-charges execution started.")
    
    try:
        # 2. Get Project Context
        # Using a fallback if environment variables are not set
        project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not project_id:
            # Hardcoded fallback for your specific project if env vars fail
            project_id = "project-babb1b90-331a-4e2e-a1b"

        # 3. Initialize Clients (Inside the handler)
        # This prevents the "Port 8080" timeout because initialization 
        # happens AFTER the container is successfully listening.
        sm_client = secretmanager.SecretManagerServiceClient()
        bq_client = bigquery.Client(project=project_id)

        # 4. Access Secret Manager for Stripe API Key
        secret_path = f"projects/{project_id}/secrets/STRIPE_API_KEY/versions/latest"
        try:
            secret_response = sm_client.access_secret_version(request={"name": secret_path})
            stripe.api_key = secret_response.payload.data.decode("UTF-8")
        except Exception as secret_error:
            print(f"FAILED TO ACCESS SECRET: {str(secret_error)}")
            return f"Error: Ensure Secret Manager Secret Accessor role is granted. {str(secret_error)}", 500

        # 5. Fetch Stripe Charges for the last 24 hours
        now = datetime.datetime.now(datetime.UTC)
        twenty_four_hours_ago = int((now - datetime.timedelta(hours=24)).timestamp())
        
        charges_data = []
        batch_uuid = str(uuid.uuid4())
        
        print(f"Fetching charges from Stripe created >= {twenty_four_hours_ago}")
        charges = stripe.Charge.list(created={"gte": twenty_four_hours_ago}, limit=100)

        for charge in charges.auto_paging_iter():
            charge_dict = charge.to_dict_recursive()
            
            charges_data.append({
                "id": charge_dict['id'],
                "stripe_created": charge_dict['created'],
                "data": charge_dict, # BigQuery JSON type handles this
                "ingestion_metadata": {
                    "source_name": "Stripe_Primary",
                    "processing_timestamp": now.isoformat(),
                    "batch_uuid": batch_uuid
                }
            })

        if not charges_data:
            print("No charges found in the specified 24-hour window.")
            return "No charges found.", 200

        # 6. BigQuery Sync (Staging to Master)
        dataset_id = "stripe_dataset"
        master_table = f"{project_id}.{dataset_id}.stripe_charges"
        staging_table = f"{project_id}.{dataset_id}.stripe_charges_staging"

        # Load into Staging table (Overwrite each time)
        load_config = bigquery.LoadJobConfig(
            schema=TABLE_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
        )
        
        print(f"Loading {len(charges_data)} records into staging: {staging_table}")
        load_job = bq_client.load_table_from_json(charges_data, staging_table, job_config=load_config)
        load_job.result() # Wait for load to complete

        # 7. Perform Deduplicated MERGE
        # If ID matches, update the record (Snapshot update); if new, insert it.
        merge_query = f"""
        MERGE `{master_table}` T
        USING `{staging_table}` S
        ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET 
            T.data = S.data, 
            T.ingestion_metadata = S.ingestion_metadata
        WHEN NOT MATCHED THEN
          INSERT (id, stripe_created, data, ingestion_metadata)
          VALUES (id, stripe_created, data, ingestion_metadata)
        """
        
        print("Executing BigQuery MERGE for deduplication...")
        query_job = bq_client.query(merge_query)
        query_job.result()

        success_msg = f"Successfully synced {len(charges_data)} charges to BigQuery."
        print(success_msg)
        return success_msg, 200

    except Exception as e:
        # Every error is caught here and logged to Cloud Logging (stderr)
        error_msg = f"Execution Error: {str(e)}"
        print(error_msg)
        return error_msg, 500
