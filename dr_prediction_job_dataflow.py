
import datarobot as dr
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime
import sys
import logging


# Set up logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting the script...")

PROJECT_ID = 'kw-data-science-playgorund'

#########################################################


class DRPredictionJobDoFn(beam.DoFn):
    def __init__(self, housing_type):
        self.housing_type = housing_type
        self.logger = logging.getLogger(__name__)

    def process(self, df_prophet):
        import json
        import time
        from datetime import timedelta
        from typing import Optional, TypedDict, Union, Tuple
        import datarobot as dr
        import hvac
        import google.auth
        from google.auth.credentials import Credentials
        from google.auth.credentials import Credentials as GCPCredentials
        from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials
        from google.auth.transport.requests import Request as GCPAuthRequest
        from google.cloud.iam_credentials_v1 import IAMCredentialsClient
        import logging
        from google.cloud.bigquery import Client as BigQueryClient
        from google.cloud.storage import Client as GcsClient

        # from src.gcp import get_gcp_credentials

        VAULT_URL = "https://prod.vault.kw.com"
        VAULT_ROLE = "gcp-role-prod-eim"
        VAULT_SERVICE_ACCOUNT_EMAIL = "465186156112-compute@developer.gserviceaccount.com"
        VAULT_EXPIRATION = timedelta(minutes=15)  # Max expiration Vault allows is 15 minutes

        GCP_PROJECT_ID = 'kw-data-science-playgorund'

        def get_gcp_credentials() -> Tuple[Credentials, str]:
            try:
                credentials, project_id = google.auth.default()  # Automatically fetches GCP credentials
                assert isinstance(credentials, Credentials)
                if project_id != GCP_PROJECT_ID:
                    logging.warning(
                        f"Forcing GCP project to {repr(GCP_PROJECT_ID)}, "
                        f"but got credentials with default project: {repr(project_id)}."
                    )
                return credentials, GCP_PROJECT_ID
            except Exception as e:
                logging.error(f"Error while obtaining GCP credentials: {e}")
                raise


        def get_bigquery_client():
            credentials, project = get_gcp_credentials()
            return BigQueryClient(credentials=credentials, project=project)

        def get_gcs_client() -> GcsClient:
            credentials, project = get_gcp_credentials()
            return GcsClient(credentials=credentials, project=project)

        def vault_get_data_robot_key() -> str:
            hvac_client = create_vault_client_gcp()
            secret = hvac_client.secrets.kv.read_secret_version("eim/manual/data-lake/source/data-robot-gcp-SA-API-key")
            return secret["data"]["data"]["Integration"]

        class AddressApiSecret(TypedDict):
            commandleadaccelerator: str
            datalake: str
            datascience: str
            kellermanage: str
            listingv4: str
            listingv5: str
            produser: str
            property: str
            testuser: str
            uptimecheck: str

        def vault_get_address_api_key() -> AddressApiSecret:
            hvac_client = create_vault_client_gcp()
            secret = hvac_client.secrets.kv.read_secret_version("eim/manual/address/address_api")
            return secret["data"]["data"]

        def create_vault_client_gcp(
                *,
                vault_url: str = VAULT_URL,
                vault_role: str = VAULT_ROLE,
                expiration: timedelta = VAULT_EXPIRATION,
                service_account_email: str = VAULT_SERVICE_ACCOUNT_EMAIL,
        ) -> hvac.Client:
            credentials, _ = get_gcp_credentials()
            if getattr(credentials, "service_account_email", None) != service_account_email:
                credentials = ImpersonatedCredentials(
                    source_credentials=credentials,
                    target_principal=service_account_email,
                    target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    lifetime=int(expiration.total_seconds()),
                )
                credentials.refresh(GCPAuthRequest())
                logging.info(f"Impersonating service account: {service_account_email}")

            iam_cred_client = IAMCredentialsClient(credentials=credentials)
            now = int(time.time())
            payload = {
                "sub": service_account_email,
                "iat": now,
                "exp": int(now + expiration.total_seconds()),
                "aud": f"vault/{vault_role}",
            }

            response = iam_cred_client.sign_jwt(
                name=f"projects/-/serviceAccounts/{service_account_email}",
                delegates=[f"projects/-/serviceAccounts/{service_account_email}"],
                payload=json.dumps(payload),
            )

            hvac_client = hvac.Client(url=vault_url)
            hvac_client.auth.gcp.login(role=vault_role, jwt=response.signed_jwt)
            if not hvac_client.is_authenticated():
                raise Exception(
                    "Vault client authentication failed, even though it should succeed."
                )
            return hvac_client


        token = vault_get_data_robot_key()
        logging.info(token)
        endpoint = 'https://app.datarobot.com/api/v2'
        client = dr.Client(endpoint=endpoint, token=token)

        for data_store in dr.DataStore.list():
            if data_store.canonical_name == 'BigQuery via DR PROD Service Account':
                data_store_id = data_store.id
        logging.info(f"Data_store ID: {data_store_id}")

        for credential in dr.Credential.list():
            if credential.name == '(SA) example2@example.com':
                credential_id_example = credential.credential_id
            if credential.name == '(SA) Write-to-GCS (playgorund)':  # 'Write-to-GCS (playgorund)'
                credential_id_write_to_gs = credential.credential_id
        logging.info(f"Credential ID: {credential_id_write_to_gs}")
        #
        for deployment in dr.Deployment.list():
            if f'{self.housing_type}' in deployment.label and deployment.status == 'active':
                deploymentId = deployment.id
                logging.info(f"deployment ID for {deployment.label}: {deploymentId}")
                if 'No_big_ma' in f'{self.housing_type}':
                    input_table = 'no_big_metroareas_last_nonzeros'
                    input_schema = 'avm_us_not_sold'
                    output_table_name = f'{self.housing_type}_datarobot_predictions'
                else:
                    input_table = f'{self.housing_type}_us_last'
                    input_schema = f'avm_us_{self.housing_type}_not_sold_new_source'
                    output_table_name = f'us_{self.housing_type}_datarobot_predictions'


        job = dr.BatchPredictionJob.score(
            deploymentId,
            # num_concurrent      = 3,  # the number of concurrent workers on the prediction server
            # chunk_size          = 400000,
            intake_settings={
                'type': 'jdbc',
                'table': input_table,
                'schema': input_schema,
                'catalog': 'kw-data-science-playgorund',
                'data_store_id': data_store_id,
                # data_store.id      for BigQuery via DR Service Account (#2)
                'credential_id': credential_id_example
                # cred.credential_id for example2@example.com placeholder
            },
            output_settings={
                'type': 'bigquery',
                'dataset': 'avm_predictions',  # BigQuery schema in which target table lives
                'table': output_table_name,  # BigQuery target table name
                'bucket': 'kw-ds-vertex-ai-test',  # 'kw-data-science-dev',
                # Google Cloud Storage (GS) name where interim results will be written
                'credential_id': credential_id_write_to_gs
                # '63d95eddaf47b386a15f57fa'  # cred.credential_id for Write-to-GCS
            },
            passthrough_columns=['listing_id', 'close_dt']
        )
        job.get_status()
        job.wait_for_completion(max_wait=14400)

        return print(f"{self.housing_type} DONE!!!")

# Define the composite transform for property type processing
class ProcessPropertyType(beam.PTransform):
    def __init__(self, housing_type):
        self.housing_type = housing_type

    def expand(self, pcoll):
        return (
            pcoll
            | f"Empty task for {self.housing_type}" >> beam.Create([None])
            | f"Pushing DR prediction job for {self.housing_type}" >> beam.ParDo(DRPredictionJobDoFn(self.housing_type))
        )

def run():
    import pandas as pd

    PROJECT_ID = "kw-data-science-playgorund"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'dr-prediction-job',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/dr_prediction_job_template',
        setup_file='./dr_setup/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = True

    housing_type_list = ['No_big_ma']
    MONTH = datetime.now().month

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # if MONTH in [2, 3, 5, 6, 8, 9, 11, 12]:
        results = []
        for housing_type in housing_type_list:
            results.append(
                pipeline
            | f"Process {housing_type}" >> ProcessPropertyType(
                    housing_type=housing_type
                )
            )
        # elif MONTH in [1, 4, 7, 10]:
        #     pipeline | f"Empty pipeline" >> beam.Create([None])

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python dr_prediction_job_dataflow.py --worker_machine_type=e2-standard-4

#
# ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
#
# curl -X POST \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H "Content-Type: application/json" \
#     -d '{
#         "jobName": "dr-prediction-job-",
#         "parameters": {},
#         "environment": {
#             "tempLocation": "gs://kw-ds-vertex-ai-test/temp",
#             "zone": "us-east1-b"
#         }
#     }' \
#     "https://dataflow.googleapis.com/v1b3/projects/kw-data-science-playgorund/locations/us-east1/templates:launch?gcsPath=gs://kw-ds-vertex-ai-test/templates/dr_predition_job_template"
