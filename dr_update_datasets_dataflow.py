
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


class DRUpdateDatasetsDoFn(beam.DoFn):
    def __init__(self, dts):
        self.dts = dts
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

        dataset_ids = {ds.id: ds.get_details().data_source_id for ds in dr.Dataset.list() if ds.name == self.dts}
        dataset_names = {ds.id: ds.name for ds in dr.Dataset.list() if ds.name == self.dts}

        for dataset_id, data_source_id in dataset_ids.items():
            print(f'Updating Dataset {dataset_names.get(dataset_id)}')
            dr.Dataset.create_version_from_data_source(dataset_id=dataset_id,
                                                       data_source_id=data_source_id,
                                                       max_wait=3600)

            logger.info(f"Dataset {dataset_names.get(dataset_id)} was updated!")

        return print(f"{self.dts} DONE!!!")

class ProcessDataset(beam.PTransform):
    def __init__(self, dts):
        self.dts = dts

    def expand(self, pcoll):
        return (
            pcoll
            | f"Empty task for {self.dts}" >> beam.Create([None])
            | f"Update {self.dts}" >> beam.ParDo(DRUpdateDatasetsDoFn(self.dts))
        )

def run():
    PROJECT_ID = "kw-data-science-playgorund"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'dr-update-datasets',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/dr_update_datasets_template',
        setup_file='./dr_setup/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = True

    update_list = ['Austin_combined_photo_not_sold', 'Austin_combined_photo',
                   'Atlanta_combined_photo_not_sold', 'Atlanta_combined_photo',
                   'Seattle_combined_photo_not_sold', 'Seattle_combined_photo',

                   'US_houses_2019', 'US_houses_not_sold_last',
                   'US_condos_2019', 'US_condos_not_sold_last',
                   'no_big_ma_combined', 'no_big_ma_combined_not_sold',

         'Hawaii_27980, 25900, 46520, 28180_combined_not_sold',
         'Alaska_11260, 21820, 27940, 28540_combined_not_sold', 'Grand_rapids_24340, 26090_combined_not_sold',
         'Oklahoma_36420, 46140, 44660, 43060_combined_not_sold', 'Indianapolis_26900, 14020, 18020_combined_not_sold',
         'Punta_gorda_35840, 39460, 15980_combined_not_sold', 'Naples_keywest_34940, 28580_combined_not_sold',
         'Richmond_40060_combined_not_sold', 'Columbus_18140_combined_not_sold',
         'Washington_47900_combined_not_sold', 'Tucson_46060_combined_not_sold',
         'Tampa_45300_combined_not_sold', 'Seattle_42660_combined_not_sold',
         'Saint_louis_41180_combined_not_sold', 'San_francisco_41860_combined_not_sold',
         'San_diego_41740_combined_not_sold', 'San_antonio_41700_combined_not_sold',
         'Sacramento_40900_combined_not_sold', 'Riverside_40140_combined_not_sold',
         'Raleigh_39580_combined_not_sold', 'Portland_38900_combined_not_sold',
         'Pittsburgh_38300_combined_not_sold', 'Phoenix_38060_combined_not_sold',
         'Philadelphia_37980_combined_not_sold', 'Orlando_36740_combined_not_sold',
         'New_york_35620_combined_not_sold', 'Nashville_34980_combined_not_sold',
         'Minneapolis_33460_combined_not_sold', 'Miami_fort_33100_combined_not_sold',
         'Los_angeles_31080_combined_not_sold', 'Las_vegas_29820_combined_not_sold',
         'Jacksonville_27260_combined_not_sold', 'Houston_26420_combined_not_sold',
         'Fort_collins_22660_combined_not_sold', 'Detroit_19820_combined_not_sold',
         'Denver_19740_combined_not_sold', 'Dallas_fort_19100_combined_not_sold',
         'Cleveland_17460_combined_not_sold', 'Colorado_springs_17820_combined_not_sold',
         'Cincinnati_17140_combined_not_sold', 'Chicago_16980_combined_not_sold',
         'Charlotte_16740_combined_not_sold', 'Baltimore_12580_combined_not_sold',
         'Boston_14460_combined_not_sold', 'Atlanta_12060_combined_not_sold',
         'Austin_12420_combined_not_sold',
         'Hawaii_27980, 25900, 46520, 28180_combined', 'Alaska_11260, 21820, 27940, 28540_combined',
         'Grand_rapids_24340, 26090_combined', 'Oklahoma_36420, 46140, 44660, 43060_combined',
         'Indianapolis_26900, 14020, 18020_combined', 'Punta_gorda_35840, 39460, 15980_combined',
         'Naples_keywest_34940, 28580_combined', 'Richmond_40060_combined',
         'Columbus_18140_combined', 'Washington_47900_combined',
         'Tucson_46060_combined', 'Tampa_45300_combined',
         'Seattle_42660_combined', 'Saint_louis_41180_combined',
         'San_francisco_41860_combined', 'San_diego_41740_combined',
         'San_antonio_41700_combined', 'Sacramento_40900_combined',
         'Riverside_40140_combined', 'Raleigh_39580_combined',
         'Portland_38900_combined', 'Pittsburgh_38300_combined',
         'Phoenix_38060_combined', 'Philadelphia_37980_combined',
         'Orlando_36740_combined', 'New_york_35620_combined',
         'Nashville_34980_combined', 'Minneapolis_33460_combined',
         'Miami_fort_33100_combined', 'Los_angeles_31080_combined',
         'Las_vegas_29820_combined', 'Jacksonville_27260_combined',
         'Houston_26420_combined', 'Fort_collins_22660_combined',
         'Detroit_19820_combined', 'Denver_19740_combined',
         'Dallas_fort_19100_combined', 'Cleveland_17460_combined',
         'Colorado_springs_17820_combined', 'Cincinnati_17140_combined',
         'Chicago_16980_combined', 'Charlotte_16740_combined',
         'Baltimore_12580_combined', 'Boston_14460_combined',
         'Atlanta_12060_combined', 'Austin_12420_combined']

    with beam.Pipeline(options=pipeline_options) as pipeline:

        results = []
        for dts in update_list:
            results.append(
                pipeline
            | f"Process {dts}" >> ProcessDataset(dts=dts)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python dr_update_datasets_dataflow.py --worker_machine_type=e2-standard-4

#
# ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
#
# curl -X POST \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H "Content-Type: application/json" \
#     -d '{
#         "jobName": "dr-update-datasets-job",
#         "parameters": {},
#         "environment": {
#             "tempLocation": "gs://kw-ds-vertex-ai-test/temp",
#             "zone": "us-east1-b"
#         }
#     }' \
#     "https://dataflow.googleapis.com/v1b3/projects/kw-data-science-playgorund/locations/us-east1/templates:launch?gcsPath=gs://kw-ds-vertex-ai-test/templates/dr_update_datasets_template"
