
import datarobot as dr
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime
import sys
import logging
import json
import time
from datetime import timedelta, datetime
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


# Set up logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting the script...")

PROJECT_ID = 'kw-data-science-playgorund'

month_dict = {'01_2025': 'Jan_2025',
              '02_2025': 'Feb_2025',
              '03_2025': 'Mar_2025',
              '04_2025': 'Apr_2025',
              '05_2025': 'May_2025',
              '06_2025': 'Jun_2025',
              '07_2025': 'Jul_2025',
              '08_2025': 'Aug_2025',
              '09_2025': 'Sep_2025',
              '10_2025': 'Oct_2025',
              '11_2025': 'Nov_2025',
              '12_2025': 'Dec_2025'}
DATE = datetime.today().strftime('%m_%Y')
MONTH = month_dict.get(DATE)
logging.info(f'Month: {MONTH}')

#########################################################


class DRPredictModelDoFn(beam.DoFn):
    def __init__(self, project_name, month_year, endpoint, token):
        self.project_name = project_name
        self.endpoint = endpoint
        self.token = token
        self.month_year = month_year

        self.logger = logging.getLogger(__name__)

    def process(self, df_prophet):
        import json
        import time
        from datetime import timedelta, datetime
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


        def get_project_id_by_name(project_name):
            all_projects = dr.Project.list()  # List all projects
            for project in all_projects:
                if project.project_name == project_name:
                    return project.id
            return None  # Return None if no matching project is found

        def predict_by_project(PROJECT_NAME, month_year):

            PROJECT_ID = get_project_id_by_name(PROJECT_NAME)

            if PROJECT_ID:
                logging.info(f"Project ID for '{PROJECT_NAME}': {PROJECT_ID}")
                project = dr.Project.get(project_id=PROJECT_ID)

                # # ----------------------------------------- Choose model to predict -----------------------------------------------#
                model_list = project.get_models(order_by='sample_pct',
                                                use_new_models_retrieval=True)

                best_models_list = [mod for mod in model_list if (int(mod.sample_pct) == 100) &
                                    (mod.featurelist_name in ['All_trends', 'Statical_list'])]
                holdout_list = sorted([mod.metrics.get('MAPE').get('holdout') for mod in best_models_list])
                logging.info(f'Holdout list: {holdout_list}')
                best_holdout = holdout_list[0]
                logging.info(f'Best holdout: {best_holdout}')
                best_model = \
                    [mod for mod in best_models_list if mod.metrics.get('MAPE').get('holdout') == best_holdout][0]

                # # ------------------------------------- delete all other models except 100% BP ------------------------------------#

                best_models_blueprints = [mod.blueprint_id for mod in best_models_list]
                logging.info(f'Best model blueprint: {best_models_blueprints}')
                final_model_list = [mod for mod in model_list if mod.blueprint_id in best_models_blueprints]
                logging.info(f'Final model list: {final_model_list}')

                for mod in model_list:
                    if mod in final_model_list:
                        logging.info(f'Do not delete {mod}')
                    else:
                        try:
                            mod.delete()
                            logging.info(f'Delete model: {mod}')
                        except Exception as e:
                            logging.info(e)

                # # ------------------------------------- Upload dataset for prediction ---------------------------------------------#
                logging.info('Determining dataset ID')
                # if 'reduced' in PROJECT_NAME:
                #     DTS = PROJECT_NAME.split(f'_reduced')[0]
                # else:
                DTS = PROJECT_NAME.split(f'_{month_year}')[0]
                DATASET_NAME = f'{DTS}_not_sold'
                DATASET_NONZEROS_NAME = f'{DTS}_not_sold_nonzeros'

                dataset_names = {ds.name: ds.id for ds in dr.Dataset.list() if ds.name in [DATASET_NAME,
                                                                                           DATASET_NONZEROS_NAME]}
                DATASET_ID = dataset_names.get(DATASET_NAME)
                DATASET_NONZEROS_ID = dataset_names.get(DATASET_NONZEROS_NAME)

                if (DATASET_ID != None) & (DATASET_NONZEROS_ID != None):
                    logging.info(f'Dataset ID: {DATASET_ID}, Dataset nonzeros ID: {DATASET_NONZEROS_ID}')

                    if len(project.get_datasets()) == 0:
                        logging.info(f'Uploading new dataset...')
                        new_dataset = project.upload_dataset_from_catalog(dataset_id=DATASET_ID)
                        new_nonzeros_dataset = project.upload_dataset_from_catalog(dataset_id=DATASET_NONZEROS_ID)
                    else:
                        logging.info(f'List of uploaded datasets:{project.get_datasets()}')
                        new_dataset = project.get_datasets()[0]
                        new_nonzeros_dataset = project.get_datasets()[1]
                    logging.info('Datasets were found')


                    best_model.request_predictions(dataset=new_dataset)
                    best_model.request_predictions(dataset=new_nonzeros_dataset)
                    best_model.request_feature_effect()

                    logging.info('Request predictions were started')
                    STRING_TO_RETURN = f"Request prediction was started"
                else:
                    logging.info(f"DataSet wasn't found with the name: {DATASET_NAME}")
                    STRING_TO_RETURN = f"DataSet wasn't found with the name: {DATASET_NAME}"

            else:
                logging.info(f"No project found with the name '{PROJECT_NAME}'.")
                STRING_TO_RETURN = f"No project found with the name '{PROJECT_NAME}'."

        client = dr.Client(endpoint=self.endpoint, token=self.token)

        predict_by_project(self.project_name, self.month_year)

        # return logging.info(STRING_TO_RETURN")

class ProcessDataset(beam.PTransform):
    def __init__(self, project_name, month_year, endpoint, token):
        self.project_name = project_name
        self.endpoint = endpoint
        self.token = token
        self.month_year = month_year

    def expand(self, pcoll):
        return (
            pcoll
            | f"Empty task for {self.project_name}" >> beam.Create([None])
            | f"Predict for project {self.project_name}" >> beam.ParDo(DRPredictModelDoFn(self.project_name,
                                                                                          self.month_year,
                                                                                          self.endpoint,
                                                                                          self.token))
        )


def run():
    PROJECT_ID = "kw-data-science-playgorund"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'dr-predict-joined-models',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/dr_predict_joined_models_template',
        setup_file='./dr_setup/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = True


    with beam.Pipeline(options=pipeline_options) as pipeline:

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

        def vault_get_data_robot_personal_key() -> str:
            hvac_client = create_vault_client_gcp()
            secret = hvac_client.secrets.kv.read_secret_version("eim/manual/data-lake/source/data-robot-Iryna-personal")
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
                timeout=180,
                payload=json.dumps(payload),
            )

            hvac_client = hvac.Client(url=vault_url)
            hvac_client.auth.gcp.login(role=vault_role, jwt=response.signed_jwt)
            if not hvac_client.is_authenticated():
                raise Exception(
                    "Vault client authentication failed, even though it should succeed."
                )
            return hvac_client

        token = vault_get_data_robot_personal_key()
        endpoint = 'https://app.datarobot.com/api/v2'
        client = dr.Client(endpoint=endpoint, token=token)

        results = []
        project_list = [pr.project_name for pr in dr.Project.list() if (MONTH in pr.project_name) &
                        ('photo' not in pr.project_name) & ('combined' not in pr.project_name) &
                        (pr.holdout_unlocked == True) & ('houses' not in pr.project_name) &
                        ('condos' not in pr.project_name)]

        logging.info(f'Project list: {project_list}')

        # project_list = [pr.project_name for pr in dr.Project.list() if (MONTH in pr.project_name) &
        #                 ('photo' not in pr.project_name) & (pr.holdout_unlocked==True) &
        #                 ('US_condos_2019' not in pr.project_name)]
        if project_list:
            for PROJECT_NAME in project_list:
                results.append(
                        pipeline
                        | f"Process {PROJECT_NAME}" >> ProcessDataset(
                    project_name=PROJECT_NAME, month_year = MONTH, endpoint=endpoint, token=token)
                )
        else:
            # Ensure at least one step is added to the pipeline
            pipeline | "Dummy step" >> beam.Create([None])

        # for PROJECT_NAME in project_list:
        #     results.append(
        #         pipeline
        #     | f"Process {PROJECT_NAME}" >> ProcessDataset(project_name=PROJECT_NAME, endpoint=endpoint, token=token)
        #         )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
