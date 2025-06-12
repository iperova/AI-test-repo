
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


class DRTrainModelDoFn(beam.DoFn):
    def __init__(self, city):
        self.city = city

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

        VAULT_URL = "https://prod.vault.kw.com"
        VAULT_ROLE = "gcp-role-prod-eim"
        VAULT_SERVICE_ACCOUNT_EMAIL = "465186156112-compute@developer.gserviceaccount.com"
        VAULT_EXPIRATION = timedelta(minutes=15)  # Max expiration Vault allows is 15 minutes

        GCP_PROJECT_ID = 'kw-data-science-playgorund'
        exclude_feature_list = ['listing_id', 'close_dt', 'close_dt (Year)', 'close_dt (Month)',
                                'close_dt (Day of Week)', 'close_dt (Day of Month)', 'latitude',
                                'longitude', 'geometry']
        exclude_statical_list = ['listing_id', 'close_dt', 'close_dt (Year)', 'close_dt (Month)',
                                 'close_dt (Day of Week)',
                                 'close_dt (Day of Month)', 'latitude', 'longitude', 'geometry', 'public_water',
                                 'well_water',
                                 'other_water_source', 'no_water_source', 'private_water', 'not_connected_water',
                                 'culinary_water',
                                 'stucco_materials', 'ceramic_floorings', 'tile_floorings', 'wood_materials',
                                 'garage_parking',
                                 'fiberglass_roof_type', 'wood_floorings', 'no_floorings_info', 'carpet_floorings',
                                 'hardwood_floorings', 'linoleum_floorings', 'tile_floorings',
                                 'laminate_floorings', 'ceramic_floorings', 'vinyl_floorings', 'carport_parking',
                                 'street_parking',
                                 'assigned_parking', 'two_car_parking', 'attached_parking', 'door_opener_parking',
                                 'guest_parking',
                                 'driveway_parking', 'no_construction_materials', 'other_floorings',
                                 'no_parking_features',
                                 'other_roof_type', 'shingle_roof_type', 'tile_roof_type', 'composition_roof_type',
                                 'asphalt_roof_type', 'metal_roof_type', 'architectural_roof_type', 'pitched_roof_type',
                                 'flat_roof_type', 'concrete_roof_type', 'slate_roof_type', 'wood_roof_type',
                                 'underground_utilities', 'sewer', 'natural_gas', 'vinyl_materials', 'brick_materials',
                                 'aluminum_materials', 'frame_materials', 'site_built_materials', 'block_materials',
                                 'concrete_materials', 'cement_materials', 'no_roof_type',
                                 'other_construction_materials', 'stone_materials', 'detached_parking',
                                 ]
        my_dataset_photo_list = ['Austin_photo',  'Atlanta_photo',  'Seattle_photo']

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

        TARGET_NAME = 'close_price'
        FEATURE_LIST_PHOTO_NAME = 'All_trends_photos'
        STATICAL_LIST_NAME_PHOTOS = 'Statical_list_photos'

        # if datetime.today().strftime('%m_%Y')[0] == '0':
        #     DATE = datetime.today().strftime('%m_%Y')[1:]
        # else:
        #     DATE = datetime.today().strftime('%m_%Y')

        DATE = datetime.today().strftime('%m_%Y')

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
                payload=json.dumps(payload),
            )

            hvac_client = hvac.Client(url=vault_url)
            hvac_client.auth.gcp.login(role=vault_role, jwt=response.signed_jwt)
            if not hvac_client.is_authenticated():
                raise Exception(
                    "Vault client authentication failed, even though it should succeed."
                )
            return hvac_client

        def get_project_id_by_name(project_name):
            all_projects = dr.Project.list()  # List all projects
            for project in all_projects:
                if project.project_name == project_name:
                    return project.id
            return None  # Return None if no matching project is found


        def project_photo_train(CITY):


            PROJECT_NAME = f'{CITY.capitalize()}_photo_{month_dict.get(DATE)}'

            logging.info('Determining dataset ID')
            dataset_names = {ds.name: ds.id for ds in dr.Dataset.list() if ds.name in my_dataset_photo_list}
            for dataset_name in dataset_names.keys():
                if (CITY.capitalize() in dataset_name)  & ('not_sold' not in dataset_name) & ('photo' in dataset_name):
                    logging.info(f'Dataset name:{dataset_name}')
                    DATASET_ID_PHOTOS = dataset_names.get(dataset_name)


            logging.info(f'dataset ID was determined: {DATASET_ID_PHOTOS}')
            # # ------------------------------------ Choose or create project (no photos) ----------------------------------------#

            PROJECT_ID = get_project_id_by_name(PROJECT_NAME)

            if PROJECT_ID:
                logging.info(f"Project ID for '{PROJECT_NAME}': {PROJECT_ID}")
                project = dr.Project.get(project_id=PROJECT_ID)
            else:
                logging.info(f"No project found with the name '{PROJECT_NAME}'.")
                project = dr.Project.create_from_dataset(dataset_id=DATASET_ID_PHOTOS, project_name=PROJECT_NAME)
                logging.info(f'Project was created. Project ID:{project.id}, project name:{project.project_name}')

            # # ----------------------------- Set informative feature lists and final feature lists -----------------------------#
            informative_feature_list = None
            for feature_list in project.get_featurelists():
                if 'informative' in feature_list.name.lower():
                    informative_feature_list = feature_list
                    break
            if informative_feature_list is None:
                raise ValueError("Informative feature list not found.")

            # all_feature_list = [feature.name for feature in project.get_features()]
            final_feature_list = [feature for feature in informative_feature_list.features if
                                  feature not in exclude_statical_list]
            feature_list_names = [fl for fl in project.get_featurelists() if fl.name == STATICAL_LIST_NAME_PHOTOS]

            if len(feature_list_names) == 1:
                logging.info('Feature list found')
                feature_list_photos = feature_list_names[0]
            else:
                logging.info('Creating feature list')
                feature_list_photos = project.create_featurelist(name=STATICAL_LIST_NAME_PHOTOS,
                                                                 features=final_feature_list)
                logging.info('Feature list was created')

            project.set_partitioning_method(cv_method='random',
                                            validation_type='TVH',
                                            validation_pct=2,
                                            holdout_pct=2)
            logging.info('Partitioning method was set')

            project.analyze_and_model(
                target=TARGET_NAME,
                metric='Gamma Deviance',
                mode=dr.AUTOPILOT_MODE.MANUAL,
                featurelist_id=feature_list_photos.id,
                worker_count=2
            )

            # # -------------------------------- Choose blueprint ---------------------------------------------------#
            xgboost_blueprints = [bp for bp in project.get_blueprints() if 'eXtreme' in bp.model_type]
            logging.info(f'XGBoost blueprints length: {len(xgboost_blueprints)}')

            logging.info(f'Training with photos starts')
            for bp in xgboost_blueprints:
                project.train(
                    trainable=bp.id,
                    featurelist_id=feature_list_photos.id,
                    sample_pct=96  # % of dataset to train
                )

            return logging.info(f'Finished {CITY}')

        token = vault_get_data_robot_personal_key()
        # logging.info(token)
        endpoint = 'https://app.datarobot.com/api/v2'
        client = dr.Client(endpoint=endpoint, token=token)

        project_photo_train(self.city)

        return print(f"{self.city} training starts!!!")

class ProcessDataset(beam.PTransform):
    def __init__(self, city):
        self.city = city

    def expand(self, pcoll):
        return (
            pcoll
            | f"Empty task for {self.city}" >> beam.Create([None])
            | f"Train model for {self.city}" >> beam.ParDo(DRTrainModelDoFn(self.city))
        )

def run():
    PROJECT_ID = "kw-data-science-playgorund"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'dr-train-joined-photomodels',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/dr_train_joined_photomodels_template',
        setup_file='./dr_setup/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = True


    with beam.Pipeline(options=pipeline_options) as pipeline:
        city_list = ['austin', 'atlanta', 'seattle']
        results = []
        for CITY in city_list:
            results.append(
                pipeline
            | f"Process {CITY}" >> ProcessDataset(city=CITY)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
