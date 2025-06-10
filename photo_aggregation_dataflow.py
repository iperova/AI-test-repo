

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
# import pandas as pd
# import numpy as np
# import logging
# from prophet import Prophet
# from scipy import stats
# import datetime
import sys
import logging


# Set up logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
# Add logging to the module
logger = logging.getLogger(__name__)
# Add INFO logs
logger.info("Starting the script...")

PROJECT_ID = 'kw-data-science-playgorund'

#########################################################

class ExecuteBigQuerySQL(beam.DoFn):
    def __init__(self, query):
        self.query = query

    def process(self, element):
        from google.cloud import bigquery
        # Initialize BigQuery client
        client = bigquery.Client()
        # Execute SQL
        query_job = client.query(self.query)
        query_job.result()  # Wait for the query to complete
        yield f"Tables for  were created successfully"




# Define the composite transform for property type processing
class ProcessPropertyType(beam.PTransform):
    def __init__(self, query_template, city, cbsa):
        self.query_template = query_template
        self.city = city
        self.cbsa = cbsa


    def expand(self, pcoll):
        # import pandas as pd
        query = self.query_template.format(query_template=self.query_template,
                                           city=self.city,
                                           cbsa=self.cbsa)

        return (
                   pcoll
                | "Start" >> beam.Create([None])  # A dummy PCollection to trigger the SQL execution
                | "Execute SQL" >> beam.ParDo(ExecuteBigQuerySQL(query))
                | "Print Results" >> beam.Map(print)
            )



def run():
    # import pandas as pd

    PROJECT_ID = "kw-data-science-playgorund"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'photo-aggregation-pipeline',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/photo_aggregation_template',
        setup_file='./prophet_join/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = False

    city_list = ["austin", "atlanta", "seattle"]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Create a PCollection of dummy input (used to kick off sub-pipelines)
        # dummy_input = pipeline | "Start Pipeline" >> beam.Create([None])

        # Process each property type in parallel
        results = []
        for CITY in city_list:
            if CITY == 'austin':
                cbsa = 12420
            elif CITY == 'atlanta':
                cbsa = 12060
            elif CITY == 'seattle':
                cbsa = 42660
            else:
                logger.info("No city to process")

            QUERY_TEMPLATE = f"""
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results` AS

        WITH 
        t_path AS
        (
            SELECT *
            FROM `kw-data-science-playgorund.avm_{CITY}_photo.indoor_photos` AS i
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.house_plan` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.closet` USING (mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.bathroom` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.bedroom` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.immaculate_kitchen_light` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.immaculate_kitchen_dark` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.kitchen` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.living_room` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.outdated_bathroom` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.outdated_kitchen` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.private_pool` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.stylish_bedroom` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.stylish_house` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.stylish_living_room` USING(mls_id, mls_number,path)
            LEFT JOIN `kw-data-science-playgorund.avm_{CITY}_photo.unfurnished_houses` USING(mls_id, mls_number,path)
        ),
        t_impute_nan AS
        (
            SELECT mls_id, mls_number, path, indoor_photos, 
            CASE WHEN house_plan IS NULL THEN 0 ELSE house_plan END AS house_plan,
            CASE WHEN closet IS NULL THEN 0 ELSE closet END AS closet,
            CASE WHEN bathroom IS NULL OR unfurnished_houses = 1 THEN 0 ELSE bathroom END AS bathroom,
            CASE WHEN bedroom IS NULL OR unfurnished_houses = 1 THEN 0 ELSE bedroom END AS bedroom, 
            CASE WHEN immaculate_kitchen_light IS NULL THEN 0 ELSE immaculate_kitchen_light END AS immaculate_kitchen_light,
            CASE WHEN immaculate_kitchen_dark IS NULL THEN 0 ELSE immaculate_kitchen_dark END AS  immaculate_kitchen_dark,
            CASE WHEN kitchen IS NULL THEN 0 ELSE kitchen END AS kitchen,
            CASE WHEN living_room IS NULL OR unfurnished_houses = 1 THEN 0 ELSE living_room END AS  living_room,
            CASE WHEN outdated_bathroom IS NULL OR unfurnished_houses = 1 THEN 0 ELSE outdated_bathroom END AS outdated_bathroom,
            CASE WHEN outdated_kitchen IS NULL THEN 0 ELSE outdated_kitchen END AS outdated_kitchen,
            CASE WHEN private_pool IS NULL THEN 0 ELSE private_pool END AS private_pool,
            CASE WHEN stylish_bedroom IS NULL OR unfurnished_houses = 1 THEN 0 ELSE stylish_bedroom END AS stylish_bedroom,
            CASE WHEN stylish_house IS NULL OR unfurnished_houses = 1 THEN 0 ELSE stylish_house END AS stylish_house,
            CASE WHEN stylish_living_room IS NULL OR unfurnished_houses = 1 THEN 0 ELSE stylish_living_room END AS stylish_living_room,
            CASE WHEN unfurnished_houses IS NULL THEN 0 ELSE unfurnished_houses END AS unfurnished_houses,
            FROM t_path
        ),
        
        t_immaculate_kitchen AS
        (
            SELECT * EXCEPT(immaculate_kitchen_light, immaculate_kitchen_dark),
            CASE WHEN immaculate_kitchen_light = 1 OR immaculate_kitchen_dark = 1 THEN 1 ELSE 0 END AS immaculate_kitchen
            FROM t_impute_nan
        )
        
        SELECT DISTINCT mls_id, mls_number, path, indoor_photos, house_plan, closet, kitchen, living_room, bathroom, bedroom, immaculate_kitchen, 
        outdated_kitchen, stylish_bedroom, outdated_bathroom, stylish_living_room, stylish_house, unfurnished_houses, private_pool
        FROM t_immaculate_kitchen;
        
        
        CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results_by_mls_number` AS
        
        
          SELECT mls_id, mls_number, 
          CASE WHEN SUM(indoor_photos) = 0 THEN 0 ELSE ROUND(SUM(unfurnished_houses)/SUM(indoor_photos)*100, 2) END AS percent_unfurnished_rooms,
          CASE WHEN SUM(indoor_photos) = 0 THEN 0 ELSE ROUND(SUM(stylish_house)/SUM(indoor_photos)*100, 2) END AS percent_stylish_house,
          CASE WHEN SUM(kitchen) = 0 THEN 0 ELSE ROUND(SUM(immaculate_kitchen)/SUM(kitchen)*100, 2) END AS percent_immaculate_kitchens,
          CASE WHEN SUM(kitchen) = 0 THEN 0 ELSE ROUND(SUM(outdated_kitchen)/SUM(kitchen)*100, 2) END AS percent_outdated_kitchens,
          CASE WHEN SUM(bedroom) = 0 THEN 0 ELSE ROUND(SUM(stylish_bedroom)/SUM(bedroom)*100, 2) END AS percent_stylish_bedrooms,
          CASE WHEN SUM(bathroom) = 0 THEN 0 ELSE ROUND(SUM(outdated_bathroom)/SUM(bathroom)*100, 2) END AS percent_outdated_bathrooms,
          CASE WHEN SUM(living_room) = 0 THEN 0 ELSE ROUND(SUM(stylish_living_room)/SUM(living_room)*100, 2) END AS percent_stylish_living_rooms,
          MAX(private_pool) AS private_pool
        FROM kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results
        GROUP BY 1,2
        ORDER BY 1,2;
        
        -- SOLD
        CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_houses_new_source.houses_{CITY}_prophet_photo_nonbinary` AS
        SELECT * EXCEPT (city_long, address, mls_id, mls_number,  metropolitan_area_name, cbsa, listing_status,
        postal_code, postal_code_short, state_prov_short, current_listing_price)
        FROM `kw-data-science-playgorund.avm_us_houses_new_source.houses_us_2019_prophet_raw` 
            JOIN `kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results_by_mls_number` USING (mls_id, mls_number)
            WHERE cbsa = {cbsa};
        
        CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_condos_new_source.condos_{CITY}_prophet_photo_nonbinary` AS
        SELECT * EXCEPT (city_long, address, mls_id, mls_number,  metropolitan_area_name, cbsa, listing_status,
        postal_code, postal_code_short, state_prov_short, current_listing_price)
        FROM `kw-data-science-playgorund.avm_us_condos_new_source.condos_us_2019_prophet_raw` 
            JOIN `kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results_by_mls_number` USING (mls_id, mls_number)
            WHERE cbsa = {cbsa};
        
        -- NOT SOLD
        CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_condos_not_sold_new_source.condos_{CITY}_photo_nonbinary_last` AS
        SELECT * EXCEPT (GEOID, city_long, address, mls_id, mls_number,  metropolitan_area_name, cbsa,
        postal_code, postal_code_short, state_prov_short, current_listing_price, sa_mls_id)
        FROM `kw-data-science-playgorund.avm_us_condos_not_sold_new_source.condos_us_prophet`
            JOIN `kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results_by_mls_number` USING (mls_id, mls_number)
            WHERE cbsa = {cbsa};
        
        
        CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_houses_not_sold_new_source.houses_{CITY}_photo_nonbinary_last` AS
        SELECT * EXCEPT (GEOID, city_long, address, mls_id, mls_number,  metropolitan_area_name, cbsa, postal_code, postal_code_short, state_prov_short, 
                    current_listing_price, sa_mls_id)
        FROM `kw-data-science-playgorund.avm_us_houses_not_sold_new_source.houses_us_prophet` 
            JOIN `kw-data-science-playgorund.avm_{CITY}_photo.{CITY}_nonbinary_results_by_mls_number` USING (mls_id, mls_number)
            WHERE cbsa = {cbsa};

            """

            results.append(
                pipeline
                | f"Process {CITY}" >> ProcessPropertyType(query_template=QUERY_TEMPLATE,
                                                           city=CITY,
                                                           cbsa=cbsa)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python photo_aggregation_dataflow.py --worker_machine_type=e2-standard-4

# ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
#
# curl -X POST \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H "Content-Type: application/json" \
#     -d '{
#         "jobName": "photo-aggregation-pipeline",
#         "parameters": {},
#         "environment": {
#             "tempLocation": "gs://kw-ds-vertex-ai-test/temp",
#             "zone": "us-east1-b"
#         }
#     }' \
#     "https://dataflow.googleapis.com/v1b3/projects/kw-data-science-playgorund/locations/us-east1/templates:launch?gcsPath=gs://kw-ds-vertex-ai-test/templates/photo_aggregation_template"
