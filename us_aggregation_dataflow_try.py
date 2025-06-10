

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
        yield f"Phoenix aggregation tables were created successfully"




# Define the composite transform for property type processing
class ProcessPropertyType(beam.PTransform):
    def __init__(self, housing_type, prop_subtypes, query_template, price):
        self.housing_type = housing_type
        self.prop_subtypes = prop_subtypes
        self.query_template = query_template
        self.price = price


    def expand(self, pcoll):
        # import pandas as pd
        query = self.query_template.format(housing_type=self.housing_type, prop_subtypes=self.prop_subtypes,
                                           query_template=self.query_template, price=self.price)

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
        job_name = 'us-aggregation-try-df-pipeline',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/us_aggregation_try_df_template',
        setup_file='./prophet_join/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = False

    property_types = ["condos", "houses"]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Create a PCollection of dummy input (used to kick off sub-pipelines)
        # dummy_input = pipeline | "Start Pipeline" >> beam.Create([None])

        # Process each property type in parallel
        results = []
        for housing_type in property_types:
            if housing_type == 'houses':
                prop_list = ['single family detached']
                PROP_SUBTYPES = str(prop_list).replace('[', '').replace(']', '')
                PRICE = 65000
            elif housing_type == 'condos':
                prop_list = ['condominium', 'townhouse', 'apartment', 'single family attached', 'duplex',
                             'triplex', 'quadruplex', 'stock cooperative', 'adult community', 'vacation home']
                PROP_SUBTYPES = str(prop_list).replace('[', '').replace(']', '')
                PRICE = 55000
            else:
                logger.info("No housing type to process")

            QUERY_TEMPLATE = f"""
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.t_metro_areas` AS
            
            WITH split_states AS (
                SELECT 
                    name AS metropolitan_area_name, 
                    cbsa, 
                    ST_GeogFromText(wkt) AS metro_polygon,
                    SPLIT(name, '-') AS state_array
                FROM `data-lake-prod-pres-2c917b19.partners.precisely__cbsa_boundaries_tbl`
            )
            SELECT 
                metropolitan_area_name, cbsa, metro_polygon, 
                TRIM(REGEXP_EXTRACT(state, r'\b[A-Z]{2}\b')) AS state_prov_short
            FROM split_states, 
            UNNEST(state_array) AS state
            WHERE REGEXP_CONTAINS(state, r'\b[A-Z]{2}\b');
              """

            results.append(
                pipeline
                | f"Process {housing_type}" >> ProcessPropertyType(query_template=QUERY_TEMPLATE,
                                                                   housing_type=housing_type,
                                                                   prop_subtypes=PROP_SUBTYPES,
                                                                   price=PRICE)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python us_aggregation_dataflow_try.py --worker_machine_type=e2-standard-4

# ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
#
# curl -X POST \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H "Content-Type: application/json" \
#     -d '{
#         "jobName": "us-aggregation-try-df-pipeline",
#         "parameters": {},
#         "environment": {
#             "tempLocation": "gs://kw-ds-vertex-ai-test/temp",
#             "zone": "us-east1-b"
#         }
#     }' \
#     "https://dataflow.googleapis.com/v1b3/projects/kw-data-science-playgorund/locations/us-east1/templates:launch?gcsPath=gs://kw-ds-vertex-ai-test/templates/us_aggregation_try_df_template"
