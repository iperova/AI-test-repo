

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging
from google.cloud import bigquery

#########################################################
PROJECT_ID = 'kw-data-science-playgorund'

class ExecuteBigQuerySQL(beam.DoFn):
    def __init__(self, project_id, schema_name, destination_table):
        self.project_id = project_id
        self.schema_name = schema_name
        self.destination_table = destination_table

    def process(self, element):
        from google.cloud import bigquery
        # Initialize BigQuery client
        client = bigquery.Client()

        # Dynamic SQL generation
        dynamic_sql = """
            SELECT STRING_AGG('SELECT * FROM `kw-data-science-playgorund.avm_prophet_us.' || table_id || '`', ' UNION ALL ')
            FROM
            (
              SELECT table_id
              FROM `kw-data-science-playgorund.avm_prophet_us.__TABLES__`
                WHERE table_id LIKE ('trend_by_geoid%month%')
                  AND row_count>0
              ORDER BY row_count DESC
            )
        """

        # Construct EXECUTE IMMEDIATE SQL
        sql = f"""
            DECLARE project_id STRING DEFAULT '{self.project_id}';
            DECLARE schema_name STRING DEFAULT '{self.schema_name}';
            DECLARE destination_table STRING DEFAULT '{self.destination_table}';

            DECLARE dynamic_sql STRING;
            SET dynamic_sql = ({dynamic_sql});

            EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE `' || project_id || '.' || schema_name || '.' || destination_table || '` AS ' || dynamic_sql;
        """

        # Execute SQL
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to complete
        yield f"Table {self.schema_name}.{self.destination_table} created successfully"


def run():

    PROJECT_ID = 'kw-data-science-playgorund'
    SCHEMA_NAME = "avm_prophet_us"
    DESTINATION_TABLE = "all_states_prophet_geoid_month"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'us-prophet-join',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/us_prophet_join_template',
        setup_file='./prophet_join/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Start" >> beam.Create([None])  # A dummy PCollection to trigger the SQL execution
            | "Execute SQL" >> beam.ParDo(ExecuteBigQuerySQL(PROJECT_ID, SCHEMA_NAME, DESTINATION_TABLE))
            | "Print Results" >> beam.Map(print)
        )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()



# python us_prophet_join_dataflow.py --worker_machine_type=e2-standard-4




# gcloud scheduler jobs create http dataflow_unempl_test \
#     --schedule="0 0 1 * *" \
#     --uri="https://dataflow.googleapis.com/v1b3/projects/kw-data-science-playgorund/locations/us-east1/templates:launch?gcsPath=gs://kw-ds-vertex-ai-test/templates/unemployment_template" \
#     --http-method=POST \
#     --oauth-service-account-email="i_perova-itssv@kw.com" \
#     --message-body='{
#         "jobName": "unemployment-test-cli",
#         "gcsPath": "gs://kw-ds-vertex-ai-test/templates/unemployment_template",
#         "parameters": {},
#         "environment": {
#             "tempLocation": "gs://kw-ds-vertex-ai-test/temp/",
#             "zone": "us-east1-b"
#         }
#     }' \
#     --time-zone="EET"
#     -- location="us-east1"

