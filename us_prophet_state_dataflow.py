

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import pandas as pd
import numpy as np
import logging
from prophet import Prophet
from scipy import stats
import datetime
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

def state_analysis(df_prophet):
    import pandas as pd
    import numpy as np
    from prophet import Prophet
    from scipy import stats
    import datetime
    import sys
    import logging
    # Set up logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    # Add logging to the module
    logger = logging.getLogger(__name__)
    # Add INFO logs
    logger.info("Starting the script...")

    def remove_outliers(dtfr):
        import numpy as np
        from scipy import stats

        threshold = 1.96
        z_scores = np.abs(stats.zscore(dtfr['dollar_sqft_avg'].to_list()))
        dtfr['zscore'] = z_scores
        final_df = dtfr[dtfr['zscore'] <= threshold]
        return final_df

    def prophet_features_upd(df):
        from prophet import Prophet

        m = Prophet(yearly_seasonality=3,
                    changepoint_prior_scale=5,
                    weekly_seasonality=False,
                    daily_seasonality=False)
        m.fit(df)
        future = m.make_future_dataframe(periods=5, freq='M')
        forecast = m.predict(future)
        return forecast


    df_forecast = pd.DataFrame(columns=['ds', 'trend', 'yhat_lower', 'yhat_upper', 'trend_lower', 'trend_upper',
                                        'additive_terms', 'additive_terms_lower', 'additive_terms_upper',
                                        'yearly', 'yearly_lower', 'yearly_upper', 'multiplicative_terms',
                                        'multiplicative_terms_lower', 'multiplicative_terms_upper', 'yhat',
                                        'state_prov_short'])
    logger.info(f"df_forecast has shape: {df_forecast.shape}")

    for state in df_prophet.state_prov_short.unique():
        df_temp = pd.DataFrame(df_prophet[df_prophet['state_prov_short'] == state].sort_values('ds'))
        if len(df_temp) > 1:
            logger.info(f"Removing outliers for state: {state}")
            new_df = remove_outliers(df_temp).rename(columns={'dollar_sqft_avg': 'y'})
            if len(new_df) > 1:
                logger.info(f"Forecast for state:{state}")
                forecast = prophet_features_upd(new_df[['ds', 'y']])
                forecast['state_prov_short'] = state
                df_forecast = pd.concat([df_forecast, forecast])

    logger.info(f"Aggregation finished, df_forecast shape:{df_forecast.shape}")

    df_to_bq = df_forecast[['state_prov_short', 'ds', 'yearly']] \
        .rename(columns={'ds': 'close_dt', 'yearly': f'yearly_state'})

    df_to_bq['state_prov_short'] = df_to_bq['state_prov_short'].astype(str)
    df_to_bq[f'yearly_state'] = df_to_bq[f'yearly_state'].astype(float)
    print(df_to_bq.info())
    print(df_to_bq.to_dict(orient='records'))
    return df_to_bq.to_dict(orient='records')


# Define the composite transform for property type processing
class ProcessPropertyType(beam.PTransform):
    def __init__(self, prop_type, project_id, query_template, output_table):
        self.prop_type = prop_type
        self.project_id = project_id
        self.query_template = query_template
        self.output_table = output_table

    def expand(self, pcoll):
        import pandas as pd
        query = self.query_template.format(prop_type=self.prop_type)
        output_table = self.output_table.format(prop_type=self.prop_type)

        return (
            pcoll
            | f"Read data from BQ for {self.prop_type}" >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
            | f"Combine into one element for {self.prop_type}" >> beam.combiners.ToList()
            | f"Convert to Pandas for {self.prop_type}" >> beam.Map(lambda x: pd.DataFrame(x))
            | f"Apply Prophet forecast for {self.prop_type}" >> beam.FlatMap(state_analysis)
            | f"Write results for {self.prop_type} to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=output_table,
                schema="state_prov_short:STRING, close_dt:TIMESTAMP, yearly_state:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


def run():
    import pandas as pd

    PROJECT_ID = "kw-data-science-playgorund"

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # Change to 'DirectRunner' for local testing 'DataflowRunner'
        project=PROJECT_ID,
        job_name = 'us-prophet-state-pipeline',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/us_prophet_state_template',
        setup_file='./us_prophet/setup.py',
        region='us-east1'
    )

    pipeline_options.view_as(SetupOptions).save_main_session = False

    property_types = ["condos", "houses"]

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Create a PCollection of dummy input (used to kick off sub-pipelines)
        # dummy_input = pipeline | "Start Pipeline" >> beam.Create([None])

        # Process each property type in parallel
        results = []
        for prop_type in property_types:
            QUERY_TEMPLATE = f"""SELECT
                                *
                            FROM `kw-data-science-playgorund.avm_prophet_us_{prop_type}.prophet_by_state`
                            ORDER BY state_prov_short, ds;"""

            OUTPUT_TABLE = f"avm_prophet_us_{prop_type}.trend_by_state"

            results.append(
                pipeline
                | f"Process {prop_type}" >> ProcessPropertyType(
                    prop_type=prop_type,
                    project_id=PROJECT_ID,
                    query_template=QUERY_TEMPLATE,
                    output_table=OUTPUT_TABLE,
                )
            )

        # # Merge all sub-pipeline results (optional, if you need a combined output)
        # _ = results | "Flatten results" >> beam.Flatten()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python us_prophet_state_dataflow.py --worker_machine_type=e2-standard-4
