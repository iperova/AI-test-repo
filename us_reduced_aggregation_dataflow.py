

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
    def __init__(self, housing_type, query_template, additional_query):
        self.housing_type = housing_type
        self.query_template = query_template
        self.additional_query = additional_query


    def expand(self, pcoll):
        # import pandas as pd
        query = self.query_template.format(housing_type=self.housing_type,
                                           query_template=self.query_template,
                                           additional_query=self.additional_query)

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
        job_name = 'us-reduced-aggregation-pipeline',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/us_reduced_aggregation_template',
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

                ADDITIONAL_QUERY = """
                                CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_houses_new_source.houses_us_2022_prophet_reduced`  AS
                                SELECT *
                                FROM `kw-data-science-playgorund.avm_us_houses_new_source.houses_us_2019_prophet` TABLESAMPLE SYSTEM (25 PERCENT);
                                """
            elif housing_type == 'condos':

                ADDITIONAL_QUERY =''
            else:
                logger.info("No housing type to process")

            QUERY_TEMPLATE = f"""
            
CREATE TEMP TABLE t_raw_data AS 
(
  SELECT 
    * EXCEPT(listing_description, virtual_tour_url, roof_types, utilities, water_source, parking_features, 
    construction_materials, flooring),
    ST_GEOGFROMTEXT('POINT(' || lon || ' ' || lat || ')') AS list_geo
  FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019`
  WHERE lat BETWEEN -90 AND 90
  ORDER BY listing_id
);

CREATE TEMP TABLE  t_bg_data AS 
(
  SELECT
    GEOID,
    ST_GeogFromText(geometry) AS polygon
  FROM `kw-data-science-playgorund.marketcenter_profit.census_block_group_data`
);

CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg` AS
WITH
t_raw_bg_data AS 
(
  SELECT 
    *
  FROM 
    t_raw_data, t_bg_data 
  WHERE ST_INTERSECTS(t_raw_data.list_geo, t_bg_data.polygon)
),

t_metro_areas_data AS 
(
  SELECT 
    t_raw_bg_data.*, t_metro_areas.metropolitan_area_name, CAST(t_metro_areas.cbsa AS INT64) as cbsa --EXCEPT(metro_polygon)
  FROM 
    t_raw_bg_data, `kw-data-science-playgorund.price_prediction.metro_areas` AS t_metro_areas 
  WHERE ST_INTERSECTS(t_raw_bg_data.list_geo, t_metro_areas.metro_polygon)
),

t_no_metro_areas_data AS 
(
  SELECT 
    t_raw_bg_data.* 
  FROM 
    t_raw_bg_data LEFT JOIN t_metro_areas_data ON t_raw_bg_data.listing_id = t_metro_areas_data.listing_id
  WHERE t_metro_areas_data.listing_id IS NULL
),

t_new_cbsa AS
(
  SELECT 
  r.*, 
  m.metropolitan_area_name,
  CAST(m.cbsa AS INT64) AS cbsa
FROM t_no_metro_areas_data r, `kw-data-science-playgorund.price_prediction.metro_areas` AS m 
WHERE r.state_prov_short = m.state_prov_short
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.listing_id ORDER BY ST_DISTANCE(r.list_geo, m.metro_polygon)) = 1
),


t_all AS
(
  SELECT *
  FROM t_metro_areas_data

  UNION ALL

  SELECT 
    * 
  FROM t_new_cbsa
)

SELECT *
FROM t_all
QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id order by listing_id desc) = 1;

            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec` AS 
            WITH 
            t_precisely AS 
            (
              SELECT
                *
              FROM
                `kw-data-science-playgorund.price_prediction.precisely`
            ),
            
            t_no_polygon AS 
            (
              SELECT
                * EXCEPT (code, latitude, longitude, landarea, waterarea, population_density, 
                population_grow, population_estimate, population_median_age, percent_white_population,
                percent_black_population, percent_asian_population, percent_other_race_population, 
                contract_rent_percent_median, household_income_median, home_value_median)
              FROM 
                `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg` AS list
                  LEFT JOIN t_precisely AS pr ON list.GEOID = pr.code
              WHERE pr.code IS NULL
            ),
            
            t_add_polygon AS 
            (
              SELECT 
                * EXCEPT (code, latitude, longitude)
              FROM 
                t_no_polygon, t_precisely 
              WHERE ST_INTERSECTS(t_no_polygon.polygon, ST_GEOGFROMTEXT('POINT(' || t_precisely.longitude || ' ' || t_precisely.latitude || ')')) 
            ),
            
            t_polygon AS 
            (
              SELECT
                * EXCEPT (code, latitude, longitude)
              FROM 
                `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg`AS list
                  LEFT JOIN t_precisely AS pr ON list.GEOID = pr.code
              WHERE pr.code IS NOT NULL
            ),
            
            t_all AS 
            (
              SELECT 
                *
              FROM
                t_add_polygon
              UNION ALL
              SELECT
                *
              FROM
                t_polygon
            )
            
            SELECT 
              *,
              DATE_TRUNC(close_dt, month) AS close_month,
              EXTRACT(ISOYEAR FROM close_dt) AS close_year,
              EXTRACT(ISOWEEK FROM close_dt) AS num_week
            FROM 
              t_all
            WHERE TRUE
              AND landarea < 1000
              AND waterarea < 50
              AND population_density < 150000
              AND population_grow < 10000
              AND population_grow > -1000
              AND population_estimate < 8000
              AND population_estimate > -750;
            
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ` AS
            
            WITH 
            t_econ AS
            (
              SELECT * EXCEPT (close_year, num_week, State, DATE, year_added, year_built, state_prov_short, close_month, 
              original_listing_category, property_type),
              l.state_prov_short
              FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec` AS l
              LEFT JOIN `kw-data-science-playgorund.price_prediction.state_population` USING (state_prov_short)
                LEFT JOIN `kw-data-science-playgorund.price_prediction.temperature_tornado` USING (state_prov_short)
                    LEFT JOIN `kw-data-science-playgorund.price_prediction.mortgage_not_seasonality_upd` AS m USING (close_year, num_week)
                      LEFT JOIN `kw-data-science-playgorund.price_prediction.case_shiller_hpi_not_seasonality_upd` AS cs 
                      ON (l.close_month = DATE(cs.close_month))
                        LEFT JOIN `kw-data-science-playgorund.price_prediction.unemployment_rate_upd` AS u 
                        ON l.close_month = DATE(u.close_month) AND l.state_prov_short = u.state_prov_short
            )
            
            SELECT 
                * EXCEPT (key, name, geometry)
            FROM t_econ, `kw-data-science-playgorund.price_prediction.metroareas_nielsen_dma_boundaries` AS b
            WHERE ST_INTERSECTS(t_econ.list_geo, b.geometry);
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.schools_us_2019`
            AS
            WITH t_school AS
            (
            SELECT 
                *
            FROM 
                `kw-data-science-playgorund.price_prediction.schools`
            ),
            
            t_listings_postal AS 
            (
            SELECT 
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ` AS l, t_school
            WHERE ST_INTERSECTS(l.list_geo, t_school.geom)
            ),
            
            t_elementary AS
            (
            SELECT 
                *
            FROM t_listings_postal
            WHERE ed_level IN ('P', 'PM', 'E', 'O', 'PMH')
            AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    pctile_y1 DESC -- distance
                ) = 1
            ),
            
            t_elementary_nan AS 
            (
            SELECT 
                l.listing_id,
                l.list_geo,
                l.state_prov_short,
                l.postal_code,
                l.postal_code_short,
                l.address,
                l.city_long,
                t_elementary.distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ` AS l
                LEFT JOIN t_elementary USING (listing_id)
            WHERE t_elementary.distance IS NULL
            ),
            
            t_elementary_state AS 
            (
            SELECT
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM t_elementary_nan 
                LEFT JOIN t_school ON t_elementary_nan.state_prov_short = t_school.state_prov
            ),
            
            t_elementary_add AS
            (
            SELECT 
                *
            FROM t_elementary_state
            WHERE ed_level IN ('P', 'PM', 'E', 'O', 'PMH')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    distance
                ) = 1
            ),
            
            t_elementary_final AS 
            (
            SELECT *
            FROM t_elementary
            UNION ALL
            SELECT *
            FROM t_elementary_add
            ),
            
            t_middle AS
            (
            SELECT 
                *
            FROM t_listings_postal
            WHERE ed_level IN ('M', 'PM', 'MH', 'O', 'PMH', 'S')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    pctile_y1 DESC -- distance
                ) = 1
            ),
            
            t_middle_nan AS 
            (
            SELECT 
                l.listing_id,
                l.list_geo,
                l.state_prov_short,
                l.postal_code,
                l.postal_code_short,
                l.address,
                l.city_long,
                t_middle.distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ` AS l
                LEFT JOIN t_middle USING (listing_id)
            WHERE t_middle.distance IS NULL
            ),
            
            t_middle_state AS 
            (
            SELECT
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM t_middle_nan 
                LEFT JOIN t_school ON t_middle_nan.state_prov_short = t_school.state_prov
            ),
            
            t_middle_add AS
            (
            SELECT 
                *
            FROM t_middle_state
            WHERE ed_level IN ('M', 'PM', 'MH', 'O', 'PMH', 'S')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    distance
                ) = 1
            ),
            
            t_middle_final AS 
            (
            SELECT *
            FROM t_middle
            UNION ALL
            SELECT *
            FROM t_middle_add
            ),
            
            t_high AS
            (
            SELECT 
                *
            FROM t_listings_postal
            WHERE ed_level IN ('H', 'MH', 'O', 'PMH', 'S')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    pctile_y1 DESC -- distance
                ) = 1
            ),
            
            t_high_nan AS 
            (
            SELECT 
                l.listing_id,
                l.list_geo,
                l.state_prov_short,
                l.postal_code,
                l.postal_code_short,
                l.address,
                l.city_long,
                t_high.distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ` AS l
                LEFT JOIN t_high USING (listing_id)
            WHERE t_high.distance IS NULL
            
            ),
            
            t_high_state AS 
            (
            SELECT
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM t_high_nan 
                LEFT JOIN t_school ON t_high_nan.state_prov_short = t_school.state_prov
            ),
            
            t_high_add AS
            (
            SELECT 
                *
            FROM t_high_state
            WHERE ed_level IN ('H', 'MH', 'O', 'PMH', 'S')
                 AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    distance
                ) = 1
            ),
            
            t_high_final AS 
            (
            SELECT *
            FROM t_high
            UNION ALL
            SELECT *
            FROM t_high_add
            )
            
            SELECT 
            listing_id,
            ROUND(t_elementary_final.distance/1000, 2) AS elem_distance,
            ROUND(t_elementary_final.pup_teach, 2) AS elem_pup_teach,
            ROUND(t_elementary_final.pov_lev, 2) AS elem_pov_lev,
            ROUND(t_elementary_final.pctile_y1, 2) AS elem_pctile_y1,
            ROUND(t_middle_final.distance/1000, 2) AS middle_distance,
            ROUND(t_middle_final.pup_teach, 2) AS middle_pup_teach,
            ROUND(t_middle_final.pov_lev, 2) AS middle_pov_lev,
            ROUND(t_middle_final.pctile_y1, 2) AS middle_pctile_y1,
            ROUND(t_high_final.distance/1000, 2) AS high_distance,
            ROUND(t_high_final.pup_teach, 2) AS high_pup_teach,
            ROUND(t_high_final.pov_lev, 2) AS high_pov_lev,
            ROUND(t_high_final.pctile_y1, 2) AS high_pctile_y1
            
            FROM t_elementary_final 
              FULL JOIN t_middle_final USING (listing_id) 
                FULL JOIN t_high_final USING (listing_id)
            ORDER BY listing_id;
            
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ_school` AS
            
            SELECT * EXCEPT (full_bathroom_count, total_bathroom_count),
                CASE WHEN full_bathroom_count IS NULL  
                     THEN total_bathroom_count ELSE full_bathroom_count END AS full_bathroom_count
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ`
                INNER JOIN `kw-data-science-playgorund.avm_us_{housing_type}_new_source.schools_us_2019` USING (listing_id);
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.{housing_type}_us_2019_prophet_raw`  AS
            
            WITH
            t_listings AS 
            (
            SELECT * EXCEPT (listing_dt, property_subtype, list_geo, polygon, dma_name, original_listing_price, lot_size_area),
                    CASE WHEN lot_size_area IS NULL THEN 0 ELSE lot_size_area END AS lot_size_area,
                    EXTRACT(YEAR FROM close_dt) AS close_year,
                    EXTRACT(MONTH FROM close_dt)  AS close_month,
                    EXTRACT(QUARTER FROM close_dt) AS quarter
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019_bg_prec_econ_school`
            WHERE TRUE
                AND city_long IS NOT NULL
                AND state_prov_short IS NOT NULL
                AND living_area  IS NOT NULL
                AND total_bedroom_count  IS NOT NULL
                AND full_bathroom_count  IS NOT NULL
                AND garage  IS NOT NULL
                AND parking  IS NOT NULL
                AND has_security_system  IS NOT NULL
                AND has_fireplace  IS NOT NULL
                AND has_patio  IS NOT NULL
                AND has_pool  IS NOT NULL
                AND has_porch  IS NOT NULL
                AND is_water_front  IS NOT NULL
                AND is_new_construction  IS NOT NULL
                AND has_mother_in_law IS NOT NULL
                AND has_sauna IS NOT NULL
                AND has_skylight IS NOT NULL
                AND has_vaulted_ceiling IS NOT NULL
                AND has_wet_bar IS NOT NULL
                AND is_cable_ready IS NOT NULL
                AND is_wired IS NOT NULL
                AND has_hot_tub_spa IS NOT NULL
                AND has_barbecue_area IS NOT NULL
                AND has_deck IS NOT NULL
                AND has_disabled_access IS NOT NULL
                AND has_dock IS NOT NULL
                AND has_garden IS NOT NULL
                AND has_gated_entry IS NOT NULL
                AND has_green_house IS NOT NULL
                AND has_pond IS NOT NULL
                AND has_rv_parking  IS NOT NULL
                AND has_sports_court  IS NOT NULL
                AND has_sprinkler_system IS NOT NULL
                AND building_years_old IS NOT NULL
                AND virtual_tour_ind IS NOT NULL
                AND photo_count IS NOT NULL
                -- AND houses_on_market_month IS NOT NULL
                AND landarea IS NOT NULL
                AND waterarea IS NOT NULL
                AND population_density IS NOT NULL
                AND population_grow IS NOT NULL
                AND population_estimate IS NOT NULL
                AND population_median_age IS NOT NULL
                AND percent_white_population IS NOT NULL
                AND percent_black_population IS NOT NULL
                AND percent_asian_population IS NOT NULL
                AND percent_other_race_population IS NOT NULL
                AND contract_rent_percent_median IS NOT NULL
                AND household_income_median IS NOT NULL
                AND home_value_median IS NOT NULL
                -- AND tax_rate IS NOT NULL
                AND state_population IS NOT NULL
                AND annual_temperature IS NOT NULL
                AND tornado_index IS NOT NULL
                AND unemployment_rate IS NOT NULL
                AND case_shiller IS NOT NULL
                AND home_price_index IS NOT NULL
                AND mortgage IS NOT NULL      
                AND elem_distance IS NOT NULL 
                AND elem_pup_teach IS NOT NULL
                AND elem_pov_lev IS NOT NULL 
                AND elem_pctile_y1 IS NOT NULL 
                AND middle_distance IS NOT NULL 
                AND middle_pup_teach IS NOT NULL 
                AND middle_pov_lev IS NOT NULL
                AND middle_pctile_y1 IS NOT NULL 
                AND high_distance IS NOT NULL
                AND high_pup_teach IS NOT NULL
                AND high_pov_lev IS NOT NULL
                AND high_pctile_y1 IS NOT NULL
            ),
            
            -- t_prophet_geoid_month AS 
            -- (
            --   SELECT * EXCEPT(close_dt),
            --     EXTRACT(YEAR FROM close_dt) AS close_year,
            --     EXTRACT(MONTH FROM close_dt) AS close_month
            --   FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.all_states_prophet_geoid_month`
            -- ),
            
            t_prophet_geoid_quarter AS 
            (
              SELECT * EXCEPT(close_dt),
                EXTRACT(YEAR FROM close_dt) AS close_year,
                EXTRACT(QUARTER FROM close_dt) AS quarter
              FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.all_states_prophet_geoid_quarter`
              QUALIFY ROW_NUMBER() OVER (PARTITION BY GEOID, state_prov_short, close_year, quarter ORDER BY close_dt) = 1
            ),
            
            t_prophet_state AS 
            (
              SELECT * EXCEPT(close_dt),
                EXTRACT(YEAR FROM close_dt) AS close_year,
                EXTRACT(MONTH FROM close_dt) AS close_month
              FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.trend_by_state`
              QUALIFY ROW_NUMBER() OVER (PARTITION BY state_prov_short, close_year, close_month ORDER BY close_dt) = 1
            ),
            
            t_add_diff AS 
            (
              SELECT * 
              FROM t_listings
                   --  JOIN t_prophet_geoid_month USING(GEOID, state_prov_short, close_year, close_month)
                        JOIN t_prophet_geoid_quarter USING(GEOID, state_prov_short, close_year, quarter)
                            JOIN t_prophet_state USING(state_prov_short, close_year, close_month)
            )
            
            SELECT
                * EXCEPT(sa_mls_id, lat, lon, GEOID, close_year, close_month, quarter),
                lat AS latitude, 
                lon AS longitude 
            FROM t_add_diff;
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.{housing_type}_us_2019_prophet`  AS
            
            SELECT
                * EXCEPT(city_long, address, mls_id, mls_number, listing_status, metropolitan_area_name, cbsa, postal_code, postal_code_short, 
                state_prov_short, current_listing_price)
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_new_source.{housing_type}_us_2019_prophet_raw`;
            
            {ADDITIONAL_QUERY}
            
            
CREATE TEMP TABLE t_raw_data_not AS 
(
  SELECT 
    * EXCEPT(listing_description, virtual_tour_url, roof_types, utilities, water_source, parking_features, construction_materials, flooring),
    ST_GEOGFROMTEXT('POINT(' || lon || ' ' || lat || ')') AS list_geo
  FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us`
  WHERE lat BETWEEN -90 AND 90
  ORDER BY listing_id
);

CREATE TEMP TABLE  t_bg_data_not AS 
(
  SELECT
    GEOID,
    ST_GeogFromText(geometry) AS polygon
  FROM `kw-data-science-playgorund.marketcenter_profit.census_block_group_data`
);

CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg` AS
WITH
t_raw_bg_data AS 
(
  SELECT 
    *
  FROM 
    t_raw_data_not, t_bg_data_not 
  WHERE ST_INTERSECTS(t_raw_data_not.list_geo, t_bg_data_not.polygon)
),

t_metro_areas_data AS 
(
  SELECT 
    t_raw_bg_data.*, t_metro_areas_not.metropolitan_area_name, CAST(t_metro_areas_not.cbsa AS INT64) as cbsa --EXCEPT(metro_polygon)
  FROM 
    t_raw_bg_data, `kw-data-science-playgorund.price_prediction.metro_areas` AS t_metro_areas_not 
  WHERE ST_INTERSECTS(t_raw_bg_data.list_geo, t_metro_areas_not.metro_polygon)
),

t_no_metro_areas_data AS 
(
  SELECT 
    t_raw_bg_data.* 
  FROM 
    t_raw_bg_data LEFT JOIN t_metro_areas_data ON t_raw_bg_data.listing_id = t_metro_areas_data.listing_id
  WHERE t_metro_areas_data.listing_id IS NULL
),

t_new_cbsa AS
(
  SELECT 
  r.*, 
  m.metropolitan_area_name,
  CAST(m.cbsa AS INT64) AS cbsa,
  -- ST_DISTANCE(r.list_geo, m.metro_polygon) AS distance_meters
FROM t_no_metro_areas_data r, `kw-data-science-playgorund.price_prediction.metro_areas` AS m 
WHERE r.state_prov_short = m.state_prov_short
QUALIFY ROW_NUMBER() OVER (PARTITION BY r.listing_id ORDER BY ST_DISTANCE(r.list_geo, m.metro_polygon)) = 1
),


t_all AS
(
  SELECT *
  FROM t_metro_areas_data

  UNION ALL

  SELECT 
    *  --EXCEPT(distance_meters)--'Empty' AS metropolitan_area_name, 0 AS cbsa
  FROM t_new_cbsa
)

SELECT *
FROM t_all
QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id order by listing_id desc) = 1;
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec` AS
            WITH 
            t_precisely AS 
            (
              SELECT
                *
              FROM
                `kw-data-science-playgorund.price_prediction.precisely`
            ),
            
            t_no_polygon AS 
            (
              SELECT
                * EXCEPT (code, latitude, longitude, landarea, waterarea, population_density, 
                population_grow, population_estimate, population_median_age, percent_white_population,
                percent_black_population, percent_asian_population, percent_other_race_population, 
                contract_rent_percent_median, household_income_median, home_value_median)
              FROM 
                `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg` AS list
                  LEFT JOIN t_precisely AS pr ON list.GEOID = pr.code
              WHERE pr.code IS NULL
            ),
            
            t_add_polygon AS 
            (
              SELECT 
                * EXCEPT (code, latitude, longitude)
              FROM 
                t_no_polygon, t_precisely 
              WHERE ST_INTERSECTS(t_no_polygon.polygon, ST_GEOGFROMTEXT('POINT(' || t_precisely.longitude || ' ' || t_precisely.latitude || ')')) 
            ),
            
            
            t_polygon AS 
            (
              SELECT
                * EXCEPT (code, latitude, longitude)
              FROM 
                `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg`AS list
                  LEFT JOIN t_precisely AS pr ON list.GEOID = pr.code
              WHERE pr.code IS NOT NULL
            ),
            
            t_all AS 
            (
              SELECT 
                *
              FROM
                t_add_polygon
              UNION ALL
              SELECT
                *
              FROM
                t_polygon
            )
            
            SELECT 
              *,
              EXTRACT(ISOYEAR FROM CURRENT_DATE) AS close_year,
              DATE_TRUNC(CURRENT_DATE, month) AS close_month,
              EXTRACT(ISOWEEK FROM CURRENT_DATE) AS num_week
            FROM 
              t_all;
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ` AS
            
            WITH 
            t_econ AS
            (
              SELECT * EXCEPT (close_year, num_week, State, DATE, year_added, year_built, state_prov_short, close_month,
              original_listing_category, property_type),
              l.state_prov_short
              FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec` AS l
              LEFT JOIN `kw-data-science-playgorund.price_prediction.state_population` USING (state_prov_short)
                LEFT JOIN `kw-data-science-playgorund.price_prediction.temperature_tornado` USING (state_prov_short)
                  LEFT JOIN `kw-data-science-playgorund.price_prediction.mortgage_not_seasonality_upd` AS m USING (close_year, num_week)
                    LEFT JOIN `kw-data-science-playgorund.price_prediction.case_shiller_hpi_not_seasonality_upd` AS cs 
                    ON (l.close_month = DATE(cs.close_month))
                       LEFT JOIN `kw-data-science-playgorund.price_prediction.unemployment_rate_upd` AS u 
                       ON l.close_month = DATE(u.close_month) AND l.state_prov_short = u.state_prov_short
            )
            
            SELECT 
                * EXCEPT (key, name, geometry)
            FROM t_econ, `kw-data-science-playgorund.price_prediction.metroareas_nielsen_dma_boundaries` AS b
            WHERE ST_INTERSECTS(t_econ.list_geo, b.geometry);
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.schools_us`
            --OPTIONS(description='''description''') 
            AS
            WITH t_school AS
            (
            SELECT 
                *
            FROM 
                `kw-data-science-playgorund.price_prediction.schools`
            ),
            
            t_listings_postal AS 
            (
            SELECT 
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ` AS l, t_school
            WHERE ST_INTERSECTS(l.list_geo, t_school.geom)
            ),
            
            t_elementary AS
            (
            SELECT 
                *
            FROM t_listings_postal
            WHERE ed_level IN ('P', 'PM', 'E', 'O', 'PMH')
            AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    pctile_y1 DESC -- distance
                ) = 1
            ),
            
            t_elementary_nan AS 
            (
            SELECT 
                l.listing_id,
                l.list_geo,
                l.state_prov_short,
                l.postal_code,
                l.postal_code_short,
                l.address,
                l.city_long,
                t_elementary.distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ` AS l
                LEFT JOIN t_elementary USING (listing_id)
            WHERE t_elementary.distance IS NULL
            ),
            
            t_elementary_state AS 
            (
            SELECT
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM t_elementary_nan 
                LEFT JOIN t_school ON t_elementary_nan.state_prov_short = t_school.state_prov
            ),
            
            t_elementary_add AS
            (
            SELECT 
                *
            FROM t_elementary_state
            WHERE ed_level IN ('P', 'PM', 'E', 'O', 'PMH')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    distance
                ) = 1
            ),
            
            t_elementary_final AS 
            (
            SELECT *
            FROM t_elementary
            UNION ALL
            SELECT *
            FROM t_elementary_add
            ),
            
            t_middle AS
            (
            SELECT 
                *
            FROM t_listings_postal
            WHERE ed_level IN ('M', 'PM', 'MH', 'O', 'PMH', 'S')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    pctile_y1 DESC -- distance
                ) = 1
            ),
            
            t_middle_nan AS 
            (
            SELECT 
                l.listing_id,
                l.list_geo,
                l.state_prov_short,
                l.postal_code,
                l.postal_code_short,
                l.address,
                l.city_long,
                t_middle.distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ` AS l
                LEFT JOIN t_middle USING (listing_id)
            WHERE t_middle.distance IS NULL
            ),
            
            t_middle_state AS 
            (
            SELECT
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM t_middle_nan 
                LEFT JOIN t_school ON t_middle_nan.state_prov_short = t_school.state_prov
            ),
            
            t_middle_add AS
            (
            SELECT 
                *
            FROM t_middle_state
            WHERE ed_level IN ('M', 'PM', 'MH', 'O', 'PMH', 'S')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    distance
                ) = 1
            ),
            
            t_middle_final AS 
            (
            SELECT *
            FROM t_middle
            UNION ALL
            SELECT *
            FROM t_middle_add
            ),
            
            t_high AS
            (
            SELECT 
                *
            FROM t_listings_postal
            WHERE ed_level IN ('H', 'MH', 'O', 'PMH', 'S')
                AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    pctile_y1 DESC -- distance
                ) = 1
            ),
            
            t_high_nan AS 
            (
            SELECT 
                l.listing_id,
                l.list_geo,
                l.state_prov_short,
                l.postal_code,
                l.postal_code_short,
                l.address,
                l.city_long,
                t_high.distance
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ` AS l
                LEFT JOIN t_high USING (listing_id)
            WHERE t_high.distance IS NULL
            
            ),
            
            t_high_state AS 
            (
            SELECT
                listing_id,
                state_prov_short,
                postal_code,
                postal_code_short,
                address,
                city_long,
                t_school.obj_id, 
                t_school.obj_name, 
                t_school.obj_subtcd, 
                t_school.ed_level,
                t_school.city AS school_city,
                t_school.pup_teach, 
                t_school.pov_lev, 
                t_school.pctile_y1,
                ST_DISTANCE(list_geo, lat_lon) AS distance
            FROM t_high_nan 
                LEFT JOIN t_school ON t_high_nan.state_prov_short = t_school.state_prov
            ),
            
            t_high_add AS
            (
            SELECT 
                *
            FROM t_high_state
            WHERE ed_level IN ('H', 'MH', 'O', 'PMH', 'S')
                 AND obj_subtcd = 'PUB'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY
                    listing_id -- unique listing identifier
                ORDER BY
                    distance
                ) = 1
            ),
            
            t_high_final AS 
            (
            SELECT *
            FROM t_high
            UNION ALL
            SELECT *
            FROM t_high_add
            )
            
            SELECT 
            listing_id,
            ROUND(t_elementary_final.distance/1000, 2) AS elem_distance,
            ROUND(t_elementary_final.pup_teach, 2) AS elem_pup_teach,
            ROUND(t_elementary_final.pov_lev, 2) AS elem_pov_lev,
            ROUND(t_elementary_final.pctile_y1, 2) AS elem_pctile_y1,
            ROUND(t_middle_final.distance/1000, 2) AS middle_distance,
            ROUND(t_middle_final.pup_teach, 2) AS middle_pup_teach,
            ROUND(t_middle_final.pov_lev, 2) AS middle_pov_lev,
            ROUND(t_middle_final.pctile_y1, 2) AS middle_pctile_y1,
            ROUND(t_high_final.distance/1000, 2) AS high_distance,
            ROUND(t_high_final.pup_teach, 2) AS high_pup_teach,
            ROUND(t_high_final.pov_lev, 2) AS high_pov_lev,
            ROUND(t_high_final.pctile_y1, 2) AS high_pctile_y1
            
            FROM t_elementary_final FULL JOIN t_middle_final USING (listing_id) FULL JOIN t_high_final USING (listing_id)
            ORDER BY listing_id;
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ_school` AS
            
            SELECT * EXCEPT (full_bathroom_count, total_bathroom_count),
                CASE WHEN full_bathroom_count IS NULL 
                     THEN total_bathroom_count ELSE full_bathroom_count END AS full_bathroom_count,
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ`
                INNER JOIN `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.schools_us` USING (listing_id);
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_prophet_raw`  AS
            WITH
            t_listings AS 
            (
            SELECT * EXCEPT (listing_dt, property_subtype, list_geo, polygon, dma_name,  original_listing_price, lot_size_area),
                    CASE WHEN lot_size_area IS NULL THEN 0 ELSE lot_size_area END AS lot_size_area,
                    EXTRACT(ISOYEAR FROM CURRENT_DATE) AS close_year,
                    EXTRACT(MONTH FROM CURRENT_DATE) AS close_month,
                    EXTRACT(QUARTER FROM CURRENT_DATE)  AS quarter
            FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us_bg_prec_econ_school`
            WHERE TRUE
                AND city_long IS NOT NULL
                AND state_prov_short IS NOT NULL
                AND living_area  IS NOT NULL
                AND total_bedroom_count  IS NOT NULL
                AND full_bathroom_count  IS NOT NULL
                AND garage  IS NOT NULL
                AND parking  IS NOT NULL
                AND has_security_system  IS NOT NULL
                AND has_fireplace  IS NOT NULL
                AND has_patio  IS NOT NULL
                AND has_pool  IS NOT NULL
                AND has_porch  IS NOT NULL
                AND is_water_front  IS NOT NULL
                AND is_new_construction  IS NOT NULL
                AND has_mother_in_law IS NOT NULL
                AND has_sauna IS NOT NULL
                AND has_skylight IS NOT NULL
                AND has_vaulted_ceiling IS NOT NULL
                AND has_wet_bar IS NOT NULL
                AND is_cable_ready IS NOT NULL
                AND is_wired IS NOT NULL
                AND has_hot_tub_spa IS NOT NULL
                AND has_barbecue_area IS NOT NULL
                AND has_deck IS NOT NULL
                AND has_disabled_access IS NOT NULL
                AND has_dock IS NOT NULL
                AND has_garden IS NOT NULL
                AND has_gated_entry IS NOT NULL
                AND has_green_house IS NOT NULL
                AND has_pond IS NOT NULL
                AND has_rv_parking  IS NOT NULL
                AND has_sports_court  IS NOT NULL
                AND has_sprinkler_system IS NOT NULL
                AND building_years_old IS NOT NULL
                AND virtual_tour_ind IS NOT NULL
                AND photo_count IS NOT NULL
                AND landarea IS NOT NULL
                AND waterarea IS NOT NULL
                AND population_density IS NOT NULL
                AND population_grow IS NOT NULL
                AND population_estimate IS NOT NULL
                AND population_median_age IS NOT NULL
                AND percent_white_population IS NOT NULL
                AND percent_black_population IS NOT NULL
                AND percent_asian_population IS NOT NULL
                AND percent_other_race_population IS NOT NULL
                AND contract_rent_percent_median IS NOT NULL
                AND household_income_median IS NOT NULL
                AND home_value_median IS NOT NULL
                AND state_population IS NOT NULL
                AND annual_temperature IS NOT NULL
                AND tornado_index IS NOT NULL
                AND unemployment_rate IS NOT NULL
                AND case_shiller IS NOT NULL
                AND home_price_index IS NOT NULL
                AND mortgage IS NOT NULL      
                AND elem_distance IS NOT NULL 
                AND elem_pup_teach IS NOT NULL
                AND elem_pov_lev IS NOT NULL 
                AND elem_pctile_y1 IS NOT NULL 
                AND middle_distance IS NOT NULL 
                AND middle_pup_teach IS NOT NULL 
                AND middle_pov_lev IS NOT NULL
                AND middle_pctile_y1 IS NOT NULL 
                AND high_distance IS NOT NULL
                AND high_pup_teach IS NOT NULL
                AND high_pov_lev IS NOT NULL
                AND high_pctile_y1 IS NOT NULL
            ),
            
            t_prophet_geoid_quarter AS 
            (
              SELECT * EXCEPT(close_dt), 
                EXTRACT(QUARTER FROM close_dt) AS quarter,
                EXTRACT(YEAR FROM close_dt) AS close_year
              FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.all_states_prophet_geoid_quarter` 
              QUALIFY ROW_NUMBER() OVER (PARTITION BY GEOID, state_prov_short, close_year, quarter ORDER BY close_dt) = 1
            ),
            
            t_prophet_state AS 
            (
              SELECT * EXCEPT(close_dt),
                EXTRACT(YEAR FROM close_dt) AS close_year,
                EXTRACT(MONTH FROM close_dt) AS close_month
              FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.trend_by_state`
              QUALIFY ROW_NUMBER() OVER (PARTITION BY state_prov_short, close_year, close_month ORDER BY close_dt) = 1
            ),
            
            t_add_diff AS 
            (
              SELECT * 
              FROM t_listings
                  LEFT JOIN t_prophet_geoid_quarter USING(GEOID, state_prov_short, close_year, quarter)
                    LEFT JOIN t_prophet_state USING(state_prov_short, close_year, close_month)
            )
            
            SELECT
                * EXCEPT(lat, lon, close_year, close_month, quarter),
                lat AS latitude, 
                lon AS longitude,
                CURRENT_DATE() AS close_dt
            FROM t_add_diff;
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_prophet`  AS
            
            WITH 
            t_quarter AS
            (
              SELECT GEOID, close_dt, state_prov_short, trend_geoid_quarter, yearly_geoid_quarter
              FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.all_states_prophet_geoid_quarter`
              WHERE close_dt < CURRENT_TIMESTAMP()
              QUALIFY ROW_NUMBER() OVER(
                PARTITION BY
                 GEOID
                ORDER BY
                close_dt DESC
              ) = 1
            ORDER BY GEOID
            ),
            
            t_yearly AS
            (
              SELECT  state_prov_short, close_dt, yearly_state
              FROM `kw-data-science-playgorund.avm_prophet_us_{housing_type}.trend_by_state`
              WHERE close_dt < CURRENT_TIMESTAMP()
              QUALIFY ROW_NUMBER() OVER(
                PARTITION BY
                 state_prov_short
                ORDER BY
                close_dt DESC
              ) = 1
            ORDER BY state_prov_short
            ),
            
            t_quarter_null AS
            (
              SELECT listing_id, GEOID, trend_geoid_quarter, yearly_geoid_quarter
              FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_prophet_raw`
              WHERE trend_geoid_quarter IS NULL
            ),
            
            t_yearly_null AS
            (
              SELECT listing_id, state_prov_short, yearly_state
              FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_prophet_raw`
              WHERE yearly_state IS NULL
            ),
            
            t_quarter_null_filled AS
            (
              SELECT t_quarter_null.* EXCEPT(trend_geoid_quarter, yearly_geoid_quarter), 
                      t_quarter.trend_geoid_quarter AS trend_geoid_quarter_new, 
                      t_quarter.yearly_geoid_quarter AS yearly_geoid_quarter_new
              FROM t_quarter_null
               LEFT JOIN t_quarter USING(GEOID)
            ),
            
            t_yearly_null_filled AS
            (
              SELECT t_yearly_null.* EXCEPT (yearly_state), 
                    t_yearly.yearly_state AS yearly_state_new
              FROM t_yearly_null
                LEFT JOIN t_yearly USING(state_prov_short)
            ),
            
            t_all_not_null AS
            (
              SELECT * 
              FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_prophet_raw`
                  LEFT JOIN t_quarter_null_filled USING (listing_id, GEOID)
                    LEFT JOIN t_yearly_null_filled USING (listing_id, state_prov_short)
            )
            
            SELECT * EXCEPT (trend_geoid_quarter_new, yearly_geoid_quarter_new, yearly_state, yearly_state_new,  
            trend_geoid_quarter, yearly_geoid_quarter),
              
              CASE WHEN trend_geoid_quarter_new IS NULL AND trend_geoid_quarter IS NOT NULL THEN trend_geoid_quarter 
                   WHEN trend_geoid_quarter IS NULL AND trend_geoid_quarter_new IS NOT NULL THEN trend_geoid_quarter_new
                   WHEN trend_geoid_quarter_new IS NOT NULL AND trend_geoid_quarter IS NOT NULL THEN trend_geoid_quarter
                   ELSE 0 END AS trend_geoid_quarter,
              CASE WHEN yearly_geoid_quarter_new IS NULL AND yearly_geoid_quarter IS NOT NULL THEN yearly_geoid_quarter 
                   WHEN yearly_geoid_quarter IS NULL AND yearly_geoid_quarter_new IS NOT NULL THEN yearly_geoid_quarter_new
                   WHEN yearly_geoid_quarter_new IS NOT NULL AND yearly_geoid_quarter IS NOT NULL THEN yearly_geoid_quarter
                   ELSE 0 END AS yearly_geoid_quarter,
              CASE WHEN yearly_state IS NULL AND yearly_state_new IS NOT NULL THEN yearly_state_new 
                  WHEN yearly_state_new IS NULL AND yearly_state IS NOT NULL THEN yearly_state
                  WHEN yearly_state IS NOT NULL AND yearly_state_new IS NOT NULL THEN yearly_state
                  ELSE 0 END AS yearly_state,
            FROM t_all_not_null;
            
            
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_last`  AS
            
              SELECT * EXCEPT(GEOID, city_long, address, mls_id, mls_number, metropolitan_area_name, cbsa,
                    postal_code, postal_code_short, state_prov_short, current_listing_price, sa_mls_id)
              FROM `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.{housing_type}_us_prophet`;

        """

            results.append(
                pipeline
                | f"Process {housing_type}" >> ProcessPropertyType(query_template=QUERY_TEMPLATE,
                                                                   housing_type=housing_type,
                                                                   additional_query=ADDITIONAL_QUERY)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python us_reduced_aggregation_dataflow.py --worker_machine_type=e2-standard-4

# ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
#
# curl -X POST \
#     -H "Authorization: Bearer $ACCESS_TOKEN" \
#     -H "Content-Type: application/json" \
#     -d '{
#         "jobName": "us-aggregation-pipeline",
#         "parameters": {},
#         "environment": {
#             "tempLocation": "gs://kw-ds-vertex-ai-test/temp",
#             "zone": "us-east1-b"
#         }
#     }' \
#     "https://dataflow.googleapis.com/v1b3/projects/kw-data-science-playgorund/locations/us-east1/templates:launch?gcsPath=gs://kw-ds-vertex-ai-test/templates/us_aggregation_template"
