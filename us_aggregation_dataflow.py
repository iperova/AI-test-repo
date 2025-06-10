

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
    def __init__(self, housing_type, prop_subtypes, query_template, price, additional_query):
        self.housing_type = housing_type
        self.prop_subtypes = prop_subtypes
        self.query_template = query_template
        self.price = price
        self.additional_query = additional_query


    def expand(self, pcoll):
        # import pandas as pd
        query = self.query_template.format(housing_type=self.housing_type, prop_subtypes=self.prop_subtypes,
                                           query_template=self.query_template, price=self.price,
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
        job_name = 'us-aggregation-pipeline',
        temp_location='gs://kw-ds-vertex-ai-test/temp',
        staging_location='gs://kw-ds-vertex-ai-test/staging',
        template_location='gs://kw-ds-vertex-ai-test/templates/us_aggregation_template',
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
                ADDITIONAL_QUERY = """
                                CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_houses_new_source.houses_us_2022_prophet_reduced`  AS
                                SELECT *
                                FROM `kw-data-science-playgorund.avm_us_houses_new_source.houses_us_2019_prophet` TABLESAMPLE SYSTEM (25 PERCENT);
                                """
            elif housing_type == 'condos':
                prop_list = ['condominium', 'townhouse', 'apartment', 'single family attached', 'duplex',
                             'triplex', 'quadruplex', 'stock cooperative', 'adult community', 'vacation home']
                PROP_SUBTYPES = str(prop_list).replace('[', '').replace(']', '')
                PRICE = 55000
                ADDITIONAL_QUERY =''
            else:
                logger.info("No housing type to process")

            QUERY_TEMPLATE = f"""
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_new_source.us_2019` AS
            WITH 
            t_listings_origin AS
            (
            SELECT
              *
            FROM `data-lake-prod-pres-2c917b19.property.listings__current_details_tbl` AS l
            WHERE 
                listing_address.postal_code NOT IN ('FL', 'RI 02', 'GA', '125L7', 'Ludow', 'al', '4817-', 'Selec', '1260.', 'ia', 'na48', 
                'javas', 'AL', '3399.', '4834-', 'ny', 'RI', '783.8', '7784-', '_4807', 'The 7', 'Drive', '167-3', '`80`7', 
                '321y6', 'Al', 'Other', 'TX', '5233+', '-')
              AND listing_address.postal_code IS NOT NULL
              -- AND LOWER(property_address.country_short) IN ('us', 'usa')
              AND is_deleted = False
              AND LOWER(property_type) IN ('residential', 'residential income', 'common interest', 'residental')
              AND LOWER(property_subtype) IN ({PROP_SUBTYPES})
              AND listing_category NOT IN ('For Rent', 'Rent', 'Rented/Leased')
              AND original_listing_category NOT IN ('For Rent', 'Rent', 'Rented/Leased')   
              AND listing_dt >= '2013-01-01'
              AND close_dt >= '2019-01-01' 
              AND year_built <= EXTRACT (YEAR from CURRENT_DATE() + INTERVAL 1 YEAR)
              AND close_price >= {PRICE}
              AND close_price <= 4000000
              AND (full_bathroom_count < 10 OR total_bathroom_count < 10)
              AND total_bedroom_count < 10
              AND year_built > 1800
              AND LOWER(listing_status) IN ('sold', 'off market')
            ),
            
            t_raw_listings AS
            (
            SELECT
              l.listing_id,
              l.sa_mls_id,
              l.mls_number,
              l.mls_id,
              l.listing_status,
              EXTRACT(YEAR FROM l.listing_dt) AS year_added,
              EXTRACT(YEAR FROM (SAFE.PARSE_DATE('%Y', Cast(l.year_built AS String)))) AS year_built,
              CAST(l.close_price AS INT64) AS close_price,
              CAST(l.current_listing_price AS INT64) AS current_listing_price,
              CAST(l.original_listing_price AS INT64) AS original_listing_price,
              LOWER(l.listing_address.address) AS address,
              LOWER(l.property_address.city_long) AS city_long,
              l.property_address.state_prov_short,
              l.listing_address.postal_code,
              CASE WHEN length(l.listing_address.postal_code) > 5 
                  THEN SPLIT(l.listing_address.postal_code, '-')[OFFSET(0)] 
                  ELSE l.listing_address.postal_code END as postal_code_short,
              l.property_address.coordinates_gp.longitude AS lon,
              l.property_address.coordinates_gp.latitude AS lat,
              l.listing_dt,
              l.close_dt,
              l.original_listing_category,
              l.property_type,
              l.property_subtype,
              CASE WHEN LOWER(l.living_area_units) IN ('square meters', 'sqm', 'sm') 
                   THEN CAST(CAST(l.living_area AS FLOAT64) * 10.76391041671 AS INT64)
                   ELSE CAST(l.living_area AS INT64) END AS living_area,
              CASE WHEN LOWER(l.lot_size_units) = 'sqm' 
                   THEN CAST(l.lot_size_area AS FLOAT64) * 0.000247105
                   WHEN LOWER(l.lot_size_units) IN ('sqft', 'square feet') 
                   THEN CAST(l.lot_size_area AS FLOAT64) * 0.0000229568
                   ELSE CAST(l.lot_size_area AS FLOAT64) END AS lot_size_area,
              CAST(l.total_bedroom_count AS INT64) AS total_bedroom_count,
              CAST(l.full_bathroom_count AS INT64) AS full_bathroom_count,
              CAST(l.total_bathroom_count AS INT64) AS total_bathroom_count,
              l.listing_description,
              CASE WHEN l.building_structure.has_garage = True THEN 1 ELSE 0 END AS garage,
              CASE WHEN l.building_structure.has_parking = True THEN 1 ELSE 0 END AS parking,
              CASE WHEN l.building_structure.interior_features.has_security_system = True THEN 1 ELSE 0 END AS has_security_system,
              CASE WHEN l.building_structure.interior_features.has_fireplace = True THEN 1 ELSE 0 END AS has_fireplace,
              CASE WHEN l.building_structure.exterior_features.has_patio = True THEN 1 ELSE 0 END AS has_patio,
              CASE WHEN l.building_structure.exterior_features.has_pool = True THEN 1 ELSE 0 END AS has_pool,
              CASE WHEN l.building_structure.exterior_features.has_porch = True THEN 1 ELSE 0 END AS has_porch,
              CASE WHEN l.building_structure.exterior_features.is_water_front = True THEN 1 ELSE 0 END AS is_water_front,
              CASE WHEN l.building_structure.is_new_construction = True THEN 1 ELSE 0 END AS is_new_construction,
              CASE WHEN l.building_structure.interior_features.is_cable_ready = True THEN 1 ELSE 0 END AS is_cable_ready,
              CASE WHEN l.building_structure.interior_features.has_mother_in_law = True THEN 1 ELSE 0 END AS has_mother_in_law,
              CASE WHEN l.building_structure.interior_features.has_sauna = True THEN 1 ELSE 0 END AS has_sauna,
              CASE WHEN l.building_structure.interior_features.has_skylight = True THEN 1 ELSE 0 END AS has_skylight,
              CASE WHEN l.building_structure.interior_features.has_vaulted_ceiling = True THEN 1 ELSE 0 END AS has_vaulted_ceiling,
              CASE WHEN l.building_structure.interior_features.has_wet_bar = True THEN 1 ELSE 0 END AS has_wet_bar,
              CASE WHEN l.building_structure.interior_features.is_wired = True THEN 1 ELSE 0 END AS is_wired,
              CASE WHEN l.building_structure.exterior_features.has_hot_tub_spa = True THEN 1 ELSE 0 END AS has_hot_tub_spa,
              CASE WHEN l.building_structure.exterior_features.has_barbecue_area = True THEN 1 ELSE 0 END AS has_barbecue_area,
              CASE WHEN l.building_structure.exterior_features.has_deck = True THEN 1 ELSE 0 END AS has_deck,
              CASE WHEN l.building_structure.exterior_features.has_disabled_access = True THEN 1 ELSE 0 END AS has_disabled_access,
              CASE WHEN l.building_structure.exterior_features.has_dock = True THEN 1 ELSE 0 END AS has_dock,
              CASE WHEN l.building_structure.exterior_features.has_garden = True THEN 1 ELSE 0 END AS has_garden,
              CASE WHEN l.building_structure.exterior_features.has_gated_entry = True THEN 1 ELSE 0 END AS has_gated_entry,
              CASE WHEN l.building_structure.exterior_features.has_green_house = True THEN 1 ELSE 0 END AS has_green_house,
              CASE WHEN l.building_structure.exterior_features.has_pond = True THEN 1 ELSE 0 END AS has_pond,    
              CASE WHEN l.building_structure.exterior_features.has_rv_parking = True THEN 1 ELSE 0 END AS has_rv_parking,
              CASE WHEN l.building_structure.exterior_features.has_sports_court = True THEN 1 ELSE 0 END AS has_sports_court,
              CASE WHEN l.building_structure.exterior_features.has_sprinkler_system = True THEN 1 ELSE 0 END AS has_sprinkler_system,  
              vt.virtual_tour_url,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.roof_types) AS x) AS roof_types,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.utilities) AS x) AS utilities,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.water_source) AS x) AS water_source,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.parking_features) AS x) AS parking_features,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.construction_materials) AS x) AS construction_materials,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.flooring) AS x) AS flooring
            FROM 
              t_listings_origin AS l, UNNEST(virtual_tours) vt
            WHERE TRUE
              AND living_area > 0
              AND living_area < 10000
            ),
            
            t_add_dates_ind AS
            (
              SELECT 
                * EXCEPT(lot_size_area),
                year_added - year_built AS building_years_old,
                -- DATE_DIFF(close_dt, listing_dt, day) AS day_on_market,
                EXTRACT(YEAR FROM close_dt) AS close_year,
                EXTRACT(MONTH FROM close_dt)  AS close_month,
                CASE WHEN virtual_tour_url IS NOT NULL THEN 1 ELSE 0 END AS virtual_tour_ind,
                CASE WHEN lot_size_area IS NULL THEN 0 ELSE lot_size_area END AS lot_size_area
              FROM 
                t_raw_listings
              WHERE TRUE 
               AND DATE_DIFF(close_dt, listing_dt, day) > 0 
               AND (lot_size_area IS NULL OR lot_size_area BETWEEN 0 AND 20)
            ),
            
            t_raw_listings_photo AS
            (
            SELECT DISTINCT
              l.listing_id,
              MAX(ARRAY_LENGTH(photo_data)) AS photo_count
            FROM 
              t_listings_origin AS l, UNNEST(photo_data)           
            GROUP BY 1
            ),
            
            t_final AS
            (
            SELECT 
              * EXCEPT(close_year, close_month),
            
              -- Roof types
              CASE WHEN roof_types IS NULL THEN 1 ELSE 0 END AS no_roof_type,
              CASE WHEN roof_types LIKE ('%shingle%') OR roof_types LIKE ('%shngl%') THEN 1 ELSE 0 END AS shingle_roof_type,
              CASE WHEN roof_types LIKE ('%tile%') THEN 1 ELSE 0 END AS tile_roof_type,
              CASE WHEN roof_types LIKE ('%comp%') THEN 1 ELSE 0 END AS composition_roof_type, 
              CASE WHEN roof_types LIKE ('%asphalt%') THEN 1 ELSE 0 END AS asphalt_roof_type,
              CASE WHEN roof_types LIKE ('%glass%') OR roof_types LIKE ('%fiberglass%') THEN 1 ELSE 0 END AS fiberglass_roof_type,
              CASE WHEN roof_types LIKE ('%metal%') OR roof_types LIKE ('%aluminum%') THEN 1 ELSE 0 END AS metal_roof_type,
              CASE WHEN roof_types LIKE ('%architec%') THEN 1 ELSE 0 END AS architectural_roof_type,
              CASE WHEN roof_types LIKE ('%pitched%') THEN 1 ELSE 0 END AS pitched_roof_type,
              CASE WHEN roof_types LIKE ('%flat%') THEN 1 ELSE 0 END AS flat_roof_type,
              CASE WHEN roof_types LIKE ('%concrete%') THEN 1 ELSE 0 END AS concrete_roof_type,
              CASE WHEN roof_types LIKE ('%slate%') THEN 1 ELSE 0 END AS slate_roof_type,
              CASE WHEN roof_types LIKE ('%wood%') OR roof_types LIKE ('%shake%') THEN 1 ELSE 0 END AS wood_roof_type,
            
              --Water source
              CASE WHEN water_source IS NULL OR water_source ='none' THEN 1 ELSE 0 END AS no_water_source,
              CASE WHEN (water_source LIKE('%public%') OR water_source LIKE('%city%') OR water_source LIKE('%community%')
                     OR water_source LIKE('%municipal%') OR water_source LIKE('%county%') OR water_source LIKE('%connected%')
                     OR water_source LIKE('%lake%') OR water_source LIKE('%river%') OR water_source LIKE('%central%')
                     OR water_source LIKE('%hcud%')) AND water_source NOT LIKE('%not connected%')
                     THEN 1 ELSE 0 END AS public_water,
              CASE WHEN water_source LIKE('%private%') OR water_source LIKE('%water company%') THEN 1 ELSE 0 END AS private_water,
              CASE WHEN water_source LIKE('%well%') OR water_source LIKE('%irrigation%') OR water_source LIKE('%cistern%')
                     OR utilities LIKE('%community well%') THEN 1 ELSE 0 END AS well_water,
              CASE WHEN water_source LIKE('%not connected%') OR water_source LIKE('%notconnected%') 
                     OR water_source LIKE('%water at street%') OR utilities LIKE('%water: not connected%') 
                     OR utilities LIKE('%water not connected%') OR utilities LIKE('%water not available%') 
                     OR utilities LIKE('%water: not available%') OR utilities LIKE('%waternotavailable%') 
                     THEN 1 ELSE 0 END AS not_connected_water,
              CASE WHEN water_source LIKE('%culinary%') THEN 1 ELSE 0 END AS culinary_water,
            
              -- Utilities
              CASE WHEN utilities LIKE('%underground%') THEN 1 ELSE 0 END AS underground_utilities,
            
              -- Sewer
              CASE WHEN utilities LIKE('%sewer not%') OR utilities LIKE('%sewer: not%') 
                     OR utilities LIKE('%sewernot%') THEN 0 ELSE 1 END AS sewer, 
            
              -- Gas
              CASE WHEN utilities LIKE('%gasnot%') OR utilities LIKE('%gas not%') 
                     OR utilities LIKE('%gas: not %') THEN 0 ELSE 1 END AS natural_gas,
            
              -- Parking features
              CASE WHEN parking_features IS NULL OR parking_features = 'no' OR parking_features = 'none' 
                    OR parking_features = 'features: none' THEN 1 ELSE 0 END AS no_parking_features,
              CASE WHEN parking_features LIKE('%attached%') AND (parking_features NOT LIKE('%attached: no%') 
                    OR parking_features NOT LIKE('%attached:no%') OR parking_features NOT LIKE ('%attached, no%') 
                    OR parking_features NOT LIKE ('%attached no%')) THEN 1 ELSE 0 END AS attached_parking,      
              CASE WHEN parking_features LIKE('%detached%') AND (parking_features NOT LIKE('%detached: no%') 
                    OR parking_features NOT LIKE('%detached:no%') OR parking_features NOT LIKE ('%detached, no%') 
                    OR parking_features NOT LIKE ('%detached no%')) THEN 1 ELSE 0 END AS detached_parking,
              CASE WHEN parking_features LIKE('%garage%') AND (parking_features NOT LIKE('%no garage%') 
                    OR parking_features NOT LIKE('%non garage%') OR parking_features NOT LIKE ('%nogarage%') 
                    OR parking_features NOT LIKE ('%garage no%') OR parking_features NOT LIKE ('%garage: no%')) 
                    THEN 1 ELSE 0 END AS garage_parking, 
              CASE WHEN parking_features LIKE('%2%') OR parking_features LIKE ('%two%')
                    THEN 1 ELSE 0 END AS two_car_parking,
              CASE WHEN parking_features LIKE('%street%') OR parking_features LIKE ('%parking lot%')
                    OR parking_features LIKE('%parking pad%') OR parking_features LIKE ('%paved%')
                    THEN 1 ELSE 0 END AS street_parking,
              CASE WHEN parking_features LIKE('%door opener%') OR parking_features LIKE ('%dooropener%')
                    THEN 1 ELSE 0 END AS door_opener_parking,
              CASE WHEN parking_features LIKE('%guest%') THEN 1 ELSE 0 END AS guest_parking,
              CASE WHEN parking_features LIKE('%driveway%') THEN 1 ELSE 0 END AS driveway_parking,
              CASE WHEN parking_features LIKE('%carport%') THEN 1 ELSE 0 END AS carport_parking,
              CASE WHEN parking_features LIKE('%assigned%') THEN 1 ELSE 0 END AS assigned_parking,
            
              -- Construction materials
              CASE WHEN construction_materials IS NULL THEN 1 ELSE 0 END AS no_construction_materials,
              CASE WHEN construction_materials LIKE('%vinyl%') THEN 1 ELSE 0 END AS vinyl_materials,
              CASE WHEN construction_materials LIKE('%brick%') THEN 1 ELSE 0 END AS brick_materials,
              CASE WHEN construction_materials LIKE('%aluminum%') THEN 1 ELSE 0 END AS aluminum_materials,
              CASE WHEN construction_materials LIKE('%wood%') OR construction_materials LIKE('%stick%') THEN 1 ELSE 0 END AS wood_materials,
              CASE WHEN construction_materials LIKE('%frame%') THEN 1 ELSE 0 END AS frame_materials,
              CASE WHEN construction_materials LIKE('%stucco%') THEN 1 ELSE 0 END AS stucco_materials,
              CASE WHEN construction_materials LIKE('%site built%') THEN 1 ELSE 0 END AS site_built_materials,
              CASE WHEN construction_materials LIKE('%block%') THEN 1 ELSE 0 END AS block_materials,
              CASE WHEN construction_materials LIKE('%concrete%') THEN 1 ELSE 0 END AS concrete_materials,
              CASE WHEN construction_materials LIKE('%cement%') THEN 1 ELSE 0 END AS cement_materials,
              CASE WHEN construction_materials LIKE('%stone%') THEN 1 ELSE 0 END AS stone_materials,
            
              -- Floorings
              CASE WHEN flooring IS NULL THEN 1 ELSE 0 END AS no_floorings_info,
              CASE WHEN flooring LIKE('%carpet%') THEN 1 ELSE 0 END AS carpet_floorings,
              CASE WHEN flooring LIKE('%vinyl%') THEN 1 ELSE 0 END AS vinyl_floorings,
              CASE WHEN flooring LIKE('%laminate%') THEN 1 ELSE 0 END AS laminate_floorings,
              CASE WHEN flooring LIKE('%tile%') THEN 1 ELSE 0 END AS tile_floorings,
              CASE WHEN flooring LIKE('%wood%') AND flooring NOT LIKE('%hardwood%') THEN 1 ELSE 0 END AS wood_floorings,
              CASE WHEN flooring LIKE('%hardwood%') THEN 1 ELSE 0 END AS hardwood_floorings,
              CASE WHEN flooring LIKE('%linoleum%') THEN 1 ELSE 0 END AS linoleum_floorings,
              CASE WHEN flooring LIKE('%ceramic%') THEN 1 ELSE 0 END AS ceramic_floorings
            
            FROM t_add_dates_ind 
              LEFT JOIN t_raw_listings_photo USING (listing_id) 
            )
            
            SELECT
              *,
              CASE WHEN no_roof_type = 0 
                    AND shingle_roof_type = 0 
                    AND tile_roof_type = 0
                    AND composition_roof_type = 0
                    AND asphalt_roof_type = 0 
                    AND metal_roof_type = 0
                    AND fiberglass_roof_type = 0 
                    AND architectural_roof_type = 0
                    AND pitched_roof_type = 0
                    AND flat_roof_type = 0
                    AND concrete_roof_type = 0
                    AND slate_roof_type = 0
                    AND wood_roof_type = 0
                    THEN 1 ELSE 0 END AS other_roof_type,
              CASE WHEN no_water_source = 0
                    AND public_water = 0
                    AND private_water = 0
                    AND well_water = 0
                    AND not_connected_water = 0
                    AND culinary_water = 0
                    THEN 1 ELSE 0 END AS other_water_source,
              CASE WHEN no_construction_materials = 0 
                    AND vinyl_materials = 0
                    AND brick_materials = 0
                    AND aluminum_materials = 0 
                    AND wood_materials = 0
                    AND frame_materials = 0 
                    AND stucco_materials = 0
                    AND site_built_materials = 0
                    AND block_materials = 0
                    AND concrete_materials = 0
                    AND cement_materials = 0
                    AND stone_materials = 0
                    THEN 1 ELSE 0 END AS other_construction_materials, 
              CASE WHEN no_floorings_info = 0 
                    AND carpet_floorings = 0 
                    AND vinyl_floorings = 0
                    AND laminate_floorings = 0
                    AND tile_floorings = 0 
                    AND wood_floorings = 0
                    AND hardwood_floorings = 0 
                    AND linoleum_floorings = 0
                    AND ceramic_floorings = 0
                    THEN 1 ELSE 0 END AS other_floorings    
            FROM t_final;
                      

CREATE TEMP TABLE t_raw_data AS 
(
  SELECT 
    * EXCEPT(listing_description, virtual_tour_url, roof_types, utilities, water_source, parking_features, construction_materials, flooring),
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
    t_raw_bg_data, `kw-data-science-playgorund.price_prediction.metro_areas` AS  t_metro_areas
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
FROM t_no_metro_areas_data r,  `kw-data-science-playgorund.price_prediction.metro_areas` AS m 
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
            SELECT * EXCEPT (listing_dt, property_subtype, list_geo, polygon, dma_name, original_listing_price, 
            lot_size_area),
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
            
            CREATE OR REPLACE TABLE `kw-data-science-playgorund.avm_us_{housing_type}_not_sold_new_source.us` AS
            WITH 
            t_listings_origin AS
            (
            SELECT
                *
            FROM `data-lake-prod-pres-2c917b19.property.listings__current_details_tbl` 
            WHERE 
                listing_address.postal_code NOT IN ('FL', 'RI 02', 'GA', '125L7', 'Ludow', 'al', '4817-', 'Selec', '1260.', 'ia', 'na48', 
                'javas', 'AL', '3399.', '4834-', 'ny', 'RI', '783.8', '7784-', '_4807', 'The 7', 'Drive', '167-3', '`80`7', 
                '321y6', 'Al', 'Other', 'TX', '5233+', '-')
              AND listing_address.postal_code IS NOT NULL
              -- AND LOWER(property_address.country_short) IN ('us', 'usa')
              AND is_deleted = False
              AND LOWER(property_type) IN ('residential', 'residential income', 'common interest', 'residental')
              AND LOWER(property_subtype) IN ({PROP_SUBTYPES})
              AND listing_category NOT IN ('For Rent', 'Rent', 'Rented/Leased')
              AND original_listing_category NOT IN ('For Rent', 'Rent', 'Rented/Leased')   
              AND listing_dt >= '2013-01-01'
              AND current_listing_price > 0
              AND LOWER(listing_status) IN ('active', 'pending')
            ORDER BY listing_id, mls_updated_at_ts 
            ),
            
            t_raw_not_sold AS
            (
            SELECT 
              l.listing_id,
              l.sa_mls_id,
              l.mls_number,
              l.mls_id,
              EXTRACT(YEAR FROM l.listing_dt) AS year_added,
              EXTRACT(YEAR FROM (SAFE.PARSE_DATE('%Y', Cast(l.year_built AS String)))) AS year_built,
              CAST(l.current_listing_price AS INT64) AS current_listing_price,
              CAST(l.original_listing_price AS INT64) AS original_listing_price,
              LOWER(l.listing_address.address) AS address,
              LOWER(l.property_address.city_long) AS city_long,
              l.property_address.state_prov_short,
              l.listing_address.postal_code,
              CASE WHEN length(l.listing_address.postal_code) > 5 
                  THEN SPLIT(l.listing_address.postal_code, '-')[OFFSET(0)] 
                  ELSE l.listing_address.postal_code END as postal_code_short,
              l.property_address.coordinates_gp.latitude AS lat,
              l.property_address.coordinates_gp.longitude AS lon,
              l.listing_dt,
              l.original_listing_category,
              l.property_type,
              l.property_subtype,
              CASE WHEN LOWER(l.living_area_units) IN ('square meters', 'sqm', 'sm') 
                   THEN CAST(CAST(l.living_area AS FLOAT64) * 10.76391041671 AS INT64)
                   ELSE CAST(l.living_area AS INT64) END AS living_area,
              CASE WHEN LOWER(l.lot_size_units) = 'sqm' 
                   THEN CAST(l.lot_size_area AS FLOAT64) * 0.000247105
                   WHEN LOWER(l.lot_size_units) IN ('sqft', 'square feet') 
                   THEN CAST(l.lot_size_area AS FLOAT64) * 0.0000229568
                   ELSE CAST(l.lot_size_area AS FLOAT64) END AS lot_size_area,
              CAST(l.total_bedroom_count AS INT64) AS total_bedroom_count,
              CAST(l.full_bathroom_count AS INT64) AS full_bathroom_count,
              CAST(l.total_bathroom_count AS INT64) AS total_bathroom_count,
              l.listing_description,
              CASE WHEN l.building_structure.has_garage = True THEN 1 ELSE 0 END AS garage,
              CASE WHEN l.building_structure.has_parking = True THEN 1 ELSE 0 END AS parking,
              CASE WHEN l.building_structure.interior_features.has_security_system = True THEN 1 ELSE 0 END AS has_security_system,
              CASE WHEN l.building_structure.interior_features.has_fireplace = True THEN 1 ELSE 0 END AS has_fireplace,
              CASE WHEN l.building_structure.exterior_features.has_patio = True THEN 1 ELSE 0 END AS has_patio,
              CASE WHEN l.building_structure.exterior_features.has_pool = True THEN 1 ELSE 0 END AS has_pool,
              CASE WHEN l.building_structure.exterior_features.has_porch = True THEN 1 ELSE 0 END AS has_porch,
              CASE WHEN l.building_structure.exterior_features.is_water_front = True THEN 1 ELSE 0 END AS is_water_front,
              CASE WHEN building_structure.is_new_construction = True THEN 1 ELSE 0 END AS is_new_construction,
              CASE WHEN l.building_structure.interior_features.is_cable_ready = True THEN 1 ELSE 0 END AS is_cable_ready,
              CASE WHEN l.building_structure.interior_features.has_mother_in_law = True THEN 1 ELSE 0 END AS has_mother_in_law,
              CASE WHEN l.building_structure.interior_features.has_sauna = True THEN 1 ELSE 0 END AS has_sauna,
              CASE WHEN l.building_structure.interior_features.has_skylight = True THEN 1 ELSE 0 END AS has_skylight,
              CASE WHEN l.building_structure.interior_features.has_vaulted_ceiling = True THEN 1 ELSE 0 END AS has_vaulted_ceiling,
              CASE WHEN l.building_structure.interior_features.has_wet_bar = True THEN 1 ELSE 0 END AS has_wet_bar,
              CASE WHEN l.building_structure.interior_features.is_wired = True THEN 1 ELSE 0 END AS is_wired,
              CASE WHEN l.building_structure.exterior_features.has_hot_tub_spa = True THEN 1 ELSE 0 END AS has_hot_tub_spa,
              CASE WHEN l.building_structure.exterior_features.has_barbecue_area = True THEN 1 ELSE 0 END AS has_barbecue_area,
              CASE WHEN l.building_structure.exterior_features.has_deck = True THEN 1 ELSE 0 END AS has_deck,
              CASE WHEN l.building_structure.exterior_features.has_disabled_access = True THEN 1 ELSE 0 END AS has_disabled_access,
              CASE WHEN l.building_structure.exterior_features.has_dock = True THEN 1 ELSE 0 END AS has_dock,
              CASE WHEN l.building_structure.exterior_features.has_garden = True THEN 1 ELSE 0 END AS has_garden,
              CASE WHEN l.building_structure.exterior_features.has_gated_entry = True THEN 1 ELSE 0 END AS has_gated_entry,
              CASE WHEN l.building_structure.exterior_features.has_green_house = True THEN 1 ELSE 0 END AS has_green_house,
              CASE WHEN l.building_structure.exterior_features.has_pond = True THEN 1 ELSE 0 END AS has_pond,    
              CASE WHEN l.building_structure.exterior_features.has_rv_parking = True THEN 1 ELSE 0 END AS has_rv_parking,
              CASE WHEN l.building_structure.exterior_features.has_sports_court = True THEN 1 ELSE 0 END AS has_sports_court,
              CASE WHEN l.building_structure.exterior_features.has_sprinkler_system = True THEN 1 ELSE 0 END AS has_sprinkler_system,  
              vt.virtual_tour_url,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.roof_types) AS x) AS roof_types,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.utilities) AS x) AS utilities,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.water_source) AS x) AS water_source,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.parking_features) AS x) AS parking_features,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.construction_materials) AS x) AS construction_materials,
              (SELECT STRING_AGG(x, ", ") FROM UNNEST(building_structure.flooring) AS x) AS flooring
            FROM 
              t_listings_origin AS l, UNNEST(virtual_tours) vt
            ),
            
            t_active_add_dates AS 
            (
              SELECT 
                * EXCEPT(lot_size_area),
                year_added - year_built AS building_years_old,
                EXTRACT(ISOYEAR FROM CURRENT_DATE) AS close_year,
                EXTRACT(MONTH FROM CURRENT_DATE) AS close_month,
                CASE WHEN virtual_tour_url IS NOT NULL THEN 1 ELSE 0 END AS virtual_tour_ind,
                CASE WHEN lot_size_area IS NULL THEN 0 ELSE lot_size_area END AS lot_size_area
              FROM 
                t_raw_not_sold  
            ),
            
            t_raw_listings_photo AS 
            (
            SELECT DISTINCT
              l.listing_id,
              MAX(ARRAY_LENGTH(photo_data)) AS photo_count
            FROM 
              t_listings_origin AS l, UNNEST(photo_data)       
            GROUP BY 1
            ),
            
            t_final AS
            (
            SELECT 
              * EXCEPT(close_year, close_month),
            
              -- Roof types
              CASE WHEN roof_types IS NULL THEN 1 ELSE 0 END AS no_roof_type,
              CASE WHEN roof_types LIKE ('%shingle%') OR roof_types LIKE ('%shngl%') THEN 1 ELSE 0 END AS shingle_roof_type,
              CASE WHEN roof_types LIKE ('%tile%') THEN 1 ELSE 0 END AS tile_roof_type,
              CASE WHEN roof_types LIKE ('%comp%') THEN 1 ELSE 0 END AS composition_roof_type, 
              CASE WHEN roof_types LIKE ('%asphalt%') THEN 1 ELSE 0 END AS asphalt_roof_type,
              CASE WHEN roof_types LIKE ('%glass%') OR roof_types LIKE ('%fiberglass%') THEN 1 ELSE 0 END AS fiberglass_roof_type,
              CASE WHEN roof_types LIKE ('%metal%') OR roof_types LIKE ('%aluminum%') THEN 1 ELSE 0 END AS metal_roof_type,
              CASE WHEN roof_types LIKE ('%architec%') THEN 1 ELSE 0 END AS architectural_roof_type,
              CASE WHEN roof_types LIKE ('%pitched%') THEN 1 ELSE 0 END AS pitched_roof_type,
              CASE WHEN roof_types LIKE ('%flat%') THEN 1 ELSE 0 END AS flat_roof_type,
              CASE WHEN roof_types LIKE ('%concrete%') THEN 1 ELSE 0 END AS concrete_roof_type,
              CASE WHEN roof_types LIKE ('%slate%') THEN 1 ELSE 0 END AS slate_roof_type,
              CASE WHEN roof_types LIKE ('%wood%') OR roof_types LIKE ('%shake%') THEN 1 ELSE 0 END AS wood_roof_type,
            
              --Water source
              CASE WHEN water_source IS NULL OR water_source ='none' THEN 1 ELSE 0 END AS no_water_source,
              CASE WHEN (water_source LIKE('%public%') OR water_source LIKE('%city%') OR water_source LIKE('%community%')
                     OR water_source LIKE('%municipal%') OR water_source LIKE('%county%') OR water_source LIKE('%connected%')
                     OR water_source LIKE('%lake%') OR water_source LIKE('%river%') OR water_source LIKE('%central%')
                     OR water_source LIKE('%hcud%')) AND water_source NOT LIKE('%not connected%')
                     THEN 1 ELSE 0 END AS public_water,
              CASE WHEN water_source LIKE('%private%') OR water_source LIKE('%water company%') THEN 1 ELSE 0 END AS private_water,
              CASE WHEN water_source LIKE('%well%') OR water_source LIKE('%irrigation%') OR water_source LIKE('%cistern%')
                     OR utilities LIKE('%community well%') THEN 1 ELSE 0 END AS well_water,
              CASE WHEN water_source LIKE('%not connected%') OR water_source LIKE('%notconnected%') 
                     OR water_source LIKE('%water at street%') OR utilities LIKE('%water: not connected%') 
                     OR utilities LIKE('%water not connected%') OR utilities LIKE('%water not available%') 
                     OR utilities LIKE('%water: not available%') OR utilities LIKE('%waternotavailable%') 
                     THEN 1 ELSE 0 END AS not_connected_water,
              CASE WHEN water_source LIKE('%culinary%') THEN 1 ELSE 0 END AS culinary_water,
            
              -- Utilities
              CASE WHEN utilities LIKE('%underground%') THEN 1 ELSE 0 END AS underground_utilities,
            
              -- Sewer
              CASE WHEN utilities LIKE('%sewer not%') OR utilities LIKE('%sewer: not%') 
                     OR utilities LIKE('%sewernot%') THEN 0 ELSE 1 END AS sewer, 
            
              -- Gas
              CASE WHEN utilities LIKE('%gasnot%') OR utilities LIKE('%gas not%') 
                     OR utilities LIKE('%gas: not %') THEN 0 ELSE 1 END AS natural_gas,
            
              -- Parking features
              CASE WHEN parking_features IS NULL OR parking_features = 'no' OR parking_features = 'none' 
                    OR parking_features = 'features: none' THEN 1 ELSE 0 END AS no_parking_features,
              CASE WHEN parking_features LIKE('%attached%') AND (parking_features NOT LIKE('%attached: no%') 
                    OR parking_features NOT LIKE('%attached:no%') OR parking_features NOT LIKE ('%attached, no%') 
                    OR parking_features NOT LIKE ('%attached no%')) THEN 1 ELSE 0 END AS attached_parking,      
              CASE WHEN parking_features LIKE('%detached%') AND (parking_features NOT LIKE('%detached: no%') 
                    OR parking_features NOT LIKE('%detached:no%') OR parking_features NOT LIKE ('%detached, no%') 
                    OR parking_features NOT LIKE ('%detached no%')) THEN 1 ELSE 0 END AS detached_parking,
              CASE WHEN parking_features LIKE('%garage%') AND (parking_features NOT LIKE('%no garage%') 
                    OR parking_features NOT LIKE('%non garage%') OR parking_features NOT LIKE ('%nogarage%') 
                    OR parking_features NOT LIKE ('%garage no%') OR parking_features NOT LIKE ('%garage: no%')) 
                    THEN 1 ELSE 0 END AS garage_parking, 
              CASE WHEN parking_features LIKE('%2%') OR parking_features LIKE ('%two%')
                    THEN 1 ELSE 0 END AS two_car_parking,
              CASE WHEN parking_features LIKE('%street%') OR parking_features LIKE ('%parking lot%')
                    OR parking_features LIKE('%parking pad%') OR parking_features LIKE ('%paved%')
                    THEN 1 ELSE 0 END AS street_parking,
              CASE WHEN parking_features LIKE('%door opener%') OR parking_features LIKE ('%dooropener%')
                    THEN 1 ELSE 0 END AS door_opener_parking,
              CASE WHEN parking_features LIKE('%guest%') THEN 1 ELSE 0 END AS guest_parking,
              CASE WHEN parking_features LIKE('%driveway%') THEN 1 ELSE 0 END AS driveway_parking,
              CASE WHEN parking_features LIKE('%carport%') THEN 1 ELSE 0 END AS carport_parking,
              CASE WHEN parking_features LIKE('%assigned%') THEN 1 ELSE 0 END AS assigned_parking,
            
              -- Construction materials
              CASE WHEN construction_materials IS NULL THEN 1 ELSE 0 END AS no_construction_materials,
              CASE WHEN construction_materials LIKE('%vinyl%') THEN 1 ELSE 0 END AS vinyl_materials,
              CASE WHEN construction_materials LIKE('%brick%') THEN 1 ELSE 0 END AS brick_materials,
              CASE WHEN construction_materials LIKE('%aluminum%') THEN 1 ELSE 0 END AS aluminum_materials,
              CASE WHEN construction_materials LIKE('%wood%') OR construction_materials LIKE('%stick%') THEN 1 ELSE 0 END AS wood_materials,
              CASE WHEN construction_materials LIKE('%frame%') THEN 1 ELSE 0 END AS frame_materials,
              CASE WHEN construction_materials LIKE('%stucco%') THEN 1 ELSE 0 END AS stucco_materials,
              CASE WHEN construction_materials LIKE('%site built%') THEN 1 ELSE 0 END AS site_built_materials,
              CASE WHEN construction_materials LIKE('%block%') THEN 1 ELSE 0 END AS block_materials,
              CASE WHEN construction_materials LIKE('%concrete%') THEN 1 ELSE 0 END AS concrete_materials,
              CASE WHEN construction_materials LIKE('%cement%') THEN 1 ELSE 0 END AS cement_materials,
              CASE WHEN construction_materials LIKE('%stone%') THEN 1 ELSE 0 END AS stone_materials,
            
              -- Floorings
              CASE WHEN flooring IS NULL THEN 1 ELSE 0 END AS no_floorings_info,
              CASE WHEN flooring LIKE('%carpet%') THEN 1 ELSE 0 END AS carpet_floorings,
              CASE WHEN flooring LIKE('%vinyl%') THEN 1 ELSE 0 END AS vinyl_floorings,
              CASE WHEN flooring LIKE('%laminate%') THEN 1 ELSE 0 END AS laminate_floorings,
              CASE WHEN flooring LIKE('%tile%') THEN 1 ELSE 0 END AS tile_floorings,
              CASE WHEN flooring LIKE('%wood%') AND flooring NOT LIKE('%hardwood%') THEN 1 ELSE 0 END AS wood_floorings,
              CASE WHEN flooring LIKE('%hardwood%') THEN 1 ELSE 0 END AS hardwood_floorings,
              CASE WHEN flooring LIKE('%linoleum%') THEN 1 ELSE 0 END AS linoleum_floorings,
              CASE WHEN flooring LIKE('%ceramic%') THEN 1 ELSE 0 END AS ceramic_floorings
            
            FROM t_active_add_dates 
              LEFT JOIN t_raw_listings_photo USING (listing_id) 
            )
            
            SELECT
              *,
              CASE WHEN no_roof_type = 0 
                    AND shingle_roof_type = 0 
                    AND tile_roof_type = 0
                    AND composition_roof_type = 0
                    AND asphalt_roof_type = 0 
                    AND metal_roof_type = 0
                    AND fiberglass_roof_type = 0 
                    AND architectural_roof_type = 0
                    AND pitched_roof_type = 0
                    AND flat_roof_type = 0
                    AND concrete_roof_type = 0
                    AND slate_roof_type = 0
                    AND wood_roof_type = 0
                    THEN 1 ELSE 0 END AS other_roof_type,
              CASE WHEN no_water_source = 0
                    AND public_water = 0
                    AND private_water = 0
                    AND well_water = 0
                    AND not_connected_water = 0
                    AND culinary_water = 0
                    THEN 1 ELSE 0 END AS other_water_source,
              CASE WHEN no_construction_materials = 0 
                    AND vinyl_materials = 0
                    AND brick_materials = 0
                    AND aluminum_materials = 0 
                    AND wood_materials = 0
                    AND frame_materials = 0 
                    AND stucco_materials = 0
                    AND site_built_materials = 0
                    AND block_materials = 0
                    AND concrete_materials = 0
                    AND cement_materials = 0
                    AND stone_materials = 0
                    THEN 1 ELSE 0 END AS other_construction_materials, 
              CASE WHEN no_floorings_info = 0 
                    AND carpet_floorings = 0 
                    AND vinyl_floorings = 0
                    AND laminate_floorings = 0
                    AND tile_floorings = 0 
                    AND wood_floorings = 0
                    AND hardwood_floorings = 0 
                    AND linoleum_floorings = 0
                    AND ceramic_floorings = 0
                    THEN 1 ELSE 0 END AS other_floorings    
            FROM t_final;   

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
                                                                   prop_subtypes=PROP_SUBTYPES,
                                                                   price=PRICE,
                                                                   additional_query=ADDITIONAL_QUERY)
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


# python us_aggregation_dataflow.py --worker_machine_type=e2-standard-4

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
