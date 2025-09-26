import duckdb
import boto3
from shapely import wkt
import geopandas as gpd
import pandas as pd
# Connect to DuckDB and load spatial extension
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region = 'eu-north-1'")

solar_farm_file = 'data/parcel_data/TZ_SAM_Q2_2025/2025-Q2_raw_polygons.gpkg'
county_boundaries_file = 'data/boundaries_data/NYS_counties.parquet'

query = f"""
WITH solar_farms AS (
    SELECT 
        * EXCLUDE (geom), 
        ST_GeomFromWKB(ST_AsWKB(geom)) AS geometry 
    FROM ST_Read('{solar_farm_file}')
),
county_boundaries AS (
    SELECT 
        * EXCLUDE (geometry), 
        ST_GeomFromWKB(ST_AsWKB(geometry)) AS geometry 
    FROM read_parquet('{county_boundaries_file}')
)

SELECT 
solar_farms.* EXCLUDE (geometry),
ST_AsText(solar_farms.geometry) AS geometry,
county_boundaries.NAME AS county_name
FROM solar_farms
JOIN county_boundaries
ON ST_Intersects(solar_farms.geometry, county_boundaries.geometry)
"""

df_solar = con.execute(query).fetchdf()

df_solar['geometry'] = df_solar['geometry'].apply(wkt.loads)
solar_gdf = gpd.GeoDataFrame(df_solar, geometry='geometry', crs='EPSG:4326')

# Use 5070 for area and distance calculations
solar_gdf = solar_gdf.to_crs('EPSG:5070')

# Dissolve by cluster_id to remove overlapping solar farm polygons
gdf_dissolved = solar_gdf.dissolve(by='cluster_id')

# Reproject to EPSG:4326
gdf_dissolved = gdf_dissolved.to_crs('EPSG:4326')
gdf_dissolved['geometry'] = gdf_dissolved['geometry'].apply(wkt.dumps)
df_solar_final = pd.DataFrame(gdf_dissolved)
df_solar_final.to_parquet('data/parcel_data/solar_farms.parquet')