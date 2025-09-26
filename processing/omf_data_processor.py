import duckdb
from shapely import wkt
import geopandas as gpd
from tqdm import tqdm
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.dataset as ds
import boto3


con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region = 'us-west-2'")

# Configure S3 credentials if needed
session = boto3.Session()
credentials = session.get_credentials()

if credentials:
    access_key = credentials.access_key
    secret_key = credentials.secret_key
con.execute(f"SET s3_access_key_id = '{access_key}'")
con.execute(f"SET s3_secret_access_key = '{secret_key}'")

# New York State Bounding Box
xmin = -79.762152
ymin = 40.496103
xmax = -71.856214
ymax = 45.01585

landuse = 's3://overturemaps-us-west-2/release/2025-09-24.0/theme=base/type=land_use/*.parquet'
infrastructure = 's3://overturemaps-us-west-2/release/2025-09-24.0/theme=base/type=infrastructure/*.parquet'
land = 's3://overturemaps-us-west-2/release/2025-09-24.0/theme=base/type=land/*.parquet'
land_cover = 's3://overturemaps-us-west-2/release/2025-09-24.0/theme=base/type=land_cover/*.parquet'

files = [(landuse, 'landuse'), (infrastructure, 'infrastructure'), (land, 'land'), (land_cover, 'land_cover')]

for f, name, type_col in tqdm(files):
    query = f"""
    WITH omf_data AS (
        SELECT
            ST_GeomFromWKB(ST_AsWKB(geometry)) AS geometry,
            {type_col} AS class
        FROM read_parquet('{f}')
        WHERE bbox.xmin BETWEEN {xmin} AND {xmax} AND bbox.ymin BETWEEN {ymin} AND {ymax}
    ),
    county_boundaries AS (
        SELECT
            ST_GeomFromWKB(ST_AsWKB(geometry)) AS geometry,
            NAME AS county_name
        FROM read_parquet('data/boundaries_data/NYS_counties.parquet')
    )

    SELECT 
        omf_data.class,
        ST_AsText(omf_data.geometry) AS geometry,
        county_boundaries.county_name
    FROM omf_data
    JOIN county_boundaries
    ON ST_Intersects(omf_data.geometry, county_boundaries.geometry)

    """

    df = con.execute(query).fetchdf()
    df['geometry_4326'] = df['geometry']
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry',crs='EPSG:4326')

    # Use 5070 for area and distance calculations
    gdf = gdf.to_crs('EPSG:5070')
    df2 = pd.DataFrame(gdf)
    df2['geometry_5070'] = df2['geometry'].apply(wkt.dumps)
    df2 = df2.drop(columns=['geometry'])

    table = pa.Table.from_pandas(df2)

    ds.write_dataset(
        table,
        base_dir=f"s3://solar-parcel-finder/data/overture/ny_{name}/",
        format="parquet",
        partitioning=["county_name"]
    )