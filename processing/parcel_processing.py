import geopandas as gpd
import pandas as pd
from shapely import wkt
import pyarrow as pa
import pyarrow.dataset as ds

f = 'data/parcel_data/NYS_Parcels/a00000016.gdbtable'
df_ny_parcels = gpd.read_file(f)

ny_parcels = df_ny_parcels.to_crs(epsg=4326)
ny_parcels['geometry_4326'] = ny_parcels['geometry']

# Use 5070 for area and distance calculations
ny_parcels = ny_parcels.to_crs(epsg=5070)
ny_parcels["area"] = ny_parcels.geometry.area
ny_parcels['geometry_5070'] = ny_parcels['geometry']
ny_parcels = ny_parcels.drop(columns=['geometry'])

ny_parcels = ny_parcels.rename(columns={'COUNTY_NAME':'county_name'})
ny_parcels = ny_parcels[['geometry_4326','geometry_5070','county_name','area']]

ny_parcels['geometry_4326'] = ny_parcels['geometry_4326'].apply(wkt.dumps)
ny_parcels['geometry_5070'] = ny_parcels['geometry_5070'].apply(wkt.dumps)

ny_parcels2 = pd.DataFrame(ny_parcels)

#Rename counties to align with county boundaries file
ny_parcels2['county_name'] = ny_parcels2['county_name'].replace({
    'NewYork': 'New York',
    'StLawrence': 'St Lawrence'
})


table = pa.Table.from_pandas(ny_parcels2)

ds.write_dataset(
    table,
    base_dir="s3://solar-parcel-finder/data/parcels/ny_parcels/",
    format="parquet",
    partitioning=["county_name"]
)