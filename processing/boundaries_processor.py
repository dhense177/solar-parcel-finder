import geopandas as gpd


bd = gpd.read_file('data/boundaries_data/NYS_Civil_Boundaries/Counties.shp')

bd = bd.to_crs('EPSG:4326')

bd.to_parquet('data/boundaries_data/NYS_counties.parquet')