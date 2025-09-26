import streamlit as st
import duckdb
import boto3
import folium
import geopandas as gpd
import pandas as pd
from shapely import wkt
import tempfile
import os
import base64

# Page config
st.set_page_config(
    page_title="Parcel Finder",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for light theme design
st.markdown("""
<style>
    /* Light theme color scheme */
    .main {
        background-color: #f8f9fa;
        color: #1d1d1f;
    }
    
    .stApp {
        background-color: #f8f9fa;
    }
    
    .stSidebar {
        background-color: #4a4a4a;
        border-right: 1px solid #e5e5e7;
    }
    
    .sidebar .sidebar-content {
        background-color: #4a4a4a;
    }
    
    .stSidebar .stSelectbox label,
    .stSidebar .stNumberInput label,
    .stSidebar .stSlider label,
    .stSidebar .stButton label,
    .stSidebar .stMarkdown,
    .stSidebar .stText,
    .stSidebar .stSelectbox > label,
    .stSidebar .stNumberInput > label,
    .stSidebar .stSlider > label,
    .stSidebar .stButton > label,
    .stSidebar .stMarkdown > div,
    .stSidebar .stText > div,
    .stSidebar .stSelectbox > div > label,
    .stSidebar .stNumberInput > div > label,
    .stSidebar .stSlider > div > label {
        color: #ffffff !important;
    }
    
    .stSidebar .stSelectbox > div > div {
        background-color: #ffffff;
        color: #1d1d1f;
    }
    
    .stSidebar .stNumberInput > div > div > input {
        background-color: #ffffff;
        color: #1d1d1f;
    }
    
    /* Ensure all sidebar text is white */
    .stSidebar * {
        color: #ffffff !important;
    }
    
    /* Override for form inputs */
    .stSidebar .stSelectbox > div > div,
    .stSidebar .stNumberInput > div > div > input,
    .stSidebar .stSlider > div > div > div > div > div > div {
        color: #1d1d1f !important;
    }
    
    /* Fix selectbox dropdown text */
    .stSidebar .stSelectbox > div > div > div {
        color: #1d1d1f !important;
    }
    
    /* Fix selectbox selected value text */
    .stSidebar .stSelectbox > div > div > div > div {
        color: #1d1d1f !important;
    }
    
    /* Fix selectbox dropdown options */
    .stSidebar .stSelectbox [data-baseweb="select"] {
        color: #1d1d1f !important;
    }
    
    .stSidebar .stSelectbox [data-baseweb="select"] > div {
        color: #1d1d1f !important;
    }
    
    .stSidebar .stSelectbox [data-baseweb="select"] > div > div {
        color: #1d1d1f !important;
    }
    
    /* Fix dropdown menu text */
    .stSidebar .stSelectbox [data-baseweb="menu"] {
        color: #1d1d1f !important;
    }
    
    .stSidebar .stSelectbox [data-baseweb="menu"] > div {
        color: #1d1d1f !important;
    }
    
    .stSidebar .stSelectbox [data-baseweb="menu"] > div > div {
        color: #1d1d1f !important;
    }
    
    .stSelectbox > div > div {
        background-color: #ffffff;
        border: 1px solid #d2d2d7;
        border-radius: 12px;
        color: #1d1d1f;
    }
    
    .stSelectbox > div > div:hover {
        border-color: #006419;
    }
    
    .stNumberInput > div > div > input {
        background-color: #ffffff;
        border: 1px solid #d2d2d7;
        border-radius: 12px;
        color: #1d1d1f;
    }
    
    .stNumberInput > div > div > input:focus {
        border-color: #006419;
        box-shadow: 0 0 0 3px rgba(0, 100, 25, 0.1);
    }
    
    .stSlider > div > div > div > div {
        background: #d2d2d7;
        height: 4px;
        border-radius: 2px;
    }
    
    .stSlider > div > div > div > div > div {
        background: #006419;
        height: 4px;
        border-radius: 2px;
    }
    
    .stSlider > div > div > div > div > div > div {
        background: #d2d2d7;
        border: 2px solid #ffffff;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
    }
    
    /* Hide slider value tooltips completely - nuclear option */
    .stSlider [data-testid="stTooltip"],
    .stSlider [role="tooltip"],
    .stSlider .stTooltip,
    .stSlider > div > div > div > div > div > div > div,
    .stSlider > div > div > div > div > div > div > span,
    .stSlider > div > div > div > div > div > div > *,
    .stSlider > div > div > div > div > div > div::before,
    .stSlider > div > div > div > div > div > div::after {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
        width: 0 !important;
        height: 0 !important;
        position: absolute !important;
        left: -9999px !important;
        top: -9999px !important;
    }
    
    /* Hide any green marks or indicators */
    .stSlider *[style*="background-color: #006419"],
    .stSlider *[style*="background: #006419"],
    .stSlider *[style*="color: #006419"] {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
    }
    
    /* Hide all slider children that might contain the green mark */
    .stSlider > div > div > div > div > div > div > * {
        display: none !important;
        visibility: hidden !important;
        opacity: 0 !important;
    }
    
    .stButton > button {
        background: #006419;
        color: #ffffff;
        border: none;
        border-radius: 12px;
        font-weight: 500;
        font-size: 16px;
        padding: 12px 24px;
        transition: all 0.2s ease;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    .stButton > button:hover {
        background: #004d14;
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0, 100, 25, 0.3);
    }
    
    .stDownloadButton > button {
        background: #f5f5f7;
        color: #1d1d1f;
        border: 1px solid #d2d2d7;
        border-radius: 12px;
        font-weight: 500;
        font-size: 16px;
        padding: 12px 24px;
        transition: all 0.2s ease;
    }
    
    .stDownloadButton > button:hover {
        background: #e5e5e7;
        border-color: #006419;
    }
    
    .metric-card {
        background: #ffffff;
        padding: 24px;
        border-radius: 16px;
        border: 1px solid #e5e5e7;
        margin: 16px 0;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
        transition: all 0.2s ease;
    }
    
    .metric-card:hover {
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        transform: translateY(-2px);
    }
    
    .welcome-card {
        background: #ffffff;
        padding: 32px;
        border-radius: 20px;
        border: 1px solid #e5e5e7;
        margin: 24px 0;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    
    .stTabs [data-baseweb="tab-list"] {
        background: #f5f5f7;
        border-radius: 12px;
        padding: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        color: #1d1d1f;
        border-radius: 8px;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: #ffffff;
        color: #1d1d1f;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    .stSuccess {
        background-color: #f0f9ff;
        border: 1px solid #bae6fd;
        border-radius: 12px;
        padding: 16px;
        color: #0369a1;
    }
    
    .stError {
        background-color: #fef2f2;
        border: 1px solid #fecaca;
        border-radius: 12px;
        padding: 16px;
        color: #dc2626;
    }
    
    .stInfo {
        background-color: #f0f9ff;
        border: 1px solid #bae6fd;
        border-radius: 12px;
        padding: 16px;
        color: #0369a1;
    }
    
    .stWarning {
        background-color: #fffbeb;
        border: 1px solid #fed7aa;
        border-radius: 12px;
        padding: 16px;
        color: #d97706;
    }
    
    /* Fix spinner text visibility */
    .stSpinner {
        color: #1d1d1f !important;
    }
    
    .stSpinner > div {
        color: #1d1d1f !important;
    }
    
    .stSpinner > div > div {
        color: #1d1d1f !important;
    }
    
    /* Fix spinner text in sidebar */
    .stSidebar .stSpinner {
        color: #ffffff !important;
    }
    
    .stSidebar .stSpinner > div {
        color: #ffffff !important;
    }
    
    .stSidebar .stSpinner > div > div {
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

# Header with solar panel image
def get_base64_image(image_path):
    with open(image_path, 'rb') as f:
        return base64.b64encode(f.read()).decode()

st.markdown("""
<div style="background: white; border-radius: 16px; padding: 2rem; margin: 2rem 0; box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1); display: flex; align-items: center; justify-content: center; gap: 2rem;">
    <div style="text-align: center;">
        <h1 style="color: #1d1d1f; font-size: 2.5rem; font-weight: 600; margin: 0; letter-spacing: -0.02em;">Solar Farm Parcel Finder</h1>
        <p style="color: #86868b; font-size: 1.1rem; margin: 0.5rem 0 0 0; font-weight: 400;">De-risking solar projects faster</p>
    </div>
    <div style="flex-shrink: 0;">
        <img src="data:image/png;base64,{}" style="width: 120px; height: 120px; opacity: 0.8;" />
    </div>
</div>
""".format(get_base64_image('visuals/solar_panels.png')), unsafe_allow_html=True)

# Initialize DuckDB connection
@st.cache_resource
def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'eu-north-1'")
    
    # Configure S3 credentials
    access_key = st.secrets["aws"]["aws_access_key_id"]
    secret_key = st.secrets["aws"]["aws_secret_access_key"]
    region = st.secrets["aws"]["aws_region"]

    # Get credentials via boto3
    # session = boto3.Session()
    # credentials = session.get_credentials()
    # if credentials:
    #     access_key = credentials.access_key
    #     secret_key = credentials.secret_key

    con.execute(f"SET s3_access_key_id = '{access_key}'")
    con.execute(f"SET s3_secret_access_key = '{secret_key}'")
    
    return con

# Get available counties
@st.cache_data
def get_counties():
    con = init_duckdb()
    counties = ['Albany','Bronx','Cayuga','Chautauqua','Cortland','Genesee','Greene','Hamilton','Kings','Lewis','Livingston','Montgomery','New York','Oneida','Ontario','Orange','Oswego','Otsego','Putnam','Queens','Rensselaer','Richmond','Rockland','Schuyler','St Lawrence','Steuben','Sullivan','Tioga','Tompkins','Ulster','Warren','Wayne']
    return sorted(counties)


# Sidebar for controls
st.sidebar.markdown("""
<div style="text-align: center; margin-bottom: 2rem;">
    <h2 style="color: #006419; margin: 0;">Search Criteria</h2>
    <p style="color: #ffffff; opacity: 0.8; margin: 0.5rem 0;">Configure your parcel search</p>
</div>
""", unsafe_allow_html=True)

# Get counties
counties = get_counties()
county = st.sidebar.selectbox("📍 Select County", counties)

# Distance input
distance = st.sidebar.slider("📏 Distance from substation (miles)", 1, 5, 3)

# Minimum parcel size
min_parcel_size = st.sidebar.slider("📐 Minimum parcel size (acres)", 1, 100, 20)

# Search button
search_button = st.sidebar.button("Find Parcels", type="primary")

# Main content
if search_button:
    with st.spinner("Searching for parcels..."):
        # Add performance warning for large counties
        if county in ['Erie', 'Suffolk', 'Nassau', 'Westchester', 'Monroe', 'Onondaga']:
            st.info("⚠️ Large county detected. Results may be limited for performance.")
        
        try:
            con = init_duckdb()
            
            # Convert to meters
            distance_meters = distance * 1609.34
            min_parcel_size_meters = min_parcel_size * 4046.86
            
            # File paths
            # parcel_files = 'data/parcel_data/ny_parcels'
            parcel_files = 's3://solar-parcel-finder/data/parcels/ny_parcels'
            infra_files = 's3://solar-parcel-finder/data/overture/ny_infrastructure'
            landuse_files = 's3://solar-parcel-finder/data/overture/ny_landuse'
            land_files = 's3://solar-parcel-finder/data/overture/ny_land'
            land_cover_files = 's3://solar-parcel-finder/data/overture/ny_land_cover'
            
            # Query
            query = f"""
                WITH parcels AS (
                    SELECT 
                    '{county}' AS county_name,
                    area,
                    geometry_4326 AS geometry_display,
                    ST_GeomFromText(geometry_5070) AS geometry,
                    ST_Buffer(ST_Envelope(ST_GeomFromText(geometry_5070)), {distance_meters}) AS bbox
                    FROM read_parquet('{parcel_files}/{county}/*.parquet')
                ),
                infrastructure AS (
                    SELECT 
                    class,
                    ST_GeomFromText(geometry_5070) AS geometry,
                    geometry_4326 AS geometry_display
                    FROM read_parquet('{infra_files}/{county}/*.parquet') 
                    WHERE class='substation'
                ),
                land_cover AS (
                    SELECT
                    class,
                    ST_GeomFromText(geometry_4326) AS geometry
                    FROM read_parquet('{land_cover_files}/{county}/*.parquet')
                    WHERE class IN ('barren', 'crop', 'grass', 'shrub')
                ),
                land_cover_removals AS (
                    SELECT
                    class,
                    ST_GeomFromText(geometry_4326) AS geometry
                    FROM read_parquet('{land_cover_files}/{county}/*.parquet')
                    WHERE class IN ('forest','wetland')
                )

                SELECT 
                    p.county_name,
                    l.class AS land_cover_class,
                    p.geometry_display AS parcel_geometry,
                    i.geometry_display AS substation_geometry,
                    MIN(ST_Distance(p.geometry, i.geometry)) AS min_distance_substation,
                    p.area

                FROM parcels AS p
                JOIN infrastructure AS i
                ON ST_Intersects(p.bbox, i.geometry)
                AND ST_DWithin(p.geometry, i.geometry, {distance_meters})
                JOIN land_cover AS l
                ON ST_Intersects(ST_GeomFromText(p.geometry_display), l.geometry)
                 WHERE p.area>{min_parcel_size_meters}

                 -- Remove long, skinny parcels
                 AND ST_Area(p.geometry) / (ST_Perimeter(p.geometry) * ST_Perimeter(p.geometry) / 16) > 0.3

                 -- Remove parcels where >50% is forest or wetland
                 -- AND (
                     -- SELECT COALESCE(SUM(ST_Area(ST_Intersection(ST_GeomFromText(p.geometry_display), lcr.geometry))), 0)
                     -- FROM land_cover_removals lcr 
                     -- WHERE ST_Intersects(ST_GeomFromText(p.geometry_display), lcr.geometry)
                 -- ) / ST_Area(ST_GeomFromText(p.geometry_display)) <= 0.5
                 GROUP BY p.county_name, p.geometry_display, i.geometry_display, p.area, p.geometry, l.class

                 -- LIMIT TO PREVENT CRASHES
                 LIMIT 1000
            """
            
            # Execute query with timeout and error handling
            import time
            import threading
            
            def execute_query_with_timeout():
                try:
                    return con.execute(query).fetchdf()
                except Exception as e:
                    raise e
            
            # Create a thread to run the query
            result_container = [None]
            exception_container = [None]
            
            def run_query():
                try:
                    result_container[0] = execute_query_with_timeout()
                except Exception as e:
                    exception_container[0] = e
            
            # Start the query in a separate thread
            query_thread = threading.Thread(target=run_query)
            query_thread.daemon = True
            query_thread.start()
            
            # Wait for the query to complete or timeout
            start_time = time.time()
            query_thread.join(timeout=20)
            execution_time = time.time() - start_time
            
            # Check if query completed
            if query_thread.is_alive():
                st.error("⚠️ Query timeout (>20 seconds). Please try a new query")
                st.stop()
            
            # Check if there was an exception
            if exception_container[0] is not None:
                st.error(f"Query failed: {str(exception_container[0])}")
                st.info("Try reducing the search distance or increasing minimum parcel size.")
                st.stop()
            
            # Check execution time
            if execution_time > 20:
                st.error("⚠️ Query took too long (>20 seconds). Please try a new query")
                st.stop()
            
            # Get the result
            df = result_container[0]
            
            # Remove duplicates
            df = df[df['parcel_geometry'].duplicated()==False].reset_index(drop=True)
            
            if len(df) > 0:
                # Convert to GeoDataFrame
                df['geometry'] = df['parcel_geometry'].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
                
                
                # Try different approaches for geometry conversion
                county_boundary_query = f"""
                SELECT 
                    ST_AsText(geometry) AS geometry_wkt,
                    NAME AS county_name
                FROM read_parquet('s3://solar-parcel-finder/data/boundaries/NYS_counties.parquet')
                WHERE NAME = '{county}'
                """
                county_df = con.execute(county_boundary_query).fetchdf()
                
                
                if len(county_df) > 0:
                    # Convert WKT to geometry
                    county_df['geometry'] = county_df['geometry_wkt'].apply(wkt.loads)
                    county_gdf = gpd.GeoDataFrame(county_df, geometry='geometry', crs='EPSG:4326')
                else:
                    st.warning(f"No county boundary found for {county}")
                    county_gdf = None
                
                # Create map
                if county_gdf is not None:
                    # Center map on county boundary
                    bounds = county_gdf.total_bounds
                    center_lat = (bounds[1] + bounds[3]) / 2
                    center_lon = (bounds[0] + bounds[2]) / 2
                    m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
                else:
                    # Fallback to parcel centroid
                    m = folium.Map(
                        location=[gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()], 
                        zoom_start=10
                    )
                
                # Add county boundary first (so it's behind everything)
                if county_gdf is not None:
                    folium.GeoJson(
                        county_gdf,
                        style_function=lambda x: {
                            'fillColor': 'transparent', 
                            'color': '#006419', 
                            'weight': 3,
                            'opacity': 0.8
                        },
                        popup=folium.Popup(f"{county} County Boundary", parse_html=False)
                    ).add_to(m)
                
                # Add landuse polygons (below parcels and solar farms)
                try:
                    if county_gdf is not None:
                        # Get county boundary geometry for intersection
                        county_geom = county_gdf.geometry.iloc[0]
                        county_wkt = wkt.dumps(county_geom)
                        
                        # Query landuse data that intersects with the county boundary
                        landuse_query = f"""
                        WITH county_boundary AS (
                            SELECT ST_GeomFromText('{county_wkt}') AS geometry
                        ),
                        landuse_data AS (
                            SELECT 
                                ST_GeomFromText(geometry_4326) AS geometry,
                                class
                            FROM read_parquet('s3://solar-parcel-finder/data/overture/ny_landuse/{county}/*.parquet')
                        )
                        SELECT 
                            ST_AsText(l.geometry) AS geometry_wkt,
                            l.class
                        FROM landuse_data l
                        JOIN county_boundary c
                        ON ST_Intersects(l.geometry, c.geometry)
                        """
                        
                        landuse_df = con.execute(landuse_query).fetchdf()
                        
                        if len(landuse_df) > 0:
                            # Convert WKT string to geometry
                            landuse_df['geometry'] = landuse_df['geometry_wkt'].apply(wkt.loads)
                            landuse_gdf = gpd.GeoDataFrame(landuse_df, geometry='geometry', crs='EPSG:4326')
                            
                            # Define colors for different landuse types
                            def get_landuse_color(feature):
                                landuse_type = feature['properties']['class']
                                color_map = {
                                    'forest': '#228B22',      # Forest Green
                                    'grass': '#90EE90',       # Light Green
                                    'meadow': '#98FB98',      # Pale Green
                                    'farmland': '#9ACD32',    # Yellow Green
                                    'industrial': '#696969',  # Dim Gray
                                    'brownfield': '#8B4513'   # Saddle Brown
                                }
                                return color_map.get(landuse_type, '#CCCCCC')  # Default gray
                            
                            # Add landuse polygons to map
                            folium.GeoJson(
                                landuse_gdf,
                                style_function=lambda x: {
                                    'fillColor': get_landuse_color(x),
                                    'color': 'transparent',
                                    'weight': 0,
                                    'opacity': 0.6,
                                    'fillOpacity': 0.3
                                }
                            ).add_to(m)
                            
                        else:
                            st.info(f"No landuse data found within {county} County")
                    else:
                        st.warning("Cannot filter landuse: county boundary not available")
                        
                except Exception as e:
                    st.error(f"Error loading landuse data: {str(e)}")
                    # Try to debug the landuse data structure
                    try:
                        debug_landuse_query = f"""
                        SELECT * FROM read_parquet('s3://solar-parcel-finder/data/overture/ny_landuse/{county}/*.parquet') LIMIT 1
                        """
                        debug_landuse_df = con.execute(debug_landuse_query).fetchdf()
                    except Exception as debug_e:
                        st.error(f"Debug query also failed: {str(debug_e)}")
                
                # Add land_cover polygons (below landuse, solar farms and parcels)
                try:
                    if county_gdf is not None:
                        # Get county boundary geometry for intersection
                        county_geom = county_gdf.geometry.iloc[0]
                        county_wkt = wkt.dumps(county_geom)
                        
                        # Query land_cover data that intersects with the county boundary
                        land_cover_query = f"""
                        WITH county_boundary AS (
                            SELECT ST_GeomFromText('{county_wkt}') AS geometry
                        ),
                        land_cover_data AS (
                            SELECT 
                                ST_GeomFromText(geometry_4326) AS geometry,
                                class
                            FROM read_parquet('s3://solar-parcel-finder/data/overture/ny_land_cover/{county}/*.parquet')
                        )
                        SELECT 
                            ST_AsText(lc.geometry) AS geometry_wkt,
                            lc.class
                        FROM land_cover_data lc
                        JOIN county_boundary c
                        ON ST_Intersects(lc.geometry, c.geometry)
                        """
                        
                        land_cover_df = con.execute(land_cover_query).fetchdf()
                        
                        if len(land_cover_df) > 0:
                            # Convert WKT string to geometry
                            land_cover_df['geometry'] = land_cover_df['geometry_wkt'].apply(wkt.loads)
                            land_cover_gdf = gpd.GeoDataFrame(land_cover_df, geometry='geometry', crs='EPSG:4326')
                            
                            # Define colors for different land cover types
                            def get_land_cover_color(feature):
                                land_cover_type = feature['properties']['class']
                                color_map = {
                                    'forest': '#228B22',        # Forest Green
                                    'grassland': '#9ACD32',     # Yellow Green
                                    'cropland': '#8FBC8F',      # Dark Sea Green
                                    'urban': '#696969',         # Dim Gray
                                    'water': '#4682B4',         # Steel Blue
                                    'wetland': '#20B2AA',       # Light Sea Green
                                    'shrubland': '#32CD32',     # Lime Green
                                    'barren': '#D2B48C',        # Tan
                                    'snow': '#F0F8FF',          # Alice Blue
                                    'ice': '#B0E0E6'            # Powder Blue
                                }
                                return color_map.get(land_cover_type, '#CCCCCC')  # Default gray
                            
                            # Add land_cover polygons to map
                            folium.GeoJson(
                                land_cover_gdf,
                                style_function=lambda x: {
                                    'fillColor': get_land_cover_color(x),
                                    'color': 'transparent',
                                    'weight': 0,
                                    'opacity': 0.4,
                                    'fillOpacity': 0.15
                                }
                            ).add_to(m)
                            
                        else:
                            st.info(f"No land cover data found within {county} County")
                    else:
                        st.warning("Cannot filter land cover: county boundary not available")
                        
                except Exception as e:
                    st.error(f"Error loading land cover data: {str(e)}")
                
                # Add parcels
                folium.GeoJson(
                    gdf, 
                    style_function=lambda x: {'fillColor': 'transparent', 'color': 'blue', 'weight': 2}
                ).add_to(m)
                
                # Add solar panel boundaries (only within selected county)
                try:
                    # Only add solar farms if we have a county boundary
                    if county_gdf is not None:
                        # Get county boundary geometry for intersection
                        county_geom = county_gdf.geometry.iloc[0]
                        county_wkt = wkt.dumps(county_geom)
                        
                        # Query solar farms that intersect with the county boundary
                        solar_query = f"""
                        WITH county_boundary AS (
                            SELECT ST_GeomFromText('{county_wkt}') AS geometry
                        ),
                        solar_farms AS (
                            SELECT 
                                ST_GeomFromText(geometry) AS geometry
                            FROM read_parquet('s3://solar-parcel-finder/data/parcels/solar_farms.parquet')
                        )
                        SELECT 
                            ST_AsText(s.geometry) AS geometry_wkt
                        FROM solar_farms s
                        JOIN county_boundary c
                        ON ST_Intersects(s.geometry, c.geometry)
                        """
                        
                        solar_df = con.execute(solar_query).fetchdf()
                        
                        if len(solar_df) > 0:
                            # Convert WKT string to geometry
                            solar_df['geometry'] = solar_df['geometry_wkt'].apply(wkt.loads)
                            solar_gdf = gpd.GeoDataFrame(solar_df, geometry='geometry', crs='EPSG:4326')
                            
                            # Add solar farms to map
                            folium.GeoJson(
                                solar_gdf,
                                style_function=lambda x: {
                                    'fillColor': 'yellow', 
                                    'color': 'orange', 
                                    'weight': 2,
                                    'opacity': 0.8,
                                    'fillOpacity': 0.3
                                }
                            ).add_to(m)
                            
                        else:
                            st.info(f"No solar farms found within {county} County")
                    else:
                        st.warning("Cannot filter solar farms: county boundary not available")
                        
                except Exception as e:
                    st.error(f"Error loading solar farms: {str(e)}")
                    # Try alternative approach without spatial filtering
                    try:
                        solar_query = """
                        SELECT geometry
                        FROM read_parquet('s3://solar-parcel-finder/data/parcels/solar_farms.parquet')
                        LIMIT 10
                        """
                        solar_df = con.execute(solar_query).fetchdf()
                        
                        if len(solar_df) > 0:
                            # Convert WKT string to geometry
                            solar_df['geometry'] = solar_df['geometry'].apply(wkt.loads)
                            solar_gdf = gpd.GeoDataFrame(solar_df, geometry='geometry', crs='EPSG:4326')
                            
                            # Add solar farms to map (without county filtering)
                            folium.GeoJson(
                                solar_gdf,
                                style_function=lambda x: {
                                    'fillColor': 'yellow', 
                                    'color': 'orange', 
                                    'weight': 2,
                                    'opacity': 0.8,
                                    'fillOpacity': 0.3
                                }
                            ).add_to(m)
                            
                    except Exception as e2:
                        st.error(f"Alternative method also failed: {str(e2)}")
                
                # Add legend to map
                legend_html = '''
                <div style="position: fixed; 
                            top: 20px; right: 20px; width: 180px; height: 120px; 
                            background-color: rgba(255, 255, 255, 0.95); 
                            border: 2px solid #006419; border-radius: 12px; 
                            z-index: 9999; font-size: 14px; padding: 12px;
                            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
                <p style="margin: 0 0 10px 0; font-weight: 600; color: #006419; font-size: 16px; text-align: center;">Legend</p>
                <div style="display: flex; align-items: center; margin: 6px 0;">
                    <div style="width: 16px; height: 16px; background-color: yellow; border: 2px solid orange; margin-right: 8px; border-radius: 2px;"></div>
                    <span style="color: #1d1d1f;">Solar Sites</span>
                </div>
                <div style="display: flex; align-items: center; margin: 6px 0;">
                    <div style="width: 16px; height: 16px; background-color: transparent; border: 2px solid blue; margin-right: 8px; border-radius: 2px;"></div>
                    <span style="color: #1d1d1f;">Search Parcels</span>
                </div>
                <div style="display: flex; align-items: center; margin: 6px 0;">
                    <div style="width: 16px; height: 16px; background-color: transparent; border: 3px solid #006419; margin-right: 8px; border-radius: 2px;"></div>
                    <span style="color: #1d1d1f;">County Boundary</span>
                </div>
                </div>
                '''
                m.get_root().html.add_child(folium.Element(legend_html))
                
                # Save map to temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as tmp_file:
                    m.save(tmp_file.name)
                    
                    # Display results - Map as main focus
                    st.markdown(f"""
                    <div style="text-align: center; margin-bottom: 1rem;">
                        <h2 style="color: #006419; margin: 0 0 0.5rem 0; font-size: 1.5rem;">{county} County Results</h2>
                        <p style="color: #1d1d1f; margin: 0; opacity: 0.9; font-size: 1rem;"><strong>Found {len(gdf)} parcels</strong> within {distance} miles of substations • Minimum size: {min_parcel_size} acres</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Display map with more space
                    with open(tmp_file.name, 'r') as f:
                        map_html = f.read()
                    st.components.v1.html(map_html, height=700)
                    
                    # Compact metrics and downloads below the map
                    st.markdown("---")
                    
                    # Calculate statistics
                    avg_area = df['area'].mean() / 4046.86  # Convert to acres
                    avg_distance = df['min_distance_substation'].mean() / 1609.34  # Convert to miles
                    
                    # Metrics in a horizontal layout
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown(f"""
                        <div style="text-align: center; padding: 1rem; background: rgba(0, 100, 25, 0.1); border-radius: 8px; border: 1px solid #006419;">
                            <h4 style="color: #006419; margin: 0 0 0.5rem 0; font-size: 0.9rem;">TOTAL PARCELS</h4>
                            <p style="color: #1d1d1f; font-size: 1.8rem; font-weight: 700; margin: 0;">{len(gdf)}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f"""
                        <div style="text-align: center; padding: 1rem; background: rgba(0, 100, 25, 0.1); border-radius: 8px; border: 1px solid #006419;">
                            <h4 style="color: #006419; margin: 0 0 0.5rem 0; font-size: 0.9rem;">AVERAGE AREA</h4>
                            <p style="color: #1d1d1f; font-size: 1.8rem; font-weight: 700; margin: 0;">{avg_area:.1f} acres</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        st.markdown(f"""
                        <div style="text-align: center; padding: 1rem; background: rgba(0, 100, 25, 0.1); border-radius: 8px; border: 1px solid #006419;">
                            <h4 style="color: #006419; margin: 0 0 0.5rem 0; font-size: 0.9rem;">AVG DISTANCE</h4>
                            <p style="color: #1d1d1f; font-size: 1.8rem; font-weight: 700; margin: 0;">{avg_distance:.2f} miles</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Download section at the very bottom
                    st.markdown("---")
                    st.markdown("""
                    <div style="text-align: center; margin: 2rem 0;">
                        <h3 style="color: #006419; margin: 0 0 1rem 0;">Download Results</h3>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Download buttons in a row
                    download_col1, download_col2 = st.columns(2)
                    
                    with download_col1:
                        # Convert to CSV for download
                        csv_data = gdf.drop(columns=['geometry']).to_csv(index=False)
                        st.download_button(
                            label="📊 Download CSV Data",
                            data=csv_data,
                            file_name=f"{county}_parcels.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with download_col2:
                        # Download map as HTML
                        with open(tmp_file.name, 'r') as f:
                            map_html_content = f.read()
                        st.download_button(
                            label="🗺️ Download Interactive Map",
                            data=map_html_content,
                            file_name=f"{county}_map.html",
                            mime="text/html",
                            use_container_width=True
                        )
                
                # Clean up temp file
                os.unlink(tmp_file.name)
                
            else:
                st.warning(f"No parcels found in {county} County matching your criteria.")
                st.info("Try adjusting the distance or minimum parcel size.")
                
        except Exception as e:
            st.error(f"Error searching for parcels: {str(e)}")
            st.info("Make sure the data files are available and accessible.")

else:
    # Welcome message
    st.markdown("""
    <div class="welcome-card">
        <h2 style="color: #006419; margin: 0 0 1rem 0;">Welcome to New York State Solar Farm Parcel Finder</h2>
        <p style="color: #1d1d1f; margin: 0 0 1rem 0; font-size: 1.1rem;">
            Find qualified parcels near substations with precision and speed.
        </p>
        <div style="background: #f8f9fa; padding: 1.5rem; border-radius: 8px; margin: 1rem 0; border: 1px solid #e5e5e7;">
            <h3 style="color: #006419; margin: 0 0 1rem 0;">🎯 How it works:</h3>
            <ul style="color: #1d1d1f; margin: 0; padding-left: 1.5rem;">
                <li><strong>Distance:</strong> Set proximity to infrastructure (1-10 miles)</li>
                <li><strong>Parcel Size:</strong> Filter by minimum acreage (1-100 acres)</li>
                <li><strong>County:</strong> Select from available counties in New York State</li>
                <li><strong>Results:</strong> Get interactive maps and downloadable data</li>
            </ul>
        </div>
        <p style="color: #86868b; margin: 1rem 0 0 0; text-align: center;">
            Use the sidebar to configure your search and click "Find Parcels" to begin.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Show available counties
    st.markdown("""
    <div class="welcome-card">
        <h2 style="color: #006419; margin: 0 0 1rem 0;">📍 Available Counties</h2>
        <p style="color: #86868b; margin: 0;">
            Select from the following counties in the sidebar:
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    counties = get_counties()
    # Display counties in a grid
    cols = st.columns(3)
    for i, county in enumerate(counties):
        with cols[i % 3]:
            st.markdown(f"""
            <div style="background: #ffffff; padding: 1rem; border-radius: 8px; border: 1px solid #e5e5e7; margin: 0.5rem 0; text-align: center; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);">
                <p style="color: #006419; margin: 0; font-weight: 600;">{county}</p>
            </div>
            """, unsafe_allow_html=True)