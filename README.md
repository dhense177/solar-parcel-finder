# Parcel Finder App

A web application for finding suitable parcels near substations for solar farms, with customizable criteria.

## Features

- Interactive map showing parcels meeting your criteria
- Customizable distance from substations (1-10 miles)
- Minimum parcel size filter (1-100 acres)
- County selection (New York State only)
- Download results as CSV or HTML map
- Statistics and metrics

## Methods

- Parcels are filtered by their size (in acres) and their distance to nearest substation (in miles), the values of which are user specified
- Further filtering to reduce the set of candidate parcels
    - Shape is not very long and thin
    - Area of parcel is not made up mostly of forests or wetlands

## Data Sources

- New York State boundaries (https://gis.ny.gov/parcels)
- New York State county boundaries (https://gis.ny.gov/civil-boundaries)
- Solar Sites (https://www.transitionzero.org/products/solar-asset-mapper)
- New York State Parcels (https://gis.ny.gov/parcels)
- Overture Maps Geographic Data (https://docs.overturemaps.org/guides/)


## App Deployment

Deployed as a streamlit app at the following address: https://solar-parcel-finder-nda2ycxzbzjakzx3zuougd.streamlit.app/
