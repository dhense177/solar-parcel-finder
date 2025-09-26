# Parcel Finder App

A web application for finding suitable parcels near substations for solar farms, with customizable criteria.

## Features

- Interactive map showing parcels meeting your criteria
- Customizable distance from substations (1-10 miles)
- Minimum parcel size filter (1-100 acres)
- County selection (New York State only)
- Download results as CSV or HTML map
- Statistics and metrics

## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the app:
```bash
streamlit run app.py
```

## Deployment

### Option 1: Vercel

1. Install Vercel CLI:
```bash
npm i -g vercel
```

2. Deploy:
```bash
vercel
```

3. Add environment variables in Vercel dashboard:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### Option 2: S3 + CloudFront

1. Build static files:
```bash
streamlit build app.py
```

2. Upload to S3 bucket
3. Configure CloudFront distribution
4. Set up AWS credentials

## Data Requirements

The app expects the following data structure:
- `data/parcel_data/ny_parcels/{county}/*.parquet` - Parcel data
- S3 bucket with infrastructure, land use, and land data

## Configuration

Update the file paths in `app.py` to match your data structure:
- `parcel_files`
- `infra_files` 
- `landuse_files`
- `land_files`
