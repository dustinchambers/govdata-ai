#!/usr/bin/env python3
"""
Ethica.Design - Civic Data Intelligence Platform
Data Download Script: Denver Crime + 311 Service Requests

Downloads and performs initial cleaning of Denver open data for MVP analysis.
"""

import pandas as pd
import requests
import json
from datetime import datetime
import os

# Configuration
DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"

# Create directories
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

print("=" * 80)
print("ETHICA.DESIGN - CIVIC DATA INTELLIGENCE PLATFORM")
print("MVP Data Download Script")
print("=" * 80)
print()

# ============================================================================
# DATASET 1: CRIME DATA
# ============================================================================

print("📊 DOWNLOADING CRIME DATA...")
print("-" * 80)

crime_url = "https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv"

try:
    print(f"Fetching from: {crime_url}")
    crime_df = pd.read_csv(crime_url)
    
    # Save raw data
    raw_crime_path = f"{RAW_DIR}/crime_raw.csv"
    crime_df.to_csv(raw_crime_path, index=False)
    
    print(f"✅ Downloaded {len(crime_df):,} crime records")
    print(f"   Saved to: {raw_crime_path}")
    print(f"   Date range: {crime_df['FIRST_OCCURRENCE_DATE'].min()} to {crime_df['FIRST_OCCURRENCE_DATE'].max()}")
    print(f"   Columns: {', '.join(crime_df.columns[:5])}... ({len(crime_df.columns)} total)")
    print()
    
except Exception as e:
    print(f"❌ Error downloading crime data: {e}")
    print()

# ============================================================================
# DATASET 2: 311 SERVICE REQUESTS
# ============================================================================

print("📞 DOWNLOADING 311 SERVICE REQUESTS...")
print("-" * 80)

# Denver 311 data via ArcGIS Open Data API
# We'll use the REST API to get GeoJSON format
service_311_url = "https://services.arcgis.com/IeNV3JvLahcjYxEC/arcgis/rest/services/311_Service_Requests/FeatureServer/0/query"

# Query parameters - get all records from last 12 months
params = {
    "where": "1=1",  # Get all records
    "outFields": "*",  # All fields
    "returnGeometry": "true",
    "f": "json",
    "resultRecordCount": 32000  # Max records per request
}

try:
    print(f"Fetching from: Denver 311 ArcGIS API")
    print("Note: This may take a minute...")
    
    response = requests.get(service_311_url, params=params)
    data = response.json()
    
    # Convert to DataFrame
    if 'features' in data:
        records = []
        for feature in data['features']:
            record = feature['attributes']
            # Add geometry if available
            if 'geometry' in feature and feature['geometry']:
                record['longitude'] = feature['geometry'].get('x')
                record['latitude'] = feature['geometry'].get('y')
            records.append(record)
        
        service_311_df = pd.DataFrame(records)
        
        # Save raw data
        raw_311_path = f"{RAW_DIR}/311_requests_raw.csv"
        service_311_df.to_csv(raw_311_path, index=False)
        
        print(f"✅ Downloaded {len(service_311_df):,} 311 service requests")
        print(f"   Saved to: {raw_311_path}")
        print(f"   Columns: {', '.join(service_311_df.columns[:5])}... ({len(service_311_df.columns)} total)")
        
        # Show service type breakdown
        if 'CASE_TYPE' in service_311_df.columns:
            print(f"\n   Top 5 Service Types:")
            top_types = service_311_df['CASE_TYPE'].value_counts().head(5)
            for service_type, count in top_types.items():
                print(f"     - {service_type}: {count:,}")
        print()
        
    else:
        print(f"❌ No features found in 311 data response")
        print(f"   Response keys: {data.keys()}")
        print()
        
except Exception as e:
    print(f"❌ Error downloading 311 data: {e}")
    print()

# ============================================================================
# DATASET 3: NEIGHBORHOOD BOUNDARIES (GeoJSON)
# ============================================================================

print("🗺️  DOWNLOADING NEIGHBORHOOD BOUNDARIES...")
print("-" * 80)

neighborhoods_url = "https://www.denvergov.org/media/gis/DataCatalog/statistical_neighborhoods/geojson/statistical_neighborhoods.geojson"

try:
    print(f"Fetching from: {neighborhoods_url}")
    
    response = requests.get(neighborhoods_url)
    neighborhoods_data = response.json()
    
    # Save raw GeoJSON
    raw_neighborhoods_path = f"{RAW_DIR}/neighborhoods_raw.geojson"
    with open(raw_neighborhoods_path, 'w') as f:
        json.dump(neighborhoods_data, f)
    
    # Also create a simple CSV with neighborhood names for reference
    if 'features' in neighborhoods_data:
        neighborhoods_list = []
        for feature in neighborhoods_data['features']:
            props = feature['properties']
            neighborhoods_list.append({
                'NBHD_ID': props.get('NBHD_ID'),
                'NBHD_NAME': props.get('NBHD_NAME'),
                'SUM_AREA': props.get('SUM_AREA')
            })
        
        neighborhoods_df = pd.DataFrame(neighborhoods_list)
        neighborhoods_csv_path = f"{RAW_DIR}/neighborhoods_list.csv"
        neighborhoods_df.to_csv(neighborhoods_csv_path, index=False)
        
        print(f"✅ Downloaded {len(neighborhoods_data['features'])} neighborhoods")
        print(f"   Saved GeoJSON to: {raw_neighborhoods_path}")
        print(f"   Saved CSV list to: {neighborhoods_csv_path}")
        print(f"   Sample neighborhoods: {', '.join(neighborhoods_df['NBHD_NAME'].head(5).tolist())}...")
        print()
    
except Exception as e:
    print(f"❌ Error downloading neighborhood data: {e}")
    print()

# ============================================================================
# DOWNLOAD SUMMARY
# ============================================================================

print("=" * 80)
print("DOWNLOAD SUMMARY")
print("=" * 80)
print()
print("✅ Data successfully downloaded to: ./data/raw/")
print()
print("Next steps:")
print("1. Run: python scripts/02_clean_data.py")
print("2. Run: python scripts/03_analyze_correlations.py")
print()
print("Files created:")
for root, dirs, files in os.walk(RAW_DIR):
    for file in files:
        filepath = os.path.join(root, file)
        size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"  - {filepath} ({size:.2f} MB)")
print()
print("=" * 80)
