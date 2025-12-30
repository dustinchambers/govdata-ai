#!/usr/bin/env python3
"""
UPDATED: Download script for Denver's current open data portal
Denver has migrated to a new data platform - this script uses the updated URLs
"""

import pandas as pd
import requests
import json
from datetime import datetime
import os
import time

# Configuration
DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
os.makedirs(RAW_DIR, exist_ok=True)

print("=" * 80)
print("ETHICA.DESIGN - CIVIC DATA INTELLIGENCE PLATFORM")
print("UPDATED Data Download Script (Dec 2025)")
print("=" * 80)
print()

# ============================================================================
# DATASET 1: CRIME DATA (Updated URL)
# ============================================================================

print("📊 DOWNLOADING CRIME DATA...")
print("-" * 80)
print("Trying Denver's Open Data Catalog via Socrata API...")

# Denver uses Socrata for their open data
# Crime dataset ID: s8hx-dx8s (this may need updating)
crime_api_url = "https://www.denvergov.org/resource/s8hx-dx8s.json"

# Parameters - get as much data as possible
# Socrata limits to 50K records per request, so we'll need to paginate
params = {
    "$limit": 50000,
    "$offset": 0,
    "$order": "first_occurrence_date DESC"
}

try:
    print(f"Fetching from Socrata API: {crime_api_url}")
    print("Note: This may take a few minutes for large datasets...")
    
    all_records = []
    offset = 0
    batch_size = 50000
    
    while True:
        params["$offset"] = offset
        print(f"  Fetching batch starting at offset {offset}...")
        
        response = requests.get(crime_api_url, params=params, timeout=60)
        
        if response.status_code != 200:
            print(f"  Status code: {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            break
        
        batch = response.json()
        
        if not batch:
            print(f"  No more records found")
            break
        
        all_records.extend(batch)
        print(f"  Got {len(batch)} records (total so far: {len(all_records):,})")
        
        if len(batch) < batch_size:
            # Last batch
            break
        
        offset += batch_size
        time.sleep(1)  # Be nice to the API
    
    if all_records:
        crime_df = pd.DataFrame(all_records)
        
        # Save raw data
        raw_crime_path = f"{RAW_DIR}/crime_raw.csv"
        crime_df.to_csv(raw_crime_path, index=False)
        
        print(f"✅ Downloaded {len(crime_df):,} crime records")
        print(f"   Saved to: {raw_crime_path}")
        if 'first_occurrence_date' in crime_df.columns:
            print(f"   Date range: {crime_df['first_occurrence_date'].min()} to {crime_df['first_occurrence_date'].max()}")
        print(f"   Columns: {', '.join(crime_df.columns[:5])}... ({len(crime_df.columns)} total)")
        print()
    else:
        print("❌ No crime data retrieved")
        print()
        
except Exception as e:
    print(f"❌ Error downloading crime data: {e}")
    print()
    print("TROUBLESHOOTING:")
    print("1. Check if Denver's open data portal is accessible:")
    print("   https://www.denvergov.org/opendata")
    print("2. Search for 'crime' dataset and get the new API endpoint")
    print("3. Update the crime_api_url in this script")
    print()

# ============================================================================
# DATASET 2: 311 SERVICE REQUESTS (Updated approach)
# ============================================================================

print("📞 DOWNLOADING 311 SERVICE REQUESTS...")
print("-" * 80)

# Try the Socrata endpoint for 311 data
service_311_api_url = "https://www.denvergov.org/resource/fism-mqn8.json"

params = {
    "$limit": 50000,
    "$where": "opened_date > '2024-01-01'",  # Last year of data
    "$order": "opened_date DESC"
}

try:
    print(f"Fetching from Socrata API: {service_311_api_url}")
    
    response = requests.get(service_311_api_url, params=params, timeout=60)
    
    if response.status_code == 200:
        data = response.json()
        
        if data:
            service_311_df = pd.DataFrame(data)
            
            # Save raw data
            raw_311_path = f"{RAW_DIR}/311_requests_raw.csv"
            service_311_df.to_csv(raw_311_path, index=False)
            
            print(f"✅ Downloaded {len(service_311_df):,} 311 service requests")
            print(f"   Saved to: {raw_311_path}")
            print(f"   Columns: {', '.join(service_311_df.columns[:5])}... ({len(service_311_df.columns)} total)")
            
            # Show service type breakdown
            if 'case_type' in service_311_df.columns:
                print(f"\n   Top 5 Service Types:")
                top_types = service_311_df['case_type'].value_counts().head(5)
                for service_type, count in top_types.items():
                    print(f"     - {service_type}: {count:,}")
            print()
        else:
            print("❌ No 311 data found")
            print()
    else:
        print(f"❌ HTTP {response.status_code}: {response.text[:200]}")
        print()
        
except Exception as e:
    print(f"❌ Error downloading 311 data: {e}")
    print()

# ============================================================================
# DATASET 3: NEIGHBORHOOD BOUNDARIES
# ============================================================================

print("🗺️  DOWNLOADING NEIGHBORHOOD BOUNDARIES...")
print("-" * 80)

# Try the GeoJSON endpoint
neighborhoods_url = "https://www.denvergov.org/media/gis/DataCatalog/statistical_neighborhoods/geojson/statistical_neighborhoods.geojson"

try:
    print(f"Fetching from: {neighborhoods_url}")
    
    response = requests.get(neighborhoods_url, timeout=30)
    
    if response.status_code == 200:
        # Try to parse as JSON
        neighborhoods_data = response.json()
        
        # Save raw GeoJSON
        raw_neighborhoods_path = f"{RAW_DIR}/neighborhoods_raw.geojson"
        with open(raw_neighborhoods_path, 'w') as f:
            json.dump(neighborhoods_data, f)
        
        # Also create a simple CSV
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
            print()
    else:
        print(f"❌ HTTP {response.status_code}")
        print()
        
except Exception as e:
    print(f"❌ Error downloading neighborhood data: {e}")
    print()

# ============================================================================
# ALTERNATIVE: MANUAL DOWNLOAD INSTRUCTIONS
# ============================================================================

print("=" * 80)
print("ALTERNATIVE: MANUAL DOWNLOAD")
print("=" * 80)
print()
print("If the automatic download isn't working, you can manually download:")
print()
print("1. Go to: https://www.denvergov.org/opendata")
print("2. Search for 'crime'")
print("3. Download the CSV file")
print("4. Save as: data/raw/crime_raw.csv")
print()
print("5. Search for '311' or 'service requests'")
print("6. Download the CSV file")
print("7. Save as: data/raw/311_requests_raw.csv")
print()
print("Then run: python scripts/02_clean_data.py")
print()
print("=" * 80)

# ============================================================================
# SUMMARY
# ============================================================================

print()
print("DOWNLOAD SUMMARY")
print("=" * 80)
print()
print("Files in data/raw/:")
for root, dirs, files in os.walk(RAW_DIR):
    for file in files:
        filepath = os.path.join(root, file)
        size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"  - {file} ({size:.2f} MB)")
print()
print("If you have at least crime_raw.csv and 311_requests_raw.csv,")
print("you can proceed to: python scripts/02_clean_data.py")
print()
print("=" * 80)
