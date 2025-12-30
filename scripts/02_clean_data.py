#!/usr/bin/env python3
"""
Ethica.Design - Civic Data Intelligence Platform
Data Cleaning Script

Cleans and prepares Denver crime and 311 data for correlation analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# Configuration
DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"

print("=" * 80)
print("ETHICA.DESIGN - DATA CLEANING")
print("=" * 80)
print()

# ============================================================================
# CLEAN CRIME DATA
# ============================================================================

print("🧹 CLEANING CRIME DATA...")
print("-" * 80)

try:
    crime_df = pd.read_csv(f"{RAW_DIR}/crime_raw.csv")
    print(f"Loaded {len(crime_df):,} crime records")
    
    # Parse dates
    crime_df['first_occurrence_date'] = pd.to_datetime(
        crime_df['FIRST_OCCURRENCE_DATE'], 
        errors='coerce'
    )
    crime_df['reported_date'] = pd.to_datetime(
        crime_df['REPORTED_DATE'], 
        errors='coerce'
    )
    
    # Extract useful time components
    crime_df['year'] = crime_df['first_occurrence_date'].dt.year
    crime_df['month'] = crime_df['first_occurrence_date'].dt.month
    crime_df['day_of_week'] = crime_df['first_occurrence_date'].dt.day_name()
    crime_df['hour'] = crime_df['first_occurrence_date'].dt.hour
    
    # Standardize column names
    crime_df.rename(columns={
        'INCIDENT_ID': 'incident_id',
        'OFFENSE_TYPE_ID': 'offense_type',
        'OFFENSE_CATEGORY_ID': 'offense_category',
        'INCIDENT_ADDRESS': 'address',
        'GEO_LAT': 'latitude',
        'GEO_LON': 'longitude',
        'NEIGHBORHOOD_ID': 'neighborhood',
        'DISTRICT_ID': 'district',
        'IS_CRIME': 'is_crime',
        'IS_TRAFFIC': 'is_traffic'
    }, inplace=True)
    
    # Filter to actual crimes (not just incidents)
    crime_df = crime_df[crime_df['is_crime'] == 1].copy()
    
    # Remove records with missing location data
    initial_count = len(crime_df)
    crime_df = crime_df.dropna(subset=['latitude', 'longitude', 'neighborhood'])
    removed = initial_count - len(crime_df)
    
    print(f"✅ Cleaned crime data:")
    print(f"   - Parsed dates and extracted time components")
    print(f"   - Filtered to actual crimes: {len(crime_df):,} records")
    print(f"   - Removed {removed:,} records with missing location data")
    print(f"   - Date range: {crime_df['first_occurrence_date'].min()} to {crime_df['first_occurrence_date'].max()}")
    
    # Save cleaned data
    cleaned_crime_path = f"{PROCESSED_DIR}/crime_cleaned.csv"
    crime_df.to_csv(cleaned_crime_path, index=False)
    print(f"   - Saved to: {cleaned_crime_path}")
    print()
    
except Exception as e:
    print(f"❌ Error cleaning crime data: {e}")
    print()

# ============================================================================
# CLEAN 311 SERVICE REQUEST DATA
# ============================================================================

print("🧹 CLEANING 311 SERVICE REQUEST DATA...")
print("-" * 80)

try:
    service_311_df = pd.read_csv(f"{RAW_DIR}/311_requests_raw.csv")
    print(f"Loaded {len(service_311_df):,} 311 service requests")
    
    # Identify date columns (they vary by dataset version)
    date_columns = [col for col in service_311_df.columns if 'DATE' in col.upper() or 'TIME' in col.upper()]
    print(f"   Date columns found: {date_columns}")
    
    # Common column patterns in Denver 311 data
    # Try to find opened/closed date columns
    opened_col = None
    closed_col = None
    
    for col in service_311_df.columns:
        col_upper = col.upper()
        if 'OPEN' in col_upper or 'CREATE' in col_upper or 'SUBMIT' in col_upper:
            if 'DATE' in col_upper or 'TIME' in col_upper:
                opened_col = col
        if 'CLOSE' in col_upper or 'RESOLVE' in col_upper or 'COMPLETE' in col_upper:
            if 'DATE' in col_upper or 'TIME' in col_upper:
                closed_col = col
    
    print(f"   Identified opened date column: {opened_col}")
    print(f"   Identified closed date column: {closed_col}")
    
    # Parse dates if found
    if opened_col:
        service_311_df['opened_date'] = pd.to_datetime(
            service_311_df[opened_col], 
            errors='coerce'
        )
        service_311_df['year'] = service_311_df['opened_date'].dt.year
        service_311_df['month'] = service_311_df['opened_date'].dt.month
    
    if closed_col:
        service_311_df['closed_date'] = pd.to_datetime(
            service_311_df[closed_col], 
            errors='coerce'
        )
        # Calculate response time in days
        service_311_df['response_time_days'] = (
            service_311_df['closed_date'] - service_311_df['opened_date']
        ).dt.total_seconds() / (24 * 3600)
    
    # Standardize column names
    # Note: Denver 311 data uses "Case Summary" as the request type/category
    rename_map = {}
    for col in service_311_df.columns:
        col_upper = col.upper()
        if 'CASE_ID' in col_upper or 'SERVICE_ID' in col_upper or 'OBJECTID' in col_upper:
            rename_map[col] = 'case_id'
        elif col == 'Case Summary':
            # Case Summary contains the request category/type in Denver 311 data
            rename_map[col] = 'case_type'
        elif 'CASE_TYPE' in col_upper or 'SERVICE_TYPE' in col_upper or 'REQUEST_TYPE' in col_upper:
            rename_map[col] = 'case_type'
        elif 'STATUS' in col_upper or col == 'Case Status':
            rename_map[col] = 'status'
        elif 'NEIGHBORHOOD' in col_upper and 'NAME' in col_upper:
            rename_map[col] = 'neighborhood'
        elif col == 'Neighborhood':
            rename_map[col] = 'neighborhood'
        elif col == 'latitude' or col == 'LATITUDE' or col == 'Latitude':
            rename_map[col] = 'latitude'
        elif col == 'longitude' or col == 'LONGITUDE' or col == 'Longitude':
            rename_map[col] = 'longitude'

    service_311_df.rename(columns=rename_map, inplace=True)
    
    # Filter for streetlight-related requests (our primary focus for MVP)
    if 'case_type' in service_311_df.columns:
        # Find streetlight-related requests
        # Convert to string first to handle any null/mixed type values
        service_311_df['case_type'] = service_311_df['case_type'].astype(str)

        streetlight_keywords = ['LIGHT', 'STREET', 'LAMP', 'ILLUMINATION']
        streetlight_mask = service_311_df['case_type'].str.upper().str.contains(
            '|'.join(streetlight_keywords),
            na=False
        )
        
        streetlight_df = service_311_df[streetlight_mask].copy()
        
        print(f"✅ Cleaned 311 data:")
        print(f"   - Total requests: {len(service_311_df):,}")
        print(f"   - Streetlight-related: {len(streetlight_df):,}")
        
        if opened_col:
            print(f"   - Date range: {service_311_df['opened_date'].min()} to {service_311_df['opened_date'].max()}")
        
        # Show top request types
        if 'case_type' in service_311_df.columns:
            print(f"\n   Top 10 Request Types:")
            top_types = service_311_df['case_type'].value_counts().head(10)
            for service_type, count in top_types.items():
                print(f"     - {service_type}: {count:,}")
        
        # Assign neighborhoods to streetlight requests using crime data spatial reference
        print(f"\n   Assigning neighborhoods to streetlight requests...")
        try:
            # Load crime data which has neighborhood assignments
            if os.path.exists(f"{PROCESSED_DIR}/crime_cleaned.csv"):
                crime_ref = pd.read_csv(f"{PROCESSED_DIR}/crime_cleaned.csv")
                crime_ref = crime_ref[crime_ref['neighborhood'].notna() &
                                     crime_ref['latitude'].notna() &
                                     crime_ref['longitude'].notna()].copy()

                # For each streetlight request with lat/lon, find nearest crime neighborhood
                from scipy.spatial import cKDTree

                # Build KDTree from crime locations
                crime_coords = crime_ref[['latitude', 'longitude']].values
                tree = cKDTree(crime_coords)

                # Find neighborhoods for streetlights with coordinates
                mask = streetlight_df['latitude'].notna() & streetlight_df['longitude'].notna()
                streetlight_coords = streetlight_df.loc[mask, ['latitude', 'longitude']].values

                if len(streetlight_coords) > 0:
                    distances, indices = tree.query(streetlight_coords)
                    assigned_neighborhoods = crime_ref.iloc[indices]['neighborhood'].values

                    # Update neighborhood column for records with coordinates
                    streetlight_df.loc[mask, 'neighborhood'] = assigned_neighborhoods

                    assigned_count = streetlight_df['neighborhood'].notna().sum()
                    print(f"     - Assigned {assigned_count} / {len(streetlight_df)} streetlight requests to neighborhoods")
            else:
                print(f"     - Crime data not yet processed, skipping neighborhood assignment")
        except Exception as e:
            print(f"     - Could not assign neighborhoods: {e}")

        # Save cleaned data
        cleaned_311_path = f"{PROCESSED_DIR}/311_requests_cleaned.csv"
        service_311_df.to_csv(cleaned_311_path, index=False)
        print(f"\n   - Saved all 311 requests to: {cleaned_311_path}")

        streetlight_path = f"{PROCESSED_DIR}/311_streetlights_cleaned.csv"
        streetlight_df.to_csv(streetlight_path, index=False)
        print(f"   - Saved streetlight requests to: {streetlight_path}")
        print()
    else:
        print(f"⚠️  Could not identify case_type column for filtering")
        print(f"   Available columns: {list(service_311_df.columns)}")
        print()
    
except Exception as e:
    print(f"❌ Error cleaning 311 data: {e}")
    import traceback
    traceback.print_exc()
    print()

# ============================================================================
# CLEANING SUMMARY
# ============================================================================

print("=" * 80)
print("CLEANING SUMMARY")
print("=" * 80)
print()
print("✅ Data successfully cleaned and saved to: ./data/processed/")
print()
print("Next steps:")
print("1. Run: python scripts/03_analyze_correlations.py")
print("2. Review: ./analysis/ folder for initial findings")
print()
print("Files created:")
for root, dirs, files in os.walk(PROCESSED_DIR):
    for file in files:
        filepath = os.path.join(root, file)
        size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"  - {filepath} ({size:.2f} MB)")
print()
print("=" * 80)
