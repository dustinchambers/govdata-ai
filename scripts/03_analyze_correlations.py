#!/usr/bin/env python3
"""
Ethica.Design - Civic Data Intelligence Platform
Correlation Analysis Script

Analyzes correlations between crime and infrastructure (311 streetlights).
Prepares data for AI-powered insight generation.
"""

import pandas as pd
import numpy as np
from scipy import stats
import json
import os

# Configuration
DATA_DIR = "data"
PROCESSED_DIR = f"{DATA_DIR}/processed"
ANALYSIS_DIR = "analysis"

os.makedirs(ANALYSIS_DIR, exist_ok=True)

print("=" * 80)
print("ETHICA.DESIGN - CORRELATION ANALYSIS")
print("Crime + Infrastructure Analysis")
print("=" * 80)
print()

# ============================================================================
# LOAD CLEANED DATA
# ============================================================================

print("📂 LOADING CLEANED DATA...")
print("-" * 80)

try:
    crime_df = pd.read_csv(f"{PROCESSED_DIR}/crime_cleaned.csv")
    print(f"✅ Loaded {len(crime_df):,} crime records")

    service_311_df = pd.read_csv(f"{PROCESSED_DIR}/311_requests_cleaned.csv")
    print(f"✅ Loaded {len(service_311_df):,} total 311 requests")

    streetlight_df = pd.read_csv(f"{PROCESSED_DIR}/311_streetlights_cleaned.csv")
    print(f"✅ Loaded {len(streetlight_df):,} streetlight requests")
    print()

except Exception as e:
    print(f"❌ Error loading data: {e}")
    print("   Make sure to run 01_download_data.py and 02_clean_data.py first!")
    exit(1)

# ============================================================================
# AGGREGATE BY NEIGHBORHOOD
# ============================================================================

print("📊 AGGREGATING DATA BY NEIGHBORHOOD...")
print("-" * 80)

# Crime by neighborhood
crime_by_hood = crime_df.groupby('neighborhood').agg({
    'incident_id': 'count',  # Total crimes
    'is_traffic': 'sum',  # Traffic crimes
}).rename(columns={
    'incident_id': 'total_crimes',
    'is_traffic': 'traffic_crimes'
})

# Calculate property crime (common correlation with infrastructure)
property_crimes = ['theft', 'burglary', 'motor-vehicle-theft', 'larceny']
crime_df['is_property_crime'] = crime_df['offense_category'].str.lower().str.contains(
    '|'.join(property_crimes), 
    na=False
)
property_crime_by_hood = crime_df[crime_df['is_property_crime']].groupby('neighborhood').size()
crime_by_hood['property_crimes'] = property_crime_by_hood

# Violent crime
violent_crimes = ['assault', 'robbery', 'murder', 'sexual-assault']
crime_df['is_violent_crime'] = crime_df['offense_category'].str.lower().str.contains(
    '|'.join(violent_crimes), 
    na=False
)
violent_crime_by_hood = crime_df[crime_df['is_violent_crime']].groupby('neighborhood').size()
crime_by_hood['violent_crimes'] = violent_crime_by_hood

print(f"✅ Aggregated crime data:")
print(f"   - Neighborhoods with crime data: {len(crime_by_hood)}")
print(f"   - Total crimes analyzed: {crime_by_hood['total_crimes'].sum():,}")
print(f"   - Property crimes: {crime_by_hood['property_crimes'].sum():,}")
print(f"   - Violent crimes: {crime_by_hood['violent_crimes'].sum():,}")
print()

# 311 Streetlights by neighborhood
if 'neighborhood' in streetlight_df.columns:
    streetlight_by_hood = streetlight_df.groupby('neighborhood').agg({
        'case_id': 'count',  # Total requests
        'response_time_days': ['mean', 'median'],  # Response time
        'status': lambda x: (x.str.upper() == 'OPEN').sum()  # Pending requests
    })
    
    # Flatten column names
    streetlight_by_hood.columns = [
        'total_streetlight_requests',
        'avg_response_time_days',
        'median_response_time_days',
        'pending_streetlight_requests'
    ]
    
    print(f"✅ Aggregated streetlight data:")
    print(f"   - Neighborhoods with streetlight data: {len(streetlight_by_hood)}")
    print(f"   - Total streetlight requests: {streetlight_by_hood['total_streetlight_requests'].sum():,.0f}")
    print(f"   - Pending requests: {streetlight_by_hood['pending_streetlight_requests'].sum():,.0f}")
    print(f"   - Avg response time: {streetlight_by_hood['avg_response_time_days'].mean():.1f} days")
    print()
else:
    print(f"⚠️  Could not aggregate streetlight data by neighborhood")
    print(f"   Available columns: {list(streetlight_df.columns)}")
    print()

# ============================================================================
# MERGE DATA FOR CORRELATION ANALYSIS
# ============================================================================

print("🔗 MERGING DATASETS FOR CORRELATION...")
print("-" * 80)

# Merge crime and streetlight data
analysis_df = crime_by_hood.merge(
    streetlight_by_hood,
    left_index=True,
    right_index=True,
    how='outer'
)

# Fill NaN with 0 (neighborhoods with no streetlight requests)
analysis_df = analysis_df.fillna(0)

print(f"✅ Merged dataset:")
print(f"   - Neighborhoods: {len(analysis_df)}")
print(f"   - Features: {list(analysis_df.columns)}")
print()

# ============================================================================
# CORRELATION ANALYSIS
# ============================================================================

print("🔍 CALCULATING CORRELATIONS...")
print("-" * 80)

correlations = {}

# Correlation 1: Total crimes vs. pending streetlight requests
if len(analysis_df) > 0:
    corr, p_value = stats.pearsonr(
        analysis_df['total_crimes'],
        analysis_df['pending_streetlight_requests']
    )
    correlations['total_crime_vs_pending_lights'] = {
        'correlation': float(corr),
        'p_value': float(p_value),
        'significant': bool(p_value < 0.05)
    }
    
    print(f"📈 Total Crime vs. Pending Streetlight Requests:")
    print(f"   - Correlation: {corr:.3f}")
    print(f"   - P-value: {p_value:.4f}")
    print(f"   - Significant: {'YES ✅' if p_value < 0.05 else 'NO ❌'}")
    print()

# Correlation 2: Property crimes vs. pending streetlight requests
if len(analysis_df) > 0:
    corr, p_value = stats.pearsonr(
        analysis_df['property_crimes'],
        analysis_df['pending_streetlight_requests']
    )
    correlations['property_crime_vs_pending_lights'] = {
        'correlation': float(corr),
        'p_value': float(p_value),
        'significant': bool(p_value < 0.05)
    }
    
    print(f"📈 Property Crime vs. Pending Streetlight Requests:")
    print(f"   - Correlation: {corr:.3f}")
    print(f"   - P-value: {p_value:.4f}")
    print(f"   - Significant: {'YES ✅' if p_value < 0.05 else 'NO ❌'}")
    print()

# Correlation 3: Total crimes vs. average response time
if len(analysis_df) > 0 and analysis_df['avg_response_time_days'].sum() > 0:
    # Only use neighborhoods with actual response times
    mask = analysis_df['avg_response_time_days'] > 0
    if mask.sum() > 2:  # Need at least 3 points
        corr, p_value = stats.pearsonr(
            analysis_df[mask]['total_crimes'],
            analysis_df[mask]['avg_response_time_days']
        )
        correlations['total_crime_vs_response_time'] = {
            'correlation': float(corr),
            'p_value': float(p_value),
            'significant': bool(p_value < 0.05)
        }
        
        print(f"📈 Total Crime vs. Streetlight Response Time:")
        print(f"   - Correlation: {corr:.3f}")
        print(f"   - P-value: {p_value:.4f}")
        print(f"   - Significant: {'YES ✅' if p_value < 0.05 else 'NO ❌'}")
        print()

# ============================================================================
# IDENTIFY HIGH/LOW PERFORMERS
# ============================================================================

print("🎯 IDENTIFYING HIGH/LOW PERFORMING NEIGHBORHOODS...")
print("-" * 80)

# Calculate crime rate per streetlight request
analysis_df['crime_per_light_request'] = (
    analysis_df['total_crimes'] / 
    (analysis_df['total_streetlight_requests'] + 1)  # +1 to avoid division by zero
)

# Top 5 neighborhoods with most pending streetlight requests
top_pending = analysis_df.nlargest(5, 'pending_streetlight_requests')
print("⚠️  Top 5 Neighborhoods with Most Pending Streetlight Requests:")
for idx, row in top_pending.iterrows():
    print(f"   {idx}: {row['pending_streetlight_requests']:.0f} pending, {row['total_crimes']:.0f} crimes")
print()

# Top 5 neighborhoods with highest crime rates
top_crime = analysis_df.nlargest(5, 'total_crimes')
print("🚨 Top 5 Neighborhoods with Most Crime:")
for idx, row in top_crime.iterrows():
    print(f"   {idx}: {row['total_crimes']:.0f} crimes, {row['pending_streetlight_requests']:.0f} pending lights")
print()

# Best performers - low crime, low pending lights
analysis_df['performance_score'] = (
    100 - 
    (analysis_df['total_crimes'] / analysis_df['total_crimes'].max() * 50) -
    (analysis_df['pending_streetlight_requests'] / analysis_df['pending_streetlight_requests'].max() * 50)
)

top_performers = analysis_df.nlargest(5, 'performance_score')
print("✨ Top 5 Best Performing Neighborhoods:")
for idx, row in top_performers.iterrows():
    print(f"   {idx}: Score {row['performance_score']:.1f}, {row['total_crimes']:.0f} crimes, {row['pending_streetlight_requests']:.0f} pending")
print()

# ============================================================================
# SAVE RESULTS
# ============================================================================

print("💾 SAVING ANALYSIS RESULTS...")
print("-" * 80)

# Save aggregated data
analysis_csv_path = f"{ANALYSIS_DIR}/neighborhood_analysis.csv"
analysis_df.to_csv(analysis_csv_path)
print(f"✅ Saved neighborhood analysis to: {analysis_csv_path}")

# Save correlations
correlations_path = f"{ANALYSIS_DIR}/correlations.json"
with open(correlations_path, 'w') as f:
    json.dump(correlations, f, indent=2)
print(f"✅ Saved correlations to: {correlations_path}")

# Create summary for AI analysis
ai_summary = {
    'dataset_summary': {
        'total_neighborhoods': len(analysis_df),
        'total_crimes': int(analysis_df['total_crimes'].sum()),
        'total_streetlight_requests': int(analysis_df['total_streetlight_requests'].sum()),
        'pending_streetlight_requests': int(analysis_df['pending_streetlight_requests'].sum()),
        'avg_response_time_days': float(analysis_df['avg_response_time_days'].mean())
    },
    'correlations': correlations,
    'top_pending_neighborhoods': [
        {
            'neighborhood': idx,
            'pending_lights': int(row['pending_streetlight_requests']),
            'total_crimes': int(row['total_crimes']),
            'property_crimes': int(row['property_crimes'])
        }
        for idx, row in top_pending.iterrows()
    ],
    'top_crime_neighborhoods': [
        {
            'neighborhood': idx,
            'total_crimes': int(row['total_crimes']),
            'pending_lights': int(row['pending_streetlight_requests']),
            'avg_response_time': float(row['avg_response_time_days'])
        }
        for idx, row in top_crime.iterrows()
    ]
}

ai_summary_path = f"{ANALYSIS_DIR}/ai_analysis_input.json"
with open(ai_summary_path, 'w') as f:
    json.dump(ai_summary, f, indent=2)
print(f"✅ Saved AI analysis input to: {ai_summary_path}")
print()

# ============================================================================
# ANALYSIS SUMMARY
# ============================================================================

print("=" * 80)
print("ANALYSIS SUMMARY")
print("=" * 80)
print()
print("✅ Correlation analysis complete!")
print()
print("Key Findings:")
print(f"  • Analyzed {len(analysis_df)} neighborhoods")
print(f"  • {analysis_df['total_crimes'].sum():,.0f} total crimes")
print(f"  • {analysis_df['pending_streetlight_requests'].sum():,.0f} pending streetlight repairs")
print()
print("Significant Correlations:")
for key, value in correlations.items():
    if value.get('significant'):
        print(f"  ✅ {key.replace('_', ' ').title()}: {value['correlation']:.3f} (p={value['p_value']:.4f})")
print()
print("Next steps:")
print("1. Review: ./analysis/neighborhood_analysis.csv")
print("2. Review: ./analysis/correlations.json")
print("3. Run: python scripts/04_generate_ai_insights.py")
print("   (This will use Claude to generate natural language insights)")
print()
print("=" * 80)
