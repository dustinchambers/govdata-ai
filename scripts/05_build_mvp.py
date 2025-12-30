#!/usr/bin/env python3
"""
CIVIC VALUE INDEX - MVP Builder
Creates neighborhood intelligence reports combining:
- Crime data
- 311 service quality
- City budget allocation
- Real estate market data

Target customers: Real estate agents, investors, home buyers
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
import os

# Configuration
DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"
OUTPUT_DIR = "mvp_output"
REPORTS_DIR = f"{OUTPUT_DIR}/reports"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

print("=" * 80)
print("CIVIC VALUE INDEX - MVP BUILDER")
print("Building Neighborhood Intelligence Platform")
print("=" * 80)
print()

# ============================================================================
# STEP 1: LOAD ALL DATASETS
# ============================================================================

print("📂 STEP 1: LOADING ALL DATASETS...")
print("-" * 80)

# Load processed crime data
crime_df = pd.read_csv(f"{PROCESSED_DIR}/crime_cleaned.csv", low_memory=False)
print(f"✅ Loaded {len(crime_df):,} crime records")

# Load processed 311 data
service_311_df = pd.read_csv(f"{PROCESSED_DIR}/311_requests_cleaned.csv", low_memory=False)
streetlight_df = pd.read_csv(f"{PROCESSED_DIR}/311_streetlights_cleaned.csv", low_memory=False)
print(f"✅ Loaded {len(service_311_df):,} 311 service requests")
print(f"✅ Loaded {len(streetlight_df):,} streetlight requests")

# Load Checkbook budget data
checkbook_df = pd.read_csv(f"{RAW_DIR}/checkbook_raw.csv", low_memory=False)
print(f"✅ Loaded {len(checkbook_df):,} budget transactions")

# Load Zillow neighborhood home values
zillow_df = pd.read_csv(f"{RAW_DIR}/zillow_neighborhood_values.csv", low_memory=False)
print(f"✅ Loaded Zillow data for {len(zillow_df):,} neighborhoods nationwide")
print()

# ============================================================================
# STEP 2: PROCESS ZILLOW DATA FOR DENVER
# ============================================================================

print("🏡 STEP 2: EXTRACTING DENVER REAL ESTATE DATA...")
print("-" * 80)

# Filter for Denver, CO
denver_zillow = zillow_df[
    (zillow_df['City'] == 'Denver') &
    (zillow_df['State'] == 'CO')
].copy()

print(f"Found {len(denver_zillow)} Denver neighborhoods in Zillow data")

# Get most recent home values (last column with data)
value_cols = [col for col in denver_zillow.columns if col.startswith('20')]
if value_cols:
    latest_col = sorted(value_cols)[-1]
    denver_zillow['current_home_value'] = denver_zillow[latest_col]

    # Calculate YoY appreciation if we have data from 12 months ago
    if len(value_cols) >= 12:
        year_ago_col = sorted(value_cols)[-13]
        denver_zillow['yoy_appreciation'] = (
            (denver_zillow[latest_col] - denver_zillow[year_ago_col]) /
            denver_zillow[year_ago_col] * 100
        )

    print(f"✅ Using home values from {latest_col}")
    print(f"   Median Denver home value: ${denver_zillow['current_home_value'].median():,.0f}")

# Normalize neighborhood names to match crime data
denver_zillow['neighborhood_normalized'] = denver_zillow['RegionName'].str.lower().str.replace(' ', '-')

print()

# ============================================================================
# STEP 3: AGGREGATE CRIME DATA BY NEIGHBORHOOD
# ============================================================================

print("🚨 STEP 3: AGGREGATING CRIME DATA...")
print("-" * 80)

crime_df['first_occurrence_date'] = pd.to_datetime(crime_df['first_occurrence_date'])

# Get recent crime (last 12 months)
cutoff_date = crime_df['first_occurrence_date'].max() - pd.Timedelta(days=365)
recent_crime = crime_df[crime_df['first_occurrence_date'] >= cutoff_date]

crime_by_hood = recent_crime.groupby('neighborhood').agg({
    'incident_id': 'count',
    'is_traffic': 'sum'
}).rename(columns={
    'incident_id': 'total_crimes_12mo',
    'is_traffic': 'traffic_crimes_12mo'
})

# Crime categories
property_crimes = ['theft', 'burglary', 'motor-vehicle-theft', 'larceny', 'arson']
violent_crimes = ['assault', 'robbery', 'murder', 'sexual-assault']

recent_crime['is_property'] = recent_crime['offense_category'].str.lower().str.contains('|'.join(property_crimes), na=False)
recent_crime['is_violent'] = recent_crime['offense_category'].str.lower().str.contains('|'.join(violent_crimes), na=False)

property_by_hood = recent_crime[recent_crime['is_property']].groupby('neighborhood').size()
violent_by_hood = recent_crime[recent_crime['is_violent']].groupby('neighborhood').size()

crime_by_hood['property_crimes_12mo'] = property_by_hood
crime_by_hood['violent_crimes_12mo'] = violent_by_hood

# Crime trend (compare last 6mo vs previous 6mo)
six_mo_ago = crime_df['first_occurrence_date'].max() - pd.Timedelta(days=180)
recent_6mo = crime_df[crime_df['first_occurrence_date'] >= six_mo_ago]
prev_6mo = crime_df[(crime_df['first_occurrence_date'] >= cutoff_date) &
                     (crime_df['first_occurrence_date'] < six_mo_ago)]

recent_6mo_count = recent_6mo.groupby('neighborhood').size()
prev_6mo_count = prev_6mo.groupby('neighborhood').size()
crime_by_hood['crime_trend_pct'] = ((recent_6mo_count - prev_6mo_count) / prev_6mo_count * 100).fillna(0)

print(f"✅ Aggregated crime for {len(crime_by_hood)} neighborhoods")
print(f"   Total crimes (last 12 months): {crime_by_hood['total_crimes_12mo'].sum():,}")
print()

# ============================================================================
# STEP 4: AGGREGATE 311 SERVICE QUALITY
# ============================================================================

print("🛠️  STEP 4: AGGREGATING 311 SERVICE QUALITY...")
print("-" * 80)

# Parse dates
service_311_df['opened_date'] = pd.to_datetime(service_311_df['opened_date'], errors='coerce')
service_311_df['closed_date'] = pd.to_datetime(service_311_df['closed_date'], errors='coerce')

# Calculate response time
service_311_df['response_time_days'] = (
    service_311_df['closed_date'] - service_311_df['opened_date']
).dt.total_seconds() / (24 * 3600)

# Filter for valid requests with neighborhood data
valid_311 = service_311_df[
    (service_311_df['neighborhood'].notna()) &
    (service_311_df['response_time_days'] >= 0) &
    (service_311_df['response_time_days'] < 365)
]

service_311_by_hood = valid_311.groupby('neighborhood').agg({
    'case_id': 'count',
    'response_time_days': ['mean', 'median']
}).reset_index()

service_311_by_hood.columns = ['neighborhood', 'total_311_requests', 'avg_response_days', 'median_response_days']

print(f"✅ Aggregated 311 data for neighborhoods")
print(f"   Total valid requests: {len(valid_311):,}")
print()

# ============================================================================
# STEP 5: AGGREGATE BUDGET SPENDING BY NEIGHBORHOOD
# ============================================================================

print("💰 STEP 5: ANALYZING BUDGET ALLOCATION...")
print("-" * 80)

# The checkbook data doesn't have neighborhood info, so we'll calculate per-capita spending
checkbook_df['Amount'] = pd.to_numeric(checkbook_df['Amount'], errors='coerce')
checkbook_df['Year'] = pd.to_numeric(checkbook_df['Year'], errors='coerce')

# Get recent spending (2024-2025)
recent_budget = checkbook_df[checkbook_df['Year'] >= 2024]

# Key departments
safety_spending = recent_budget[recent_budget['Department'].str.contains('POLICE|SAFETY|FIRE', case=False, na=False)]['Amount'].sum()
public_works_spending = recent_budget[recent_budget['Department'].str.contains('PUBLIC WORKS', case=False, na=False)]['Amount'].sum()
total_spending = recent_budget['Amount'].sum()

print(f"✅ Total city spending (2024-2025): ${total_spending:,.0f}")
print(f"   Safety (Police/Fire): ${safety_spending:,.0f}")
print(f"   Public Works: ${public_works_spending:,.0f}")
print()

# ============================================================================
# STEP 6: MERGE ALL DATA & BUILD NEIGHBORHOOD PROFILES
# ============================================================================

print("🔗 STEP 6: MERGING DATA & BUILDING PROFILES...")
print("-" * 80)

# Start with crime data (our base)
profiles = crime_by_hood.copy()
profiles = profiles.reset_index()

# Merge 311 service quality
profiles = profiles.merge(service_311_by_hood, on='neighborhood', how='left')

# Merge Zillow real estate data
profiles = profiles.merge(
    denver_zillow[['neighborhood_normalized', 'current_home_value', 'yoy_appreciation']],
    left_on='neighborhood',
    right_on='neighborhood_normalized',
    how='left'
)

# Fill NaN values
profiles = profiles.fillna(0)

print(f"✅ Built profiles for {len(profiles)} neighborhoods")
print()

# ============================================================================
# STEP 7: CALCULATE CIVIC VALUE INDEX SCORES
# ============================================================================

print("📊 STEP 7: CALCULATING CIVIC VALUE INDEX SCORES...")
print("-" * 80)

def normalize_score(series, inverse=False):
    """Normalize to 0-100 scale"""
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([50] * len(series))

    normalized = (series - min_val) / (max_val - min_val) * 100
    if inverse:
        normalized = 100 - normalized
    return normalized

# SAFETY SCORE (0-100, higher is better)
profiles['safety_score'] = (
    normalize_score(profiles['total_crimes_12mo'], inverse=True) * 0.4 +
    normalize_score(profiles['violent_crimes_12mo'], inverse=True) * 0.4 +
    normalize_score(profiles['crime_trend_pct'], inverse=True) * 0.2
)

# SERVICE QUALITY SCORE (0-100, higher is better)
profiles['service_score'] = (
    normalize_score(profiles['median_response_days'], inverse=True) * 0.6 +
    normalize_score(profiles['total_311_requests']) * 0.4  # More requests = more engagement
)

# MARKET PERFORMANCE SCORE (0-100, higher is better)
profiles['market_score'] = (
    normalize_score(profiles['current_home_value']) * 0.5 +
    normalize_score(profiles['yoy_appreciation']) * 0.5
)

# CIVIC VALUE RATIO (Cost-effectiveness)
# This is the secret sauce: Service quality per dollar of property value
profiles['cost_per_crime'] = profiles['total_crimes_12mo'] / (profiles['current_home_value'] / 1000000)
profiles['civic_value_ratio'] = normalize_score(profiles['cost_per_crime'], inverse=True)

# OVERALL CIVIC VALUE INDEX (0-100)
profiles['civic_value_index'] = (
    profiles['safety_score'] * 0.30 +
    profiles['service_score'] * 0.25 +
    profiles['market_score'] * 0.25 +
    profiles['civic_value_ratio'] * 0.20
)

# Round scores
score_cols = ['safety_score', 'service_score', 'market_score', 'civic_value_ratio', 'civic_value_index']
profiles[score_cols] = profiles[score_cols].round(1)

print("✅ Calculated Civic Value Index scores:")
print(f"   Average score: {profiles['civic_value_index'].mean():.1f}")
print(f"   Top neighborhood: {profiles.nlargest(1, 'civic_value_index')['neighborhood'].values[0]}")
print(f"   Score: {profiles['civic_value_index'].max():.1f}")
print()

# ============================================================================
# STEP 8: SAVE MASTER DATA
# ============================================================================

print("💾 STEP 8: SAVING MASTER DATA...")
print("-" * 80)

profiles.to_csv(f"{OUTPUT_DIR}/neighborhood_profiles.csv", index=False)
print(f"✅ Saved master data: {OUTPUT_DIR}/neighborhood_profiles.csv")
print()

# ============================================================================
# STEP 9: GENERATE SAMPLE REPORTS
# ============================================================================

print("📝 STEP 9: GENERATING SAMPLE REPORTS...")
print("-" * 80)

# Select diverse sample neighborhoods
top_5 = profiles.nlargest(5, 'civic_value_index')
bottom_5 = profiles.nsmallest(5, 'civic_value_index')
random_5 = profiles.sample(min(5, len(profiles)))

sample_neighborhoods = pd.concat([top_5, bottom_5]).drop_duplicates()

report_count = 0
for idx, hood in sample_neighborhoods.iterrows():
    report_count += 1

    report = f"""
{'=' * 80}
CIVIC VALUE INDEX - NEIGHBORHOOD INTELLIGENCE REPORT
Generated: {datetime.now().strftime('%B %d, %Y')}
{'=' * 80}

NEIGHBORHOOD: {hood['neighborhood'].upper()}

{'=' * 80}
OVERALL CIVIC VALUE INDEX: {hood['civic_value_index']:.1f}/100
{'=' * 80}

{'▓' * int(hood['civic_value_index'] / 5)}{' ' * (20 - int(hood['civic_value_index'] / 5))}

COMPONENT SCORES:
-----------------
🛡️  Safety Score:              {hood['safety_score']:.1f}/100
🛠️  Service Quality Score:     {hood['service_score']:.1f}/100
📈 Market Performance Score:   {hood['market_score']:.1f}/100
💎 Civic Value Ratio:          {hood['civic_value_ratio']:.1f}/100

{'=' * 80}
SAFETY PROFILE
{'=' * 80}

Total Crimes (Last 12 months):    {hood['total_crimes_12mo']:.0f}
  • Property Crimes:              {hood['property_crimes_12mo']:.0f}
  • Violent Crimes:               {hood['violent_crimes_12mo']:.0f}
  • Traffic Incidents:            {hood['traffic_crimes_12mo']:.0f}

Crime Trend:                      {hood['crime_trend_pct']:+.1f}%
{('🔺 INCREASING' if hood['crime_trend_pct'] > 0 else '🔻 DECREASING')}

{'=' * 80}
SERVICE QUALITY
{'=' * 80}

Total 311 Service Requests:       {hood['total_311_requests']:.0f}
Average Response Time:            {hood['avg_response_days']:.1f} days
Median Response Time:             {hood['median_response_days']:.1f} days

Service Rating: {'⭐' * min(5, int(hood['service_score'] / 20))}

{'=' * 80}
REAL ESTATE MARKET
{'=' * 80}

Current Median Home Value:        ${hood['current_home_value']:,.0f}
Year-over-Year Appreciation:      {hood['yoy_appreciation']:.1f}%

Market Momentum: {'🔥 HOT' if hood['yoy_appreciation'] > 5 else '📊 STABLE' if hood['yoy_appreciation'] > 0 else '❄️ COOLING'}

{'=' * 80}
INVESTMENT INSIGHTS
{'=' * 80}

CIVIC VALUE ANALYSIS:
This neighborhood scores {hood['civic_value_index']:.0f}/100 on our Civic Value Index.

KEY INSIGHT:
{f"🏆 EXCELLENT VALUE - This is a TOP TIER neighborhood with high scores across safety, service quality, and market performance." if hood['civic_value_index'] > 70 else
 f"✅ GOOD VALUE - This neighborhood offers solid fundamentals with room for growth." if hood['civic_value_index'] > 50 else
 f"⚠️ OPPORTUNITY ZONE - Lower current scores may indicate undervaluation or areas needing municipal attention."}

WHAT THIS MEANS FOR YOU:
-------------------------

FOR HOME BUYERS:
{f"• Expect premium pricing due to excellent safety and service scores" if hood['civic_value_index'] > 70 else
 f"• Moderate pricing with good city services and acceptable safety" if hood['civic_value_index'] > 50 else
 f"• Lower entry price point, consider future infrastructure investment"}

FOR INVESTORS:
{f"• Strong hold for appreciation, low crime risk" if hood['safety_score'] > 70 and hood['market_score'] > 60 else
 f"• Watch for turnaround signals: improving crime trends + rising home values" if hood['crime_trend_pct'] < 0 and hood['yoy_appreciation'] > 3 else
 f"• Higher risk/reward profile - monitor city investment trends"}

FOR RENTERS:
{f"• Premium rental market, expect higher rents but excellent quality of life" if hood['civic_value_index'] > 70 else
 f"• Good balance of affordability and livability" if hood['civic_value_index'] > 50 else
 f"• More affordable, prioritize security measures"}

{'=' * 80}
COMPETITIVE POSITIONING
{'=' * 80}

This neighborhood ranks #{profiles[profiles['civic_value_index'] >= hood['civic_value_index']].shape[0]} out of {len(profiles)} Denver neighborhoods.

Percentile Rank: {(1 - (profiles[profiles['civic_value_index'] >= hood['civic_value_index']].shape[0] / len(profiles))) * 100:.0f}th percentile

{'=' * 80}
DATA SOURCES
{'=' * 80}
• Crime Data: City and County of Denver (2020-2025)
• 311 Service Data: Denver 311 System
• Budget Data: Denver Checkbook (Transparent Denver)
• Real Estate Data: Zillow Home Value Index (ZHVI)

Analysis by: Civic Value Index Platform
Report ID: {hood['neighborhood']}-{datetime.now().strftime('%Y%m%d')}

{'=' * 80}
DISCLAIMER: This report is for informational purposes only and should not be
considered financial, legal, or investment advice. All data is subject to change.
{'=' * 80}
"""

    # Save report
    filename = f"{REPORTS_DIR}/{hood['neighborhood'].replace(' ', '_')}_report.txt"
    with open(filename, 'w') as f:
        f.write(report)

    print(f"✅ Generated report {report_count}: {hood['neighborhood']}")

print()
print(f"Generated {report_count} sample neighborhood reports")
print()

# ============================================================================
# SUMMARY
# ============================================================================

print("=" * 80)
print("MVP BUILD COMPLETE!")
print("=" * 80)
print()
print(f"📊 Analyzed {len(profiles)} Denver neighborhoods")
print(f"📝 Generated {report_count} sample reports")
print()
print("Output files:")
print(f"  • Master data: {OUTPUT_DIR}/neighborhood_profiles.csv")
print(f"  • Sample reports: {REPORTS_DIR}/")
print()
print("Next steps:")
print("  1. Review sample reports")
print("  2. Test with real estate agents")
print("  3. Build web interface")
print("  4. Add payment/subscription system")
print()
print("=" * 80)
