#!/usr/bin/env python3
"""
Ethica.Design - Civic Data Intelligence Platform
Exploratory Data Analysis Script

Deep exploratory analysis without predefined hypotheses.
Discovers correlations, outliers, and hidden patterns in Denver civic data.
"""

import pandas as pd
import numpy as np
from scipy import stats
from scipy.cluster.hierarchy import dendrogram, linkage
import json
import os

# Configuration
DATA_DIR = "data"
PROCESSED_DIR = f"{DATA_DIR}/processed"
ANALYSIS_DIR = "analysis"
EXPLORATORY_DIR = f"{ANALYSIS_DIR}/exploratory"

os.makedirs(EXPLORATORY_DIR, exist_ok=True)

print("=" * 80)
print("ETHICA.DESIGN - EXPLORATORY DATA ANALYSIS")
print("Discovering Hidden Patterns in Denver Civic Data")
print("=" * 80)
print()

# ============================================================================
# LOAD DATA
# ============================================================================

print("📂 LOADING DATA...")
print("-" * 80)

crime_df = pd.read_csv(f"{PROCESSED_DIR}/crime_cleaned.csv")
service_311_df = pd.read_csv(f"{PROCESSED_DIR}/311_requests_cleaned.csv")
streetlight_df = pd.read_csv(f"{PROCESSED_DIR}/311_streetlights_cleaned.csv")

print(f"✅ Loaded {len(crime_df):,} crime records")
print(f"✅ Loaded {len(service_311_df):,} 311 service requests")
print(f"✅ Loaded {len(streetlight_df):,} streetlight requests")
print()

# Parse dates
crime_df['first_occurrence_date'] = pd.to_datetime(crime_df['first_occurrence_date'])
crime_df['reported_date'] = pd.to_datetime(crime_df['reported_date'])
streetlight_df['opened_date'] = pd.to_datetime(streetlight_df['opened_date'])
streetlight_df['closed_date'] = pd.to_datetime(streetlight_df['closed_date'])

# ============================================================================
# PART 1: COMPREHENSIVE CORRELATION ANALYSIS
# ============================================================================

print("🔍 PART 1: COMPREHENSIVE CORRELATION ANALYSIS")
print("-" * 80)

# Aggregate multiple dimensions by neighborhood
print("Building multi-dimensional neighborhood profiles...")

# Crime dimensions
crime_by_hood = crime_df.groupby('neighborhood').agg({
    'incident_id': 'count',
    'is_traffic': 'sum',
    'year': lambda x: x.mode()[0] if len(x) > 0 else None,  # Most common year
    'month': 'mean',  # Average month
    'hour': 'mean',  # Average hour of crime
}).rename(columns={
    'incident_id': 'total_crimes',
    'is_traffic': 'traffic_crimes',
    'year': 'most_common_crime_year',
    'month': 'avg_crime_month',
    'hour': 'avg_crime_hour'
})

# Crime by time of day
crime_df['time_period'] = pd.cut(crime_df['hour'],
                                  bins=[0, 6, 12, 18, 24],
                                  labels=['night', 'morning', 'afternoon', 'evening'])
time_crimes = crime_df.groupby(['neighborhood', 'time_period']).size().unstack(fill_value=0)
crime_by_hood['night_crimes'] = time_crimes.get('night', 0)
crime_by_hood['morning_crimes'] = time_crimes.get('morning', 0)
crime_by_hood['afternoon_crimes'] = time_crimes.get('afternoon', 0)
crime_by_hood['evening_crimes'] = time_crimes.get('evening', 0)

# Crime by day of week
crime_df['is_weekend'] = crime_df['day_of_week'].isin(['Saturday', 'Sunday'])
weekend_crimes = crime_df[crime_df['is_weekend']].groupby('neighborhood').size()
crime_by_hood['weekend_crimes'] = weekend_crimes
crime_by_hood['weekday_crimes'] = crime_by_hood['total_crimes'] - crime_by_hood['weekend_crimes'].fillna(0)

# Crime categories
property_crimes = ['theft', 'burglary', 'motor-vehicle-theft', 'larceny', 'arson', 'vandalism']
violent_crimes = ['assault', 'robbery', 'murder', 'sexual-assault']
crime_df['is_property_crime'] = crime_df['offense_category'].str.lower().str.contains('|'.join(property_crimes), na=False)
crime_df['is_violent_crime'] = crime_df['offense_category'].str.lower().str.contains('|'.join(violent_crimes), na=False)

property_by_hood = crime_df[crime_df['is_property_crime']].groupby('neighborhood').size()
violent_by_hood = crime_df[crime_df['is_violent_crime']].groupby('neighborhood').size()
crime_by_hood['property_crimes'] = property_by_hood
crime_by_hood['violent_crimes'] = violent_by_hood

# 311 Service dimensions
if 'neighborhood' in streetlight_df.columns:
    streetlight_by_hood = streetlight_df[streetlight_df['neighborhood'].notna()].groupby('neighborhood').agg({
        'case_id': 'count',
        'response_time_days': ['mean', 'median', 'std', 'min', 'max'],
        'opened_date': lambda x: (pd.to_datetime(x).max() - pd.to_datetime(x).min()).days
    })

    streetlight_by_hood.columns = [
        'total_streetlight_requests',
        'avg_response_time',
        'median_response_time',
        'std_response_time',
        'min_response_time',
        'max_response_time',
        'streetlight_timespan_days'
    ]

    # Merge datasets
    analysis_df = crime_by_hood.merge(streetlight_by_hood, left_index=True, right_index=True, how='outer')
    analysis_df = analysis_df.fillna(0)

    print(f"✅ Built profiles for {len(analysis_df)} neighborhoods")
    print(f"   Features: {len(analysis_df.columns)}")
    print()

    # Calculate ALL correlations
    print("Computing full correlation matrix...")
    correlation_matrix = analysis_df.corr()

    # Find strongest correlations (excluding self-correlations)
    correlations_list = []
    for i in range(len(correlation_matrix.columns)):
        for j in range(i+1, len(correlation_matrix.columns)):
            col1 = correlation_matrix.columns[i]
            col2 = correlation_matrix.columns[j]
            corr_value = correlation_matrix.iloc[i, j]

            if not np.isnan(corr_value) and abs(corr_value) > 0.3:  # Only meaningful correlations
                correlations_list.append({
                    'feature1': col1,
                    'feature2': col2,
                    'correlation': corr_value,
                    'abs_correlation': abs(corr_value)
                })

    correlations_df = pd.DataFrame(correlations_list).sort_values('abs_correlation', ascending=False)

    print(f"✅ Found {len(correlations_df)} significant correlations (|r| > 0.3)")
    print()
    print("🌟 TOP 15 STRONGEST CORRELATIONS:")
    print("-" * 80)
    for idx, row in correlations_df.head(15).iterrows():
        direction = "↗️ Positive" if row['correlation'] > 0 else "↘️ Negative"
        print(f"{direction:15} {row['correlation']:6.3f}  {row['feature1']:30} ↔ {row['feature2']}")
    print()

    # Save correlation matrix
    correlation_matrix.to_csv(f"{EXPLORATORY_DIR}/correlation_matrix.csv")
    correlations_df.to_csv(f"{EXPLORATORY_DIR}/top_correlations.csv", index=False)

# ============================================================================
# PART 2: OUTLIER DETECTION (NORMALIZED)
# ============================================================================

print("🎯 PART 2: OUTLIER DETECTION")
print("-" * 80)

# Normalize data using z-scores
print("Normalizing data using z-scores...")
numeric_cols = analysis_df.select_dtypes(include=[np.number]).columns
z_scores = np.abs(stats.zscore(analysis_df[numeric_cols], nan_policy='omit'))
z_scores_df = pd.DataFrame(z_scores, columns=numeric_cols, index=analysis_df.index)

# Find outliers (z-score > 2.5)
outliers = {}
for col in numeric_cols:
    outlier_mask = z_scores_df[col] > 2.5
    if outlier_mask.any():
        outlier_neighborhoods = analysis_df[outlier_mask].index.tolist()
        outlier_values = analysis_df.loc[outlier_mask, col].tolist()
        z_values = z_scores_df.loc[outlier_mask, col].tolist()

        outliers[col] = [
            {
                'neighborhood': hood,
                'value': float(val),
                'z_score': float(z),
                'interpretation': 'high' if val > analysis_df[col].median() else 'low'
            }
            for hood, val, z in zip(outlier_neighborhoods, outlier_values, z_values)
        ]

print(f"✅ Found outliers in {len(outliers)} features")
print()
print("🚨 TOP OUTLIER NEIGHBORHOODS BY FEATURE:")
print("-" * 80)

for feature, outlier_list in sorted(outliers.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
    print(f"\n{feature}:")
    for outlier in sorted(outlier_list, key=lambda x: x['z_score'], reverse=True)[:3]:
        print(f"   {outlier['neighborhood']:25} = {outlier['value']:>10.1f} (z={outlier['z_score']:.2f}, {outlier['interpretation']})")

print()

# Multi-dimensional outliers (neighborhoods that are outliers in multiple ways)
outlier_counts = {}
for feature, outlier_list in outliers.items():
    for outlier in outlier_list:
        hood = outlier['neighborhood']
        if hood not in outlier_counts:
            outlier_counts[hood] = []
        outlier_counts[hood].append({
            'feature': feature,
            'z_score': outlier['z_score'],
            'interpretation': outlier['interpretation']
        })

multi_outliers = {k: v for k, v in outlier_counts.items() if len(v) >= 3}
print("\n🎭 MULTI-DIMENSIONAL OUTLIERS (unusual in 3+ ways):")
print("-" * 80)
for hood, features in sorted(multi_outliers.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
    print(f"\n{hood} ({len(features)} outlier features):")
    for feat in sorted(features, key=lambda x: x['z_score'], reverse=True)[:5]:
        print(f"   - {feat['feature']:30} z={feat['z_score']:.2f} ({feat['interpretation']})")

print()

# Save outlier analysis
with open(f"{EXPLORATORY_DIR}/outliers.json", 'w') as f:
    json.dump(outliers, f, indent=2)

with open(f"{EXPLORATORY_DIR}/multi_dimensional_outliers.json", 'w') as f:
    json.dump(multi_outliers, f, indent=2)

# ============================================================================
# PART 3: TEMPORAL PATTERNS
# ============================================================================

print("\n⏰ PART 3: TEMPORAL PATTERN ANALYSIS")
print("-" * 80)

# Crime temporal patterns
print("Analyzing crime temporal patterns...")
hourly_crime = crime_df.groupby('hour').size()
daily_crime = crime_df.groupby('day_of_week').size()
monthly_crime = crime_df.groupby('month').size()

# Streetlight temporal patterns
streetlight_df['opened_hour'] = pd.to_datetime(streetlight_df['opened_date']).dt.hour
streetlight_df['opened_month'] = pd.to_datetime(streetlight_df['opened_date']).dt.month
hourly_lights = streetlight_df.groupby('opened_hour').size()
monthly_lights = streetlight_df.groupby('opened_month').size()

print(f"\n📊 Crime Peak Times:")
print(f"   Peak hour: {hourly_crime.idxmax()}:00 ({hourly_crime.max():,} crimes)")
print(f"   Lowest hour: {hourly_crime.idxmin()}:00 ({hourly_crime.min():,} crimes)")
print(f"   Peak month: {monthly_crime.idxmax()} ({monthly_crime.max():,} crimes)")

print(f"\n💡 Streetlight Request Peak Times:")
print(f"   Peak hour: {hourly_lights.idxmax()}:00 ({hourly_lights.max():,} requests)")
print(f"   Peak month: {monthly_lights.idxmax()} ({monthly_lights.max():,} requests)")

# Are crimes and streetlight requests correlated by time?
hourly_corr = hourly_crime.corr(hourly_lights.reindex(hourly_crime.index, fill_value=0))
monthly_corr = monthly_crime.corr(monthly_lights.reindex(monthly_crime.index, fill_value=0))

print(f"\n⏱️  Temporal Synchronicity:")
print(f"   Hourly correlation: {hourly_corr:.3f}")
print(f"   Monthly correlation: {monthly_corr:.3f}")

# Save temporal data
temporal_analysis = {
    'hourly_crime': hourly_crime.to_dict(),
    'monthly_crime': monthly_crime.to_dict(),
    'hourly_streetlights': hourly_lights.to_dict(),
    'monthly_streetlights': monthly_lights.to_dict(),
    'hourly_correlation': float(hourly_corr),
    'monthly_correlation': float(monthly_corr)
}

with open(f"{EXPLORATORY_DIR}/temporal_patterns.json", 'w') as f:
    json.dump(temporal_analysis, f, indent=2)

# ============================================================================
# PART 4: CRIME TYPE DEEP DIVE
# ============================================================================

print("\n\n🔬 PART 4: CRIME TYPE ANALYSIS")
print("-" * 80)

print("Analyzing crime offense categories...")
crime_types = crime_df['offense_category'].value_counts().head(20)

print(f"\nTop 10 Crime Categories:")
for crime_type, count in crime_types.head(10).items():
    pct = (count / len(crime_df)) * 100
    print(f"   {crime_type:40} {count:>7,} ({pct:>5.1f}%)")

# Which crime types are most common at night?
night_crimes = crime_df[crime_df['time_period'] == 'night']
night_types = night_crimes['offense_category'].value_counts().head(10)

print(f"\nTop 5 Night Crimes (midnight-6am):")
for crime_type, count in night_types.head(5).items():
    pct = (count / len(night_crimes)) * 100
    print(f"   {crime_type:40} {count:>7,} ({pct:>5.1f}%)")

# Crime type diversity by neighborhood (Shannon entropy)
from scipy.stats import entropy

neighborhood_diversity = {}
for hood in crime_df['neighborhood'].unique():
    hood_crimes = crime_df[crime_df['neighborhood'] == hood]['offense_category'].value_counts()
    hood_crimes_norm = hood_crimes / hood_crimes.sum()
    diversity = entropy(hood_crimes_norm)
    neighborhood_diversity[hood] = {
        'entropy': float(diversity),
        'num_crime_types': len(hood_crimes),
        'total_crimes': int(hood_crimes.sum())
    }

diversity_df = pd.DataFrame(neighborhood_diversity).T.sort_values('entropy', ascending=False)

print(f"\n🌈 Most Diverse Crime Portfolios (top 5):")
for hood, row in diversity_df.head(5).iterrows():
    print(f"   {hood:30} entropy={row['entropy']:.2f}, {row['num_crime_types']} types, {row['total_crimes']:.0f} crimes")

print(f"\n🎯 Least Diverse Crime Portfolios (top 5):")
for hood, row in diversity_df.tail(5).iterrows():
    print(f"   {hood:30} entropy={row['entropy']:.2f}, {row['num_crime_types']} types, {row['total_crimes']:.0f} crimes")

diversity_df.to_csv(f"{EXPLORATORY_DIR}/neighborhood_crime_diversity.csv")

# ============================================================================
# PART 5: RESPONSE TIME ANOMALIES
# ============================================================================

print("\n\n⚡ PART 5: RESPONSE TIME ANOMALY DETECTION")
print("-" * 80)

# Filter valid response times
valid_response = streetlight_df[
    (streetlight_df['response_time_days'].notna()) &
    (streetlight_df['response_time_days'] >= 0) &
    (streetlight_df['response_time_days'] < 365)  # Less than a year
]

print(f"Analyzing {len(valid_response):,} streetlight requests with valid response times...")

# Response time statistics
response_stats = {
    'mean': float(valid_response['response_time_days'].mean()),
    'median': float(valid_response['response_time_days'].median()),
    'std': float(valid_response['response_time_days'].std()),
    'min': float(valid_response['response_time_days'].min()),
    'max': float(valid_response['response_time_days'].max()),
    'q25': float(valid_response['response_time_days'].quantile(0.25)),
    'q75': float(valid_response['response_time_days'].quantile(0.75))
}

# IQR outlier detection
iqr = response_stats['q75'] - response_stats['q25']
lower_bound = response_stats['q25'] - 1.5 * iqr
upper_bound = response_stats['q75'] + 1.5 * iqr

fast_outliers = valid_response[valid_response['response_time_days'] < lower_bound]
slow_outliers = valid_response[valid_response['response_time_days'] > upper_bound]

print(f"\n📈 Response Time Statistics:")
print(f"   Mean: {response_stats['mean']:.1f} days")
print(f"   Median: {response_stats['median']:.1f} days")
print(f"   Std Dev: {response_stats['std']:.1f} days")
print(f"   Range: {response_stats['min']:.1f} - {response_stats['max']:.1f} days")

print(f"\n⚡ Unusually FAST responses (<{lower_bound:.1f} days): {len(fast_outliers)}")
if len(fast_outliers) > 0:
    print(f"   Fastest: {fast_outliers['response_time_days'].min():.1f} days")
    fast_hoods = fast_outliers.groupby('neighborhood').size().sort_values(ascending=False).head(3)
    print(f"   Top neighborhoods with fast responses:")
    for hood, count in fast_hoods.items():
        print(f"      {hood}: {count} fast repairs")

print(f"\n🐌 Unusually SLOW responses (>{upper_bound:.1f} days): {len(slow_outliers)}")
if len(slow_outliers) > 0:
    print(f"   Slowest: {slow_outliers['response_time_days'].max():.1f} days")
    slow_hoods = slow_outliers.groupby('neighborhood').size().sort_values(ascending=False).head(3)
    print(f"   Top neighborhoods with slow responses:")
    for hood, count in slow_hoods.items():
        print(f"      {hood}: {count} slow repairs")

# Save response time analysis
response_analysis = {
    'statistics': response_stats,
    'outlier_bounds': {
        'lower': float(lower_bound),
        'upper': float(upper_bound)
    },
    'fast_outliers': len(fast_outliers),
    'slow_outliers': len(slow_outliers)
}

with open(f"{EXPLORATORY_DIR}/response_time_analysis.json", 'w') as f:
    json.dump(response_analysis, f, indent=2)

# ============================================================================
# SUMMARY REPORT
# ============================================================================

print("\n\n" + "=" * 80)
print("EXPLORATORY ANALYSIS SUMMARY")
print("=" * 80)

summary = {
    'data_overview': {
        'neighborhoods_analyzed': len(analysis_df),
        'total_crimes': int(crime_df.shape[0]),
        'total_311_requests': int(service_311_df.shape[0]),
        'total_streetlight_requests': int(streetlight_df.shape[0])
    },
    'correlation_insights': {
        'significant_correlations_found': len(correlations_df),
        'strongest_positive': {
            'features': f"{correlations_df.iloc[0]['feature1']} ↔ {correlations_df.iloc[0]['feature2']}",
            'correlation': float(correlations_df.iloc[0]['correlation'])
        } if len(correlations_df) > 0 and correlations_df.iloc[0]['correlation'] > 0 else None,
        'strongest_negative': {
            'features': f"{correlations_df[correlations_df['correlation'] < 0].iloc[0]['feature1']} ↔ {correlations_df[correlations_df['correlation'] < 0].iloc[0]['feature2']}",
            'correlation': float(correlations_df[correlations_df['correlation'] < 0].iloc[0]['correlation'])
        } if len(correlations_df[correlations_df['correlation'] < 0]) > 0 else None
    },
    'outlier_insights': {
        'features_with_outliers': len(outliers),
        'multi_dimensional_outliers': len(multi_outliers)
    },
    'temporal_insights': {
        'crime_peak_hour': int(hourly_crime.idxmax()),
        'crime_peak_month': int(monthly_crime.idxmax()),
        'streetlight_peak_hour': int(hourly_lights.idxmax()),
        'hourly_correlation': float(hourly_corr),
        'monthly_correlation': float(monthly_corr)
    },
    'response_time_insights': response_analysis
}

with open(f"{EXPLORATORY_DIR}/summary.json", 'w') as f:
    json.dump(summary, f, indent=2)

print("\n✅ Exploratory analysis complete!")
print(f"\n📁 All results saved to: {EXPLORATORY_DIR}/")
print("\nFiles created:")
print(f"   - correlation_matrix.csv (full correlation matrix)")
print(f"   - top_correlations.csv (strongest correlations)")
print(f"   - outliers.json (statistical outliers by feature)")
print(f"   - multi_dimensional_outliers.json (unusual neighborhoods)")
print(f"   - temporal_patterns.json (time-based patterns)")
print(f"   - neighborhood_crime_diversity.csv (crime type diversity)")
print(f"   - response_time_analysis.json (response time anomalies)")
print(f"   - summary.json (executive summary)")
print()
print("=" * 80)
