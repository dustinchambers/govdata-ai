#!/usr/bin/env python3
"""
Denver Crime + Infrastructure Analysis Pipeline
Processes crime and 311 data to find correlations and generate AI insights
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from anthropic import Anthropic

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "backend" / "api" / "data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_and_sample_crime_data():
    """Load crime data and create a manageable sample for analysis"""
    print("📊 Loading crime data...")
    crime_file = DATA_DIR / "crime.csv"

    # Load the data
    df = pd.read_csv(crime_file)
    print(f"   Loaded {len(df):,} crime records")

    # Parse dates
    df['FIRST_OCCURRENCE_DATE'] = pd.to_datetime(df['FIRST_OCCURRENCE_DATE'])
    df['year'] = df['FIRST_OCCURRENCE_DATE'].dt.year
    df['month'] = df['FIRST_OCCURRENCE_DATE'].dt.month

    # Filter to recent data (last 3 years for faster processing)
    recent_data = df[df['year'] >= 2022].copy()
    print(f"   Filtered to {len(recent_data):,} records (2022+)")

    return recent_data

def load_and_filter_311_data():
    """Load 311 data and filter for streetlight-related requests"""
    print("💡 Loading 311 service requests...")
    requests_file = DATA_DIR / "raw" / "311_requests_raw.csv"

    # Load data
    df = pd.read_csv(requests_file, low_memory=False)
    print(f"   Loaded {len(df):,} 311 requests")

    # Convert to string type and handle NaN values
    df['Topic'] = df['Topic'].astype(str).fillna('')
    df['Case Summary'] = df['Case Summary'].astype(str).fillna('')

    # Filter for streetlight-related requests
    streetlight_keywords = ['street light', 'streetlight', 'light out', 'lamp out', 'lighting']
    mask = df['Topic'].str.lower().str.contains('|'.join(streetlight_keywords), na=False) | \
           df['Case Summary'].str.lower().str.contains('|'.join(streetlight_keywords), na=False)

    streetlights = df[mask].copy()
    print(f"   Found {len(streetlights):,} streetlight-related requests")

    # Parse dates
    streetlights['Case Created Date'] = pd.to_datetime(streetlights['Case Created Date'], errors='coerce')
    streetlights['Case Closed Date'] = pd.to_datetime(streetlights['Case Closed Date'], errors='coerce')

    # Calculate response time (days)
    streetlights['response_days'] = (
        streetlights['Case Closed Date'] - streetlights['Case Created Date']
    ).dt.days

    return streetlights

def analyze_by_neighborhood(crime_df, streetlight_df):
    """Analyze crime and infrastructure by neighborhood"""
    print("🏘️  Analyzing by neighborhood...")

    # Crime counts by neighborhood
    crime_by_hood = crime_df.groupby('NEIGHBORHOOD_ID').agg({
        'INCIDENT_ID': 'count',
        'IS_CRIME': 'sum',
        'IS_TRAFFIC': 'sum'
    }).rename(columns={
        'INCIDENT_ID': 'total_incidents',
        'IS_CRIME': 'crime_count',
        'IS_TRAFFIC': 'traffic_count'
    })

    # Property crimes (burglary, theft, etc.)
    property_crimes = crime_df[crime_df['OFFENSE_CATEGORY_ID'].str.contains(
        'larceny|theft|burglary|auto-theft', case=False, na=False
    )]
    property_by_hood = property_crimes.groupby('NEIGHBORHOOD_ID').size().to_frame('property_crime_count')

    # Streetlight requests by neighborhood
    streetlight_by_hood = streetlight_df.groupby('Neighborhood').agg({
        'OBJECTID': 'count',
        'response_days': 'mean',
        'Case Status': lambda x: (x == 'Open').sum()
    }).rename(columns={
        'OBJECTID': 'streetlight_requests',
        'response_days': 'avg_response_days',
        'Case Status': 'open_requests'
    })

    # Merge the data
    analysis = crime_by_hood.join(property_by_hood, how='left')
    analysis = analysis.join(streetlight_by_hood, how='left')
    analysis = analysis.fillna(0)

    # Calculate correlation score (0-100)
    if len(analysis) > 0:
        # Normalize metrics
        analysis['crime_norm'] = (analysis['crime_count'] - analysis['crime_count'].min()) / \
                                 (analysis['crime_count'].max() - analysis['crime_count'].min())
        analysis['streetlight_norm'] = (analysis['streetlight_requests'] - analysis['streetlight_requests'].min()) / \
                                       (analysis['streetlight_requests'].max() - analysis['streetlight_requests'].min())

        # Crime-infrastructure index (higher = worse)
        analysis['crime_infrastructure_index'] = (
            (analysis['crime_norm'] * 0.6) +
            (analysis['streetlight_norm'] * 0.4)
        ) * 100

    print(f"   Analyzed {len(analysis)} neighborhoods")
    return analysis

def calculate_statistics(crime_df, streetlight_df, neighborhood_analysis):
    """Calculate key statistics for the analysis"""
    print("📈 Calculating statistics...")

    # Overall stats
    total_crimes = len(crime_df)
    total_streetlight_requests = len(streetlight_df)
    avg_response_time = streetlight_df['response_days'].mean()

    # Correlation analysis
    # Find neighborhoods with high vs low streetlight issues
    top_neighborhoods = neighborhood_analysis.nlargest(10, 'crime_infrastructure_index')
    bottom_neighborhoods = neighborhood_analysis.nsmallest(10, 'crime_infrastructure_index')

    high_streetlight_crime = top_neighborhoods['crime_count'].mean()
    low_streetlight_crime = bottom_neighborhoods['crime_count'].mean()

    if low_streetlight_crime > 0:
        crime_difference_pct = ((high_streetlight_crime - low_streetlight_crime) / low_streetlight_crime) * 100
    else:
        crime_difference_pct = 0

    stats = {
        'total_crimes_analyzed': int(total_crimes),
        'total_streetlight_requests': int(total_streetlight_requests),
        'avg_streetlight_response_days': float(avg_response_time) if not pd.isna(avg_response_time) else 0,
        'neighborhoods_analyzed': len(neighborhood_analysis),
        'correlation_finding': {
            'high_infrastructure_issues_avg_crime': float(high_streetlight_crime),
            'low_infrastructure_issues_avg_crime': float(low_streetlight_crime),
            'crime_difference_percentage': float(crime_difference_pct)
        },
        'top_concern_neighborhoods': [
            {
                'neighborhood_id': str(idx),
                'crime_count': int(row['crime_count']),
                'streetlight_requests': int(row['streetlight_requests']),
                'index_score': float(row['crime_infrastructure_index'])
            }
            for idx, row in top_neighborhoods.head(5).iterrows()
        ]
    }

    return stats

def generate_ai_insights(stats, neighborhood_analysis):
    """Use Claude to generate natural language insights"""
    print("🤖 Generating AI insights with Claude...")

    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        print("   ⚠️  Warning: No ANTHROPIC_API_KEY found. Skipping AI insights.")
        return {
            'summary': 'AI insights require an Anthropic API key',
            'key_findings': [],
            'recommendations': []
        }

    client = Anthropic(api_key=api_key)

    # Prepare context for Claude
    context = f"""
You are analyzing Denver crime and infrastructure data. Here are the key statistics:

OVERALL METRICS:
- Total crimes analyzed: {stats['total_crimes_analyzed']:,}
- Total streetlight repair requests: {stats['total_streetlight_requests']:,}
- Average streetlight repair time: {stats['avg_streetlight_response_days']:.1f} days
- Neighborhoods analyzed: {stats['neighborhoods_analyzed']}

CORRELATION FINDING:
- Neighborhoods with high streetlight issues have {stats['correlation_finding']['crime_difference_percentage']:.1f}% more crime on average
- High-issue neighborhoods: {stats['correlation_finding']['high_infrastructure_issues_avg_crime']:.0f} avg crimes
- Low-issue neighborhoods: {stats['correlation_finding']['low_infrastructure_issues_avg_crime']:.0f} avg crimes

TOP 5 CONCERN NEIGHBORHOODS:
{chr(10).join([f"- Neighborhood {n['neighborhood_id']}: {n['crime_count']} crimes, {n['streetlight_requests']} streetlight requests (Index: {n['index_score']:.1f})" for n in stats['top_concern_neighborhoods']])}

Please provide:
1. A 2-3 sentence executive summary
2. 3-5 key findings (each 1-2 sentences)
3. 3-5 actionable recommendations for city officials

Be specific, data-driven, and focus on civic impact.
"""

    try:
        message = client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=1500,
            messages=[{
                "role": "user",
                "content": context
            }]
        )

        response_text = message.content[0].text

        # Parse the response (simple parsing - could be improved)
        insights = {
            'full_analysis': response_text,
            'summary': response_text.split('\n\n')[0] if '\n\n' in response_text else response_text[:300],
            'generated_at': datetime.now().isoformat()
        }

        print("   ✓ AI insights generated successfully")
        return insights

    except Exception as e:
        print(f"   ⚠️  Error generating AI insights: {e}")
        return {
            'summary': 'Error generating AI insights',
            'error': str(e)
        }

def save_results(stats, insights, neighborhood_analysis):
    """Save analysis results as JSON files"""
    print("💾 Saving results...")

    # Save main analysis
    output = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'data_source': 'Denver Open Data',
            'analysis_type': 'Crime + Infrastructure Correlation'
        },
        'statistics': stats,
        'ai_insights': insights,
        'neighborhoods': neighborhood_analysis.head(20).to_dict('index')  # Top 20 for API
    }

    output_file = OUTPUT_DIR / 'analysis_results.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"   ✓ Saved to {output_file}")

    # Also save a lightweight summary for quick API responses
    summary = {
        'last_updated': datetime.now().isoformat(),
        'total_crimes': stats['total_crimes_analyzed'],
        'total_streetlight_requests': stats['total_streetlight_requests'],
        'key_finding': f"Neighborhoods with more streetlight issues have {stats['correlation_finding']['crime_difference_percentage']:.0f}% more crime",
        'ai_summary': insights.get('summary', 'AI insights not available')
    }

    summary_file = OUTPUT_DIR / 'summary.json'
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    print(f"   ✓ Saved summary to {summary_file}")

def main():
    """Main pipeline execution"""
    print("\n" + "="*60)
    print("🏙️  DENVER CRIME + INFRASTRUCTURE ANALYSIS PIPELINE")
    print("="*60 + "\n")

    start_time = datetime.now()

    try:
        # Step 1: Load data
        crime_df = load_and_sample_crime_data()
        streetlight_df = load_and_filter_311_data()

        # Step 2: Analyze by neighborhood
        neighborhood_analysis = analyze_by_neighborhood(crime_df, streetlight_df)

        # Step 3: Calculate statistics
        stats = calculate_statistics(crime_df, streetlight_df, neighborhood_analysis)

        # Step 4: Generate AI insights
        insights = generate_ai_insights(stats, neighborhood_analysis)

        # Step 5: Save results
        save_results(stats, insights, neighborhood_analysis)

        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETE")
        print(f"   Time elapsed: {elapsed:.1f} seconds")
        print(f"   Output directory: {OUTPUT_DIR}")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0

if __name__ == '__main__':
    exit(main())
