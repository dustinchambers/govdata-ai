#!/usr/bin/env python3
"""
Ethica.Design - Civic Data Intelligence Platform
AI Insights Generation Script

Uses Claude API to generate natural language insights from correlation data.
"""

import anthropic
import json
import os
from datetime import datetime

# Configuration
ANALYSIS_DIR = "analysis"
OUTPUTS_DIR = "outputs"

os.makedirs(OUTPUTS_DIR, exist_ok=True)

# API Key - set environment variable: export ANTHROPIC_API_KEY=your_key_here
api_key = os.environ.get("ANTHROPIC_API_KEY")
if not api_key:
    print("❌ Error: ANTHROPIC_API_KEY environment variable not set")
    print("   Set it with: export ANTHROPIC_API_KEY=your_key_here")
    exit(1)

print("=" * 80)
print("ETHICA.DESIGN - AI INSIGHTS GENERATION")
print("Powered by Claude")
print("=" * 80)
print()

# ============================================================================
# LOAD ANALYSIS DATA
# ============================================================================

print("📂 LOADING ANALYSIS DATA...")
print("-" * 80)

try:
    with open(f"{ANALYSIS_DIR}/ai_analysis_input.json", 'r') as f:
        analysis_data = json.load(f)
    
    print(f"✅ Loaded analysis data")
    print(f"   - Neighborhoods: {analysis_data['dataset_summary']['total_neighborhoods']}")
    print(f"   - Total crimes: {analysis_data['dataset_summary']['total_crimes']:,}")
    print(f"   - Pending lights: {analysis_data['dataset_summary']['pending_streetlight_requests']:,}")
    print()
    
except Exception as e:
    print(f"❌ Error loading analysis data: {e}")
    print("   Make sure to run 03_analyze_correlations.py first!")
    exit(1)

# ============================================================================
# GENERATE AI INSIGHTS
# ============================================================================

print("🤖 GENERATING AI INSIGHTS WITH CLAUDE...")
print("-" * 80)

client = anthropic.Anthropic(api_key=api_key)

# Create prompt for Claude
prompt = f"""You are analyzing civic data for Denver, Colorado as part of the Ethica.Design Civic Data Intelligence Platform MVP.

I'm providing you with correlation analysis between crime data and infrastructure (specifically streetlight repair requests). Please analyze this data and generate insights.

DATASET SUMMARY:
- Total Neighborhoods Analyzed: {analysis_data['dataset_summary']['total_neighborhoods']}
- Total Crimes: {analysis_data['dataset_summary']['total_crimes']:,}
- Total Streetlight Requests: {analysis_data['dataset_summary']['total_streetlight_requests']:,}
- Pending Streetlight Repairs: {analysis_data['dataset_summary']['pending_streetlight_requests']:,}
- Average Response Time: {analysis_data['dataset_summary']['avg_response_time_days']:.1f} days

CORRELATIONS FOUND:
{json.dumps(analysis_data['correlations'], indent=2)}

TOP 5 NEIGHBORHOODS WITH MOST PENDING STREETLIGHT REPAIRS:
{json.dumps(analysis_data['top_pending_neighborhoods'], indent=2)}

TOP 5 NEIGHBORHOODS WITH MOST CRIME:
{json.dumps(analysis_data['top_crime_neighborhoods'], indent=2)}

Please provide:

1. **Executive Summary** (2-3 sentences): What's the key finding?

2. **Detailed Findings** (3-5 bullet points): What patterns do you see?

3. **Specific Insights** (3-5 examples): Call out specific neighborhoods with actionable insights

4. **Recommendations** (3-5 bullet points): What should the city do?

5. **ROI Estimation**: If Denver prioritized fixing streetlights in high-crime areas, what might be the impact? Make reasonable assumptions about cost per streetlight repair ($1,500) and value of prevented crime.

6. **Equity Analysis**: Are there patterns suggesting some neighborhoods get worse service?

Format your response as clear, compelling insights that city officials and residents would find valuable.
"""

try:
    print("   Calling Claude API...")
    
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )
    
    insights = response.content[0].text
    
    print("✅ Generated AI insights")
    print()
    
except Exception as e:
    print(f"❌ Error calling Claude API: {e}")
    exit(1)

# ============================================================================
# GENERATE NEIGHBORHOOD SCORECARDS
# ============================================================================

print("📊 GENERATING NEIGHBORHOOD SCORECARDS...")
print("-" * 80)

scorecards = []

# Generate scorecards for top 10 most impactful neighborhoods
top_neighborhoods = sorted(
    analysis_data['top_crime_neighborhoods'][:10],
    key=lambda x: x['total_crimes'],
    reverse=True
)

for i, hood in enumerate(top_neighborhoods[:5], 1):  # Just do top 5 for the demo
    print(f"   Generating scorecard {i}/5: {hood['neighborhood']}...")
    
    scorecard_prompt = f"""Generate a brief neighborhood scorecard for {hood['neighborhood']} in Denver.

Data:
- Total Crimes: {hood['total_crimes']}
- Pending Streetlight Repairs: {hood['pending_lights']}
- Average Response Time: {hood['avg_response_time']:.1f} days

Provide:
1. A Crime-Infrastructure Index score (0-100, where 100 is worst)
2. A 2-3 sentence summary
3. Top priority action (1 sentence)

Be concise and actionable."""
    
    try:
        scorecard_response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[
                {
                    "role": "user",
                    "content": scorecard_prompt
                }
            ]
        )
        
        scorecards.append({
            'neighborhood': hood['neighborhood'],
            'data': hood,
            'scorecard': scorecard_response.content[0].text
        })
        
    except Exception as e:
        print(f"   ⚠️  Error generating scorecard: {e}")

print(f"✅ Generated {len(scorecards)} neighborhood scorecards")
print()

# ============================================================================
# SAVE INSIGHTS
# ============================================================================

print("💾 SAVING AI INSIGHTS...")
print("-" * 80)

# Save main insights
insights_path = f"{OUTPUTS_DIR}/ai_insights.md"
with open(insights_path, 'w') as f:
    f.write("# Ethica.Design - Denver Crime + Infrastructure Analysis\n\n")
    f.write(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
    f.write("---\n\n")
    f.write(insights)
    f.write("\n\n---\n\n")
    f.write("## Neighborhood Scorecards\n\n")
    
    for scorecard in scorecards:
        f.write(f"### {scorecard['neighborhood']}\n\n")
        f.write(f"**Data:**\n")
        f.write(f"- Total Crimes: {scorecard['data']['total_crimes']}\n")
        f.write(f"- Pending Streetlights: {scorecard['data']['pending_lights']}\n")
        f.write(f"- Avg Response Time: {scorecard['data']['avg_response_time']:.1f} days\n\n")
        f.write(f"**Analysis:**\n\n")
        f.write(scorecard['scorecard'])
        f.write("\n\n---\n\n")

print(f"✅ Saved AI insights to: {insights_path}")

# Save JSON version
insights_json_path = f"{OUTPUTS_DIR}/ai_insights.json"
with open(insights_json_path, 'w') as f:
    json.dump({
        'generated_at': datetime.now().isoformat(),
        'main_insights': insights,
        'scorecards': scorecards,
        'data_summary': analysis_data['dataset_summary'],
        'correlations': analysis_data['correlations']
    }, f, indent=2)

print(f"✅ Saved AI insights JSON to: {insights_json_path}")
print()

# ============================================================================
# DISPLAY PREVIEW
# ============================================================================

print("=" * 80)
print("AI INSIGHTS PREVIEW")
print("=" * 80)
print()
print(insights[:500] + "...")
print()
print("=" * 80)
print("COMPLETE!")
print("=" * 80)
print()
print("✅ AI insights generated successfully!")
print()
print("Files created:")
print(f"  - {insights_path}")
print(f"  - {insights_json_path}")
print()
print("Next steps:")
print("1. Review insights in: ./outputs/ai_insights.md")
print("2. Build dashboard to visualize these insights")
print("3. Create demo video showing the platform")
print()
print("=" * 80)
