#!/usr/bin/env python3
"""
Civic Value Index - Web Application
Neighborhood intelligence reports for Denver real estate
"""

from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import os
from datetime import datetime
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import inch

app = Flask(__name__)

# Load neighborhood data
DATA_FILE = '../mvp_output/neighborhood_profiles.csv'
neighborhoods_df = pd.read_csv(DATA_FILE)

# Ensure numeric columns are properly typed
numeric_cols = ['total_crimes_12mo', 'property_crimes_12mo', 'violent_crimes_12mo',
                'traffic_crimes_12mo', 'crime_trend_pct', 'total_311_requests',
                'avg_response_days', 'median_response_days', 'current_home_value',
                'yoy_appreciation', 'safety_score', 'service_score', 'market_score',
                'civic_value_ratio', 'civic_value_index']

for col in numeric_cols:
    if col in neighborhoods_df.columns:
        neighborhoods_df[col] = pd.to_numeric(neighborhoods_df[col], errors='coerce').fillna(0)

@app.route('/')
def index():
    """Landing page"""
    # Get top 5 neighborhoods for showcase
    top_neighborhoods = neighborhoods_df.nlargest(5, 'civic_value_index')[['neighborhood', 'civic_value_index', 'current_home_value']].to_dict('records')

    return render_template('index.html',
                         total_neighborhoods=len(neighborhoods_df),
                         top_neighborhoods=top_neighborhoods)

@app.route('/search')
def search():
    """Search for neighborhoods"""
    query = request.args.get('q', '').lower()

    if not query:
        return jsonify([])

    # Filter neighborhoods matching query
    matches = neighborhoods_df[
        neighborhoods_df['neighborhood'].str.lower().str.contains(query, na=False)
    ][['neighborhood', 'civic_value_index', 'current_home_value']].head(10)

    results = matches.to_dict('records')
    return jsonify(results)

@app.route('/neighborhood/<name>')
def neighborhood_report(name):
    """Display neighborhood report"""
    # Find neighborhood (case-insensitive)
    hood_data = neighborhoods_df[
        neighborhoods_df['neighborhood'].str.lower() == name.lower()
    ]

    if hood_data.empty:
        return render_template('404.html', neighborhood=name), 404

    hood = hood_data.iloc[0].to_dict()

    # Calculate rank
    rank = (neighborhoods_df['civic_value_index'] > hood['civic_value_index']).sum() + 1
    percentile = int((1 - (rank / len(neighborhoods_df))) * 100)

    # Format data for display
    hood['rank'] = rank
    hood['total_neighborhoods'] = len(neighborhoods_df)
    hood['percentile'] = percentile

    # Determine tier
    if hood['civic_value_index'] > 70:
        hood['tier'] = 'TOP TIER'
        hood['tier_class'] = 'success'
        hood['buyer_advice'] = 'Expect premium pricing due to excellent safety and service scores'
        hood['investor_advice'] = 'Strong hold for appreciation, low crime risk'
        hood['renter_advice'] = 'Premium rental market, expect higher rents but excellent quality of life'
    elif hood['civic_value_index'] > 50:
        hood['tier'] = 'GOOD VALUE'
        hood['tier_class'] = 'info'
        hood['buyer_advice'] = 'Moderate pricing with good city services and acceptable safety'
        hood['investor_advice'] = 'Watch for turnaround signals: improving crime trends + rising home values'
        hood['renter_advice'] = 'Good balance of affordability and livability'
    else:
        hood['tier'] = 'OPPORTUNITY ZONE'
        hood['tier_class'] = 'warning'
        hood['buyer_advice'] = 'Lower entry price point, consider future infrastructure investment'
        hood['investor_advice'] = 'Higher risk/reward profile - monitor city investment trends'
        hood['renter_advice'] = 'More affordable, prioritize security measures'

    # Market momentum
    if hood['yoy_appreciation'] > 5:
        hood['market_momentum'] = 'HOT 🔥'
    elif hood['yoy_appreciation'] > 0:
        hood['market_momentum'] = 'STABLE 📊'
    else:
        hood['market_momentum'] = 'COOLING ❄️'

    # Crime trend
    hood['crime_trend_label'] = 'DECREASING 🔻' if hood['crime_trend_pct'] < 0 else 'INCREASING 🔺'

    return render_template('report.html', hood=hood, now=datetime.now())

@app.route('/api/neighborhoods')
def api_neighborhoods():
    """API endpoint for all neighborhoods"""
    data = neighborhoods_df[['neighborhood', 'civic_value_index', 'safety_score',
                             'service_score', 'market_score', 'current_home_value',
                             'total_crimes_12mo']].to_dict('records')
    return jsonify(data)

@app.route('/download/<name>')
def download_report(name):
    """Generate and download PDF report"""
    # Find neighborhood
    hood_data = neighborhoods_df[
        neighborhoods_df['neighborhood'].str.lower() == name.lower()
    ]

    if hood_data.empty:
        return "Neighborhood not found", 404

    hood = hood_data.iloc[0].to_dict()

    # Calculate rank
    rank = (neighborhoods_df['civic_value_index'] > hood['civic_value_index']).sum() + 1
    percentile = int((1 - (rank / len(neighborhoods_df))) * 100)

    # Create PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []

    # Styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1a5490'),
        spaceAfter=30,
        alignment=1
    )

    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#1a5490'),
        spaceAfter=12
    )

    # Title
    story.append(Paragraph("CIVIC VALUE INDEX", title_style))
    story.append(Paragraph("Neighborhood Intelligence Report", styles['Heading3']))
    story.append(Spacer(1, 0.3*inch))

    # Neighborhood name
    story.append(Paragraph(f"<b>{hood['neighborhood'].upper()}</b>", title_style))
    story.append(Spacer(1, 0.2*inch))

    # Overall score
    story.append(Paragraph(f"Overall Civic Value Index: <b>{hood['civic_value_index']:.1f}/100</b>", heading_style))
    story.append(Spacer(1, 0.3*inch))

    # Component scores table
    score_data = [
        ['Component', 'Score'],
        ['Safety Score', f"{hood['safety_score']:.1f}/100"],
        ['Service Quality Score', f"{hood['service_score']:.1f}/100"],
        ['Market Performance Score', f"{hood['market_score']:.1f}/100"],
        ['Civic Value Ratio', f"{hood['civic_value_ratio']:.1f}/100"],
    ]

    score_table = Table(score_data, colWidths=[3*inch, 2*inch])
    score_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a5490')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))

    story.append(score_table)
    story.append(Spacer(1, 0.3*inch))

    # Safety Profile
    story.append(Paragraph("SAFETY PROFILE", heading_style))
    safety_text = f"""
    Total Crimes (Last 12 months): {int(hood['total_crimes_12mo'])}<br/>
    • Property Crimes: {int(hood['property_crimes_12mo'])}<br/>
    • Violent Crimes: {int(hood['violent_crimes_12mo'])}<br/>
    • Traffic Incidents: {int(hood['traffic_crimes_12mo'])}<br/>
    <br/>
    Crime Trend: {hood['crime_trend_pct']:+.1f}%
    """
    story.append(Paragraph(safety_text, styles['Normal']))
    story.append(Spacer(1, 0.2*inch))

    # Real Estate Market
    story.append(Paragraph("REAL ESTATE MARKET", heading_style))
    market_text = f"""
    Current Median Home Value: ${int(hood['current_home_value']):,}<br/>
    Year-over-Year Appreciation: {hood['yoy_appreciation']:.1f}%
    """
    story.append(Paragraph(market_text, styles['Normal']))
    story.append(Spacer(1, 0.2*inch))

    # Competitive Positioning
    story.append(Paragraph("COMPETITIVE POSITIONING", heading_style))
    position_text = f"""
    This neighborhood ranks #{rank} out of {len(neighborhoods_df)} Denver neighborhoods.<br/>
    Percentile Rank: {percentile}th percentile
    """
    story.append(Paragraph(position_text, styles['Normal']))
    story.append(Spacer(1, 0.3*inch))

    # Footer
    story.append(Spacer(1, 0.5*inch))
    footer_text = f"""
    <i>Generated: {datetime.now().strftime('%B %d, %Y')}<br/>
    Report ID: {hood['neighborhood']}-{datetime.now().strftime('%Y%m%d')}<br/>
    <br/>
    Data Sources: Denver Crime Data, Denver 311, Denver Checkbook, Zillow ZHVI<br/>
    Analysis by: Civic Value Index Platform</i>
    """
    story.append(Paragraph(footer_text, styles['Normal']))

    # Build PDF
    doc.build(story)
    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f'{hood["neighborhood"]}_civic_value_report.pdf',
        mimetype='application/pdf'
    )

@app.route('/about')
def about():
    """About page"""
    return render_template('about.html')

@app.route('/pricing')
def pricing():
    """Pricing page"""
    return render_template('pricing.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
