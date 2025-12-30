"""
GovData-AI FastAPI Backend
Serves Denver crime + infrastructure analysis via REST API
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, Any
import os

app = FastAPI(
    title="GovData-AI API",
    description="AI-powered civic data analysis for Denver",
    version="1.0.0"
)

# CORS configuration (allow frontend to call API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "https://*.vercel.app",
        "*"  # In production, replace with specific domain
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data directory
DATA_DIR = Path(__file__).parent / "data"

def load_json_file(filename: str) -> Dict[str, Any]:
    """Load a JSON file from the data directory"""
    file_path = DATA_DIR / filename
    if not file_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Data file '{filename}' not found. Run the data processing pipeline first."
        )

    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error reading data file: {e}"
        )

@app.get("/")
async def root():
    """Root endpoint - API status"""
    return {
        "service": "GovData-AI API",
        "status": "online",
        "version": "1.0.0",
        "description": "AI-powered civic data analysis for Denver",
        "endpoints": {
            "GET /": "This status page",
            "GET /health": "Health check",
            "GET /api/analysis": "Full analysis results",
            "GET /api/summary": "Quick summary",
            "GET /api/neighborhoods": "Neighborhood data"
        }
    }

@app.get("/health")
async def health():
    """Health check endpoint for Railway"""
    # Check if data files exist
    analysis_exists = (DATA_DIR / "analysis_results.json").exists()
    summary_exists = (DATA_DIR / "summary.json").exists()

    return {
        "status": "healthy" if analysis_exists else "no_data",
        "timestamp": datetime.now().isoformat(),
        "data_available": {
            "analysis": analysis_exists,
            "summary": summary_exists
        },
        "environment": {
            "python_version": os.sys.version.split()[0],
            "api_key_configured": bool(os.getenv("ANTHROPIC_API_KEY"))
        }
    }

@app.get("/api/summary")
async def get_summary():
    """Get quick summary of analysis"""
    data = load_json_file("summary.json")
    return JSONResponse(content=data)

@app.get("/api/analysis")
async def get_analysis():
    """Get full analysis results"""
    data = load_json_file("analysis_results.json")
    return JSONResponse(content=data)

@app.get("/api/neighborhoods")
async def get_neighborhoods():
    """Get neighborhood-level data"""
    data = load_json_file("analysis_results.json")

    if "neighborhoods" not in data:
        raise HTTPException(
            status_code=404,
            detail="Neighborhood data not found in analysis results"
        )

    return JSONResponse(content={
        "neighborhoods": data["neighborhoods"],
        "metadata": data.get("metadata", {})
    })

@app.get("/api/stats")
async def get_statistics():
    """Get statistical analysis"""
    data = load_json_file("analysis_results.json")

    if "statistics" not in data:
        raise HTTPException(
            status_code=404,
            detail="Statistics not found in analysis results"
        )

    return JSONResponse(content=data["statistics"])

@app.get("/api/insights")
async def get_ai_insights():
    """Get AI-generated insights"""
    data = load_json_file("analysis_results.json")

    if "ai_insights" not in data:
        raise HTTPException(
            status_code=404,
            detail="AI insights not found in analysis results"
        )

    return JSONResponse(content=data["ai_insights"])

# For local development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True
    )
