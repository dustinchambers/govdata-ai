// API Configuration
const API_BASE_URL = 'https://web-production-5a583.up.railway.app';

// Global data cache
let neighborhoodsData = null;

// ========== UTILITY FUNCTIONS ==========

function formatNumber(num) {
    if (!num && num !== 0) return '0';
    return Math.round(num).toLocaleString();
}

function capitalizeWords(str) {
    return str.split(/[-\s]/).map(word =>
        word.charAt(0).toUpperCase() + word.slice(1)
    ).join(' ');
}

function calculateCivicScore(neighborhood) {
    // Calculate Civic Value Score (0-100)
    // Higher crime = lower score
    // More streetlight requests = lower score
    // This reveals neighborhoods that are under-served

    const crimeCount = neighborhood.crime_count || 0;
    const streetlightRequests = neighborhood.streetlight_requests || 0;
    const totalIncidents = neighborhood.total_incidents || crimeCount;

    // Normalize against dataset
    const maxCrime = 5000; // Approximate max in dataset
    const maxStreetlights = 50; // Approximate max

    // Score components (inverted - lower is better)
    const crimeScore = Math.max(0, 100 - (crimeCount / maxCrime) * 60);
    const serviceScore = Math.max(0, 100 - (streetlightRequests / maxStreetlights) * 40);

    // Combined score
    const score = (crimeScore * 0.7) + (serviceScore * 0.3);

    return Math.round(Math.max(0, Math.min(100, score)));
}

function getScoreClass(score) {
    if (score >= 80) return 'score-high';
    if (score >= 50) return 'score-medium';
    return 'score-low';
}

function getScoreDescription(score) {
    if (score >= 80) return 'High civic value - well-served neighborhood';
    if (score >= 50) return 'Medium value - adequate services';
    return 'Low value - under-served neighborhood';
}

// ========== API CALLS ==========

async function fetchNeighborhoodsData() {
    if (neighborhoodsData) return neighborhoodsData;

    try {
        console.log('Fetching from:', `${API_BASE_URL}/api/neighborhoods`);
        const response = await fetch(`${API_BASE_URL}/api/neighborhoods`);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        console.log('Received data:', {
            hasNeighborhoods: !!data.neighborhoods,
            count: data.neighborhoods ? Object.keys(data.neighborhoods).length : 0
        });

        neighborhoodsData = data.neighborhoods;
        return neighborhoodsData;
    } catch (error) {
        console.error('Error fetching neighborhoods:', error);
        throw error;
    }
}

// ========== LANDING PAGE ==========

async function loadNeighborhoods() {
    try {
        const neighborhoods = await fetchNeighborhoodsData();
        const datalist = document.getElementById('neighborhoods');

        if (!datalist) return;

        // Clear existing options
        datalist.innerHTML = '';

        // Populate datalist with neighborhood names
        Object.keys(neighborhoods).sort().forEach(id => {
            const option = document.createElement('option');
            option.value = capitalizeWords(id);
            option.setAttribute('data-id', id);
            datalist.appendChild(option);
        });

        console.log(`✓ Loaded ${Object.keys(neighborhoods).length} neighborhoods for autocomplete`);
    } catch (error) {
        console.error('Error loading neighborhoods:', error);
        // Show error to user
        const searchInput = document.getElementById('neighborhoodSearch');
        if (searchInput) {
            searchInput.placeholder = 'Unable to load neighborhoods - try again';
        }
    }
}

function searchNeighborhoods() {
    const input = document.getElementById('neighborhoodSearch');
    const searchTerm = input.value.trim();

    if (searchTerm) {
        // Redirect to search page with query
        window.location.href = `search.html?q=${encodeURIComponent(searchTerm)}`;
    } else {
        // No search term, go to all neighborhoods
        window.location.href = 'search.html';
    }
}

// ========== SEARCH RESULTS PAGE ==========

async function loadAllNeighborhoods() {
    const listContainer = document.getElementById('neighborhoodsList');
    const loadingState = document.getElementById('loadingState');
    const errorState = document.getElementById('errorState');
    const resultsCount = document.getElementById('resultsCount');

    try {
        const neighborhoods = await fetchNeighborhoodsData();

        // Convert to array and add scores
        const neighborhoodsArray = Object.entries(neighborhoods).map(([id, data]) => ({
            id,
            ...data,
            civicScore: calculateCivicScore(data),
            name: capitalizeWords(id)
        }));

        // Sort by civic score (lowest first - these are most under-served)
        neighborhoodsArray.sort((a, b) => a.civicScore - b.civicScore);

        // Hide loading, show results
        loadingState.classList.add('hidden');
        resultsCount.textContent = `${neighborhoodsArray.length} neighborhoods analyzed`;

        // Store for filtering and sorting
        window.allNeighborhoods = neighborhoodsArray;

        // Render neighborhoods
        renderNeighborhoodsList(neighborhoodsArray);

    } catch (error) {
        loadingState.classList.add('hidden');
        errorState.classList.remove('hidden');
        console.error('Error loading neighborhoods:', error);
    }
}

function sortNeighborhoods() {
    if (!window.allNeighborhoods) return;

    const sortBy = document.getElementById('sortBy').value;
    const neighborhoods = [...window.allNeighborhoods];

    // Apply sorting
    switch(sortBy) {
        case 'civic-asc':
            neighborhoods.sort((a, b) => a.civicScore - b.civicScore);
            break;
        case 'civic-desc':
            neighborhoods.sort((a, b) => b.civicScore - a.civicScore);
            break;
        case 'name-asc':
            neighborhoods.sort((a, b) => a.name.localeCompare(b.name));
            break;
        case 'name-desc':
            neighborhoods.sort((a, b) => b.name.localeCompare(a.name));
            break;
        case 'crime-desc':
            neighborhoods.sort((a, b) => (b.crime_count || 0) - (a.crime_count || 0));
            break;
        case 'crime-asc':
            neighborhoods.sort((a, b) => (a.crime_count || 0) - (b.crime_count || 0));
            break;
        case 'service-desc':
            neighborhoods.sort((a, b) => (b.streetlight_requests || 0) - (a.streetlight_requests || 0));
            break;
        case 'service-asc':
            neighborhoods.sort((a, b) => (a.streetlight_requests || 0) - (b.streetlight_requests || 0));
            break;
    }

    renderNeighborhoodsList(neighborhoods);
}

function renderNeighborhoodsList(neighborhoods) {
    const listContainer = document.getElementById('neighborhoodsList');

    if (neighborhoods.length === 0) {
        listContainer.innerHTML = '<div class="loading-state"><p>No neighborhoods found</p></div>';
        return;
    }

    listContainer.innerHTML = neighborhoods.map(hood => `
        <div class="neighborhood-card" onclick="window.location.href='neighborhood.html?id=${hood.id}'">
            <div class="neighborhood-score">
                <div class="score-value">${hood.civicScore}</div>
                <div class="score-label">Civic Score</div>
            </div>

            <div class="neighborhood-info">
                <h3>${hood.name}</h3>
                <div class="neighborhood-stats">
                    <div class="stat-item">
                        <span>🚨</span>
                        <span>${formatNumber(hood.crime_count || 0)} crimes</span>
                    </div>
                    <div class="stat-item">
                        <span>💡</span>
                        <span>${formatNumber(hood.streetlight_requests || 0)} service requests</span>
                    </div>
                </div>
            </div>

            <div class="neighborhood-arrow">→</div>
        </div>
    `).join('');
}

function filterNeighborhoods(searchTerm) {
    if (!window.allNeighborhoods) return;

    const term = searchTerm.toLowerCase();
    const filtered = window.allNeighborhoods.filter(hood =>
        hood.name.toLowerCase().includes(term)
    );

    renderNeighborhoodsList(filtered);
}

// ========== NEIGHBORHOOD DETAIL PAGE ==========

async function loadNeighborhoodDetail(neighborhoodId) {
    try {
        const neighborhoods = await fetchNeighborhoodsData();
        const hood = neighborhoods[neighborhoodId];

        if (!hood) {
            document.getElementById('loadingHeader').innerHTML = '<p>Neighborhood not found</p>';
            return;
        }

        const civicScore = calculateCivicScore(hood);
        const name = capitalizeWords(neighborhoodId);

        // Update page title
        document.title = `${name} - CivicValue`;
        document.getElementById('pageTitle').textContent = `${name} - CivicValue`;

        // Show header content
        document.getElementById('loadingHeader').classList.add('hidden');
        document.getElementById('neighborhoodHeaderContent').classList.remove('hidden');

        // Populate header
        document.getElementById('headerNeighborhoodName').textContent = name;
        document.getElementById('neighborhoodTitle').textContent = name;
        document.getElementById('civicScore').textContent = civicScore;
        document.getElementById('scoreDescription').textContent = getScoreDescription(civicScore);
        document.getElementById('totalCrimes').textContent = formatNumber(hood.crime_count || 0);
        document.getElementById('serviceRequests').textContent = formatNumber(hood.streetlight_requests || 0);

        // Score Breakdown
        const crimeNorm = hood.crime_norm || 0;
        const streetlightNorm = hood.streetlight_norm || 0;

        document.getElementById('scoreBreakdown').innerHTML = `
            <div class="score-item">
                <div class="score-info">
                    <h3>Crime Impact</h3>
                    <p>Lower crime = higher civic value score</p>
                </div>
                <div class="score-metrics">
                    <div class="metric-value">
                        <div class="metric-number">${formatNumber(hood.crime_count || 0)}</div>
                        <div class="metric-label">Total Crimes</div>
                    </div>
                </div>
            </div>

            <div class="score-item">
                <div class="score-info">
                    <h3>City Services</h3>
                    <p>Fewer outstanding service requests = better served</p>
                </div>
                <div class="score-metrics">
                    <div class="metric-value">
                        <div class="metric-number">${formatNumber(hood.streetlight_requests || 0)}</div>
                        <div class="metric-label">Service Requests</div>
                    </div>
                </div>
            </div>

            <div class="score-item">
                <div class="score-info">
                    <h3>Overall Civic Value</h3>
                    <p>Combined score measuring how well the city serves this neighborhood</p>
                </div>
                <div class="score-metrics">
                    <div class="metric-value">
                        <div class="metric-number ${getScoreClass(civicScore)}" style="font-size: 2.5rem;">${civicScore}</div>
                        <div class="metric-label">Civic Score</div>
                    </div>
                </div>
            </div>
        `;

        // Crime Analysis
        const propertyCrime = hood.property_crime_count || 0;
        const trafficCount = hood.traffic_count || 0;

        document.getElementById('crimeAnalysis').innerHTML = `
            <div class="analysis-grid">
                <div class="analysis-stat">
                    <div class="analysis-stat-value">${formatNumber(hood.crime_count || 0)}</div>
                    <div class="analysis-stat-label">Total Crimes</div>
                </div>
                <div class="analysis-stat">
                    <div class="analysis-stat-value">${formatNumber(propertyCrime)}</div>
                    <div class="analysis-stat-label">Property Crimes</div>
                </div>
                <div class="analysis-stat">
                    <div class="analysis-stat-value">${formatNumber(trafficCount)}</div>
                    <div class="analysis-stat-label">Traffic Incidents</div>
                </div>
            </div>
            <p style="margin-top: 1.5rem; color: var(--gray-600);">
                This neighborhood has experienced ${formatNumber(hood.crime_count || 0)} reported crimes
                in the analysis period. Property crimes account for ${formatNumber(propertyCrime)} of these incidents.
            </p>
        `;

        // Services Analysis
        document.getElementById('servicesAnalysis').innerHTML = `
            <div class="analysis-grid">
                <div class="analysis-stat">
                    <div class="analysis-stat-value">${formatNumber(hood.streetlight_requests || 0)}</div>
                    <div class="analysis-stat-label">Streetlight Requests</div>
                </div>
                <div class="analysis-stat">
                    <div class="analysis-stat-value">${formatNumber(hood.open_requests || 0)}</div>
                    <div class="analysis-stat-label">Open Requests</div>
                </div>
                <div class="analysis-stat">
                    <div class="analysis-stat-value">${(hood.avg_response_days || 0).toFixed(1)}</div>
                    <div class="analysis-stat-label">Avg Response (days)</div>
                </div>
            </div>
            <p style="margin-top: 1.5rem; color: var(--gray-600);">
                The city has received ${formatNumber(hood.streetlight_requests || 0)} streetlight repair
                requests from this neighborhood, with an average response time of ${(hood.avg_response_days || 0).toFixed(1)} days.
            </p>
        `;

        // AI Insights
        const fetchAIInsights = async () => {
            try {
                const response = await fetch(`${API_BASE_URL}/api/insights`);
                const data = await response.json();

                if (data.full_analysis) {
                    document.getElementById('aiInsights').innerHTML = `
                        <div class="ai-insights">
                            <pre>${data.full_analysis}</pre>
                        </div>
                    `;
                } else {
                    document.getElementById('aiInsightsSection').classList.add('hidden');
                }
            } catch (error) {
                document.getElementById('aiInsightsSection').classList.add('hidden');
            }
        };

        fetchAIInsights();

        // Quick Facts
        document.getElementById('quickFacts').innerHTML = `
            <div class="fact-item">
                <span class="fact-label">Civic Score</span>
                <span class="fact-value ${getScoreClass(civicScore)}">${civicScore}/100</span>
            </div>
            <div class="fact-item">
                <span class="fact-label">Total Incidents</span>
                <span class="fact-value">${formatNumber(hood.total_incidents || 0)}</span>
            </div>
            <div class="fact-item">
                <span class="fact-label">Crime Count</span>
                <span class="fact-value">${formatNumber(hood.crime_count || 0)}</span>
            </div>
            <div class="fact-item">
                <span class="fact-label">Service Requests</span>
                <span class="fact-value">${formatNumber(hood.streetlight_requests || 0)}</span>
            </div>
        `;

        // Value Gap
        if (civicScore < 50) {
            document.getElementById('valueGapText').textContent = `This neighborhood has a low Civic Value Score (${civicScore}), indicating it may be under-served relative to its needs. This represents a potential value opportunity.`;
        } else if (civicScore < 80) {
            document.getElementById('valueGapText').textContent = `This neighborhood has a moderate Civic Value Score (${civicScore}), indicating adequate but not exceptional city services relative to crime levels.`;
        } else {
            document.getElementById('valueGapText').textContent = `This neighborhood has a high Civic Value Score (${civicScore}), indicating strong city services and low crime rates.`;
        }

    } catch (error) {
        console.error('Error loading neighborhood detail:', error);
        document.getElementById('loadingHeader').innerHTML = '<p>Error loading neighborhood data</p>';
    }
}
