// Civic Value Index - Main JavaScript

// Smooth scrolling
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            target.scrollIntoView({
                behavior: 'smooth'
            });
        }
    });
});

// Initialize tooltips if Bootstrap is loaded
if (typeof bootstrap !== 'undefined') {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

// Analytics placeholder
function trackEvent(category, action, label) {
    // Add Google Analytics or other tracking here
    console.log(`Event: ${category} - ${action} - ${label}`);
}

// Search tracking
const searchInput = document.getElementById('neighborhood-search');
if (searchInput) {
    searchInput.addEventListener('focus', () => {
        trackEvent('Search', 'Focus', 'Neighborhood Search');
    });
}

// Download tracking
document.querySelectorAll('a[href^="/download/"]').forEach(link => {
    link.addEventListener('click', function() {
        trackEvent('Download', 'PDF', this.getAttribute('href'));
    });
});
