document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        syncAllProductsBtn: document.getElementById('sync-all-products-btn'),
        progressBarsContainer: document.getElementById('progress-bars'),
    };

    const startSync = async (endpoint, payload = {}) => {
        elements.syncAllProductsBtn.disabled = true;
        elements.syncAllProductsBtn.setAttribute('aria-busy', 'true');
        elements.progressBarsContainer.innerHTML = '<p>Starting sync jobs...</p>';

        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!response.ok) throw new Error('Failed to start sync jobs.');
            
        } catch (error) {
            console.error(error);
            elements.progressBarsContainer.innerHTML = `<p style="color: red;">${error.message}</p>`;
            elements.syncAllProductsBtn.disabled = false;
            elements.syncAllProductsBtn.removeAttribute('aria-busy');
        }
    };

    elements.syncAllProductsBtn.addEventListener('click', () => startSync('/api/sync-control/products'));
});