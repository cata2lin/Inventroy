document.addEventListener('DOMContentLoaded', () => {
    const syncAllProductsBtn = document.getElementById('sync-all-products-btn');
    const progressBarsContainer = document.getElementById('progress-bars');
    let pollingInterval = null;

    const renderProgressBar = (task) => {
        const percentage = task.total > 0 ? (task.processed / task.total) * 100 : (task.done ? 100 : 0);
        return `
            <div>
                <strong>${task.title}</strong>
                <small>${task.note || (task.done ? (task.ok ? 'Completed' : 'Failed') : 'Starting...')}</small>
                <progress value="${percentage}" max="100"></progress>
            </div>
        `;
    };

    const pollTaskStatus = async () => {
        try {
            const response = await fetch('/api/sync-control/status');
            const data = await response.json();
            const tasks = data.tasks || [];
            if (tasks.length > 0) {
                progressBarsContainer.innerHTML = tasks.map(renderProgressBar).join('');
                if (tasks.every(t => t.done)) {
                    clearInterval(pollingInterval);
                    pollingInterval = null;
                    syncAllProductsBtn.disabled = false;
                    syncAllProductsBtn.removeAttribute('aria-busy');
                }
            } else {
                progressBarsContainer.innerHTML = '<p>No active or recent sync jobs.</p>';
                syncAllProductsBtn.disabled = false;
                syncAllProductsBtn.removeAttribute('aria-busy');
            }
        } catch (error) {
            console.error('Failed to poll task status:', error);
            clearInterval(pollingInterval);
        }
    };

    const startSync = async (endpoint, payload = {}) => {
        syncAllProductsBtn.disabled = true;
        syncAllProductsBtn.setAttribute('aria-busy', 'true');
        progressBarsContainer.innerHTML = '<p>Starting sync jobs...</p>';

        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!response.ok) throw new Error('Failed to start sync jobs.');
            if (pollingInterval) clearInterval(pollingInterval);
            pollingInterval = setInterval(pollTaskStatus, 2500);
        } catch (error) {
            progressBarsContainer.innerHTML = `<p style="color: red;">${error.message}</p>`;
            syncAllProductsBtn.disabled = false;
            syncAllProductsBtn.removeAttribute('aria-busy');
        }
    };

    // Use the new endpoint that syncs products AND runs stock reconciliation
    syncAllProductsBtn.addEventListener('click', () => startSync('/api/sync-control/products-and-reconcile'));
    pollTaskStatus();
});