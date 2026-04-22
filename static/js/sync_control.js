// static/js/sync_control.js
document.addEventListener('DOMContentLoaded', () => {
    const syncAllProductsBtn = document.getElementById('sync-all-products-btn');
    const reconcileBtn = document.getElementById('reconcile-btn');
    const progressBarsContainer = document.getElementById('progress-bars');
    let pollingInterval = null;

    // --- Metrics ---
    const loadMetrics = async () => {
        try {
            // Load store count
            const storesRes = await fetch('/api/config/stores');
            const stores = await storesRes.json();
            const enabledStores = stores.filter(s => s.enabled);
            document.getElementById('metric-stores').textContent = enabledStores.length;

            // Load barcode stats from stock endpoint (lightweight)
            try {
                const stockRes = await fetch('/api/stock/by-barcode?max_stock=999999');
                const stockData = await stockRes.json();
                document.getElementById('metric-barcodes').textContent = (stockData.results || []).length.toLocaleString();
            } catch {
                document.getElementById('metric-barcodes').textContent = '—';
            }

            // Load last sync info
            const statusRes = await fetch('/api/sync-control/status');
            const statusData = await statusRes.json();
            const tasks = statusData.tasks || [];
            const completedTasks = tasks.filter(t => t.done);
            const failedTasks = completedTasks.filter(t => !t.ok);

            if (completedTasks.length > 0) {
                document.getElementById('metric-last-sync').textContent = 'Recent';
                document.getElementById('metric-last-sync').style.fontSize = '1.5rem';
            } else {
                document.getElementById('metric-last-sync').textContent = 'Never';
                document.getElementById('metric-last-sync').style.fontSize = '1.5rem';
            }

            if (tasks.some(t => !t.done)) {
                document.getElementById('metric-sync-status').innerHTML = '<span style="color: var(--color-info)">Running</span>';
            } else if (failedTasks.length > 0) {
                document.getElementById('metric-sync-status').innerHTML = '<span style="color: var(--color-danger)">Errors</span>';
            } else {
                document.getElementById('metric-sync-status').innerHTML = '<span style="color: var(--color-success)">Healthy</span>';
            }
            document.getElementById('metric-sync-status').style.fontSize = '1.5rem';

        } catch (error) {
            console.error('Failed to load metrics:', error);
        }
    };

    // --- Progress Rendering ---
    const renderProgressBar = (task) => {
        const percentage = task.total > 0 ? (task.processed / task.total) * 100 : (task.done ? 100 : 0);
        const statusClass = task.done ? (task.ok ? 'badge-success' : 'badge-danger') : 'badge-info';
        const statusText = task.done ? (task.ok ? 'Done' : 'Failed') : 'Running';
        return `
            <div class="sync-progress-item">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <strong>${task.title}</strong>
                    <span class="badge ${statusClass}">${statusText}</span>
                </div>
                <small>${task.note || 'Starting...'}</small>
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
                    reconcileBtn.disabled = false;
                    reconcileBtn.removeAttribute('aria-busy');
                    loadMetrics(); // Refresh metrics after completion
                }
            } else {
                progressBarsContainer.innerHTML = '<p>No active or recent sync jobs.</p>';
                syncAllProductsBtn.disabled = false;
                syncAllProductsBtn.removeAttribute('aria-busy');
                reconcileBtn.disabled = false;
                reconcileBtn.removeAttribute('aria-busy');
            }
        } catch (error) {
            console.error('Failed to poll task status:', error);
            clearInterval(pollingInterval);
        }
    };

    const startSync = async (endpoint, triggerBtn, payload = {}) => {
        syncAllProductsBtn.disabled = true;
        reconcileBtn.disabled = true;
        triggerBtn.setAttribute('aria-busy', 'true');
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
            progressBarsContainer.innerHTML = `<p style="color: var(--color-danger);">${error.message}</p>`;
            syncAllProductsBtn.disabled = false;
            reconcileBtn.disabled = false;
            triggerBtn.removeAttribute('aria-busy');
        }
    };

    // --- Event Listeners ---
    syncAllProductsBtn.addEventListener('click', () =>
        startSync('/api/sync-control/products-and-reconcile', syncAllProductsBtn)
    );

    reconcileBtn.addEventListener('click', () =>
        startSync('/api/sync-control/reconcile-stock', reconcileBtn)
    );

    // Initial load
    loadMetrics();
    pollTaskStatus();
});