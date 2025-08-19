// static/js/sync_control.js

document.addEventListener('DOMContentLoaded', () => {
    const syncAllOrdersBtn = document.getElementById('sync-all-orders-btn');
    const progressBarsContainer = document.getElementById('progress-bars');
    let pollingInterval = null;

    const renderProgressBar = (task) => {
        const percentage = task.total > 0 ? (task.progress / task.total) * 100 : 0;
        let colorClass = '';
        if (task.status === 'completed') colorClass = 'progress-success';
        if (task.status === 'failed') colorClass = 'progress-failed';

        return `
            <div class="progress-item ${colorClass}">
                <strong>${task.store_name}</strong>
                <small>${task.message}</small>
                <progress value="${task.progress}" max="${task.total}"></progress>
                <span>${Math.round(percentage)}%</span>
            </div>
        `;
    };

    const pollTaskStatus = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getSyncStatus);
            if (!response.ok) throw new Error('Failed to fetch status.');
            const tasks = await response.json();
            
            progressBarsContainer.innerHTML = Object.values(tasks).map(renderProgressBar).join('');

            // If all tasks are finished, stop polling
            const isAllDone = Object.values(tasks).every(t => t.status === 'completed' || t.status === 'failed');
            if (isAllDone) {
                clearInterval(pollingInterval);
                pollingInterval = null;
                syncAllOrdersBtn.disabled = false;
                syncAllOrdersBtn.removeAttribute('aria-busy');
            }
        } catch (error) {
            console.error(error);
            clearInterval(pollingInterval);
            pollingInterval = null;
            syncAllOrdersBtn.disabled = false;
            syncAllOrdersBtn.removeAttribute('aria-busy');
        }
    };

    syncAllOrdersBtn.addEventListener('click', async () => {
        syncAllOrdersBtn.disabled = true;
        syncAllOrdersBtn.setAttribute('aria-busy', 'true');
        progressBarsContainer.innerHTML = '<p>Starting sync jobs...</p>';

        try {
            const response = await fetch(API_ENDPOINTS.syncAllOrders, { method: 'POST' });
            if (!response.ok) throw new Error('Failed to start sync jobs.');
            
            // Start polling for updates every 2 seconds
            if (pollingInterval) clearInterval(pollingInterval);
            pollingInterval = setInterval(pollTaskStatus, 2000);
        } catch (error) {
            console.error(error);
            progressBarsContainer.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
            syncAllOrdersBtn.disabled = false;
            syncAllOrdersBtn.removeAttribute('aria-busy');
        }
    });

    // Initial load of any existing tasks
    pollTaskStatus();
});