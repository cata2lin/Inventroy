// static/js/sync_control.js

document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        storeSelector: document.getElementById('store-selector'),
        syncAllProductsBtn: document.getElementById('sync-all-products-btn'),
        syncAllOrdersBtn: document.getElementById('sync-all-orders-btn'),
        syncSelectedProductsBtn: document.getElementById('sync-selected-products-btn'),
        syncSelectedOrdersBtn: document.getElementById('sync-selected-orders-btn'),
        startDate: document.getElementById('start-date'),
        endDate: document.getElementById('end-date'),
        toast: document.getElementById('toast'),
        progressContainer: document.getElementById('progress-container'),
    };

    let activePoll = null;

    const showToast = (message, type = 'info', duration = 5000) => {
        elements.toast.textContent = message;
        elements.toast.className = `show ${type}`;
        setTimeout(() => { elements.toast.className = ''; }, duration);
    };

    const getSelectedStoreIds = () => {
        return Array.from(elements.storeSelector.selectedOptions).map(option => parseInt(option.value, 10));
    };

    const disableButtons = (disable = true) => {
        document.querySelectorAll('button').forEach(button => {
            button.disabled = disable;
            if (disable) {
                button.setAttribute('aria-busy', 'true');
            } else {
                button.removeAttribute('aria-busy');
            }
        });
    };

    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            elements.storeSelector.innerHTML = '';
            stores.forEach(store => {
                const option = new Option(store.name, store.id);
                elements.storeSelector.add(option);
            });
        } catch (error) {
            elements.storeSelector.innerHTML = '<option>Could not load stores</option>';
            showToast(error.message, 'error');
        }
    };

    const pollSyncStatus = (taskId) => {
        if (activePoll) {
            clearInterval(activePoll);
        }

        activePoll = setInterval(async () => {
            try {
                const response = await fetch(API_ENDPOINTS.getSyncStatus(taskId));
                if (!response.ok) return;
                
                const statusData = await response.json();
                let progressHtml = '';
                let isCompleted = false;

                if (statusData.status === 'not_found') return;

                for (const storeName in statusData) {
                    if (storeName === 'overall' && statusData[storeName].status === 'completed') {
                        isCompleted = true;
                        break;
                    }
                    if (storeName !== 'overall') {
                        const { progress, total } = statusData[storeName];
                        const percentage = total > 0 ? (progress / total * 100).toFixed(0) : 0;
                        progressHtml += `
                            <label for="${storeName}-progress">${storeName}: ${progress} / ${total} (${percentage}%)</label>
                            <progress id="${storeName}-progress" value="${progress}" max="${total}"></progress>
                        `;
                    }
                }
                
                elements.progressContainer.innerHTML = progressHtml;

                if (isCompleted) {
                    clearInterval(activePoll);
                    activePoll = null;
                    showToast('Sync completed successfully!', 'success');
                    setTimeout(() => {
                        elements.progressContainer.innerHTML = '';
                    }, 3000);
                    disableButtons(false);
                }

            } catch (error) {
                console.error("Polling error:", error);
                clearInterval(activePoll);
                activePoll = null;
                disableButtons(false);
            }
        }, 2000);
    };

    const handleSyncRequest = async (endpoint, payload) => {
        if (activePoll) {
            showToast('A sync is already in progress. Please wait.', 'error');
            return;
        }
        disableButtons(true);
        elements.progressContainer.innerHTML = 'Starting sync...';
        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
            showToast(result.message, 'info');
            if (result.task_id) {
                pollSyncStatus(result.task_id);
            } else {
                disableButtons(false);
            }
        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
            disableButtons(false);
        }
    };

    elements.syncAllProductsBtn.addEventListener('click', () => {
        handleSyncRequest(API_ENDPOINTS.syncProducts, { store_ids: [] });
    });

    elements.syncAllOrdersBtn.addEventListener('click', () => {
        handleSyncRequest(API_ENDPOINTS.syncOrders, { store_ids: [] });
    });

    elements.syncSelectedProductsBtn.addEventListener('click', () => {
        const storeIds = getSelectedStoreIds();
        if (storeIds.length === 0) {
            showToast('Please select at least one store.', 'error');
            return;
        }
        handleSyncRequest(API_ENDPOINTS.syncProducts, { store_ids: storeIds });
    });

    elements.syncSelectedOrdersBtn.addEventListener('click', () => {
        const storeIds = getSelectedStoreIds();
        const startDate = elements.startDate.value;
        const endDate = elements.endDate.value;
        handleSyncRequest(API_ENDPOINTS.syncOrders, { 
            store_ids: storeIds,
            start_date: startDate || null,
            end_date: endDate || null,
        });
    });

    loadStores();
});