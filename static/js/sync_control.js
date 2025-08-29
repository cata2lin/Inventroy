// static/js/sync_control.js

document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        syncAllOrdersBtn: document.getElementById('sync-all-orders-btn'),
        syncAllProductsBtn: document.getElementById('sync-all-products-btn'),
        reconcileStockBtn: document.getElementById('reconcile-stock-btn'),
        storeSelect: document.getElementById('store-select'),
        syncSingleStoreBtn: document.getElementById('sync-single-store-btn'),
        startDate: document.getElementById('start-date'),
        endDate: document.getElementById('end-date'),
        syncDateRangeBtn: document.getElementById('sync-date-range-btn'),
        syncAllInventoryMaxBtn: document.getElementById('sync-all-inventory-max-btn'), // FIX: Added new button
        progressBarsContainer: document.getElementById('progress-bars'),
    };

    let pollingInterval = null;
    let activeButtons = new Set([elements.syncAllOrdersBtn, elements.syncAllProductsBtn, elements.syncSingleStoreBtn, elements.syncDateRangeBtn, elements.reconcileStockBtn, elements.syncAllInventoryMaxBtn]); // FIX: Added the new button here too

    const renderProgressBar = (task) => { /* ... same as before ... */ };

    const pollTaskStatus = async () => { /* ... same as before ... */ };

    const startSync = async (endpoint, payload = {}) => {
        activeButtons.forEach(btn => {
            btn.disabled = true;
            btn.setAttribute('aria-busy', 'true');
        });
        elements.progressBarsContainer.innerHTML = '<p>Starting sync jobs...</p>';

        try {
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!response.ok) throw new Error('Failed to start sync jobs.');
            
            if (pollingInterval) clearInterval(pollingInterval);
            pollingInterval = setInterval(pollTaskStatus, 2000);
        } catch (error) {
            console.error(error);
            elements.progressBarsContainer.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
            activeButtons.forEach(btn => {
                btn.disabled = false;
                btn.removeAttribute('aria-busy');
            });
        }
    };
    
    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            const stores = await response.json();
            stores.forEach(store => elements.storeSelect.add(new Option(store.name, store.id)));
        } catch (error) {
            console.error("Could not load stores:", error);
        }
    };

    elements.syncAllOrdersBtn.addEventListener('click', () => startSync(API_ENDPOINTS.syncOrders));
    elements.syncAllProductsBtn.addEventListener('click', () => startSync(API_ENDPOINTS.syncProducts));
    elements.reconcileStockBtn.addEventListener('click', () => startSync(API_ENDPOINTS.reconcileStock));
    
    elements.storeSelect.addEventListener('change', () => {
        elements.syncSingleStoreBtn.disabled = !elements.storeSelect.value;
    });

    elements.syncSingleStoreBtn.addEventListener('click', () => {
        const storeId = elements.storeSelect.value;
        if (storeId) {
            startSync(API_ENDPOINTS.syncOrders, { store_id: parseInt(storeId, 10) });
        }
    });

    elements.syncDateRangeBtn.addEventListener('click', () => {
        const startDate = elements.startDate.value;
        const endDate = elements.endDate.value;
        if (!startDate || !endDate) {
            alert("Please select both a start and end date.");
            return;
        }
        startSync(API_ENDPOINTS.syncOrders, { start_date: startDate, end_date: endDate });
    });

    // FIX: Add event listener for the new button
    elements.syncAllInventoryMaxBtn.addEventListener('click', () => {
        startSync(API_ENDPOINTS.syncAllInventoryMax);
    });

    loadStores();
    pollTaskStatus();
});