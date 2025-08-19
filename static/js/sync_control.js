// static/js/sync_control.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        storeSelector: document.getElementById('store-selector'),
        syncAllProductsBtn: document.getElementById('sync-all-products-btn'),
        syncAllOrdersBtn: document.getElementById('sync-all-orders-btn'),
        syncSelectedProductsBtn: document.getElementById('sync-selected-products-btn'),
        syncSelectedOrdersBtn: document.getElementById('sync-selected-orders-btn'),
        startDate: document.getElementById('start-date'),
        endDate: document.getElementById('end-date'),
        toast: document.getElementById('toast'),
    };

    // --- Utility Functions ---
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

    // --- Data Loading ---
    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            elements.storeSelector.innerHTML = ''; // Clear loading message
            stores.forEach(store => {
                const option = new Option(store.name, store.id);
                elements.storeSelector.add(option);
            });
        } catch (error) {
            elements.storeSelector.innerHTML = '<option>Could not load stores</option>';
            showToast(error.message, 'error');
        }
    };

    // --- API Call Handlers ---
    const handleSyncRequest = async (endpoint, payload, button) => {
        disableButtons(true);
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
            showToast(result.message, 'success');
        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            disableButtons(false);
        }
    };

    // --- Event Listeners ---
    elements.syncAllProductsBtn.addEventListener('click', () => {
        handleSyncRequest(API_ENDPOINTS.syncProducts, {});
    });

    elements.syncAllOrdersBtn.addEventListener('click', () => {
        handleSyncRequest(API_ENDPOINTS.syncOrders, {});
    });

    elements.syncSelectedProductsBtn.addEventListener('click', () => {
        const storeIds = getSelectedStoreIds();
        handleSyncRequest(API_ENDPOINTS.syncProducts, { store_ids: storeIds });
    });

    elements.syncSelectedOrdersBtn.addEventListener('click', () => {
        const storeIds = getSelectedStoreIds();
        const startDate = elements.startDate.value;
        const endDate = elements.endDate.value;
        handleSyncRequest(API_ENDPOINTS.syncOrders, { 
            store_ids: storeIds,
            start_date: startDate,
            end_date: endDate,
        });
    });

    // --- Initial Load ---
    loadStores();
});