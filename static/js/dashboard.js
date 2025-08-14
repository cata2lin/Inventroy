// static/js/dashboard.js

document.addEventListener('DOMContentLoaded', () => {
    const storeSelector = document.getElementById('storeSelector');
    const syncButton = document.getElementById('syncButton');
    const tabs = document.querySelectorAll('[role="tab"]');
    const tabPanels = document.querySelectorAll('[role="tabpanel"]');
    const toast = document.getElementById('toast');

    let currentStoreId = null;

    // --- Utility Functions ---
    const showToast = (message, duration = 3000) => {
        toast.textContent = message;
        toast.classList.add('show');
        setTimeout(() => {
            toast.classList.remove('show');
        }, duration);
    };

    const renderLoading = (element) => {
        element.innerHTML = '<p><progress></progress><br>Loading data...</p>';
    };

    // --- Tab Management ---
    const switchTab = (targetId) => {
        tabPanels.forEach(panel => {
            panel.setAttribute('aria-hidden', panel.id !== targetId);
        });
        tabs.forEach(tab => {
            tab.classList.toggle('secondary', tab.dataset.target === targetId);
        });
        if (currentStoreId) {
            const view = targetId.split('-')[0];
            loadViewData(view);
        }
    };
    
    tabs.forEach(tab => {
        tab.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab(e.target.dataset.target);
        });
    });

    // --- Data Loading ---
    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            storeSelector.innerHTML = '<option value="" disabled selected>Select a store</option>';
            stores.forEach(store => {
                const option = document.createElement('option');
                option.value = store.id;
                option.textContent = store.name;
                storeSelector.appendChild(option);
            });
        } catch (error) {
            console.error(error);
            storeSelector.innerHTML = '<option value="">Could not load stores</option>';
        }
    };

    const loadViewData = async (view) => {
        if (!currentStoreId) return;
        const contentEl = document.getElementById(`${view}-content`);
        renderLoading(contentEl);

        const endpointMap = {
            orders: API_ENDPOINTS.getOrders(currentStoreId),
            fulfillments: API_ENDPOINTS.getFulfillments(currentStoreId),
            inventory: API_ENDPOINTS.getInventoryDashboard(currentStoreId)
        };

        try {
            const response = await fetch(endpointMap[view]);
            if (!response.ok) throw new Error(`Failed to load ${view}.`);
            const data = await response.json();
            renderTable(contentEl, data, view);
        } catch (error) {
            console.error(error);
            contentEl.innerHTML = `<p>Error loading ${view}: ${error.message}</p>`;
        }
    };

    const renderTable = (element, data, view) => {
        if (!data || data.length === 0) {
            element.innerHTML = `<p>No ${view} data found for this store.</p>`;
            return;
        }

        const headersMap = {
            orders: ['Order', 'Date', 'Customer Email', 'Total', 'Financial Status', 'Fulfillment'],
            fulfillments: ['Order', 'Fulfillment ID', 'Created', 'Company', 'Tracking Number', 'Status'],
            inventory: ['Product', 'Variant (SKU)', 'Inventory Policy', 'Available Qty', 'Location']
        };

        const headers = headersMap[view];
        const table = document.createElement('table');
        table.className = 'striped';
        const thead = table.createTHead();
        const tbody = table.createTBody();
        const headerRow = thead.insertRow();
        headers.forEach(h => headerRow.insertCell().textContent = h);

        data.forEach(item => {
            const row = tbody.insertRow();
            if (view === 'orders') {
                row.innerHTML = `
                    <td>${item.name}</td>
                    <td>${new Date(item.created_at).toLocaleDateString()}</td>
                    <td>${item.email || 'N/A'}</td>
                    <td>${item.total_price} ${item.currency}</td>
                    <td>${item.financial_status || 'N/A'}</td>
                    <td><span class="status-${(item.fulfillment_status || 'unfulfilled').toLowerCase()}">${item.fulfillment_status || 'Unfulfilled'}</span></td>
                `;
            } else if (view === 'fulfillments') {
                 row.innerHTML = `
                    <td>${item.order_name}</td>
                    <td>${item.id}</td>
                    <td>${new Date(item.created_at).toLocaleString()}</td>
                    <td>${item.tracking_company || 'N/A'}</td>
                    <td>${item.tracking_number || 'N/A'}</td>
                    <td>${item.status || 'N/A'}</td>
                `;
            } else if (view === 'inventory') {
                 row.innerHTML = `
                    <td>${item.product_title}</td>
                    <td>${item.variant_title} (${item.sku || 'N/A'})</td>
                    <td>${item.inventory_policy}</td>
                    <td><strong>${item.available_quantity !== null ? item.available_quantity : 'N/A'}</strong></td>
                    <td>${item.location_name}</td>
                `;
            }
        });

        element.innerHTML = '';
        element.appendChild(table);
    };

    // --- Event Listeners ---
    storeSelector.addEventListener('change', () => {
        currentStoreId = storeSelector.value;
        syncButton.disabled = !currentStoreId;
        const activePanel = document.querySelector('[role="tabpanel"][aria-hidden="false"]');
        if (activePanel) {
            const view = activePanel.id.split('-')[0];
            loadViewData(view);
        }
    });

    syncButton.addEventListener('click', async () => {
        if (!currentStoreId) return;
        syncButton.setAttribute('aria-busy', 'true');
        syncButton.disabled = true;
        showToast(`Starting data sync for store ID ${currentStoreId}...`);

        try {
            const response = await fetch(API_ENDPOINTS.syncOrders(currentStoreId), { method: 'POST' });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Sync request failed.');
            showToast(result.message);
        } catch (error) {
            console.error(error);
            showToast(`Error: ${error.message}`);
        } finally {
            syncButton.removeAttribute('aria-busy');
            syncButton.disabled = false;
        }
    });

    // --- Initial Load ---
    loadStores();
    switchTab('orders-panel'); // Set initial active tab
});