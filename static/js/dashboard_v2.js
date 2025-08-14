// static/js/dashboard_v2.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        metricsContainer: document.getElementById('metrics-container'),
        filters: {
            search: document.getElementById('search-input'),
            tags: document.getElementById('tags-input'),
            stores: document.getElementById('store-filter-list'),
            financialStatus: document.getElementById('financial-status-filter'),
            fulfillmentStatus: document.getElementById('fulfillment-status-filter'),
            hasNote: document.getElementById('has-note-filter'),
            columns: document.getElementById('column-visibility-filter'),
            startDate: document.getElementById('start-date'),
            endDate: document.getElementById('end-date'),
            reset: document.getElementById('reset-filters'),
        },
        tableContainer: document.getElementById('orders-table-container'),
        pagination: {
            prev: document.getElementById('prev-button'),
            next: document.getElementById('next-button'),
            indicator: document.getElementById('page-indicator'),
        },
    };

    // --- State Management ---
    const allColumns = [
        { key: 'order_name', label: 'Order' }, { key: 'store_name', label: 'Store' },
        { key: 'created_at', label: 'Date' }, { key: 'total_price', label: 'Total' },
        { key: 'fulfillment_status', label: 'Fulfillment' }, { key: 'cancelled', label: 'Cancelled' },
        { key: 'note', label: 'Note' }, { key: 'tags', label: 'Tags' },
    ];

    let state = {};
    let currentOrders = [];

    // --- Core Functions ---
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const updateUrl = () => {
        const params = new URLSearchParams();
        if (state.page > 1) params.set('page', state.page);
        if (state.sortBy !== 'created_at') params.set('sortBy', state.sortBy);
        if (state.sortOrder !== 'desc') params.set('sortOrder', state.sortOrder);
        state.hiddenColumns.forEach(col => params.append('hide', col));
        Object.entries(state.filters).forEach(([key, value]) => {
            const paramKeyMap = { financial_status: 'fs', fulfillment_status: 'ffs', has_note: 'note', store_ids: 'stores', start_date: 'start', end_date: 'end' };
            const paramKey = paramKeyMap[key] || key;
            if (value && value.length > 0) {
                if (Array.isArray(value)) value.forEach(v => params.append(paramKey, v));
                else params.set(paramKey, value);
            }
        });
        const newUrl = `${window.location.pathname}?${params.toString()}`;
        window.history.replaceState({ path: newUrl }, '', newUrl);
    };

    const fetchOrders = async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        updateUrl();
        
        const params = new URLSearchParams({
            skip: (state.page - 1) * 50, limit: 50,
            sort_by: state.sortBy, sort_order: state.sortOrder,
        });
        Object.entries(state.filters).forEach(([key, value]) => {
            if (value && value.length > 0) {
                if (Array.isArray(value)) value.forEach(v => params.append(key, v));
                else params.append(key, value);
            }
        });

        try {
            const response = await fetch(API_ENDPOINTS.getDashboardOrders(params));
            if (!response.ok) throw new Error(`Network response was not ok: ${response.statusText}`);
            const data = await response.json();
            currentOrders = data.orders;
            state.totalCount = data.total_count;
            renderAll(data);
        } catch (error) {
            elements.tableContainer.innerHTML = `<p style="color: var(--pico-color-red-500);">Error: ${error.message}</p>`;
        } finally {
            elements.tableContainer.removeAttribute('aria-busy');
        }
    };

    const renderAll = (data) => {
        renderMetrics(data);
        renderTable();
        updatePagination();
    };

    const renderMetrics = (data) => {
        elements.metricsContainer.innerHTML = `
            <div class="metric"><h4>${data.total_count.toLocaleString()}</h4><p>Orders Found</p></div>
            <div class="metric"><h4>${(data.total_value || 0).toLocaleString('ro-RO')} ${data.currency}</h4><p>Total Value</p></div>
            <div class="metric"><h4>${(data.total_shipping || 0).toLocaleString('ro-RO')} ${data.currency}</h4><p>Total Shipping</p></div>`;
    };

    const renderTable = () => {
        const visibleColumns = allColumns.filter(c => !state.hiddenColumns.includes(c.key));
        if (!currentOrders || currentOrders.length === 0) {
            elements.tableContainer.innerHTML = '<p>No orders found matching your criteria.</p>'; return;
        }

        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        visibleColumns.forEach(header => {
            const sortClass = state.sortBy === header.key ? `class="${state.sortOrder}"` : '';
            tableHtml += `<th data-sort-by="${header.key}" ${sortClass}>${header.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        currentOrders.forEach(order => {
            tableHtml += '<tr>';
            visibleColumns.forEach(col => {
                let content = '';
                switch(col.key) {
                    case 'order_name': content = order.name || ''; break;
                    case 'store_name': content = order.store_name || ''; break;
                    case 'created_at': content = new Date(order.created_at).toLocaleDateString(); break;
                    case 'total_price': content = `${(order.total_price || 0).toLocaleString('ro-RO')} ${order.currency || ''}`; break;
                    case 'fulfillment_status': content = `<span class="status-${(order.fulfillment_status || '').toLowerCase()}">${order.fulfillment_status || 'N/A'}</span>`; break;
                    case 'cancelled': content = `<span class="${order.cancelled ? 'status-cancelled' : ''}">${order.cancelled ? `Yes (${order.cancel_reason || 'N/A'})` : 'No'}</span>`; break;
                    case 'note': content = `<div class="truncate-text" title="${order.note || ''}">${order.note || ''}</div>`; break;
                    case 'tags': content = `<div class="truncate-text" title="${order.tags || ''}">${order.tags || ''}</div>`; break;
                }
                tableHtml += `<td data-column-key="${col.key}">${content}</td>`;
            });
            tableHtml += '</tr>';
        });

        tableHtml += '</tbody></table></div>';
        elements.tableContainer.innerHTML = tableHtml;
        addSortEventListeners();
    };

    const updatePagination = () => {
        elements.pagination.indicator.textContent = `Page ${state.page} of ${Math.ceil(state.totalCount / 50)}`;
        elements.pagination.prev.disabled = state.page === 1;
        elements.pagination.next.disabled = (state.page * 50) >= state.totalCount;
    };
    
    // --- Initialization and Event Listeners ---
    const setupEventListeners = () => {
        elements.filters.search.addEventListener('input', debounce(() => {
            state.filters.search = elements.filters.search.value;
            state.page = 1;
            fetchOrders();
        }, 500));

        elements.filters.tags.addEventListener('input', debounce(() => {
            state.filters.tags = elements.filters.tags.value;
            state.page = 1;
            fetchOrders();
        }, 500));

        ['startDate', 'endDate'].forEach(key => {
            elements.filters[key].addEventListener('change', () => {
                const filterKey = { startDate: 'start_date', endDate: 'end_date' }[key];
                state.filters[filterKey] = elements.filters[key].value;
                state.page = 1;
                fetchOrders();
            });
        });

        ['financialStatus', 'fulfillmentStatus', 'hasNote'].forEach(key => {
            elements.filters[key].addEventListener('change', (e) => {
                const filterKey = { financialStatus: 'financial_status', fulfillmentStatus: 'fulfillment_status', hasNote: 'has_note' }[key];
                state.filters[filterKey] = e.target.value;
                state.page = 1;
                fetchOrders();
            });
        });
        
        elements.filters.stores.addEventListener('change', () => {
            state.filters.store_ids = Array.from(elements.filters.stores.querySelectorAll('input:checked')).map(cb => cb.value);
            state.page = 1;
            fetchOrders();
        });

        elements.filters.columns.addEventListener('change', () => {
            state.hiddenColumns = allColumns.filter(c => !document.querySelector(`input[name="col-${c.key}"]`).checked).map(c => c.key);
            renderTable(); // Re-render from cache
            updateUrl();
        });

        elements.filters.reset.addEventListener('click', () => {
            window.history.pushState({}, '', window.location.pathname);
            initialize();
        });
        
        elements.pagination.prev.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchOrders(); } });
        elements.pagination.next.addEventListener('click', () => { if ((state.page * 50) < state.totalCount) { state.page++; fetchOrders(); } });
    };

    const addSortEventListeners = () => {
        elements.tableContainer.querySelectorAll('th[data-sort-by]').forEach(th => {
            th.addEventListener('click', () => {
                const newSortBy = th.dataset.sortBy;
                state.sortOrder = (state.sortBy === newSortBy && state.sortOrder === 'desc') ? 'asc' : 'desc';
                state.sortBy = newSortBy;
                state.page = 1;
                fetchOrders();
            });
        });
    };

    const initialize = async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        
        // Step 1: Populate UI shells
        elements.filters.columns.innerHTML = allColumns.map(col => `<li><label><input type="checkbox" name="col-${col.key}" value="${col.key}"> ${col.label}</label></li>`).join('');
        
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            const stores = await response.json();
            elements.filters.stores.innerHTML = stores.map(store => `<li><label><input type="checkbox" name="store" value="${store.id}"> ${store.name}</label></li>`).join('');
        } catch (error) {
            elements.filters.stores.innerHTML = '<li>Could not load stores</li>';
        }

        // Step 2: Sync state from URL now that UI is built
        updateStateFromUrl();

        // Step 3: Update UI controls to match the state
        elements.filters.search.value = state.filters.search;
        elements.filters.tags.value = state.filters.tags;
        elements.filters.startDate.value = state.filters.start_date;
        elements.filters.endDate.value = state.filters.end_date;
        document.querySelector(`input[name="fs"][value="${state.filters.financial_status || ''}"]`).checked = true;
        document.querySelector(`input[name="ffs"][value="${state.filters.fulfillment_status || ''}"]`).checked = true;
        document.querySelector(`input[name="note"][value="${state.filters.has_note || ''}"]`).checked = true;
        elements.filters.stores.querySelectorAll('input').forEach(cb => cb.checked = state.filters.store_ids.includes(cb.value));
        allColumns.forEach(col => {
            const checkbox = document.querySelector(`input[name="col-${col.key}"]`);
            if (checkbox) checkbox.checked = !state.hiddenColumns.includes(col.key);
        });

        // Step 4: Add event listeners
        setupEventListeners();

        // Step 5: Fetch initial data
        await fetchOrders();
    };

    initialize();
});