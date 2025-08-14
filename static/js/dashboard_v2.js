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

    let state = {
        page: 1, limit: 50, totalCount: 0, sortBy: 'created_at', sortOrder: 'desc',
        filters: { search: '', tags: '', store_ids: [], financial_status: '', fulfillment_status: '', has_note: '', start_date: '', end_date: '' },
        hiddenColumns: []
    };

    // --- URL State Synchronization ---
    const updateStateFromUrl = () => {
        const params = new URLSearchParams(window.location.search);
        state.page = parseInt(params.get('page') || '1', 10);
        state.sortBy = params.get('sortBy') || 'created_at';
        state.sortOrder = params.get('sortOrder') || 'desc';
        state.hiddenColumns = params.getAll('hide') || [];
        state.filters = {
            search: params.get('search') || '',
            tags: params.get('tags') || '',
            store_ids: params.getAll('stores') || [],
            financial_status: params.get('fs') || '',
            fulfillment_status: params.get('ffs') || '',
            has_note: params.get('note') || '',
            start_date: params.get('start') || '',
            end_date: params.get('end') || '',
        };
    };

    const updateUrlFromState = () => {
        const params = new URLSearchParams();
        if (state.page > 1) params.set('page', state.page);
        if (state.sortBy !== 'created_at') params.set('sortBy', state.sortBy);
        if (state.sortOrder !== 'desc') params.set('sortOrder', state.sortOrder);
        state.hiddenColumns.forEach(col => params.append('hide', col));
        if (state.filters.search) params.set('search', state.filters.search);
        if (state.filters.tags) params.set('tags', state.filters.tags);
        state.filters.store_ids.forEach(id => params.append('stores', id));
        if (state.filters.financial_status) params.set('fs', state.filters.financial_status);
        if (state.filters.fulfillment_status) params.set('ffs', state.filters.fulfillment_status);
        if (state.filters.has_note) params.set('note', state.filters.has_note);
        if (state.filters.start_date) params.set('start', state.filters.start_date);
        if (state.filters.end_date) params.set('end', state.filters.end_date);
        
        const newUrl = `${window.location.pathname}?${params.toString()}`;
        window.history.pushState({ path: newUrl }, '', newUrl);
    };

    // --- API & Rendering ---
    const fetchAndRender = debounce(async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        updateUrlFromState();
        
        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit, limit: state.limit,
            sort_by: state.sortBy, sort_order: state.sortOrder,
        });
        
        Object.entries(state.filters).forEach(([key, value]) => {
            if (value !== null && value !== undefined && value !== '') {
                if (Array.isArray(value)) {
                    if (value.length > 0) value.forEach(v => params.append(key, v));
                } else {
                    params.append(key, value);
                }
            }
        });

        try {
            const response = await fetch(API_ENDPOINTS.getDashboardOrders(params));
            if (!response.ok) throw new Error('Failed to fetch orders.');
            const data = await response.json();
            state.totalCount = data.total_count;
            renderMetrics(data);
            renderTable(data.orders);
            updatePagination();
        } catch (error) {
            elements.tableContainer.innerHTML = `<p>Error: ${error.message}</p>`;
        } finally {
            elements.tableContainer.removeAttribute('aria-busy');
        }
    }, 300);

    const renderMetrics = (data) => {
        elements.metricsContainer.innerHTML = `
            <div class="metric"><h4>${data.total_count.toLocaleString()}</h4><p>Orders Found</p></div>
            <div class="metric"><h4>${(data.total_value || 0).toLocaleString('en-US', { style: 'currency', currency: 'USD' })}</h4><p>Total Value</p></div>
            <div class="metric"><h4>${(data.total_shipping || 0).toLocaleString('en-US', { style: 'currency', currency: 'USD' })}</h4><p>Total Shipping</p></div>`;
    };

    const renderTable = (orders) => {
        const visibleColumns = allColumns.filter(c => !state.hiddenColumns.includes(c.key));
        if (!orders || orders.length === 0) {
            elements.tableContainer.innerHTML = '<p>No orders found matching your criteria.</p>'; return;
        }

        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        visibleColumns.forEach(header => {
            const sortClass = state.sortBy === header.key ? `class="${state.sortOrder}"` : '';
            tableHtml += `<th data-sort-by="${header.key}" data-column-key="${header.key}" ${sortClass}>${header.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        orders.forEach(order => {
            tableHtml += '<tr>';
            visibleColumns.forEach(col => {
                let content = '';
                switch(col.key) {
                    case 'order_name': content = order.name; break;
                    case 'store_name': content = order.store_name; break;
                    case 'created_at': content = new Date(order.created_at).toLocaleDateString(); break;
                    case 'total_price': content = (order.total_price || 0).toLocaleString('en-US', { style: 'currency', currency: order.currency || 'USD' }); break;
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
        elements.pagination.indicator.textContent = `Page ${state.page} of ${Math.ceil(state.totalCount / state.limit)}`;
        elements.pagination.prev.disabled = state.page === 1;
        elements.pagination.next.disabled = (state.page * state.limit) >= state.totalCount;
    };
    
    // --- FIXED: Made this function more robust ---
    const updateUiFromState = () => {
        elements.filters.search.value = state.filters.search;
        elements.filters.tags.value = state.filters.tags;
        elements.filters.startDate.value = state.filters.start_date;
        elements.filters.endDate.value = state.filters.end_date;
        
        const fsRadio = document.querySelector(`input[name="fs"][value="${state.filters.financial_status || ''}"]`);
        if (fsRadio) fsRadio.checked = true;

        const ffsRadio = document.querySelector(`input[name="ffs"][value="${state.filters.fulfillment_status || ''}"]`);
        if (ffsRadio) ffsRadio.checked = true;

        const noteRadio = document.querySelector(`input[name="note"][value="${state.filters.has_note || ''}"]`);
        if (noteRadio) noteRadio.checked = true;

        elements.filters.stores.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = state.filters.store_ids.includes(cb.value));
        
        allColumns.forEach(col => {
            const checkbox = document.querySelector(`input[name="col-${col.key}"]`);
            if (checkbox) {
                checkbox.checked = !state.hiddenColumns.includes(col.key);
            }
        });
    };

    const addSortEventListeners = () => {
        document.querySelectorAll('#orders-table-container th[data-sort-by]').forEach(th => {
            th.addEventListener('click', () => {
                const newSortBy = th.dataset.sortBy;
                state.sortOrder = (state.sortBy === newSortBy && state.sortOrder === 'desc') ? 'asc' : 'desc';
                state.sortBy = newSortBy;
                state.page = 1;
                fetchAndRender();
            });
        });
    };
    
    const initializeDashboard = async () => {
        allColumns.forEach(col => {
            elements.filters.columns.innerHTML += `<li><label><input type="checkbox" name="col-${col.key}" value="${col.key}"> ${col.label}</label></li>`;
        });
        
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            const stores = await response.json();
            elements.filters.stores.innerHTML = '';
            stores.forEach(store => elements.filters.stores.innerHTML += `<li><label><input type="checkbox" name="store" value="${store.id}"> ${store.name}</label></li>`);
        } catch (error) {
            elements.filters.stores.innerHTML = '<li>Could not load stores</li>';
        }

        updateStateFromUrl();
        updateUiFromState();
        fetchAndRender();
    };
    
    // --- Event Listeners ---
    ['search', 'tags', 'startDate', 'endDate'].forEach(key => {
        elements.filters[key].addEventListener('input', () => {
            const filterKeyMapping = { 'startDate': 'start_date', 'endDate': 'end_date' };
            const filterKey = filterKeyMapping[key] || key;
            state.filters[filterKey] = elements.filters[key].value;
            state.page = 1;
            fetchAndRender();
        });
    });
    
    ['financialStatus', 'fulfillmentStatus', 'hasNote'].forEach(key => {
        elements.filters[key].addEventListener('change', (e) => {
            const filterKeyMapping = { 'financialStatus': 'financial_status', 'fulfillmentStatus': 'fulfillment_status', 'hasNote': 'has_note' };
            state.filters[filterKeyMapping[key]] = e.target.value;
            state.page = 1;
            fetchAndRender();
        });
    });
    
    elements.filters.stores.addEventListener('change', () => {
        state.filters.store_ids = Array.from(elements.filters.stores.querySelectorAll('input:checked')).map(cb => cb.value);
        state.page = 1;
        fetchAndRender();
    });

    elements.filters.columns.addEventListener('change', () => {
        state.hiddenColumns = allColumns.map(c => c.key).filter(key => {
            const checkbox = document.querySelector(`input[name="col-${key}"]`);
            return checkbox && !checkbox.checked;
        });
        // This only affects rendering, no need to re-fetch from API
        renderTable(JSON.parse(elements.tableContainer.dataset.orders || '[]'));
        updateUrlFromState();
    });

    elements.filters.reset.addEventListener('click', () => {
        history.pushState({}, '', window.location.pathname);
        updateStateFromUrl();
        updateUiFromState();
        fetchAndRender();
    });
    
    elements.pagination.prev.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchAndRender(); } });
    elements.pagination.next.addEventListener('click', () => { if ((state.page * state.limit) < state.totalCount) { state.page++; fetchAndRender(); } });
    window.addEventListener('popstate', () => { initializeDashboard(); });

    initializeDashboard();
});