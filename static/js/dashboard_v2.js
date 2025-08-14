// static/js/dashboard_v2.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        metrics: {
            orders: document.getElementById('total-orders'),
            value: document.getElementById('total-value'),
            shipping: document.getElementById('total-shipping'),
        },
        filters: {
            search: document.getElementById('search-input'),
            tags: document.getElementById('tags-input'),
            stores: document.getElementById('store-filter-list'),
            financialStatus: document.getElementById('financial-status-filter'),
            fulfillmentStatus: document.getElementById('fulfillment-status-filter'),
            hasNote: document.getElementById('has-note-filter'),
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
    const state = {
        page: 1,
        limit: 50,
        totalCount: 0,
        sortBy: 'created_at',
        sortOrder: 'desc',
        filters: {},
    };

    // --- Utility & Helper Functions ---
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const formatCurrency = (value, currency) => {
        return value.toLocaleString('en-US', { style: 'currency', currency: currency || 'USD' });
    };

    // --- Main API & Rendering Logic ---
    const fetchAndRender = debounce(async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        
        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit,
            limit: state.limit,
            sort_by: state.sortBy,
            sort_order: state.sortOrder,
        });

        Object.entries(state.filters).forEach(([key, value]) => {
            if (value !== null && value !== '' && value.length !== 0) {
                if (Array.isArray(value)) {
                    value.forEach(v => params.append(key, v));
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
    }, 250);

    const renderMetrics = (data) => {
        elements.metrics.orders.textContent = data.total_count.toLocaleString();
        elements.metrics.value.textContent = formatCurrency(data.total_value);
        elements.metrics.shipping.textContent = formatCurrency(data.total_shipping);
    };

    const renderTable = (orders) => {
        if (!orders || orders.length === 0) {
            elements.tableContainer.innerHTML = '<p>No orders found matching your criteria.</p>';
            return;
        }

        const headers = [
            { key: 'order_name', label: 'Order' },
            { key: 'store_name', label: 'Store' },
            { key: 'created_at', label: 'Date' },
            { key: 'total_price', label: 'Total' },
            { key: 'fulfillment_status', label: 'Fulfillment' },
            { key: 'cancelled', label: 'Cancelled' },
            { key: 'note', label: 'Note' },
            { key: 'tags', label: 'Tags' },
        ];
        
        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        headers.forEach(header => {
            const isSorted = state.sortBy === header.key;
            const sortClass = isSorted ? `class="${state.sortOrder}"` : '';
            tableHtml += `<th data-sort-by="${header.key}" ${sortClass} style="cursor: pointer;">${header.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        orders.forEach(order => {
            const noteText = order.note || '';
            const tagsText = order.tags || '';
            tableHtml += `
                <tr>
                    <td>${order.name}</td>
                    <td>${order.store_name}</td>
                    <td>${new Date(order.created_at).toLocaleDateString()}</td>
                    <td>${formatCurrency(order.total_price, order.currency)}</td>
                    <td><span class="status-${(order.fulfillment_status || '').toLowerCase()}">${order.fulfillment_status || 'N/A'}</span></td>
                    <td class="${order.cancelled ? 'status-cancelled' : ''}">${order.cancelled ? `Yes (${order.cancel_reason || 'N/A'})` : 'No'}</td>
                    <td class="truncate-text" title="${noteText}">${noteText}</td>
                    <td class="truncate-text" title="${tagsText}">${tagsText}</td>
                </tr>
            `;
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
    
    const addSortEventListeners = () => {
        document.querySelectorAll('#orders-table-container th[data-sort-by]').forEach(th => {
            th.addEventListener('click', () => {
                const newSortBy = th.dataset.sortBy;
                if (state.sortBy === newSortBy) {
                    state.sortOrder = state.sortOrder === 'desc' ? 'asc' : 'desc';
                } else {
                    state.sortBy = newSortBy;
                    state.sortOrder = 'desc';
                }
                state.page = 1;
                fetchAndRender();
            });
        });
    };
    
    const collectFiltersAndFetch = () => {
        const selectedStores = Array.from(elements.filters.stores.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
        
        state.filters = {
            search: elements.filters.search.value.trim() || null,
            tags: elements.filters.tags.value.trim() || null,
            store_ids: selectedStores,
            financial_status: elements.filters.financialStatus.querySelector('input:checked')?.value || null,
            fulfillment_status: elements.filters.fulfillmentStatus.querySelector('input:checked')?.value || null,
            has_note: elements.filters.hasNote.querySelector('input:checked')?.value || null,
            start_date: elements.filters.startDate.value || null,
            end_date: elements.filters.endDate.value || null,
        };
        state.page = 1;
        fetchAndRender();
    };
    
    const initializeDashboard = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            const stores = await response.json();
            elements.filters.stores.innerHTML = '';
            stores.forEach(store => {
                const li = document.createElement('li');
                li.innerHTML = `<label><input type="checkbox" name="store" value="${store.id}"> ${store.name}</label>`;
                elements.filters.stores.appendChild(li);
            });
        } catch (error) {
            elements.filters.stores.innerHTML = '<li>Could not load stores</li>';
        }
        
        collectFiltersAndFetch();
    };

    // --- Event Listeners ---
    ['search', 'tags', 'startDate', 'endDate'].forEach(key => {
        elements.filters[key].addEventListener('input', collectFiltersAndFetch);
    });
    
    ['stores', 'financialStatus', 'fulfillmentStatus', 'hasNote'].forEach(key => {
        elements.filters[key].addEventListener('change', collectFiltersAndFetch);
    });

    elements.filters.reset.addEventListener('click', () => {
        elements.filters.search.value = '';
        elements.filters.tags.value = '';
        elements.filters.startDate.value = '';
        elements.filters.endDate.value = '';
        elements.filters.stores.querySelectorAll('input').forEach(i => i.checked = false);
        elements.filters.financialStatus.querySelectorAll('input')[0].checked = true;
        elements.filters.fulfillmentStatus.querySelectorAll('input')[0].checked = true;
        elements.filters.hasNote.querySelectorAll('input')[0].checked = true;
        
        document.querySelectorAll('.dropdown[open]').forEach(d => d.removeAttribute('open'));

        collectFiltersAndFetch();
    });

    elements.pagination.prev.addEventListener('click', () => {
        if (state.page > 1) {
            state.page--;
            fetchAndRender();
        }
    });

    elements.pagination.next.addEventListener('click', () => {
        if ((state.page * state.limit) < state.totalCount) {
            state.page++;
            fetchAndRender();
        }
    });

    initializeDashboard();
});