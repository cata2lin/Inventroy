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
            stores: document.getElementById('store-filter'),
            financialStatus: document.getElementById('financial-status-filter'),
            fulfillmentStatus: document.getElementById('fulfillment-status-filter'),
            startDate: document.getElementById('start-date'),
            endDate: document.getElementById('end-date'),
            apply: document.getElementById('apply-filters'),
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

    // --- Utility Functions ---
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const formatCurrency = (value) => value.toLocaleString('en-US', { style: 'currency', currency: 'USD' });

    // --- API & Rendering ---
    const fetchOrders = async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        
        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit,
            limit: state.limit,
            sort_by: state.sortBy,
            sort_order: state.sortOrder,
        });

        Object.entries(state.filters).forEach(([key, value]) => {
            if (value) {
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
    };

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
            { key: 'financial_status', label: 'Financial Status' },
            { key: 'fulfillment_status', label: 'Fulfillment Status' },
            { key: 'total_price', label: 'Total' },
        ];
        
        let tableHtml = '<table><thead><tr>';
        headers.forEach(header => {
            const isSorted = state.sortBy === header.key;
            const sortClass = isSorted ? state.sortOrder : '';
            tableHtml += `<th data-sort-by="${header.key}" class="${sortClass}">${header.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        orders.forEach(order => {
            tableHtml += `
                <tr>
                    <td>${order.name}</td>
                    <td>${order.store_name}</td>
                    <td>${new Date(order.created_at).toLocaleDateString()}</td>
                    <td><span class="status-${(order.financial_status || '').toLowerCase()}">${order.financial_status}</span></td>
                    <td><span class="status-${(order.fulfillment_status || '').toLowerCase()}">${order.fulfillment_status}</span></td>
                    <td>${formatCurrency(order.total_price)}</td>
                </tr>
            `;
        });

        tableHtml += '</tbody></table>';
        elements.tableContainer.innerHTML = tableHtml;

        // Add event listeners to new headers
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
                fetchOrders();
            });
        });
    };

    const updatePagination = () => {
        elements.pagination.indicator.textContent = `Page ${state.page}`;
        elements.pagination.prev.disabled = state.page === 1;
        elements.pagination.next.disabled = (state.page * state.limit) >= state.totalCount;
    };

    const applyAllFilters = () => {
        const selectedStores = Array.from(elements.filters.stores.selectedOptions).map(opt => opt.value);
        
        state.filters = {
            search: elements.filters.search.value || null,
            store_ids: selectedStores.length > 0 ? selectedStores : null,
            financial_status: elements.filters.financialStatus.value || null,
            fulfillment_status: elements.filters.fulfillmentStatus.value || null,
            start_date: elements.filters.startDate.value || null,
            end_date: elements.filters.endDate.value || null,
        };
        state.page = 1;
        fetchOrders();
    };
    
    // --- Initial Load ---
    const initializeDashboard = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            const stores = await response.json();
            elements.filters.stores.innerHTML = ''; // Clear loading message
            stores.forEach(store => {
                elements.filters.stores.add(new Option(store.name, store.id));
            });
        } catch (error) {
            elements.filters.stores.innerHTML = '<option>Could not load stores</option>';
        }
        await fetchOrders();
    };

    // --- Event Listeners ---
    elements.filters.apply.addEventListener('click', applyAllFilters);

    elements.filters.search.addEventListener('input', debounce(() => {
        state.filters.search = elements.filters.search.value || null;
        state.page = 1;
        fetchOrders();
    }, 500));

    elements.filters.reset.addEventListener('click', () => {
        elements.filters.search.value = '';
        elements.filters.stores.selectedIndex = -1;
        elements.filters.financialStatus.value = '';
        elements.filters.fulfillmentStatus.value = '';
        elements.filters.startDate.value = '';
        elements.filters.endDate.value = '';
        applyAllFilters();
    });

    elements.pagination.prev.addEventListener('click', () => {
        if (state.page > 1) {
            state.page--;
            fetchOrders();
        }
    });

    elements.pagination.next.addEventListener('click', () => {
        if ((state.page * state.limit) < state.totalCount) {
            state.page++;
            fetchOrders();
        }
    });

    initializeDashboard();
});