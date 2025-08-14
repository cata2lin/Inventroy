// static/js/inventory.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        metrics: document.getElementById('metrics-container'),
        filters: {
            search: document.getElementById('search-input'),
            type: document.getElementById('type-filter'),
            category: document.getElementById('category-filter'),
            minRetail: document.getElementById('min-retail-input'),
            maxRetail: document.getElementById('max-retail-input'),
            minInv: document.getElementById('min-inv-input'),
            maxInv: document.getElementById('max-inv-input'),
            reset: document.getElementById('reset-filters'),
        },
        tableContainer: document.getElementById('inventory-table-container'),
        pagination: {
            prev: document.getElementById('prev-button'),
            next: document.getElementById('next-button'),
            indicator: document.getElementById('page-indicator'),
        },
    };

    // --- State Management ---
    let state = {};

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const updateUrl = () => {
        const params = new URLSearchParams(window.location.search);
        Object.entries(state).forEach(([key, value]) => {
            if (value) params.set(key, value);
            else params.delete(key);
        });
        window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
    };

    const fetchInventory = debounce(async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        updateUrl();
        
        const params = new URLSearchParams({
            skip: ((state.page || 1) - 1) * 50, limit: 50,
            sort_by: state.sortBy, sort_order: state.sortOrder,
            ...state.filters
        });

        try {
            const response = await fetch(API_ENDPOINTS.getInventoryReport(params));
            if (!response.ok) throw new Error('Failed to fetch inventory report.');
            const data = await response.json();
            
            state.totalCount = data.total_count;
            renderAll(data);
        } catch (error) {
            elements.tableContainer.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            elements.tableContainer.removeAttribute('aria-busy');
        }
    }, 400);

    const renderAll = (data) => {
        renderMetrics(data);
        renderTable(data.inventory);
        updatePagination();
    };

    const renderMetrics = (data) => {
        elements.metrics.innerHTML = `
            <div class="metric"><h4>${(data.total_retail_value || 0).toLocaleString('ro-RO')} RON</h4><p>Total Retail Value</p></div>
            <div class="metric"><h4>${(data.total_inventory_value || 0).toLocaleString('ro-RO')} RON</h4><p>Total Inventory Value</p></div>
            <div class="metric"><h4>${(data.total_on_hand || 0).toLocaleString()}</h4><p>Total Products On Hand</p></div>`;
    };

    const renderTable = (inventory) => {
        const headers = [
            { key: 'image_url', label: 'Image', sortable: false }, { key: 'product_title', label: 'Product / Variant' },
            { key: 'sku', label: 'SKU' }, { key: 'barcode', label: 'Barcode' },
            { key: 'type', label: 'Type' }, { key: 'category', label: 'Category' },
            { key: 'status', label: 'Status' }, { key: 'price', label: 'Price' }, { key: 'cost', label: 'Cost' },
            { key: 'on_hand', label: 'On Hand' }, { key: 'committed', label: 'Committed' },
            { key: 'available', label: 'Available' }, { key: 'retail_value', label: 'Retail Value' },
            { key: 'inventory_value', label: 'Inv. Value' }
        ];

        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        headers.forEach(h => {
            const sortClass = state.sortBy === h.key ? `class="${state.sortOrder}"` : '';
            const sortable = h.sortable !== false ? `data-sort-by="${h.key}"` : '';
            tableHtml += `<th ${sortable} ${sortClass}>${h.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        (inventory || []).forEach(item => {
            tableHtml += `
                <tr>
                    <td><img src="${item.image_url || 'https://via.placeholder.com/40'}" alt="${item.product_title}" style="width: 40px; border-radius: 4px;"></td>
                    <td>${item.product_title}<br><small>${item.variant_title}</small></td>
                    <td>${item.sku || ''}</td>
                    <td>${item.barcode || ''}</td>
                    <td>${item.type || ''}</td>
                    <td>${item.category || ''}</td>
                    <td><span class="status-${(item.status || '').toLowerCase()}">${item.status}</span></td>
                    <td>${(item.price || 0).toFixed(2)}</td>
                    <td>${(item.cost || 0).toFixed(2)}</td>
                    <td>${item.on_hand}</td>
                    <td>${item.committed}</td>
                    <td>${item.available}</td>
                    <td>${(item.retail_value || 0).toFixed(2)}</td>
                    <td>${(item.inventory_value || 0).toFixed(2)}</td>
                </tr>
            `;
        });
        tableHtml += '</tbody></table></div>';
        elements.tableContainer.innerHTML = tableHtml;
        addSortEventListeners();
    };
    
    const updatePagination = () => {
        const totalPages = Math.ceil(state.totalCount / 50);
        elements.pagination.indicator.textContent = `Page ${state.page} of ${totalPages > 0 ? totalPages : 1}`;
        elements.pagination.prev.disabled = state.page === 1;
        elements.pagination.next.disabled = state.page >= totalPages;
    };

    const addSortEventListeners = () => {
        elements.tableContainer.querySelectorAll('th[data-sort-by]').forEach(th => {
            th.addEventListener('click', () => {
                const newSortBy = th.dataset.sortBy;
                state.sortOrder = (state.sortBy === newSortBy && state.sortOrder === 'desc') ? 'asc' : 'desc';
                state.sortBy = newSortBy;
                state.page = 1;
                fetchInventory();
            });
        });
    };

    const initialize = async () => {
        // Load filter options
        try {
            const response = await fetch(API_ENDPOINTS.getInventoryFilters);
            const data = await response.json();
            data.types.forEach(t => elements.filters.type.add(new Option(t, t)));
            data.categories.forEach(c => elements.filters.category.add(new Option(c, c)));
        } catch (error) {
            console.error("Could not load filter options:", error);
        }

        // Initialize state from URL
        const params = new URLSearchParams(window.location.search);
        state = {
            page: parseInt(params.get('page') || '1', 10),
            sortBy: params.get('sortBy') || 'on_hand',
            sortOrder: params.get('sortOrder') || 'desc',
            filters: {
                search: params.get('search') || '',
                product_type: params.get('product_type') || '',
                category: params.get('category') || '',
                min_retail: params.get('min_retail') || '',
                max_retail: params.get('max_retail') || '',
                min_inventory: params.get('min_inventory') || '',
                max_inventory: params.get('max_inventory') || '',
            }
        };

        // Update UI from state
        Object.entries(state.filters).forEach(([key, value]) => {
            const elKey = {product_type: 'type', category: 'category', min_retail: 'minRetail', max_retail: 'maxRetail', min_inventory: 'minInv', max_inventory: 'maxInv'}[key] || key;
            if (elements.filters[elKey]) elements.filters[elKey].value = value;
        });

        // Add event listeners
        Object.entries(elements.filters).forEach(([key, el]) => {
            if (key === 'reset') return;
            el.addEventListener('input', () => {
                const filterKey = {type: 'product_type', minRetail: 'min_retail', maxRetail: 'max_retail', minInv: 'min_inventory', maxInv: 'max_inventory'}[key] || key;
                state.filters[filterKey] = el.value;
                state.page = 1;
                fetchInventory();
            });
        });
        
        elements.filters.reset.addEventListener('click', () => {
            window.history.replaceState({}, '', window.location.pathname);
            initialize();
        });

        elements.pagination.prev.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchInventory(); }});
        elements.pagination.next.addEventListener('click', () => { if ((state.page * 50) < state.totalCount) { state.page++; fetchInventory(); }});

        await fetchInventory();
    };

    initialize();
});