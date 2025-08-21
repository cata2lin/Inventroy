// static/js/inventory.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        metrics: document.getElementById('metrics-container'),
        filters: {
            search: document.getElementById('search-input'),
            store: document.getElementById('store-filter'),
            type: document.getElementById('type-filter'),
            category: document.getElementById('category-filter'),
            status: document.getElementById('status-filter'),
            minRetail: document.getElementById('min-retail-input'),
            maxRetail: document.getElementById('max-retail-input'),
            minInv: document.getElementById('min-inv-input'),
            maxInv: document.getElementById('max-inv-input'),
            groupToggle: document.getElementById('group-toggle'),
            reset: document.getElementById('reset-filters'),
        },
        tableContainer: document.getElementById('inventory-table-container'),
        pagination: {
            prev: document.getElementById('prev-button'),
            next: document.getElementById('next-button'),
            indicator: document.getElementById('page-indicator'),
        },
        // NEW: Modal elements
        detailsModal: {
            dialog: document.getElementById('details-modal'),
            title: document.getElementById('modal-title'),
            content: document.getElementById('modal-content'),
            closeBtn: document.querySelector('#details-modal .close'),
        }
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
        const params = new URLSearchParams();
        if (state.page > 1) params.set('page', state.page);
        if (state.sortBy !== 'on_hand') params.set('sortBy', state.sortBy);
        if (state.sortOrder !== 'desc') params.set('sortOrder', state.sortOrder);
        if (state.view !== 'individual') params.set('view', state.view);
        Object.entries(state.filters).forEach(([key, value]) => {
            if (value) params.set(key, value);
        });
        window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
    };

    const fetchInventory = debounce(async () => {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        updateUrl();
        
        const params = new URLSearchParams({
            skip: ((state.page || 1) - 1) * 50, limit: 50,
            sort_by: state.sortBy, sort_order: state.sortOrder, view: state.view,
        });
        Object.entries(state.filters).forEach(([key, value]) => {
            if (value) params.append(key, value);
        });

        try {
            const response = await fetch(`/api/v2/inventory/report/?${params.toString()}`);
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
        if (state.view === 'grouped') {
            renderGroupedView(data.inventory);
        } else {
            renderIndividualView(data.inventory);
        }
        updatePagination();
    };

    const renderMetrics = (data) => {
        elements.metrics.innerHTML = `
            <div class="metric"><h4>${(data.total_retail_value || 0).toLocaleString('ro-RO')} RON</h4><p>Total Retail Value</p></div>
            <div class="metric"><h4>${(data.total_inventory_value || 0).toLocaleString('ro-RO')} RON</h4><p>Total Inventory Value</p></div>
            <div class="metric"><h4>${(data.total_on_hand || 0).toLocaleString()}</h4><p>Total Products On Hand</p></div>`;
    };

    const renderIndividualView = (inventory) => {
        const headers = [ { key: 'image_url', label: 'Image', sortable: false }, { key: 'product_title', label: 'Product / Variant' }, { key: 'store_name', label: 'Store' }, { key: 'sku', label: 'SKU' }, { key: 'barcode', label: 'Barcode' }, { key: 'type', label: 'Type' }, { key: 'category', label: 'Category' }, { key: 'status', label: 'Status' }, { key: 'price', label: 'Price' }, { key: 'cost', label: 'Cost' }, { key: 'on_hand', label: 'On Hand' }, { key: 'committed', label: 'Committed' }, { key: 'available', label: 'Available' }, { key: 'retail_value', label: 'Retail Value' }, { key: 'inventory_value', label: 'Inv. Value' } ];
        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        headers.forEach(h => {
            const sortClass = state.sortBy === h.key ? `class="${state.sortOrder}"` : '';
            const sortable = h.sortable !== false ? `data-sort-by="${h.key}"` : '';
            tableHtml += `<th ${sortable} ${sortClass}>${h.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';
        (inventory || []).forEach(item => {
            const onHand = item.on_hand || 0;
            const price = item.price || 0;
            const cost = item.cost || 0;
            const retailValue = onHand * price;
            const invValue = onHand * cost;
            tableHtml += `
                <tr class="clickable-row" onclick="openDetailsModal('${item.barcode}', '${item.product_title}')">
                    <td><img src="${item.image_url || 'https://via.placeholder.com/40'}" alt="${item.product_title}" style="width: 40px; border-radius: 4px;"></td>
                    <td>${item.product_title}<br><small>${item.variant_title}</small></td>
                    <td>${item.store_name || ''}</td>
                    <td>${item.sku || ''}</td>
                    <td>${item.barcode || ''}</td>
                    <td>${item.type || ''}</td>
                    <td>${item.category || ''}</td>
                    <td><span class="status-${(item.status || '').toLowerCase()}">${item.status}</span></td>
                    <td>${price.toFixed(2)}</td>
                    <td>${cost.toFixed(2)}</td>
                    <td>${onHand}</td>
                    <td>${item.committed}</td>
                    <td>${item.available}</td>
                    <td>${retailValue.toFixed(2)}</td>
                    <td>${invValue.toFixed(2)}</td>
                </tr>`;
        });
        tableHtml += '</tbody></table></div>';
        elements.tableContainer.innerHTML = tableHtml;
        addSortEventListeners();
    };

    const renderGroupedView = (inventory) => {
        if (!inventory || inventory.length === 0) {
            elements.tableContainer.innerHTML = '<p>No inventory groups found.</p>'; return;
        }
        let html = '';
        inventory.forEach(group => {
            html += `
            <details class="grouped-item">
                <summary class="clickable-row" onclick="openDetailsModal('${group.barcode}', '${group.primary_title}')">
                    <div class="grid">
                        <img src="${group.primary_image_url || 'https://via.placeholder.com/60'}" alt="${group.primary_title}">
                        <div class="product-info">
                            <strong>${group.primary_title}</strong>
                            <small>Barcode: ${group.barcode} &nbsp;|&nbsp; Store: ${group.primary_store}</small>
                        </div>
                        <div class="quantity-display"><h2>${group.on_hand}</h2><p>On Hand</p></div>
                        <div class="quantity-display"><h2>${group.committed}</h2><p>Committed</p></div>
                        <div class="quantity-display"><h2>${group.available}</h2><p>Available</p></div>
                    </div>
                </summary>
                <div class="variant-details">
                    <table>
                        <thead><tr><th>SKU</th><th>Store</th><th>Status</th><th>Action</th></tr></thead>
                        <tbody>
                            ${group.variants_json.map(v => `
                                <tr>
                                    <td>${v.sku}</td>
                                    <td>${v.store_name}</td>
                                    <td><span class="status-${(v.status || '').toLowerCase()}">${v.status}</span></td>
                                    <td>${!v.is_primary ? `<button class="outline" onclick="setPrimaryVariant('${group.barcode}', ${v.variant_id})">Make Primary</button>` : '<strong>Primary</strong>'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </details>`;
        });
        elements.tableContainer.innerHTML = html;
    };
    
    // --- NEW: Functions for the details modal ---
    window.openDetailsModal = async (barcode, title) => {
        if (!barcode) return;
        elements.detailsModal.title.textContent = `Details for: ${title} (${barcode})`;
        elements.detailsModal.content.setAttribute('aria-busy', 'true');
        elements.detailsModal.content.innerHTML = '<p>Loading details...</p>';
        elements.detailsModal.dialog.showModal();

        try {
            const response = await fetch(`/api/v2/inventory/product-details/${barcode}`);
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || 'Failed to load details.');
            }
            const data = await response.json();
            renderDetailsModal(data);
        } catch (error) {
            elements.detailsModal.content.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            elements.detailsModal.content.removeAttribute('aria-busy');
        }
    };

    const renderDetailsModal = (data) => {
        let html = '<div class="grid">';

        // Committed Orders Section
        html += `<article>
                    <header><strong>Committed Orders (${data.committed_orders.length})</strong></header>
                    <div class="overflow-auto" style="max-height: 200px;">
                    <table>
                        <thead><tr><th>Order</th><th>Date</th><th>Qty</th><th>Status</th><th>Action</th></tr></thead>
                        <tbody>
                            ${data.committed_orders.map(o => `
                                <tr>
                                    <td>${o.name}</td>
                                    <td>${new Date(o.created_at).toLocaleDateString()}</td>
                                    <td>${o.quantity}</td>
                                    <td><span class="status-${(o.fulfillment_status || '').toLowerCase()}">${o.fulfillment_status}</span></td>
                                    <td><a href="https://${o.shopify_url}/admin/orders/${o.id}" target="_blank" class="outline" role="button">View on Shopify</a></td>
                                </tr>
                            `).join('') || '<tr><td colspan="5">No committed orders.</td></tr>'}
                        </tbody>
                    </table>
                    </div>
                 </article>`;

        // Stock Movements Section
        html += `<article>
                    <header><strong>Stock History (Last 100)</strong></header>
                    <div class="overflow-auto" style="max-height: 200px;">
                    <table>
                        <thead><tr><th>Date</th><th>SKU</th><th>Change</th><th>New Qty</th><th>Reason</th></tr></thead>
                        <tbody>
                            ${data.stock_movements.map(m => `
                                <tr>
                                    <td>${new Date(m.created_at).toLocaleString()}</td>
                                    <td>${m.product_sku}</td>
                                    <td><strong>${m.change_quantity > 0 ? '+' : ''}${m.change_quantity}</strong></td>
                                    <td>${m.new_quantity}</td>
                                    <td><small>${m.reason || 'N/A'}<br>${m.source_info || ''}</small></td>
                                </tr>
                            `).join('') || '<tr><td colspan="5">No stock movements found.</td></tr>'}
                        </tbody>
                    </table>
                    </div>
                 </article>`;

        html += '</div>'; // close grid
        
        // All Orders Section
        html += `<article>
                    <header><strong>Order History (Last 100)</strong></header>
                    <div class="overflow-auto" style="max-height: 300px;">
                    <table>
                        <thead><tr><th>Order</th><th>Date</th><th>Qty</th><th>Financial Status</th><th>Fulfillment</th></tr></thead>
                        <tbody>
                            ${data.all_orders.map(o => `
                                <tr>
                                    <td>${o.name}</td>
                                    <td>${new Date(o.created_at).toLocaleDateString()}</td>
                                    <td>${o.quantity}</td>
                                    <td><span class="status-${(o.financial_status || '').toLowerCase()}">${o.financial_status}</span></td>
                                    <td><span class="status-${(o.fulfillment_status || '').toLowerCase()}">${o.fulfillment_status}</span></td>
                                </tr>
                            `).join('') || '<tr><td colspan="5">No historical orders found.</td></tr>'}
                        </tbody>
                    </table>
                    </div>
                 </article>`;

        elements.detailsModal.content.innerHTML = html;
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
    
    window.setPrimaryVariant = async (barcode, variantId) => {
        event.stopPropagation(); // prevent modal from opening
        try {
            const response = await fetch('/api/v2/inventory/set-primary-variant/', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ barcode, variant_id: variantId })
            });
            if (!response.ok) throw new Error('Failed to set primary variant.');
            await fetchInventory();
        } catch (error) {
            console.error(error);
            alert('Error setting primary variant.');
        }
    };

    const initialize = async () => {
        const params = new URLSearchParams(window.location.search);
        state = {
            page: parseInt(params.get('page') || '1', 10),
            sortBy: params.get('sortBy') || 'on_hand',
            sortOrder: params.get('sortOrder') || 'desc',
            view: params.get('view') || 'individual',
            filters: { search: params.get('search') || '', store_ids: params.get('store_ids') || '', product_type: params.get('product_type') || '', category: params.get('category') || '', status: params.get('status') || '', min_retail: params.get('min_retail') || '', max_retail: params.get('max_retail') || '', min_inventory: params.get('min_inventory') || '', max_inventory: params.get('max_inventory') || '', }
        };

        try {
            const [filterResp, storeResp] = await Promise.all([ fetch('/api/v2/inventory/filters/'), fetch('/api/config/stores') ]);
            const filterData = await filterResp.json();
            const storeData = await storeResp.json();
            filterData.types.forEach(t => elements.filters.type.add(new Option(t, t)));
            filterData.categories.forEach(c => elements.filters.category.add(new Option(c, c)));
            storeData.forEach(s => elements.filters.store.add(new Option(s.name, s.id)));
        } catch (error) { console.error("Could not load filter options:", error); }

        Object.entries(state.filters).forEach(([key, value]) => {
            const elKeyMap = {store_ids: 'store', product_type: 'type', category: 'category', min_retail: 'minRetail', max_retail: 'maxRetail', min_inventory: 'minInv', max_inventory: 'maxInv'};
            const elKey = elKeyMap[key] || key;
            if (elements.filters[elKey]) elements.filters[elKey].value = value;
        });
        elements.filters.groupToggle.checked = state.view === 'grouped';

        const setupEventListeners = () => {
            const filterMap = { search: 'search', store: 'store_ids', type: 'product_type', category: 'category', status: 'status', minRetail: 'min_retail', maxRetail: 'max_retail', minInv: 'min_inventory', maxInv: 'max_inventory' };
            for (const [elKey, filterKey] of Object.entries(filterMap)) {
                elements.filters[elKey].addEventListener('input', () => {
                    state.filters[filterKey] = elements.filters[elKey].value;
                    state.page = 1;
                    fetchInventory();
                });
            }
            elements.filters.groupToggle.addEventListener('change', (e) => {
                state.view = e.target.checked ? 'grouped' : 'individual';
                state.page = 1;
                fetchInventory();
            });
            elements.filters.reset.addEventListener('click', () => {
                window.history.replaceState({}, '', window.location.pathname);
                initialize();
            });

            elements.tableContainer.addEventListener('click', (event) => {
                if (event.target.tagName === 'DETAILS' && event.target.open) {
                    elements.tableContainer.querySelectorAll('details[open]').forEach((details) => {
                        if (details !== event.target) {
                            details.removeAttribute('open');
                        }
                    });
                }
            }, true);
            
            // NEW: Close modal listener
            elements.detailsModal.closeBtn.addEventListener('click', () => {
                elements.detailsModal.dialog.close();
            });
        };
        setupEventListeners();
        
        elements.pagination.prev.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchInventory(); }});
        elements.pagination.next.addEventListener('click', () => { if ((state.page * 50) < state.totalCount) { state.page++; fetchInventory(); }});

        await fetchInventory();
    };

    initialize();
});