// static/js/inventory.js

document.addEventListener('DOMContentLoaded', () => {
    const contentEl = document.getElementById('inventory-content');
    const searchInput = document.getElementById('searchInput');
    const groupToggle = document.getElementById('groupToggle');
    const prevButton = document.getElementById('prevButton');
    const nextButton = document.getElementById('nextButton');
    const pageIndicator = document.getElementById('pageIndicator');
    const totalsContainer = document.getElementById('totals-container');
    const totalRetailEl = document.getElementById('totalRetailValue');
    const totalInventoryEl = document.getElementById('totalInventoryValue');
    const adjustmentModal = document.getElementById('adjustmentModal');
    
    const state = {
        page: 1, limit: 50, search: '',
        view: 'grouped', sortBy: 'on_hand', sortOrder: 'desc',
        totalCount: 0
    };

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };
    
    const loadInventory = async () => {
        const skip = (state.page - 1) * state.limit;
        contentEl.innerHTML = '<progress></progress>';
        
        const params = new URLSearchParams({
            skip: skip, limit: state.limit, view: state.view,
            sort_by: state.sortBy, sort_order: state.sortOrder
        });
        if (state.search) params.append('search', state.search);

        try {
            const response = await fetch(API_ENDPOINTS.getGroupedInventory(params));
            if (!response.ok) throw new Error('Failed to load inventory.');
            const data = await response.json();
            
            state.totalCount = data.total_count;
            renderData(data.inventory);
            
            if (state.view === 'individual' && data.total_retail_value !== undefined) {
                totalsContainer.style.display = 'block';
                totalRetailEl.textContent = `Total Retail Value: ${data.total_retail_value.toFixed(2)}`;
                totalInventoryEl.textContent = `Total Inventory Value: ${data.total_inventory_value.toFixed(2)}`;
            } else {
                totalsContainer.style.display = 'none';
            }

            updatePagination();
        } catch (error) {
            contentEl.innerHTML = `<p>Error: ${error.message}</p>`;
        }
    };

    const renderData = (inventory) => {
        if (state.view === 'grouped') {
            renderGroupedView(inventory);
        } else {
            renderIndividualView(inventory);
        }
        addSortEventListeners();
    };

    const renderGroupedView = (inventory) => {
        if (inventory.length === 0) {
            contentEl.innerHTML = '<p>No inventory groups found.</p>';
            return;
        }
        let html = '';
        inventory.forEach(group => {
            html += `
                <article class="product-card">
                    <img src="${group.image_url || 'https://via.placeholder.com/80'}" alt="${group.title}">
                    
                    <div>
                        <strong>${group.title}</strong><br>
                        <small>Barcode: ${group.barcode}</small><br>
                        <small>Category: ${group.category || 'N/A'}</small> | <small>Type: ${group.type || 'N/A'}</small> | <small>Status: ${group.status}</small>
                        <details>
                            <summary>${group.variants.length} SKU(s)</summary>
                            <ul class="variant-list">
                                ${group.variants.map(v => `
                                    <li>
                                        <span>${v.sku} (${v.title})</span>
                                        <button class="secondary outline" onclick="setPrimaryVariant('${group.barcode}', ${v.variant_id})">Make Primary</button>
                                    </li>`).join('')}
                            </ul>
                        </details>
                    </div>
                    <div class="quantity-display"><h2>${group.on_hand}</h2><p>On Hand</p></div>
                    <div class="quantity-display"><h2>${group.committed}</h2><p>Committed</p></div>
                    <div class="quantity-display"><h2>${group.available}</h2><p>Available</p></div>
                     <div>
                        <button onclick="openAdjustmentModal(this)" data-barcode="${group.barcode}" data-title="${group.title}" data-on-hand="${group.on_hand}">Adjust Stock</button>
                    </div>
                </article>`;
        });
        contentEl.innerHTML = html;
    };
    
    const renderIndividualView = (inventory) => {
        if (inventory.length === 0) {
            contentEl.innerHTML = '<p>No products found.</p>';
            return;
        }
        const table = document.createElement('table');
        table.innerHTML = `
            <thead>
                <tr>
                    <th>Image</th>
                    <th>Product / Variant</th>
                    <th>SKU</th>
                    <th>Barcode</th>
                    <th>Type</th>
                    <th>Category</th>
                    <th>Status</th>
                    <th data-sort-by="price">Price</th>
                    <th data-sort-by="cost">Cost</th>
                    <th data-sort-by="on_hand">On Hand</th>
                    <th data-sort-by="committed">Committed</th>
                    <th data-sort-by="available">Available</th>
                    <th data-sort-by="retail_value">Retail Value</th>
                    <th data-sort-by="inventory_value">Inv. Value</th>
                </tr>
            </thead>
            <tbody>
                ${inventory.map(item => `
                    <tr>
                        <td><img src="${item.image_url || 'https://via.placeholder.com/40'}" alt="${item.product_title}"></td>
                        <td>${item.product_title}<br><small>${item.title}</small></td>
                        <td>${item.sku || 'N/A'}</td>
                        <td>${item.barcode || 'N/A'}</td>
                        <td>${item.type || 'N/A'}</td>
                        <td>${item.category || 'N/A'}</td>
                        <td>${item.status || 'N/A'}</td>
                        <td>${item.price.toFixed(2)}</td>
                        <td>${(item.cost || 0).toFixed(2)}</td>
                        <td>${item.on_hand}</td>
                        <td>${item.committed}</td>
                        <td>${item.available}</td>
                        <td>${item.retail_value.toFixed(2)}</td>
                        <td>${item.inventory_value.toFixed(2)}</td>
                    </tr>
                `).join('')}
            </tbody>`;
        contentEl.innerHTML = '';
        contentEl.appendChild(table);
    };

    const updatePagination = () => {
        pageIndicator.textContent = `Page ${state.page}`;
        prevButton.disabled = state.page === 1;
        nextButton.disabled = (state.page * state.limit) >= state.totalCount;
    };
    
    const addSortEventListeners = () => {
        document.querySelectorAll('th[data-sort-by]').forEach(th => {
            th.classList.remove('asc', 'desc');
            if (th.dataset.sortBy === state.sortBy) {
                th.classList.add(state.sortOrder);
            }
            th.onclick = () => {
                const newSortBy = th.dataset.sortBy;
                if (state.sortBy === newSortBy) {
                    state.sortOrder = state.sortOrder === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sortBy = newSortBy;
                    state.sortOrder = 'desc';
                }
                state.page = 1;
                loadInventory();
            };
        });
    };

    window.setPrimaryVariant = async (barcode, variantId) => {
        try {
            const response = await fetch(API_ENDPOINTS.setPrimaryVariant, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ barcode: barcode, variant_id: variantId })
            });
            if (!response.ok) throw new Error('Failed to set primary variant.');
            await loadInventory();
        } catch (error) {
            alert(`Error: ${error.message}`);
        }
    };

    // Modal Logic
    const toggleModal = (e) => {
        const modalId = e.currentTarget.dataset.target;
        const modal = document.getElementById(modalId);
        if (modal) modal.showModal();
    };

    window.openAdjustmentModal = (button) => {
        document.getElementById('modalProductTitle').textContent = button.dataset.title;
        document.getElementById('modalBarcode').textContent = button.dataset.barcode;
        document.getElementById('modalCurrentOnHand').textContent = button.dataset.onHand;
        document.getElementById('modalHiddenBarcode').value = button.dataset.barcode;
        document.getElementById('adjustmentValue').value = '';
        document.getElementById('adjustmentReason').value = '';
        adjustmentModal.showModal();
    };

    window.submitAdjustment = async (action) => {
        const barcode = document.getElementById('modalHiddenBarcode').value;
        const quantityInput = document.getElementById('adjustmentValue');
        const reason = document.getElementById('adjustmentReason').value;
        
        if (!quantityInput.value) {
            alert('Please enter a quantity.');
            return;
        }

        const payload = {
            barcode: barcode,
            quantity: parseInt(quantityInput.value, 10),
            reason: reason || 'Manual adjustment',
            source_info: 'Grouped Inventory Page'
        };
        
        const endpointMap = {
            set: API_ENDPOINTS.setInventoryQuantity,
            add: API_ENDPOINTS.addInventoryQuantity,
            subtract: API_ENDPOINTS.subtractInventoryQuantity
        };

        try {
            const response = await fetch(endpointMap[action], {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Adjustment failed.');
            showToast(result.message || 'Inventory updated!', 'success');
            adjustmentModal.close();
            await loadInventory();
        } catch(error) {
            alert(`Error: ${error.message}`);
        }
    };
    
    // Event Listeners
    searchInput.addEventListener('input', debounce(() => {
        state.search = searchInput.value;
        state.page = 1;
        loadInventory();
    }, 500));

    groupToggle.addEventListener('change', () => {
        state.view = groupToggle.checked ? 'grouped' : 'individual';
        state.page = 1;
        loadInventory();
    });
    
    prevButton.addEventListener('click', () => {
        if (state.page > 1) {
            state.page--;
            loadInventory();
        }
    });

    nextButton.addEventListener('click', () => {
        if ((state.page * state.limit) < state.totalCount) {
            state.page++;
            loadInventory();
        }
    });

    loadInventory();
});