// static/js/bulk_update.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        container: document.getElementById('bulk-update-container'),
        saveBtn: document.getElementById('save-changes-btn'),
        searchInput: document.getElementById('search-input'),
        storeFilter: document.getElementById('store-filter'),
        typeFilter: document.getElementById('type-filter'),
        groupToggle: document.getElementById('group-toggle'),
        toast: document.getElementById('toast'),
    };

    let allVariants = [];
    let currentView = 'individual';
    let sortState = { key: 'product_title', order: 'asc' };

    // --- Utility Functions ---
    const showToast = (message, type = 'info', duration = 5000) => {
        elements.toast.textContent = message;
        elements.toast.className = `show ${type}`;
        setTimeout(() => { elements.toast.className = ''; }, duration);
    };

    // --- Data Fetching and Population ---
    const loadAllVariants = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getAllVariantsForBulkEdit);
            if (!response.ok) throw new Error('Failed to fetch product variants.');
            allVariants = await response.json();
            populateFilters();
            render();
        } catch (error) {
            elements.container.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const populateFilters = () => {
        const stores = [...new Set(allVariants.map(v => v.store_name))];
        const types = [...new Set(allVariants.map(v => v.product_type).filter(Boolean))];
        
        elements.storeFilter.innerHTML = '<option value="">All Stores</option>';
        stores.sort().forEach(s => elements.storeFilter.add(new Option(s, s)));

        elements.typeFilter.innerHTML = '<option value="">All Types</option>';
        types.sort().forEach(t => elements.typeFilter.add(new Option(t, t)));
    };

    // --- Rendering Logic ---
    const render = () => {
        const searchTerm = elements.searchInput.value.toLowerCase();
        const selectedStore = elements.storeFilter.value;
        const selectedType = elements.typeFilter.value;

        let filteredVariants = allVariants.filter(v => {
            // MODIFIED: Added SKU to the search filter
            const matchesSearch = !searchTerm || 
                v.product_title.toLowerCase().includes(searchTerm) || 
                (v.sku && v.sku.toLowerCase().includes(searchTerm)) ||
                (v.barcode && v.barcode.toLowerCase().includes(searchTerm));
            const matchesStore = !selectedStore || v.store_name === selectedStore;
            const matchesType = !selectedType || v.product_type === selectedType;
            return matchesSearch && matchesStore && matchesType;
        });

        if (currentView === 'individual') {
            filteredVariants.sort((a, b) => {
                const valA = a[sortState.key];
                const valB = b[sortState.key];
                if (valA === null || valA === undefined) return 1;
                if (valB === null || valB === undefined) return -1;
                if (typeof valA === 'string') {
                    return sortState.order === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
                } else {
                    return sortState.order === 'asc' ? valA - valB : valB - valA;
                }
            });
        }

        if (currentView === 'grouped') {
            renderGroupedView(filteredVariants);
        } else {
            renderIndividualView(filteredVariants);
        }
    };

    const renderIndividualView = (variantsToRender) => {
        // MODIFIED: Added SKU to the table headers
        const tableHeaders = [
            { key: 'sku', label: 'SKU' },
            { key: 'product_title', label: 'Product' },
            { key: 'barcode', label: 'Barcode' },
            { key: 'product_type', label: 'Type' },
            { key: 'price', label: 'Price' },
            { key: 'cost', label: 'Cost' },
            { key: 'onHand', label: 'On Hand' },
            { key: 'available', label: 'Available' },
        ];

        let tableHtml = `<table class="bulk-update-table"><thead><tr>
            <th><input type="checkbox" id="select-all-checkbox"></th>
            <th>Image</th>
            <th data-sort-key="store_name">Store</th>
            ${tableHeaders.map(h => `<th data-sort-key="${h.key}">${h.label}</th>`).join('')}
            </tr>`;
        
        tableHtml += `<tr id="bulk-apply-row">
                        <th></th><th></th><th></th>
                        ${tableHeaders.map(h => `<td><input type="${h.type || 'text'}" placeholder="Apply..." data-bulk-apply-for="${h.key}"></td>`).join('')}
                      </tr></thead><tbody>`;

        if (variantsToRender.length === 0) {
            tableHtml += '<tr><td colspan="11">No products match the current filters.</td></tr>';
        } else {
            variantsToRender.forEach(v => {
                tableHtml += `<tr data-variant-id="${v.variant_id}" data-store-id="${v.store_id}">
                    <td><input type="checkbox" class="row-checkbox"></td>
                    <td><img src="${v.image_url || 'https://via.placeholder.com/40'}" alt="${v.product_title}"></td>
                    <td>${v.store_name}</td>
                    ${tableHeaders.map(h => `<td><input data-field-key="${h.key}" value="${v[h.key] !== null && v[h.key] !== undefined ? v[h.key] : ''}" data-original-value="${v[h.key] !== null && v[h.key] !== undefined ? v[h.key] : ''}"></td>`).join('')}
                </tr>`;
            });
        }
        tableHtml += '</tbody></table>';
        elements.container.innerHTML = tableHtml;
        addIndividualViewEventListeners();
    };
    
    const renderGroupedView = (variantsToRender) => {
        const groups = {};
        variantsToRender.forEach(v => {
            if (v.barcode) {
                if (!groups[v.barcode]) {
                    groups[v.barcode] = {
                        variants: [],
                        primary_image_url: v.image_url,
                        primary_title: v.product_title,
                        total_on_hand: 0,
                        total_available: 0
                    };
                }
                groups[v.barcode].variants.push(v);
                groups[v.barcode].total_on_hand += v.onHand || 0;
                groups[v.barcode].total_available += v.available || 0;
            }
        });

        let html = '';
        if (Object.keys(groups).length === 0) {
            html = '<p>No products with barcodes match the current filters.</p>';
        } else {
            for (const barcode in groups) {
                const group = groups[barcode];
                html += `
                <details class="grouped-item">
                    <summary>
                        <div class="grid">
                            <img src="${group.primary_image_url || 'https://via.placeholder.com/50'}" alt="${group.primary_title}">
                            <div class="product-info">
                                <strong>${group.primary_title}</strong>
                                <small>Barcode: ${barcode}</small>
                            </div>
                            <div class="quantity-display"><h2>${group.total_on_hand}</h2><p>Total On Hand</p></div>
                            <div class="quantity-display"><h2>${group.total_available}</h2><p>Total Available</p></div>
                        </div>
                    </summary>
                    <div class="variant-details">
                        <table>
                            <thead><tr><th>Store</th><th>SKU</th><th>Price</th><th>Cost</th><th>On Hand</th><th>Available</th></tr></thead>
                            <tbody>
                                ${group.variants.map(v => `
                                    <tr>
                                        <td>${v.store_name}</td>
                                        <td>${v.sku || ''}</td>
                                        <td>${v.price || 'N/A'}</td>
                                        <td>${v.cost || 'N/A'}</td>
                                        <td>${v.onHand || 0}</td>
                                        <td>${v.available || 0}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </details>`;
            }
        }
        elements.container.innerHTML = html;
    };

    // --- Event Handling ---
    const addIndividualViewEventListeners = () => {
        const currentTh = document.querySelector(`th[data-sort-key="${sortState.key}"]`);
        if (currentTh) currentTh.classList.add(sortState.order);

        document.querySelectorAll('th[data-sort-key]').forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sortKey;
                if (sortState.key === key) {
                    sortState.order = sortState.order === 'asc' ? 'desc' : 'asc';
                } else {
                    sortState.key = key;
                    sortState.order = 'asc';
                }
                render();
            });
        });

        elements.container.querySelectorAll('input[data-field-key]').forEach(input => {
            input.addEventListener('input', () => {
                const isChanged = input.value !== input.dataset.originalValue;
                input.classList.toggle('changed', isChanged);
            });
        });

        document.getElementById('select-all-checkbox').addEventListener('change', (e) => {
            document.querySelectorAll('.row-checkbox').forEach(cb => cb.checked = e.target.checked);
        });

        document.querySelectorAll('input[data-bulk-apply-for]').forEach(bulkInput => {
            bulkInput.addEventListener('change', () => {
                if (bulkInput.value === '') return;
                const fieldKey = bulkInput.dataset.bulkApplyFor;
                document.querySelectorAll('.row-checkbox:checked').forEach(checkbox => {
                    const row = checkbox.closest('tr');
                    const targetInput = row.querySelector(`input[data-field-key="${fieldKey}"]`);
                    if (targetInput) {
                        targetInput.value = bulkInput.value;
                        targetInput.dispatchEvent(new Event('input'));
                    }
                });
                bulkInput.value = '';
            });
        });
    };
    
    const handleSaveChanges = async () => {
        elements.saveBtn.setAttribute('aria-busy', 'true');
        const selectedRows = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.closest('tr'));

        if (selectedRows.length === 0) {
            showToast('No products selected.', 'error');
            elements.saveBtn.removeAttribute('aria-busy');
            return;
        }

        const payload = { updates: [] };
        selectedRows.forEach(row => {
            const changedInputs = row.querySelectorAll('input.changed');
            if (changedInputs.length > 0) {
                const update = {
                    variant_id: parseInt(row.dataset.variantId, 10),
                    store_id: parseInt(row.dataset.storeId, 10),
                    changes: {}
                };
                changedInputs.forEach(input => {
                    update.changes[input.dataset.fieldKey] = input.value;
                });
                payload.updates.push(update);
            }
        });

        if (payload.updates.length === 0) {
            showToast('No changes to save for the selected products.', 'info');
            elements.saveBtn.removeAttribute('aria-busy');
            return;
        }

        try {
            const response = await fetch(API_ENDPOINTS.processBulkUpdates, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail?.message || 'An unknown error occurred.');
            showToast(result.message, 'success');
            loadAllVariants();
        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            elements.saveBtn.removeAttribute('aria-busy');
        }
    };

    // --- Initial Setup ---
    elements.saveBtn.addEventListener('click', handleSaveChanges);
    elements.searchInput.addEventListener('input', render);
    elements.storeFilter.addEventListener('change', render);
    elements.typeFilter.addEventListener('change', render);
    elements.groupToggle.addEventListener('change', (e) => {
        currentView = e.target.checked ? 'grouped' : 'individual';
        elements.saveBtn.disabled = currentView === 'grouped';
        render();
    });
    
    loadAllVariants();
});