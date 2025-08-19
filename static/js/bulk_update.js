// static/js/bulk_update.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        container: document.getElementById('bulk-update-container'),
        saveBtn: document.getElementById('save-changes-btn'),
        searchInput: document.getElementById('search-input'),
        storeFilterList: document.getElementById('store-filter-list'),
        typeFilterList: document.getElementById('type-filter-list'),
        // ADDED: Element reference for the new status filter
        statusFilterList: document.getElementById('status-filter-list'),
        noBarcodeFilter: document.getElementById('no-barcode-filter'),
        groupToggle: document.getElementById('group-toggle'),
        toast: document.getElementById('toast'),
        generateUniqueBtn: document.getElementById('generate-unique-barcode-btn'),
        generateSameBtn: document.getElementById('generate-same-barcode-btn'),
        excelFileInput: document.getElementById('excel-file-input'),
        importExcelBtn: document.getElementById('import-excel-btn'),
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

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    // --- Data Fetching and Population ---
    const loadAllVariants = async () => {
        try {
            elements.container.setAttribute('aria-busy', 'true');
            const params = new URLSearchParams();
            const search = elements.searchInput.value;
            const store_ids = Array.from(elements.storeFilterList.querySelectorAll('input:checked')).map(cb => cb.value);
            const product_types = Array.from(elements.typeFilterList.querySelectorAll('input:checked')).map(cb => cb.value);
            // MODIFIED: Read values from the new status filter
            const statuses = Array.from(elements.statusFilterList.querySelectorAll('input:checked')).map(cb => cb.value);
            const has_no_barcode = elements.noBarcodeFilter.checked;

            if (search) params.set('search', search);
            if (has_no_barcode) params.set('has_no_barcode', true);
            store_ids.forEach(id => params.append('store_ids', id));
            product_types.forEach(type => params.append('product_types', type));
            // MODIFIED: Append statuses to the API request
            statuses.forEach(status => params.append('statuses', status));
            
            const response = await fetch(`${API_ENDPOINTS.getAllVariantsForBulkEdit}?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch product variants.');
            allVariants = await response.json();
            
            if (elements.storeFilterList.children.length <= 1) {
                populateFilters(allVariants);
            }
            render();
        } catch (error) {
            elements.container.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };
    
    const populateFilters = (variants) => {
        const stores = [...new Map(variants.map(v => [v.store_id, v.store_name])).entries()];
        const types = [...new Set(variants.map(v => v.product_type).filter(Boolean))];
        
        elements.storeFilterList.innerHTML = '';
        stores.sort((a,b) => a[1].localeCompare(b[1])).forEach(([id, name]) => {
            elements.storeFilterList.innerHTML += `<li><label><input type="checkbox" name="store" value="${id}"> ${name}</label></li>`;
        });

        elements.typeFilterList.innerHTML = '';
        types.sort().forEach(t => {
            elements.typeFilterList.innerHTML += `<li><label><input type="checkbox" name="type" value="${t}"> ${t}</label></li>`;
        });
    };

    // --- Rendering Logic ---
    const render = () => {
        let variantsToRender = [...allVariants];

        if (currentView === 'individual') {
            variantsToRender.sort((a, b) => {
                const valA = a[sortState.key];
                const valB = b[sortState.key];
                if (valA === null || valA === undefined) return 1;
                if (valB === null || valB === undefined) return -1;

                if (typeof valA === 'string') {
                    return sortState.order === 'asc' 
                        ? valA.localeCompare(valB) 
                        : valB.localeCompare(valA);
                } else {
                    return sortState.order === 'asc' ? valA - valB : valB - valA;
                }
            });
        }

        if (currentView === 'grouped') {
            renderGroupedView(variantsToRender);
        } else {
            renderIndividualView(variantsToRender);
        }
    };

    const renderIndividualView = (variantsToRender) => {
        const tableHeaders = [
            { key: 'product_title', label: 'Product' },
            { key: 'barcode', label: 'Barcode', sortable: true },
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
            <th data-sort-key="sku">SKU</th>
            ${tableHeaders.map(h => `<th ${h.sortable ? `data-sort-key="${h.key}"` : ''}>${h.label}</th>`).join('')}
            </tr>`;
        
        tableHtml += `<tr id="bulk-apply-row">
                        <th></th><th></th><th></th><th></th>
                        ${tableHeaders.map(h => `<td><input type="${h.key.includes('price') || h.key.includes('cost') || h.key.includes('Hand') || h.key.includes('available') ? 'number' : 'text'}" placeholder="Apply..." data-bulk-apply-for="${h.key}"></td>`).join('')}
                      </tr></thead><tbody>`;

        if (variantsToRender.length === 0) {
            tableHtml += '<tr><td colspan="12">No products match the current filters.</td></tr>';
        } else {
            variantsToRender.forEach(v => {
                const imageCell = v.image_url 
                    ? `<td><img src="${v.image_url}" alt="${v.product_title}"></td>` 
                    : '<td></td>';

                tableHtml += `<tr data-variant-id="${v.variant_id}" data-store-id="${v.store_id}">
                    <td><input type="checkbox" class="row-checkbox"></td>
                    ${imageCell}
                    <td>${v.store_name}</td>
                    <td>${v.sku || ''}</td>
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
                const imageTag = group.primary_image_url 
                    ? `<img src="${group.primary_image_url}" alt="${group.primary_title}">` 
                    : '';

                html += `
                <details class="grouped-item">
                    <summary>
                        <div class="grid">
                            ${imageTag}
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
    
    const handleGenerateBarcodes = async (mode) => {
        const selectedRows = Array.from(document.querySelectorAll('.row-checkbox:checked')).map(cb => cb.closest('tr'));
        if (selectedRows.length === 0) {
            showToast('No products selected.', 'error');
            return;
        }

        const variantIds = selectedRows.map(row => parseInt(row.dataset.variantId, 10));

        try {
            const response = await fetch(API_ENDPOINTS.generateBarcodes, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ variant_ids: variantIds, mode: mode }),
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || 'Failed to generate barcodes.');
            }

            selectedRows.forEach(row => {
                const variantId = row.dataset.variantId;
                if (result[variantId]) {
                    const barcodeInput = row.querySelector('input[data-field-key="barcode"]');
                    if (barcodeInput) {
                        barcodeInput.value = result[variantId];
                        barcodeInput.dispatchEvent(new Event('input'));
                    }
                }
            });
            showToast('Barcodes generated successfully. Click "Save Changes" to apply.', 'success');

        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        }
    };

    const handleImportExcel = async () => {
        const file = elements.excelFileInput.files[0];
        if (!file) {
            showToast('Please select an Excel file first.', 'error');
            return;
        }

        elements.importExcelBtn.setAttribute('aria-busy', 'true');
        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch(API_ENDPOINTS.uploadExcel, {
                method: 'POST',
                body: formData,
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail?.message || result.detail || 'An unknown error occurred during import.');
            }
            showToast(result.message || 'Excel import processed successfully.', 'success');
            loadAllVariants();
        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            elements.importExcelBtn.removeAttribute('aria-busy');
            elements.excelFileInput.value = '';
        }
    };

    // --- Initial Setup ---
    elements.saveBtn.addEventListener('click', handleSaveChanges);
    elements.searchInput.addEventListener('input', debounce(loadAllVariants, 400));
    elements.storeFilterList.addEventListener('change', loadAllVariants);
    elements.typeFilterList.addEventListener('change', loadAllVariants);
    // ADDED: Event listener for the new status filter
    elements.statusFilterList.addEventListener('change', loadAllVariants);
    elements.noBarcodeFilter.addEventListener('change', loadAllVariants);

    elements.groupToggle.addEventListener('change', (e) => {
        currentView = e.target.checked ? 'grouped' : 'individual';
        elements.saveBtn.disabled = currentView === 'grouped';
        render();
    });
    
    elements.generateUniqueBtn.addEventListener('click', (e) => {
        e.preventDefault();
        handleGenerateBarcodes('unique');
    });
    elements.generateSameBtn.addEventListener('click', (e) => {
        e.preventDefault();
        handleGenerateBarcodes('same');
    });
    
    elements.importExcelBtn.addEventListener('click', handleImportExcel);
    
    loadAllVariants();
});