// static/js/bulk_update.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const elements = {
        container: document.getElementById('bulk-update-container'),
        saveBtn: document.getElementById('save-changes-btn'),
        searchInput: document.getElementById('search-input'),
        toast: document.getElementById('toast'),
    };

    let allVariants = []; // To store the master list of variants

    // --- Utility Functions ---
    const showToast = (message, type = 'info', duration = 5000) => {
        elements.toast.textContent = message;
        elements.toast.className = `show ${type}`;
        setTimeout(() => { elements.toast.className = ''; }, duration);
    };

    // --- Data Fetching and Rendering ---
    const loadAllVariants = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getAllVariantsForBulkEdit);
            if (!response.ok) throw new Error('Failed to fetch product variants.');
            allVariants = await response.json();
            renderTable(allVariants);
        } catch (error) {
            elements.container.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const renderTable = (variantsToRender) => {
        const tableHeaders = [
            { key: 'product_title', label: 'Product', type: 'text' },
            { key: 'sku', label: 'SKU', type: 'text' },
            { key: 'barcode', label: 'Barcode', type: 'text' },
            { key: 'product_type', label: 'Type', type: 'text' },
            { key: 'price', label: 'Price', type: 'number' },
            { key: 'cost', label: 'Cost', type: 'number' },
            { key: 'onHand', label: 'On Hand', type: 'number' },
            { key: 'available', label: 'Available', type: 'number' },
        ];

        let tableHtml = `
            <table>
                <thead>
                    <tr>
                        <th><input type="checkbox" id="select-all-checkbox"></th>
                        <th>Store</th>
                        ${tableHeaders.map(h => `<th>${h.label}</th>`).join('')}
                    </tr>
                    <tr id="bulk-apply-row">
                        <th></th>
                        <th></th>
                        ${tableHeaders.map(h => `
                            <td>
                                <input type="${h.type}" placeholder="Apply to all selected..." data-bulk-apply-for="${h.key}">
                            </td>
                        `).join('')}
                    </tr>
                </thead>
                <tbody>
        `;

        if (variantsToRender.length === 0) {
            tableHtml += '<tr><td colspan="10">No products found.</td></tr>';
        } else {
            variantsToRender.forEach(v => {
                tableHtml += `
                    <tr data-variant-id="${v.variant_id}" data-store-id="${v.store_id}">
                        <td><input type="checkbox" class="row-checkbox"></td>
                        <td>${v.store_name}</td>
                        ${tableHeaders.map(h => `
                            <td>
                                <input type="${h.type}" 
                                       data-field-key="${h.key}" 
                                       value="${v[h.key] || ''}" 
                                       data-original-value="${v[h.key] || ''}">
                            </td>
                        `).join('')}
                    </tr>
                `;
            });
        }

        tableHtml += '</tbody></table>';
        elements.container.innerHTML = tableHtml;
        addTableEventListeners();
    };

    // --- Event Handling ---
    const addTableEventListeners = () => {
        // Change tracking
        elements.container.querySelectorAll('input[data-field-key]').forEach(input => {
            input.addEventListener('input', () => {
                const isChanged = input.value !== input.dataset.originalValue;
                input.classList.toggle('changed', isChanged);
            });
        });

        // Select All functionality
        document.getElementById('select-all-checkbox').addEventListener('change', (e) => {
            document.querySelectorAll('.row-checkbox').forEach(cb => cb.checked = e.target.checked);
        });

        // Bulk apply functionality
        document.querySelectorAll('input[data-bulk-apply-for]').forEach(bulkInput => {
            bulkInput.addEventListener('change', () => {
                if (bulkInput.value === '') return;
                const fieldKey = bulkInput.dataset.bulkApplyFor;
                document.querySelectorAll('.row-checkbox:checked').forEach(checkbox => {
                    const row = checkbox.closest('tr');
                    const targetInput = row.querySelector(`input[data-field-key="${fieldKey}"]`);
                    if (targetInput) {
                        targetInput.value = bulkInput.value;
                        // Manually trigger input event to register the change
                        targetInput.dispatchEvent(new Event('input'));
                    }
                });
                bulkInput.value = ''; // Clear after applying
            });
        });
    };
    
    // --- Main Actions ---
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
            if (!response.ok) {
                throw new Error(result.detail?.message || 'An unknown error occurred.');
            }
            showToast(result.message, 'success');
            // Reload data to show fresh values and clear changed state
            loadAllVariants(); 
        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            elements.saveBtn.removeAttribute('aria-busy');
        }
    };

    const handleSearch = () => {
        const searchTerm = elements.searchInput.value.toLowerCase();
        if (!searchTerm) {
            renderTable(allVariants);
            return;
        }
        const filteredVariants = allVariants.filter(v => 
            v.product_title.toLowerCase().includes(searchTerm) ||
            (v.sku && v.sku.toLowerCase().includes(searchTerm)) ||
            (v.barcode && v.barcode.toLowerCase().includes(searchTerm))
        );
        renderTable(filteredVariants);
    };

    // --- Initial Setup ---
    elements.saveBtn.addEventListener('click', handleSaveChanges);
    elements.searchInput.addEventListener('input', handleSearch);
    loadAllVariants();
});