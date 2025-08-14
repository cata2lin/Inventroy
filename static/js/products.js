// static/js/products.js

document.addEventListener('DOMContentLoaded', () => {
    const storeSelector = document.getElementById('storeSelector');
    const syncProductsButton = document.getElementById('syncProductsButton');
    const variantsContent = document.getElementById('variants-content');
    const productTableContainer = document.getElementById('product-table-container');
    const toast = document.getElementById('toast');
    let currentStoreId = null;

    const showToast = (message, type = 'info', duration = 4000) => {
        toast.textContent = message;
        toast.className = `show ${type}`;
        setTimeout(() => { toast.className = ''; }, duration);
    };

    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            storeSelector.innerHTML = '<option value="" disabled selected>Select a store</option>';
            stores.forEach(store => storeSelector.add(new Option(store.name, store.id)));
        } catch (error) {
            storeSelector.innerHTML = '<option value="">Could not load stores</option>';
        }
    };

    const loadVariants = async () => {
        if (!currentStoreId) return;
        productTableContainer.style.display = 'block';
        variantsContent.innerHTML = '<p><progress></progress><br>Loading variants...</p>';
        try {
            const response = await fetch(API_ENDPOINTS.getVariants(currentStoreId));
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || 'Failed to load variants.');
            }
            renderVariantsTable(await response.json());
        } catch (error) {
            variantsContent.innerHTML = `<p>Error loading variants: ${error.message}</p>`;
        }
    };

    const renderVariantsTable = (variants) => {
        if (!variants || variants.length === 0) {
            variantsContent.innerHTML = '<p>No product variants found for this store.</p>';
            return;
        }
        const table = document.createElement('table');
        table.innerHTML = `
            <thead>
                <tr>
                    <th>Product (Status)</th><th>Variant Title</th><th>SKU</th><th>Barcode</th>
                    <th>Price</th><th>Compare At</th><th>Cost</th><th>Available Qty</th><th>On Hand Qty</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                ${variants.map(v => `
                    <tr data-variant-id="${v.id}" data-product-id="${v.product_id_db}">
                        <td>
                            ${v.product_title}<br>
                            <small class="status-${(v.product_status || 'unknown').toLowerCase()}">${v.product_status}</small>
                        </td>
                        ${createEditableCell(v, 'title', 'text')}
                        ${createEditableCell(v, 'sku', 'text')}
                        ${createEditableCell(v, 'barcode', 'text')}
                        ${createEditableCell(v, 'price', 'text')}
                        ${createEditableCell(v, 'compareAtPrice', 'text')}
                        ${createEditableCell(v, 'cost', 'text')}
                        ${createEditableCell(v, 'available', 'number', v.inventory_management !== 'shopify', v.available_quantity)}
                        ${createEditableCell(v, 'onHand', 'number', v.inventory_management !== 'shopify', v.on_hand_quantity)}
                        <td>
                            <a href="/mutations?id=${v.product_id_db}" role="button" class="outline">Edit Product</a>
                        </td>
                    </tr>
                `).join('')}
            </tbody>`;
        variantsContent.innerHTML = '';
        variantsContent.appendChild(table);
    };

    const createEditableCell = (variant, fieldName, inputType, disabled = false, value = null) => {
        let fieldValue = value;
        if (fieldValue === null || fieldValue === undefined) {
            fieldValue = variant[fieldName] || '';
        }
        const placeholder = fieldName.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase());
        return `
            <td>
                <div class="field-group">
                    <input name="${fieldName}" type="${inputType}" value="${fieldValue}" placeholder="${placeholder}" ${disabled ? 'disabled' : ''}>
                    <button class="secondary outline" 
                            onclick="updateField(this, '${fieldName}')" 
                            ${disabled ? 'disabled' : ''}>✓</button>
                </div>
            </td>`;
    };

    window.updateField = async (button, field) => {
        const row = button.closest('tr');
        const variantId = row.dataset.variantId;
        const input = row.querySelector(`input[name="${field}"]`);
        const value = input.value;

        const payload = {
            variant_id: parseInt(variantId, 10),
            field: field,
            value: input.type === 'number' ? (value === '' ? null : parseFloat(value)) : value
        };
        
        button.setAttribute('aria-busy', 'true');
        button.disabled = true;
        input.disabled = true;

        try {
            const response = await fetch(API_ENDPOINTS.updateVariantField(currentStoreId), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Update failed.');
            showToast(result.message || 'Update successful!', 'success');
            
            if(field === 'available' || field === 'onHand' || field === 'cost') {
               await loadVariants();
            }

        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            button.removeAttribute('aria-busy');
            button.disabled = false;
            input.disabled = false;
        }
    };

    storeSelector.addEventListener('change', () => {
        currentStoreId = storeSelector.value;
        syncProductsButton.disabled = !currentStoreId;
        loadVariants();
    });

    syncProductsButton.addEventListener('click', async () => {
        if (!currentStoreId) return;
        syncProductsButton.setAttribute('aria-busy', 'true');
        syncProductsButton.disabled = true;
        showToast(`Starting inventory sync for store ID ${currentStoreId}...`);

        try {
            const response = await fetch(API_ENDPOINTS.syncInventory(currentStoreId), { method: 'POST' });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Sync request failed.');
            showToast(result.message);
            
            await loadVariants();
        } catch (error) {
            showToast(`Error: ${error.message}`, 'error');
        } finally {
            syncProductsButton.removeAttribute('aria-busy');
            syncProductsButton.disabled = false;
        }
    });

    loadStores();
});