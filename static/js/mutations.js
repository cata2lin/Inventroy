// static/js/mutations.js

document.addEventListener('DOMContentLoaded', () => {
    const storeSelect = document.getElementById('store-select');
    const mutationSelect = document.getElementById('mutation-select');
    const mutationFormContainer = document.getElementById('mutation-form-container');
    const apiResponse = document.getElementById('api-response');

    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            stores.forEach(store => {
                const option = new Option(store.name, store.id);
                storeSelect.add(option);
            });
        } catch (error) {
            console.error(error.message);
        }
    };

    const renderForm = () => {
        const mutation = mutationSelect.value;
        let html = '';

        if (mutation) {
            html += '<form id="mutation-form">';
            switch (mutation) {
                case 'setProductCategory':
                    html += `
                        <label for="productId">Product ID (gid://shopify/Product/123)</label>
                        <input type="text" id="productId" name="productId" required>
                        <label for="categoryId">Category ID (gid://shopify/TaxonomyCategory/aa-1-10-2)</label>
                        <input type="text" id="categoryId" name="categoryId" required>
                        <label for="findCategory">Find Category</label>
                        <input type="text" id="findCategory" name="findCategory">
                        <button type="button" id="find-category-btn">Find</button>
                    `;
                    break;
                case 'updateProductType':
                    html += `
                        <label for="productId">Product ID (gid://shopify/Product/123)</label>
                        <input type="text" id="productId" name="productId" required>
                        <label for="productType">Product Type</label>
                        <input type="text" id="productType" name="productType" required>
                    `;
                    break;
                case 'updateVariantPrices':
                case 'updateVariantCompareAt':
                case 'updateVariantBarcode':
                case 'updateVariantCosts':
                    html += `
                        <label for="productId">Product ID (gid://shopify/Product/123)</label>
                        <input type="text" id="productId" name="productId" required>
                        <div id="variants-container">
                            <div class="variant-row">
                                <input type="text" name="variantId" placeholder="Variant ID" required>
                                <input type="text" name="value" placeholder="Value" required>
                                <button type="button" class="remove-variant-btn">-</button>
                            </div>
                        </div>
                        <button type="button" id="add-variant-btn">+</button>
                    `;
                    break;
                case 'updateInventoryCost':
                    html += `
                        <label for="inventoryItemId">Inventory Item ID (gid://shopify/InventoryItem/444)</label>
                        <input type="text" id="inventoryItemId" name="inventoryItemId" required>
                        <label for="cost">Cost</label>
                        <input type="number" id="cost" name="cost" step="0.01" required>
                    `;
                    break;
                case 'inventorySetQuantities':
                    html += `
                        <label for="inventoryItemId">Inventory Item ID (gid://shopify/InventoryItem/30322695)</label>
                        <input type="text" id="inventoryItemId" name="inventoryItemId" required>
                        <label for="locationId">Location ID (gid://shopify/Location/124656943)</label>
                        <input type="text" id="locationId" name="locationId" required>
                        <label for="quantity">Quantity</label>
                        <input type="number" id="quantity" name="quantity" required>
                    `;
                    break;
            }
            html += '<button type="submit">Execute</button>';
            html += '</form>';
        }
        mutationFormContainer.innerHTML = html;
        attachFormListeners();
    };

    const attachFormListeners = () => {
        const form = document.getElementById('mutation-form');
        if (form) {
            form.addEventListener('submit', handleFormSubmit);
        }

        const addVariantBtn = document.getElementById('add-variant-btn');
        if (addVariantBtn) {
            addVariantBtn.addEventListener('click', addVariantRow);
        }

        const findCategoryBtn = document.getElementById('find-category-btn');
        if (findCategoryBtn) {
            findCategoryBtn.addEventListener('click', findCategory);
        }

        document.querySelectorAll('.remove-variant-btn').forEach(btn => {
            btn.addEventListener('click', (e) => e.target.closest('.variant-row').remove());
        });
    };

    const addVariantRow = () => {
        const container = document.getElementById('variants-container');
        const row = document.createElement('div');
        row.classList.add('variant-row');
        row.innerHTML = `
            <input type="text" name="variantId" placeholder="Variant ID" required>
            <input type="text" name="value" placeholder="Value" required>
            <button type="button" class="remove-variant-btn">-</button>
        `;
        row.querySelector('.remove-variant-btn').addEventListener('click', () => row.remove());
        container.appendChild(row);
    };

    const findCategory = async () => {
        const storeId = storeSelect.value;
        const query = document.getElementById('findCategory').value;

        if (!storeId || !query) {
            alert('Please select a store and enter a search query.');
            return;
        }

        try {
            const response = await fetch(API_ENDPOINTS.findCategories(storeId), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query }),
            });
            const result = await response.json();
            apiResponse.textContent = JSON.stringify(result, null, 2);
        } catch (error) {
            apiResponse.textContent = `Error: ${error.message}`;
        }
    };

    const handleFormSubmit = async (e) => {
        e.preventDefault();
        const storeId = storeSelect.value;
        const mutationName = mutationSelect.value;
        const formData = new FormData(e.target);
        const variables = {};

        switch (mutationName) {
            case 'setProductCategory':
                variables.product = {
                    id: formData.get('productId'),
                    category: formData.get('categoryId'),
                    deleteConflictingConstrainedMetafields: true
                };
                break;
            case 'updateProductType':
                variables.product = {
                    id: formData.get('productId'),
                    productType: formData.get('productType')
                };
                break;
            case 'updateVariantPrices':
            case 'updateVariantCompareAt':
            case 'updateVariantBarcode':
            case 'updateVariantCosts':
                variables.productId = formData.get('productId');
                variables.variants = [];
                const variantIds = formData.getAll('variantId');
                const values = formData.getAll('value');
                for (let i = 0; i < variantIds.length; i++) {
                    const variant = { id: variantIds[i] };
                    if (mutationName === 'updateVariantPrices') variant.price = values[i];
                    if (mutationName === 'updateVariantCompareAt') variant.compareAtPrice = values[i];
                    if (mutationName === 'updateVariantBarcode') variant.barcode = values[i];
                    if (mutationName === 'updateVariantCosts') variant.inventoryItem = { cost: parseFloat(values[i]) };
                    variables.variants.push(variant);
                }
                break;
            case 'updateInventoryCost':
                variables.id = formData.get('inventoryItemId');
                variables.input = { cost: parseFloat(formData.get('cost')) };
                break;
            case 'inventorySetQuantities':
                variables.input = {
                    name: 'available',
                    reason: 'correction',
                    quantities: [{
                        inventoryItemId: formData.get('inventoryItemId'),
                        locationId: formData.get('locationId'),
                        quantity: parseInt(formData.get('quantity'))
                    }]
                };
                break;
        }

        try {
            const response = await fetch(API_ENDPOINTS.executeMutation(storeId), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mutation_name: mutationName, variables }),
            });
            const result = await response.json();
            apiResponse.textContent = JSON.stringify(result, null, 2);
        } catch (error) {
            apiResponse.textContent = `Error: ${error.message}`;
        }
    };

    mutationSelect.addEventListener('change', renderForm);
    loadStores();
});