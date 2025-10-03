// static/js/mutations.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const storeSelect = document.getElementById('store-select');
    const productSearchInput = document.getElementById('product-search-input');
    const productSelect = document.getElementById('product-select');
    const mutationSelectionContainer = document.getElementById('mutation-selection-container');
    const mutationSelect = document.getElementById('mutation-select');
    const mutationFormContainer = document.getElementById('mutation-form-container');
    const apiResponseContainer = document.getElementById('api-response-container');
    const apiResponse = document.getElementById('api-response');

    let currentProduct = null;

    // --- Debounce function for search input ---
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    // --- API Calls ---
    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            stores.forEach(store => {
                storeSelect.add(new Option(store.name, store.id));
            });
        } catch (error) {
            console.error(error.message);
        }
    };

    const searchProducts = async () => {
        const storeId = storeSelect.value;
        const searchTerm = productSearchInput.value;
        if (!storeId || searchTerm.length < 2) {
            productSelect.innerHTML = '<option value="">-- Search for a product --</option>';
            productSelect.disabled = true;
            return;
        }

        const params = new URLSearchParams({ store_id: storeId, search: searchTerm, limit: 25 });
        try {
            const response = await fetch(`${API_ENDPOINTS.getProducts}?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to search products.');
            const data = await response.json();
            renderProductOptions(data.products);
        } catch (error) {
            console.error(error.message);
        }
    };

    const fetchProductDetails = async (productId) => {
        if (!productId) {
            currentProduct = null;
            mutationSelectionContainer.style.display = 'none';
            mutationSelect.value = '';
            mutationFormContainer.innerHTML = '';
            return;
        }
        try {
            const response = await fetch(API_ENDPOINTS.getProduct(productId));
            if (!response.ok) throw new Error('Failed to fetch product details.');
            currentProduct = await response.json();
            mutationSelectionContainer.style.display = 'block';
            renderMutationForm();
        } catch (error) {
            console.error(error.message);
            alert(`Error fetching product details: ${error.message}`);
        }
    };

    // --- UI Rendering ---
    const renderProductOptions = (products) => {
        productSelect.innerHTML = '<option value="">-- Select a product from results --</option>';
        if (products.length > 0) {
            products.forEach(p => {
                const optionText = `${p.title} (${p.variants.length > 0 ? p.variants[0].sku || 'No SKU' : 'No Variants'})`;
                productSelect.add(new Option(optionText, p.id));
            });
            productSelect.disabled = false;
        } else {
            productSelect.innerHTML = '<option value="">-- No products found --</option>';
            productSelect.disabled = true;
        }
    };

    const renderMutationForm = () => {
        const mutation = mutationSelect.value;
        if (!mutation || !currentProduct) {
            mutationFormContainer.innerHTML = '';
            return;
        }

        let html = '<form id="mutation-form">';
        switch (mutation) {
            case 'setProductCategory':
                html += `
                    <input type="hidden" name="productId" value="${currentProduct.shopify_gid}">
                    <label for="categoryId">New Category GID (e.g., gid://shopify/TaxonomyCategory/123)</label>
                    <input type="text" id="categoryId" name="categoryId" required placeholder="gid://shopify/TaxonomyCategory/123">
                    <label for="findCategory">Find Category by Name</label>
                    <div class="grid">
                        <input type="text" id="findCategoryInput" placeholder="e.g., Apparel">
                        <button type="button" id="find-category-btn" class="outline">Find</button>
                    </div>
                `;
                break;
            case 'updateProductType':
                html += `
                    <input type="hidden" name="productId" value="${currentProduct.shopify_gid}">
                    <label for="productType">New Product Type</label>
                    <input type="text" id="productType" name="productType" value="${currentProduct.product_type || ''}" required>
                `;
                break;
            case 'updateVariantPrices':
            case 'updateVariantCompareAt':
            case 'updateVariantBarcode':
            case 'updateVariantCosts':
                html += `<input type="hidden" name="productId" value="${currentProduct.shopify_gid}">`;
                html += `<h6>Update values for each variant:</h6>`;
                currentProduct.variants.forEach(v => {
                    let value = '';
                    let placeholder = 'New Value';
                    let type = 'text';
                    if (mutation === 'updateVariantPrices') { value = v.price || ''; placeholder = 'e.g., 29.99'; type = 'number'; }
                    if (mutation === 'updateVariantCompareAt') { value = v.compare_at_price || ''; placeholder = 'e.g., 39.99'; type = 'number'; }
                    if (mutation === 'updateVariantBarcode') { value = v.barcode || ''; placeholder = 'e.g., 123456789012'; }
                    if (mutation === 'updateVariantCosts') { value = v.cost_per_item || ''; placeholder = 'e.g., 12.50'; type = 'number'; }
                    html += `
                        <div class="variant-row">
                            <label for="variant_${v.id}">${v.title} (SKU: ${v.sku || 'N/A'})</label>
                            <input type="hidden" name="variantId" value="${v.shopify_gid}">
                            <input type="${type}" id="variant_${v.id}" name="value" placeholder="${placeholder}" value="${value}" ${type === 'number' ? 'step="0.01"' : ''} required>
                        </div>
                    `;
                });
                break;
            case 'updateInventoryCost':
                const costVariants = currentProduct.variants.filter(v => v.inventory_item_gid);
                if (costVariants.length > 0) {
                    html += `
                        <label for="inventoryItemId">Select Variant</label>
                        <select name="inventoryItemId" id="inventoryItemId" required>
                            <option value="">-- Choose a variant --</option>
                            ${costVariants.map(v => `<option value="${v.inventory_item_gid}" data-cost="${v.cost_per_item || ''}">${v.title}</option>`).join('')}
                        </select>
                        <label for="cost">New Cost</label>
                        <input type="number" id="cost" name="cost" step="0.01" placeholder="e.g., 7.50" required>
                    `;
                } else {
                    html += `<p>This product has no variants with trackable inventory.</p>`;
                }
                break;
            case 'inventorySetQuantities':
                const quantityVariants = currentProduct.variants.filter(v => v.inventory_item_gid);
                const locations = {};
                quantityVariants.forEach(v => {
                    v.inventory_levels.forEach(l => {
                        if (l.location && l.location.shopify_gid) {
                            locations[l.location.shopify_gid] = l.location.name;
                        }
                    });
                });

                if (quantityVariants.length > 0) {
                     html += `
                        <label for="inventoryItemId">Select Variant</label>
                        <select name="inventoryItemId" required>
                            <option value="">-- Choose a variant --</option>
                            ${quantityVariants.map(v => `<option value="${v.inventory_item_gid}">${v.title}</option>`).join('')}
                        </select>
                        
                        <label for="locationId">Select Location</label>
                        <select name="locationId" id="locationId" required>
                            <option value="">-- Choose a location --</option>
                            ${Object.entries(locations).map(([gid, name]) => `<option value="${gid}">${name}</option>`).join('')}
                        </select>

                        <label for="quantity">New 'Available' Quantity</label>
                        <input type="number" id="quantity" name="quantity" required placeholder="e.g., 100">
                    `;
                } else {
                     html += `<p>This product has no variants with trackable inventory.</p>`;
                }
                break;
        }
        // Only show the submit button if the form is not empty
        if (html !== '<form id="mutation-form">') {
            html += '<button type="submit">Execute Mutation</button>';
        }
        html += '</form>';
        mutationFormContainer.innerHTML = html;
        attachFormListeners();
    };

    const attachFormListeners = () => {
        const form = document.getElementById('mutation-form');
        if (form) form.addEventListener('submit', handleFormSubmit);

        const findCategoryBtn = document.getElementById('find-category-btn');
        if (findCategoryBtn) findCategoryBtn.addEventListener('click', findCategory);

        const inventoryItemSelect = document.getElementById('inventoryItemId');
        if (inventoryItemSelect && mutationSelect.value === 'updateInventoryCost') {
            inventoryItemSelect.addEventListener('change', (e) => {
                const selectedOption = e.target.options[e.target.selectedIndex];
                document.getElementById('cost').value = selectedOption.dataset.cost || '';
            });
        }
    };

    // --- Event Handlers ---
    const handleFormSubmit = async (e) => {
        e.preventDefault();
        apiResponseContainer.style.display = 'block';
        apiResponse.textContent = 'Executing...';

        const storeId = storeSelect.value;
        const mutationName = mutationSelect.value;
        const formData = new FormData(e.target);
        const variables = {};

        try {
            switch (mutationName) {
                case 'setProductCategory':
                    variables.product = {
                        id: formData.get('productId'),
                        productTaxonomy: {
                            productTaxonomyNodeId: formData.get('categoryId')
                        }
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
                        name: "available",
                        reason: "correction",
                        setQuantities: [{
                            inventoryItemId: formData.get('inventoryItemId'),
                            locationId: formData.get('locationId'),
                            quantity: parseInt(formData.get('quantity'), 10)
                        }]
                    };
                    break;
            }

            const response = await fetch(API_ENDPOINTS.executeMutation(storeId), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mutation_name: mutationName, variables }),
            });
            const result = await response.json();
            if (!response.ok) throw result;
            apiResponse.textContent = JSON.stringify(result, null, 2);
        } catch (error) {
            apiResponse.textContent = `Error: ${JSON.stringify(error, null, 2)}`;
        }
    };

    const findCategory = async () => {
        const storeId = storeSelect.value;
        const query = document.getElementById('findCategoryInput').value;
        if (!storeId || !query) {
            alert('Please enter a category to search for.');
            return;
        }

        apiResponseContainer.style.display = 'block';
        apiResponse.textContent = 'Searching for categories...';

        try {
            const response = await fetch(API_ENDPOINTS.findCategories(storeId), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query }),
            });
            const result = await response.json();
            if (!response.ok) throw result;
            apiResponse.textContent = "Search Results:\n" + JSON.stringify(result, null, 2);
        } catch (error) {
            apiResponse.textContent = `Error: ${JSON.stringify(error, null, 2)}`;
        }
    };

    // --- Initial Setup ---
    storeSelect.addEventListener('change', () => {
        productSearchInput.disabled = !storeSelect.value;
        productSearchInput.value = '';
        productSelect.innerHTML = '<option value="">-- Search for a product --</option>';
        productSelect.disabled = true;
        currentProduct = null;
        mutationSelectionContainer.style.display = 'none';
        apiResponseContainer.style.display = 'none';
    });

    productSearchInput.addEventListener('input', debounce(searchProducts, 400));
    productSelect.addEventListener('change', () => fetchProductDetails(productSelect.value));
    mutationSelect.addEventListener('change', renderMutationForm);

    loadStores();
});