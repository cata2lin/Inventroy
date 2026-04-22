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
  const productPreview = document.getElementById('product-preview');
  const copyBtn = document.getElementById('copy-response-btn');

  let currentProduct = null;

  // --- Debounce ---
  const debounce = (func, delay) => {
    let timeout;
    return (...args) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => func.apply(null, args), delay);
    };
  };

  // --- Helpers ---
  const invGid = (v) => v.inventory_item_gid || (v.inventory_item_id ? `gid://shopify/InventoryItem/${v.inventory_item_id}` : null);

  // --- Copy response ---
  if (copyBtn) {
    copyBtn.addEventListener('click', () => {
      const text = apiResponse.textContent;
      navigator.clipboard.writeText(text).then(() => {
        copyBtn.textContent = '✓ Copied';
        setTimeout(() => { copyBtn.textContent = 'Copy'; }, 1500);
      });
    });
  }

  // --- API Calls ---
  const loadStores = async () => {
    try {
      const response = await fetch(API_ENDPOINTS.getStores);
      if (!response.ok) throw new Error('Failed to load stores.');
      const stores = await response.json();
      stores.forEach(store => storeSelect.add(new Option(store.name, store.id)));
    } catch (error) {
      console.error(error.message);
    }
  };

  const searchProducts = async () => {
    const storeId = storeSelect.value;
    const searchTerm = productSearchInput.value.trim();
    if (!storeId || searchTerm.length < 2) {
      productSelect.innerHTML = '<option value="">— Search for a product —</option>';
      productSelect.disabled = true;
      return;
    }
    const params = new URLSearchParams({ store_id: storeId, search: searchTerm, limit: 25 });
    try {
      const response = await fetch(`${API_ENDPOINTS.getProducts}?${params.toString()}`);
      if (!response.ok) throw new Error('Failed to search products.');
      const data = await response.json();
      renderProductOptions(data.products || []);
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
      productPreview.style.display = 'none';
      return;
    }
    try {
      const response = await fetch(API_ENDPOINTS.getProduct(productId));
      if (!response.ok) throw new Error('Failed to fetch product details.');
      currentProduct = await response.json();
      mutationSelectionContainer.style.display = 'block';
      renderProductPreview();
      renderMutationForm();
    } catch (error) {
      console.error(error.message);
      alert(`Error fetching product details: ${error.message}`);
    }
  };

  // --- Product Preview ---
  const renderProductPreview = () => {
    if (!currentProduct) {
      productPreview.style.display = 'none';
      return;
    }
    productPreview.style.display = 'block';

    const previewImg = document.getElementById('preview-image');
    const previewTitle = document.getElementById('preview-title');
    const previewMeta = document.getElementById('preview-meta');
    const previewVariants = document.getElementById('preview-variants');

    previewTitle.textContent = currentProduct.title || 'Untitled Product';

    if (currentProduct.image_url) {
      previewImg.src = currentProduct.image_url;
      previewImg.style.display = 'block';
    } else {
      previewImg.style.display = 'none';
    }

    const metaParts = [];
    if (currentProduct.vendor) metaParts.push(currentProduct.vendor);
    if (currentProduct.product_type) metaParts.push(currentProduct.product_type);
    if (currentProduct.status) metaParts.push(currentProduct.status);
    previewMeta.textContent = metaParts.join(' · ') || 'No metadata';

    const variants = currentProduct.variants || [];
    if (variants.length > 0) {
      const rows = variants.slice(0, 5).map(v => {
        const stock = v.inventory_quantity ?? '—';
        return `<tr>
          <td style="font-size:0.85rem">${v.title || 'Default'}</td>
          <td style="font-size:0.85rem">${v.sku || '—'}</td>
          <td style="font-size:0.85rem">${v.barcode || '—'}</td>
          <td style="font-size:0.85rem">${v.price || '—'}</td>
          <td style="font-size:0.85rem">${stock}</td>
        </tr>`;
      }).join('');
      const moreText = variants.length > 5 ? `<small style="opacity:0.5">…and ${variants.length - 5} more variants</small>` : '';
      previewVariants.innerHTML = `
        <table role="grid" style="margin-bottom:0;">
          <thead><tr>
            <th style="font-size:0.8rem">Variant</th>
            <th style="font-size:0.8rem">SKU</th>
            <th style="font-size:0.8rem">Barcode</th>
            <th style="font-size:0.8rem">Price</th>
            <th style="font-size:0.8rem">Stock</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
        ${moreText}`;
    } else {
      previewVariants.innerHTML = '<small style="opacity:0.5">No variants</small>';
    }
  };

  // --- UI Rendering ---
  const renderProductOptions = (products) => {
    productSelect.innerHTML = '<option value="">— Select a product from results —</option>';
    if (products.length > 0) {
      products.forEach(p => {
        const firstSku = (p.variants && p.variants.length > 0) ? (p.variants[0].sku || 'No SKU') : 'No Variants';
        const optionText = `${p.title} (${firstSku})`;
        productSelect.add(new Option(optionText, p.id));
      });
      productSelect.disabled = false;
    } else {
      productSelect.innerHTML = '<option value="">— No products found —</option>';
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
          <label for="categoryId">New Category GID</label>
          <input type="text" id="categoryId" name="categoryId" required placeholder="gid://shopify/TaxonomyCategory/123">
          <label for="findCategory">Find Category by Name</label>
          <div class="grid">
            <input type="text" id="findCategoryInput" placeholder="e.g., Apparel, Rugs, Candles…">
            <button type="button" id="find-category-btn" class="outline">🔍 Find</button>
          </div>`;
        break;

      case 'updateProductType':
        html += `
          <input type="hidden" name="productId" value="${currentProduct.shopify_gid}">
          <label for="productType">New Product Type</label>
          <input type="text" id="productType" name="productType" value="${currentProduct.product_type || ''}" required>`;
        break;

      case 'updateVariantPrices':
      case 'updateVariantCompareAt':
      case 'updateVariantBarcode':
      case 'updateVariantCosts':
        html += `<input type="hidden" name="productId" value="${currentProduct.shopify_gid}">`;
        const fieldLabel = {
          'updateVariantPrices': 'Price',
          'updateVariantCompareAt': 'Compare-At Price',
          'updateVariantBarcode': 'Barcode',
          'updateVariantCosts': 'Cost',
        }[mutation];
        html += `<p style="font-size:0.85rem; opacity:0.7; margin-bottom:0.75rem;">Update <strong>${fieldLabel}</strong> for each variant:</p>`;
        (currentProduct.variants || []).forEach(v => {
          let value = '';
          let placeholder = 'New Value';
          let type = 'text';
          if (mutation === 'updateVariantPrices') { value = v.price ?? ''; placeholder = 'e.g., 29.99'; type = 'number'; }
          if (mutation === 'updateVariantCompareAt') { value = v.compare_at_price ?? ''; placeholder = 'e.g., 39.99'; type = 'number'; }
          if (mutation === 'updateVariantBarcode') { value = v.barcode ?? ''; placeholder = 'e.g., 123456789012'; }
          if (mutation === 'updateVariantCosts') { value = v.cost_per_item ?? ''; placeholder = 'e.g., 12.50'; type = 'number'; }
          html += `
            <div style="display:flex; gap:0.5rem; align-items:center; margin-bottom:0.5rem;">
              <span style="font-size:0.85rem; min-width:120px; opacity:0.7;">${v.title} <small>(${v.sku || '—'})</small></span>
              <input type="hidden" name="variantId" value="${v.shopify_gid}">
              <input type="${type}" id="variant_${v.id}" name="value" placeholder="${placeholder}" value="${value}" ${type === 'number' ? 'step="0.01"' : ''} required style="margin-bottom:0;">
            </div>`;
        });
        break;

      case 'updateInventoryCost': {
        const costVariants = (currentProduct.variants || []).filter(v => invGid(v));
        if (costVariants.length > 0) {
          html += `
            <label for="inventoryItemId">Select Variant</label>
            <select name="inventoryItemId" id="inventoryItemId" required>
              <option value="">— Choose a variant —</option>
              ${costVariants.map(v => `<option value="${invGid(v)}" data-cost="${v.cost_per_item ?? ''}">${v.title} (${v.sku || '—'})</option>`).join('')}
            </select>
            <label for="cost">New Cost</label>
            <input type="number" id="cost" name="cost" step="0.01" placeholder="e.g., 7.50" required>
          `;
        } else {
          html += `<p style="opacity:0.5;">This product has no variants with trackable inventory.</p>`;
        }
        break;
      }

      case 'inventorySetQuantities': {
        const quantityVariants = (currentProduct.variants || []).filter(v => invGid(v));
        const locations = {};
        quantityVariants.forEach(v => {
          (v.inventory_levels || []).forEach(l => {
            if (l.location && l.location.shopify_gid) {
              locations[l.location.shopify_gid] = l.location.name;
            }
          });
        });
        if (quantityVariants.length > 0) {
          html += `
            <label for="inventoryItemId">Select Variant</label>
            <select name="inventoryItemId" id="inventoryItemId" required>
              <option value="">— Choose a variant —</option>
              ${quantityVariants.map(v => `<option value="${invGid(v)}">${v.title} (${v.sku || '—'})</option>`).join('')}
            </select>
            <label for="locationId">Select Location</label>
            <select name="locationId" id="locationId" required>
              <option value="">— Choose a location —</option>
              ${Object.entries(locations).map(([gid, name]) => `<option value="${gid}">${name}</option>`).join('')}
            </select>
            <label for="quantity">New 'Available' Quantity</label>
            <input type="number" id="quantity" name="quantity" required placeholder="e.g., 100">
          `;
        } else {
          html += `<p style="opacity:0.5;">This product has no variants with trackable inventory.</p>`;
        }
        break;
      }
    }

    if (html !== '<form id="mutation-form">') {
      html += '<button type="submit">⚡ Execute Mutation</button>';
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
    const responseBadge = document.getElementById('response-badge');
    responseBadge.className = 'badge badge-info';
    responseBadge.textContent = '⏳';
    apiResponse.textContent = 'Executing mutation…';

    const storeId = storeSelect.value;
    const mutationName = mutationSelect.value;
    const formData = new FormData(e.target);
    const variables = {};

    try {
      switch (mutationName) {
        case 'setProductCategory':
          variables.product = {
            id: formData.get('productId'),
            productTaxonomy: { productTaxonomyNodeId: formData.get('categoryId') },
          };
          break;

        case 'updateProductType':
          variables.product = {
            id: formData.get('productId'),
            productType: formData.get('productType'),
          };
          break;

        case 'updateVariantPrices':
        case 'updateVariantCompareAt':
        case 'updateVariantBarcode':
        case 'updateVariantCosts': {
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
        }

        case 'updateInventoryCost':
          variables.id = formData.get('inventoryItemId');
          variables.input = { cost: parseFloat(formData.get('cost')) };
          break;

        case 'inventorySetQuantities':
          variables.input = {
            name: 'available',
            reason: 'correction',
            ignoreCompareQuantity: true,
            quantities: [
              {
                inventoryItemId: formData.get('inventoryItemId'),
                locationId: formData.get('locationId'),
                quantity: parseInt(formData.get('quantity'), 10),
              },
            ],
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

      responseBadge.className = 'badge badge-success';
      responseBadge.textContent = '✓ OK';
      apiResponse.textContent = JSON.stringify(result, null, 2);
    } catch (error) {
      responseBadge.className = 'badge badge-danger';
      responseBadge.textContent = '✗ Error';
      apiResponse.textContent = `Error: ${JSON.stringify(error, null, 2)}`;
    }
  };

  const findCategory = async () => {
    const storeId = storeSelect.value;
    const query = document.getElementById('findCategoryInput').value.trim();
    if (!storeId || !query) {
      alert('Please enter a category to search for.');
      return;
    }
    apiResponseContainer.style.display = 'block';
    const responseBadge = document.getElementById('response-badge');
    responseBadge.className = 'badge badge-info';
    responseBadge.textContent = '🔍';
    apiResponse.textContent = 'Searching for categories…';
    try {
      const response = await fetch(API_ENDPOINTS.findCategories(storeId), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      });
      const result = await response.json();
      if (!response.ok) throw result;
      responseBadge.className = 'badge badge-success';
      responseBadge.textContent = '✓ Found';
      apiResponse.textContent = "Search Results:\n" + JSON.stringify(result, null, 2);
    } catch (error) {
      responseBadge.className = 'badge badge-danger';
      responseBadge.textContent = '✗ Error';
      apiResponse.textContent = `Error: ${JSON.stringify(error, null, 2)}`;
    }
  };

  // --- Initial Setup ---
  storeSelect.addEventListener('change', () => {
    productSearchInput.disabled = !storeSelect.value;
    productSearchInput.value = '';
    productSelect.innerHTML = '<option value="">— Search for a product —</option>';
    productSelect.disabled = true;
    currentProduct = null;
    mutationSelectionContainer.style.display = 'none';
    apiResponseContainer.style.display = 'none';
    productPreview.style.display = 'none';
  });

  productSearchInput.addEventListener('input', debounce(searchProducts, 400));
  productSelect.addEventListener('change', () => fetchProductDetails(productSelect.value));
  mutationSelect.addEventListener('change', renderMutationForm);
  loadStores();
});
