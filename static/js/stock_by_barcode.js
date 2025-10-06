// static/js/stock_by_barcode.js
document.addEventListener('DOMContentLoaded', () => {
    const storeFilter = document.getElementById('store-filter');
    const stockContainer = document.getElementById('stock-container');
    let locationsMap = {};

    const loadStores = async () => {
        try {
            const response = await fetch('/api/config/stores');
            const stores = await response.json();
            stores.forEach(store => {
                storeFilter.add(new Option(store.name, store.id));
            });
        } catch (error) {
            console.error('Failed to load stores:', error);
        }
    };

    const fetchStockData = async () => {
        const storeId = storeFilter.value;
        if (!storeId) {
            stockContainer.innerHTML = '<p>Please select a store.</p>';
            return;
        }
        stockContainer.setAttribute('aria-busy', 'true');

        try {
            const response = await fetch(`/api/stock/by-barcode/${storeId}`);
            if (!response.ok) throw new Error('Failed to fetch stock data.');
            const data = await response.json();
            renderStockView(data);
        } catch (error) {
            stockContainer.innerHTML = `<p style="color:red;">${error.message}</p>`;
        } finally {
            stockContainer.removeAttribute('aria-busy');
        }
    };

    const renderStockView = (data) => {
        if (data.length === 0) {
            stockContainer.innerHTML = '<p>No products with barcodes found for this store.</p>';
            stockContainer.classList.remove('grid');
            return;
        }
        stockContainer.classList.add('grid');

        // Collect all unique locations
        locationsMap = {};
        data.forEach(group => group.variants.forEach(variant => variant.locations.forEach(loc => {
            if (loc.location_gid) locationsMap[loc.location_gid] = loc.name;
        })));
        const locationOptions = Object.entries(locationsMap)
            .map(([gid, name]) => `<option value="${gid}">${name}</option>`).join('');

        stockContainer.innerHTML = data.map(group => `
            <article class="barcode-card">
                <header>
                    <img src="${group.primary_image_url || '/static/img/placeholder.png'}" alt="${group.primary_title}">
                </header>
                <div class="card-body">
                    <strong>${group.primary_title}</strong>
                    <p>Barcode: <code>${group.barcode}</code></p>
                    <details>
                        <summary>Manage ${group.variants.length} Variants & Stock</summary>
                        <div class="barcode-group" data-barcode="${group.barcode}">
                            ${group.variants.map(v => `
                                <div class="variant-row ${v.is_barcode_primary ? 'is-primary' : ''}">
                                    <span>${v.product_title} (${v.sku || 'N/A'})</span>
                                    <button class="set-primary-btn outline" data-variant-id="${v.variant_id}" ${v.is_barcode_primary ? 'disabled' : ''}>
                                        ${v.is_barcode_primary ? 'Primary' : 'Set Primary'}
                                    </button>
                                </div>
                            `).join('')}
                            <hr>
                            <div class="update-form grid">
                                <select class="location-select" required>${locationOptions}</select>
                                <input type="number" class="quantity-input" placeholder="New Qty" required />
                                <button class="update-stock-btn">Update</button>
                            </div>
                            <small class="response-message"></small>
                        </div>
                    </details>
                </div>
            </article>
        `).join('');
    };
    
    const handleStockUpdate = async (e) => {
        const storeId = storeFilter.value;
        const groupEl = e.target.closest('.barcode-group');
        const barcode = groupEl.dataset.barcode;
        const locationGid = groupEl.querySelector('.location-select').value;
        const quantity = groupEl.querySelector('.quantity-input').value;
        const messageEl = groupEl.querySelector('.response-message');
        
        if (!storeId || !barcode || !locationGid || quantity === '') {
            messageEl.textContent = 'All fields are required.';
            messageEl.style.color = 'red';
            return;
        }

        e.target.setAttribute('aria-busy', 'true');
        messageEl.textContent = '';
        
        try {
            const response = await fetch(`/api/stock/bulk-update/${storeId}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    barcode: barcode,
                    location_gid: locationGid,
                    quantity: parseInt(quantity)
                })
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Update failed.');
            
            messageEl.textContent = 'Stock updated successfully!';
            messageEl.style.color = 'green';
            setTimeout(fetchStockData, 2000); // Refresh data after 2 seconds
        } catch (error) {
            messageEl.textContent = `Error: ${error.message}`;
            messageEl.style.color = 'red';
        } finally {
            e.target.removeAttribute('aria-busy');
        }
    };

    const handleSetPrimary = async (e) => {
        const variantId = e.target.dataset.variantId;
        if (!variantId) return;

        e.target.setAttribute('aria-busy', 'true');
        
        try {
            const response = await fetch('/api/stock/set-primary', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ variant_id: parseInt(variantId) })
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Failed to set primary variant.');
            
            await fetchStockData();
        } catch (error) {
            alert(`Error: ${error.message}`);
            e.target.removeAttribute('aria-busy');
        }
    };

    storeFilter.addEventListener('change', fetchStockData);
    stockContainer.addEventListener('click', (e) => {
        if (e.target.classList.contains('update-stock-btn')) handleStockUpdate(e);
        if (e.target.classList.contains('set-primary-btn')) handleSetPrimary(e);
    });

    loadStores();
});