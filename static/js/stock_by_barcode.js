// static/js/stock_by_barcode.js
document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const storeFilter = document.getElementById('store-filter');
    const stockContainer = document.getElementById('stock-container');
    const modal = document.getElementById('manage-variants-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    // --- State ---
    let barcodeGroupsData = []; // Store the fetched data globally
    let locationsMap = {};

    // --- Data Fetching ---
    const loadStores = async () => {
        try {
            const response = await fetch('/api/config/stores');
            const stores = await response.json();
            stores.forEach(store => storeFilter.add(new Option(store.name, store.id)));
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
            barcodeGroupsData = await response.json(); // Store data
            renderTableView();
        } catch (error) {
            stockContainer.innerHTML = `<p style="color:red;">${error.message}</p>`;
        } finally {
            stockContainer.removeAttribute('aria-busy');
        }
    };

    // --- UI Rendering ---
    const renderTableView = () => {
        if (barcodeGroupsData.length === 0) {
            stockContainer.innerHTML = '<p>No products with barcodes found for this store.</p>';
            return;
        }

        // Collect all unique locations for the dropdowns
        locationsMap = {};
        barcodeGroupsData.forEach(group => group.variants.forEach(variant => variant.locations.forEach(loc => {
            if (loc.location_gid) locationsMap[loc.location_gid] = loc.name;
        })));

        const tableRows = barcodeGroupsData.map((group, index) => {
            const totalStock = group.variants.reduce((sum, v) => sum + v.total_available, 0);
            return `
                <tr>
                    <td><img src="${group.primary_image_url || '/static/img/placeholder.png'}" alt="${group.primary_title}" class="product-image"></td>
                    <td>
                        <strong>${group.primary_title}</strong><br>
                        <small>Barcode: <code>${group.barcode}</code></small>
                    </td>
                    <td>${group.variants.length}</td>
                    <td>${totalStock}</td>
                    <td><button class="outline" data-group-index="${index}">Manage</button></td>
                </tr>
            `;
        }).join('');

        stockContainer.innerHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Image</th>
                        <th>Primary Product</th>
                        <th>Variants</th>
                        <th>Total Stock</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        `;
    };
    
    const openManageModal = (groupIndex) => {
        const group = barcodeGroupsData[groupIndex];
        if (!group) return;

        modalTitle.textContent = `Manage Barcode: ${group.barcode}`;
        
        const locationOptions = Object.entries(locationsMap)
            .map(([gid, name]) => `<option value="${gid}">${name}</option>`).join('');

        modalBody.innerHTML = `
            <h5>Variants</h5>
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
                <h5>Update Stock</h5>
                <div class="update-form grid">
                    <select class="location-select" required>${locationOptions}</select>
                    <input type="number" class="quantity-input" placeholder="New Qty" required />
                    <button class="update-stock-btn">Update</button>
                </div>
                <small class="response-message"></small>
            </div>
        `;
        modal.showModal();
    };


    // --- Event Handlers ---
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
                body: JSON.stringify({ barcode, location_gid: locationGid, quantity: parseInt(quantity) })
            });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Update failed.');
            
            messageEl.textContent = 'Stock updated successfully!';
            messageEl.style.color = 'green';
            setTimeout(async () => {
                await fetchStockData(); // Refresh main table
                modal.close(); // Close modal on success
            }, 1500);
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
            if (!response.ok) {
                const result = await response.json();
                throw new Error(result.detail || 'Failed to set primary variant.');
            }
            
            await fetchStockData(); // Refresh main table to show new primary
            modal.close();
        } catch (error) {
            alert(`Error: ${error.message}`);
            e.target.removeAttribute('aria-busy');
        }
    };
    
    // --- Initial Setup & Event Listeners ---
    storeFilter.addEventListener('change', fetchStockData);
    
    // Main listener for the page
    document.body.addEventListener('click', (e) => {
        // For opening the modal
        if (e.target.matches('.table-container button[data-group-index]')) {
            openManageModal(e.target.dataset.groupIndex);
        }
        // For closing the modal
        if (e.target.matches('.close')) {
            modal.close();
        }
        // For actions inside the modal
        if (e.target.matches('.update-stock-btn')) {
            handleStockUpdate(e);
        }
        if (e.target.matches('.set-primary-btn')) {
            handleSetPrimary(e);
        }
    });

    loadStores();
});