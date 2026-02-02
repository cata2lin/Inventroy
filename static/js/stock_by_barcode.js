// static/js/stock_by_barcode.js
document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const filters = {
        search: document.getElementById('product-search-input'),
        store: document.getElementById('store-filter'),
        sortSelect: document.getElementById('sort-select'),
        minStock: document.getElementById('min-stock'),
        maxStock: document.getElementById('max-stock'),
        minRetail: document.getElementById('min-retail'),
        maxRetail: document.getElementById('max-retail'),
    };
    const dashboard = {
        stock: document.getElementById('metric-total-stock'),
        inventoryValue: document.getElementById('metric-total-inventory-value'),
        retailValue: document.getElementById('metric-total-retail-value'),
    };
    const stockContainer = document.getElementById('stock-container');
    const modal = document.getElementById('manage-variants-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    // --- State ---
    let barcodeGroupsData = [];
    let sortField = 'title';
    let sortOrder = 'asc';

    // --- Utility ---
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const currencyFormatter = (amount) => {
        return new Intl.NumberFormat('ro-RO', { style: 'currency', currency: 'RON' }).format(amount);
    };

    // --- Data Fetching ---
    const fetchStockData = async () => {
        stockContainer.setAttribute('aria-busy', 'true');
        const params = new URLSearchParams();
        if (filters.search.value) params.set('search', filters.search.value);
        if (filters.store.value) params.set('store_id', filters.store.value);
        if (filters.minStock.value) params.set('min_stock', filters.minStock.value);
        if (filters.maxStock.value) params.set('max_stock', filters.maxStock.value);
        if (filters.minRetail.value) params.set('min_retail', filters.minRetail.value);
        if (filters.maxRetail.value) params.set('max_retail', filters.maxRetail.value);
        params.set('sort_field', sortField);
        params.set('sort_order', sortOrder);

        try {
            const response = await fetch(`/api/stock/by-barcode?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch stock data.');
            const data = await response.json();
            barcodeGroupsData = data.results;
            renderTableView();
            updateDashboard(data.metrics);
        } catch (error) {
            stockContainer.innerHTML = `<p style="color:red;">${error.message}</p>`;
        } finally {
            stockContainer.removeAttribute('aria-busy');
        }
    };

    const loadStores = async () => {
        try {
            const response = await fetch('/api/config/stores');
            const stores = await response.json();
            stores.forEach(store => filters.store.add(new Option(store.name, store.id)));
        } catch (error) {
            console.error('Failed to load stores:', error);
        }
    };

    // --- UI Rendering ---
    const updateDashboard = (metrics) => {
        dashboard.stock.textContent = metrics.total_stock.toLocaleString();
        dashboard.inventoryValue.textContent = currencyFormatter(metrics.total_inventory_value);
        dashboard.retailValue.textContent = currencyFormatter(metrics.total_retail_value);
    };

    const renderTableView = () => {
        if (barcodeGroupsData.length === 0) {
            stockContainer.innerHTML = '<p>No products found matching your criteria.</p>';
            return;
        }

        const tableRows = barcodeGroupsData.map((group, index) => `
            <tr data-group-index="${index}">
                <td><img src="${group.primary_image_url || '/static/img/placeholder.png'}" class="product-image-compact" alt="Primary product image"></td>
                <td class="product-title-cell">
                    <strong>${group.primary_title}</strong><br>
                    <small>Barcode: <code>${group.barcode}</code></small>
                </td>
                <td>${group.variants.length}</td>
                <td>${group.total_stock}</td>
                <td>${currencyFormatter(group.total_retail_value)}</td>
                <td>
                    <form class="update-form-inline">
                        <input type="number" class="quantity-input-inline" value="${group.total_stock}" required />
                        <button type="submit" class="update-stock-btn-inline" data-barcode="${group.barcode}">Set</button>
                    </form>
                </td>
            </tr>
        `).join('');

        stockContainer.innerHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Image</th>
                        <th>Primary Product</th>
                        <th>Variants</th>
                        <th>Stock Level</th>
                        <th>Total Retail (RON)</th>
                        <th>Set Stock</th>
                    </tr>
                </thead>
                <tbody>${tableRows}</tbody>
            </table>
        `;
    };

    const openManageModal = (groupIndex) => {
        const group = barcodeGroupsData[groupIndex];
        if (!group) return;
        modalTitle.textContent = `Set Primary for Barcode: ${group.barcode}`;
        modalBody.innerHTML = `
            <p><small>Click the variant you want to set as the primary display.</small></p>
            <div class="variants-grid">
                ${group.variants.map(v => `
                    <div class="variant-card ${v.is_barcode_primary ? 'is-primary' : ''}" data-variant-id="${v.variant_id}">
                        <img src="${v.image_url || '/static/img/placeholder.png'}" alt="${v.product_title}">
                        <div class="variant-card-body">
                            <strong>${v.store_name}</strong>
                            <p>${v.product_title}</p>
                            ${v.is_barcode_primary ? '<small class="primary-badge">Primary</small>' : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
        modal.showModal();
    };

    // --- Event Handlers ---
    const handleStockUpdate = async (e) => {
        e.preventDefault();
        const form = e.target;
        const button = form.querySelector('button');
        const barcode = button.dataset.barcode;
        const quantityInput = form.querySelector('.quantity-input-inline');
        const quantity = quantityInput.value;

        if (!barcode || quantity === '') return;

        button.setAttribute('aria-busy', 'true');

        try {
            const response = await fetch(`/api/stock/bulk-update`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ barcode, quantity: parseInt(quantity) })
            });
            const result = await response.json();
            if (!response.ok) {
                const errorMsg = result.detail.errors ? result.detail.errors.join('\\n') : (result.detail.message || JSON.stringify(result.detail));
                throw new Error(errorMsg);
            }
            button.classList.add('success');
            setTimeout(() => {
                button.classList.remove('success');
                quantityInput.value = quantity;
            }, 1500);
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            button.removeAttribute('aria-busy');
        }
    };

    const handleSetPrimary = async (e) => {
        const card = e.target.closest('.variant-card');
        if (!card) return;
        const variantId = card.dataset.variantId;
        if (!variantId) return;

        card.setAttribute('aria-busy', 'true');

        try {
            const response = await fetch('/api/stock/set-primary', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ variant_id: parseInt(variantId) })
            });
            if (!response.ok) throw new Error('Failed to set primary variant.');
            modal.close();
            await fetchStockData();
        } catch (error) {
            alert(`Error: ${error.message}`);
            card.removeAttribute('aria-busy');
        }
    };

    // --- Initial Setup & Event Listeners ---
    // Filter inputs with debounce
    [filters.search, filters.minStock, filters.maxStock, filters.minRetail, filters.maxRetail].forEach(el => {
        if (el) el.addEventListener('input', debounce(fetchStockData, 400));
    });

    // Store and sort - immediate change
    if (filters.store) filters.store.addEventListener('change', fetchStockData);
    if (filters.sortSelect) {
        filters.sortSelect.addEventListener('change', () => {
            const [field, order] = filters.sortSelect.value.split('-');
            sortField = field;
            sortOrder = order;
            fetchStockData();
        });
    }

    stockContainer.addEventListener('click', (e) => {
        if (e.target.classList.contains('product-image-compact')) {
            const row = e.target.closest('tr[data-group-index]');
            if (row) {
                openManageModal(row.dataset.groupIndex);
            }
        }
    });

    // CORRECTED: The listener is now attached to the container and listens for 'submit' events.
    stockContainer.addEventListener('submit', (e) => {
        if (e.target.matches('.update-form-inline')) {
            handleStockUpdate(e);
        }
    });

    modalBody.addEventListener('click', handleSetPrimary);
    modal.addEventListener('click', (e) => {
        if (e.target.matches('.close') || e.target === modal) {
            modal.close();
        }
    });

    loadStores();
    fetchStockData();
});