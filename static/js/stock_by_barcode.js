// static/js/stock_by_barcode.js
document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const searchInput = document.getElementById('product-search-input');
    const stockContainer = document.getElementById('stock-container');
    const modal = document.getElementById('manage-variants-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    // --- State ---
    let barcodeGroupsData = [];

    // --- Utility ---
    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    // --- Data Fetching ---
    const fetchStockData = async () => {
        stockContainer.setAttribute('aria-busy', 'true');
        const searchTerm = searchInput.value;
        const params = new URLSearchParams();
        if (searchTerm) {
            params.set('search', searchTerm);
        }

        try {
            const response = await fetch(`/api/stock/by-barcode?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch stock data.');
            barcodeGroupsData = await response.json();
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
            stockContainer.innerHTML = '<p>No products found matching your criteria.</p>';
            return;
        }

        const tableRows = barcodeGroupsData.map((group, index) => {
            const representativeStock = group.variants[0]?.total_available ?? 0;
            return `
                <tr data-group-index="${index}">
                    <td><img src="${group.primary_image_url || '/static/img/placeholder.png'}" alt="${group.primary_title}" class="product-image-compact"></td>
                    <td class="product-title-cell">
                        <strong>${group.primary_title}</strong><br>
                        <small>Barcode: <code>${group.barcode}</code></small>
                    </td>
                    <td>${group.variants.length}</td>
                    <td>
                        <form class="update-form-inline">
                            <input type="number" class="quantity-input-inline" value="${representativeStock}" required />
                            <button class="update-stock-btn-inline" data-barcode="${group.barcode}">Set</button>
                        </form>
                    </td>
                </tr>
            `;
        }).join('');

        stockContainer.innerHTML = `
            <table>
                <tbody>${tableRows}</tbody>
            </table>
        `;
    };
    
    const openManageModal = (groupIndex) => {
        const group = barcodeGroupsData[groupIndex];
        if (!group) return;
        modalTitle.textContent = `Manage Barcode: ${group.barcode}`;
        modalBody.innerHTML = `
            <h5>Variants Across All Stores</h5>
            <div class="barcode-group" data-barcode="${group.barcode}">
                ${group.variants.map(v => `
                    <div class="variant-row ${v.is_barcode_primary ? 'is-primary' : ''}">
                        <span><strong>${v.store_name}:</strong> ${v.product_title} (${v.sku || 'N/A'})</span>
                        <button class="set-primary-btn outline" data-variant-id="${v.variant_id}" ${v.is_barcode_primary ? 'disabled' : ''}>
                            ${v.is_barcode_primary ? 'Primary' : 'Set Primary'}
                        </button>
                    </div>
                `).join('')}
            </div>
            <small class="response-message"></small>
        `;
        modal.showModal();
    };

    // --- Event Handlers ---
    const handleStockUpdate = async (e) => {
        e.preventDefault();
        const button = e.target;
        const form = button.closest('form');
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
            // Briefly show success
            button.classList.add('success');
            setTimeout(() => button.classList.remove('success'), 1500);
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            button.removeAttribute('aria-busy');
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
            if (!response.ok) throw new Error('Failed to set primary variant.');
            modal.close();
            await fetchStockData(); // Refresh main table
        } catch (error) {
            alert(`Error: ${error.message}`);
        }
    };
    
    // --- Initial Setup & Event Listeners ---
    searchInput.addEventListener('input', debounce(fetchStockData, 400));
    
    document.body.addEventListener('click', (e) => {
        const row = e.target.closest('tr[data-group-index]');
        if (row && !e.target.closest('form')) {
            openManageModal(row.dataset.groupIndex);
        }
        if (e.target.matches('.close')) {
            modal.close();
        }
        if (e.target.matches('.set-primary-btn')) {
            handleSetPrimary(e);
        }
    });

    stockContainer.addEventListener('submit', (e) => {
        if (e.target.matches('.update-form-inline')) {
            handleStockUpdate(e);
        }
    });

    fetchStockData();
});