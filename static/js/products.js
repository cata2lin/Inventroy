// static/js/products.js

document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('products-container'),
        searchInput: document.getElementById('search-input'),
        storeFilter: document.getElementById('store-filter'),
        sortSelect: document.getElementById('sort-select'),
        prevButton: document.getElementById('prev-button'),
        nextButton: document.getElementById('next-button'),
        pageIndicator: document.getElementById('page-indicator'),
    };

    let state = {
        page: 1,
        limit: 50,
        totalCount: 0,
        storeId: '',
        search: '',
        sortField: 'title',
        sortOrder: 'asc',
    };

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const getStatusBadge = (status) => {
        if (!status) return '<span class="badge badge-active">Active</span>';
        const s = status.toLowerCase();
        if (s === 'active') return '<span class="badge badge-active">Active</span>';
        if (s === 'draft') return '<span class="badge badge-draft">Draft</span>';
        if (s === 'archived') return '<span class="badge badge-archived">Archived</span>';
        return `<span class="badge">${status}</span>`;
    };

    const getStockBadge = (stock) => {
        if (stock <= 0) return `<span class="badge badge-danger">${stock} in stock</span>`;
        if (stock <= 5) return `<span class="badge badge-warning">${stock} in stock</span>`;
        return `<span class="badge badge-success">${stock} in stock</span>`;
    };

    const fetchProducts = async () => {
        elements.container.setAttribute('aria-busy', 'true');
        elements.container.innerHTML = '';

        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit,
            limit: state.limit,
            sort_field: state.sortField,
            sort_order: state.sortOrder,
        });
        if (state.storeId) params.set('store_id', state.storeId);
        if (state.search) params.set('search', state.search);

        try {
            const response = await fetch(`${API_ENDPOINTS.getProducts}?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch products.');
            const data = await response.json();
            state.totalCount = data.total_count;
            renderProducts(data.products);
            updatePagination();
        } catch (error) {
            elements.container.innerHTML = `<p style="color: var(--color-danger);">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const renderProducts = (products) => {
        if (products.length === 0) {
            elements.container.innerHTML = '<p style="padding: 2rem; text-align: center;">No products found matching your criteria.</p>';
            return;
        }
        let content = '';
        products.forEach(product => {
            let totalStock = 0;
            if (product.variants) {
                product.variants.forEach(v => {
                    if (v.inventory_levels) {
                        v.inventory_levels.forEach(il => {
                            totalStock += il.available || 0;
                        });
                    }
                });
            }

            const isDeleted = product.deleted_at !== null && product.deleted_at !== undefined;

            content += `
                <details style="${isDeleted ? 'opacity: 0.5;' : ''}">
                    <summary style="padding: 0.75rem 1rem; cursor: pointer; border-radius: var(--radius-md); transition: background var(--transition-fast);">
                        <div style="display: flex; align-items: center; gap: 0.75rem; width: 100%;">
                            <img src="${product.image_url || '/static/img/placeholder.png'}" 
                                 alt="${product.title}" 
                                 class="product-img"
                                 onerror="this.style.display='none'">
                            <div style="flex: 1;">
                                <strong>${product.title || 'Untitled'}</strong>
                                <small style="color: var(--color-text-muted);"> (${product.variants?.length || 0} variants)</small>
                                ${isDeleted ? '<small class="badge badge-danger" style="margin-left: 0.5rem;">Soft-Deleted</small>' : ''}
                            </div>
                            <div style="text-align: right; display: flex; gap: 0.5rem; align-items: center;">
                                ${getStatusBadge(product.status)}
                                ${getStockBadge(totalStock)}
                            </div>
                        </div>
                    </summary>
                    <div style="padding: 0 1rem 1rem;">
                        <table>
                            <thead><tr><th>Variant</th><th>SKU</th><th>Barcode</th><th>Price</th><th>Stock</th></tr></thead>
                            <tbody>
                                ${(product.variants || []).map(v => {
                    const variantStock = (v.inventory_levels || []).reduce((sum, il) => sum + (il.available || 0), 0);
                    return `
                                    <tr>
                                        <td>${v.title || 'Default'}</td>
                                        <td>${v.sku || '<span style="opacity:0.3">—</span>'}</td>
                                        <td>${v.barcode ? '<code>' + v.barcode + '</code>' : '<span style="opacity:0.3">—</span>'}</td>
                                        <td>${v.price ? parseFloat(v.price).toFixed(2) : '—'}</td>
                                        <td class="${variantStock <= 0 ? 'stock-urgent' : variantStock <= 5 ? 'stock-warning' : ''}">${variantStock}</td>
                                    </tr>
                                `}).join('')}
                            </tbody>
                        </table>
                    </div>
                </details>
            `;
        });
        elements.container.innerHTML = content;
    };

    const updatePagination = () => {
        const totalPages = Math.ceil(state.totalCount / state.limit) || 1;
        elements.pageIndicator.textContent = `Page ${state.page} of ${totalPages} (${state.totalCount} products)`;
        elements.prevButton.disabled = state.page <= 1;
        elements.nextButton.disabled = state.page >= totalPages;
    };

    const loadStores = async () => {
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            const stores = await response.json();
            stores.forEach(store => {
                const option = new Option(store.name, store.id);
                elements.storeFilter.add(option);
            });
        } catch (error) {
            console.error('Failed to load stores for filter.');
        }
    };

    // Event listeners
    elements.searchInput.addEventListener('input', debounce(() => {
        state.search = elements.searchInput.value;
        state.page = 1;
        fetchProducts();
    }, 400));

    elements.storeFilter.addEventListener('change', () => {
        state.storeId = elements.storeFilter.value;
        state.page = 1;
        fetchProducts();
    });

    elements.sortSelect.addEventListener('change', () => {
        const [field, order] = elements.sortSelect.value.split('-');
        state.sortField = field;
        state.sortOrder = order;
        state.page = 1;
        fetchProducts();
    });

    elements.prevButton.addEventListener('click', () => {
        if (state.page > 1) {
            state.page--;
            fetchProducts();
        }
    });

    elements.nextButton.addEventListener('click', () => {
        const totalPages = Math.ceil(state.totalCount / state.limit);
        if (state.page < totalPages) {
            state.page++;
            fetchProducts();
        }
    });

    loadStores();
    fetchProducts();
});