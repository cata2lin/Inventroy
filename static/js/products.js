// static/js/products.js

document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('products-container'),
        searchInput: document.getElementById('search-input'),
        storeFilter: document.getElementById('store-filter'),
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
    };

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const fetchProducts = async () => {
        elements.container.setAttribute('aria-busy', 'true');
        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit,
            limit: state.limit,
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
            elements.container.innerHTML = `<p style="color: red;">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const renderProducts = (products) => {
        if (products.length === 0) {
            elements.container.innerHTML = '<p>No products found matching your criteria.</p>';
            return;
        }
        let content = '';
        products.forEach(product => {
            content += `
                <details>
                    <summary>
                        <div class="product-summary">
                            <img src="${product.image_url || '/static/img/placeholder.png'}" alt="${product.title}" width="50">
                            <strong>${product.title}</strong>
                            <span>(${product.status || 'N/A'})</span>
                        </div>
                    </summary>
                    <table>
                        <thead><tr><th>Variant</th><th>SKU</th><th>Barcode</th><th>Price</th><th>Stock</th></tr></thead>
                        <tbody>
                            ${product.variants.map(v => `
                                <tr>
                                    <td>${v.title}</td>
                                    <td>${v.sku || ''}</td>
                                    <td>${v.barcode || ''}</td>
                                    <td>${v.price || '0.00'}</td>
                                    <td>${v.inventory_quantity || 0}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </details>
            `;
        });
        elements.container.innerHTML = content;
    };

    const updatePagination = () => {
        const totalPages = Math.ceil(state.totalCount / state.limit) || 1;
        elements.pageIndicator.textContent = `Page ${state.page} of ${totalPages}`;
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