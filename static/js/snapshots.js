// static/js/snapshots.js
document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('snapshots-container'),
        dateFilter: document.getElementById('date-filter'),
        storeFilter: document.getElementById('store-filter'),
        triggerBtn: document.getElementById('trigger-snapshot-btn'),
        prevButton: document.getElementById('prev-button'),
        nextButton: document.getElementById('next-button'),
        pageIndicator: document.getElementById('page-indicator'),
    };

    let state = {
        page: 1,
        limit: 50,
        totalCount: 0,
        storeId: '',
        snapshotDate: '',
    };

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const fetchSnapshots = async () => {
        elements.container.setAttribute('aria-busy', 'true');
        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit,
            limit: state.limit,
        });
        if (state.storeId) params.set('store_id', state.storeId);
        if (state.snapshotDate) params.set('snapshot_date', state.snapshotDate);

        try {
            const response = await fetch(`/api/snapshots/?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch snapshots.');
            const data = await response.json();
            state.totalCount = data.total_count;
            renderSnapshots(data.snapshots);
            updatePagination();
        } catch (error) {
            elements.container.innerHTML = `<p style="color: red;">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const renderSnapshots = (snapshots) => {
        if (snapshots.length === 0) {
            elements.container.innerHTML = '<p>No snapshots found matching your criteria.</p>';
            return;
        }

        const tableRows = snapshots.map(s => `
            <tr>
                <td>${s.date}</td>
                <td>
                    <img src="${s.product_variant.product.image_url || '/static/img/placeholder.png'}" class="product-image-compact" alt="Product image">
                </td>
                <td>
                    <strong>${s.product_variant.product.title}</strong><br>
                    <small>${s.product_variant.title}</small>
                </td>
                <td><code>${s.product_variant.sku || 'N/A'}</code></td>
                <td>${s.on_hand}</td>
            </tr>
        `).join('');

        elements.container.innerHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Image</th>
                        <th>Product</th>
                        <th>SKU</th>
                        <th>On Hand</th>
                    </tr>
                </thead>
                <tbody>${tableRows}</tbody>
            </table>
        `;
    };

    const updatePagination = () => {
        const totalPages = Math.ceil(state.totalCount / state.limit) || 1;
        elements.pageIndicator.textContent = `Page ${state.page} of ${totalPages}`;
        elements.prevButton.disabled = state.page <= 1;
        elements.nextButton.disabled = state.page >= totalPages;
    };

    const loadStores = async () => {
        try {
            const response = await fetch('/api/config/stores');
            const stores = await response.json();
            stores.forEach(store => {
                elements.storeFilter.add(new Option(store.name, store.id));
            });
        } catch (error) {
            console.error('Failed to load stores for filter.');
        }
    };

    elements.dateFilter.addEventListener('change', () => {
        state.snapshotDate = elements.dateFilter.value;
        state.page = 1;
        fetchSnapshots();
    });

    elements.storeFilter.addEventListener('change', () => {
        state.storeId = elements.storeFilter.value;
        state.page = 1;
        fetchSnapshots();
    });

    elements.prevButton.addEventListener('click', () => {
        if (state.page > 1) {
            state.page--;
            fetchSnapshots();
        }
    });

    elements.nextButton.addEventListener('click', () => {
        const totalPages = Math.ceil(state.totalCount / state.limit);
        if (state.page < totalPages) {
            state.page++;
            fetchSnapshots();
        }
    });

    elements.triggerBtn.addEventListener('click', async () => {
        if (!confirm('Are you sure you want to trigger a manual snapshot now?')) return;
        elements.triggerBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch('/api/snapshots/trigger', { method: 'POST' });
            if (!response.ok) throw new Error('Failed to trigger snapshot.');
            alert('Snapshot process started successfully. It will run in the background.');
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            elements.triggerBtn.removeAttribute('aria-busy');
        }
    });

    loadStores();
    fetchSnapshots();
});