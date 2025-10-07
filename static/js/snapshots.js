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
        modal: document.getElementById('metrics-modal'),
        modalTitle: document.getElementById('modal-title'),
        modalBody: document.getElementById('modal-body'),
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

        const tableRows = snapshots.map(s => {
            // --- THIS IS THE FIX ---
            // Safely access nested properties, providing fallback values if data is missing.
            const variant = s.product_variant;
            const product = variant ? variant.product : null;

            const imageUrl = product ? product.image_url : '/static/img/placeholder.png';
            const productTitle = product ? product.title : '[Deleted Product]';
            const variantTitle = variant ? variant.title : '[Deleted Variant]';
            const variantId = variant ? variant.id : 'N/A';
            const sku = variant ? variant.sku : 'N/A';
            
            return `
                <tr>
                    <td>${s.date}</td>
                    <td>
                        <img src="${imageUrl}" class="product-image-compact" alt="Product image">
                    </td>
                    <td>
                        <strong>${productTitle}</strong><br>
                        <small>${variantTitle} (ID: ${variantId})</small>
                    </td>
                    <td><code>${sku || 'N/A'}</code></td>
                    <td>${s.on_hand}</td>
                    <td>
                        ${variant ? `<button class="outline" data-variant-id="${variantId}" data-product-title="${productTitle}">View Metrics</button>` : 'N/A'}
                    </td>
                </tr>
            `;
        }).join('');

        elements.container.innerHTML = `
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Image</th>
                        <th>Product</th>
                        <th>SKU</th>
                        <th>On Hand</th>
                        <th>Actions</th>
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
    
    const fetchAndShowMetrics = async (variantId, productTitle) => {
        elements.modalTitle.textContent = `Metrics for: ${productTitle}`;
        elements.modalBody.setAttribute('aria-busy', 'true');
        elements.modalBody.innerHTML = '<p>Loading metrics...</p>';
        elements.modal.showModal();

        try {
            const endDate = new Date().toISOString().split('T')[0];
            const startDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

            const response = await fetch(`/api/snapshots/metrics/${variantId}?start_date=${startDate}&end_date=${endDate}`);
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || 'Failed to fetch metrics.');
            }
            const metrics = await response.json();
            renderMetrics(metrics);
        } catch (error) {
            elements.modalBody.innerHTML = `<p style="color:red;">${error.message}</p>`;
        } finally {
            elements.modalBody.removeAttribute('aria-busy');
        }
    };

    const formatMetric = (value, decimals = 2, unit = '') => {
        if (value === null || value === undefined) return 'N/A';
        return `${parseFloat(value).toFixed(decimals)}${unit}`;
    };

    const renderMetrics = (metrics) => {
        elements.modalBody.innerHTML = `
            <div class="grid">
                <article>
                    <header><strong>Stock Levels</strong></header>
                    <p><strong>Avg Stock:</strong> ${formatMetric(metrics.average_stock_level)} units</p>
                    <p><strong>Min Stock:</strong> ${formatMetric(metrics.min_stock_level, 0)} units</p>
                    <p><strong>Max Stock:</strong> ${formatMetric(metrics.max_stock_level, 0)} units</p>
                    <p><strong>Stock Range:</strong> ${formatMetric(metrics.stock_range, 0)} units</p>
                    <p><strong>Std Deviation:</strong> ${formatMetric(metrics.stock_stddev)}</p>
                </article>
                <article>
                    <header><strong>Availability</strong></header>
                    <p><strong>Days Out of Stock:</strong> ${metrics.days_out_of_stock}</p>
                    <p><strong>Stockout Rate:</strong> ${formatMetric(metrics.stockout_rate, 2, '%')}</p>
                    <p><strong>Health Index:</strong> ${formatMetric(metrics.stock_health_index * 100, 2, '%')}</p>
                    <p><strong>Stability Index:</strong> ${formatMetric(metrics.stability_index, 2, '%')}</p>
                </article>
            </div>
            <div class="grid">
                <article>
                    <header><strong>Movement</strong></header>
                    <p><strong>Replenishment Days:</strong> ${metrics.replenishment_days}</p>
                    <p><strong>Depletion Days:</strong> ${metrics.depletion_days}</p>
                    <p><strong>Total Outflow (Sold):</strong> ${formatMetric(metrics.total_outflow, 0)} units</p>
                    <p><strong>Stock Turnover:</strong> ${formatMetric(metrics.stock_turnover)}</p>
                    <p><strong>Avg Days in Inventory:</strong> ${formatMetric(metrics.avg_days_in_inventory)}</p>
                </article>
                <article>
                    <header><strong>Stagnation</strong></header>
                    <p><strong>Dead Stock Days:</strong> ${metrics.dead_stock_days}</p>
                    <p><strong>Dead Stock Ratio:</strong> ${formatMetric(metrics.dead_stock_ratio, 2, '%')}</p>
                </article>
            </div>
             <div class="grid">
                <article>
                    <header><strong>Financials (Avg)</strong></header>
                    <p><strong>Inventory Value:</strong> ${formatMetric(metrics.avg_inventory_value)} RON</p>
                    <p><strong>Potential Sales Value:</strong> ${formatMetric(metrics.avg_sales_value)} RON</p>
                    <p><strong>Potential Gross Margin:</strong> ${formatMetric(metrics.avg_gross_margin_value)} RON</p>
                </article>
            </div>
        `;
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
    
    elements.container.addEventListener('click', (e) => {
        if (e.target.matches('button[data-variant-id]')) {
            const variantId = e.target.dataset.variantId;
            const productTitle = e.target.dataset.productTitle;
            fetchAndShowMetrics(variantId, productTitle);
        }
    });

    elements.modal.addEventListener('click', (e) => {
        if (e.target.matches('.close') || e.target === elements.modal) {
            elements.modal.close();
        }
    });

    loadStores();
    fetchSnapshots();
});