// static/js/snapshots.js
document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('snapshots-container'),
        startDateFilter: document.getElementById('start-date-filter'),
        endDateFilter: document.getElementById('end-date-filter'),
        storeFilter: document.getElementById('store-filter'),
        triggerBtn: document.getElementById('trigger-snapshot-btn'),
        prevButton: document.getElementById('prev-button'),
        nextButton: document.getElementById('next-button'),
        pageIndicator: document.getElementById('page-indicator'),
    };

    let state = {
        page: 1,
        limit: 25, // Lower limit for a wider table
        totalCount: 0,
        storeId: '',
        startDate: '',
        endDate: '',
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
        if (state.startDate) params.set('start_date', state.startDate);
        if (state.endDate) params.set('end_date', state.endDate);

        try {
            const response = await fetch(`/api/snapshots/?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch analytics.');
            const data = await response.json();
            state.totalCount = data.total_count;
            renderTable(data.snapshots);
            updatePagination();
        } catch (error) {
            elements.container.innerHTML = `<p style="color: red;">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const formatMetric = (value, decimals = 2, unit = '') => {
        if (value === null || value === undefined) return 'N/A';
        const num = parseFloat(value);
        if (isNaN(num)) return 'N/A';
        return `${num.toFixed(decimals)}${unit}`;
    };

    const renderTable = (snapshots) => {
        if (snapshots.length === 0) {
            elements.container.innerHTML = '<p>No snapshot data found for the selected criteria.</p>';
            return;
        }

        const tableHead = `
            <thead>
                <tr>
                    <th>Produs</th>
                    <th>Stoc Curent</th>
                    <th>Stoc Mediu</th>
                    <th>Stoc Min/Max</th>
                    <th>Variație Stoc</th>
                    <th>Zile Fără Stoc</th>
                    <th>Rată Epuizare</th>
                    <th>Rulaj Stoc</th>
                    <th>Zile Medii Stoc</th>
                    <th>Zile Stoc Mort</th>
                    <th>Rată Stoc Mort</th>
                    <th>Valoare Medie</th>
                    <th>Index Sănătate</th>
                </tr>
            </thead>
        `;

        const tableRows = snapshots.map(s => {
            const variant = s.product_variant;
            const product = variant ? variant.product : null;
            const metrics = s.metrics || {};

            const imageUrl = product ? product.image_url : '/static/img/placeholder.png';
            const productTitle = product ? product.title : '[Produs Șters]';
            const sku = variant ? variant.sku : 'N/A';

            return `
                <tr>
                    <td>
                        <div style="display: flex; align-items: center; gap: 1rem;">
                            <img src="${imageUrl}" class="product-image-compact" alt="Product image">
                            <div>
                                <strong>${productTitle}</strong><br>
                                <small>SKU: <code>${sku || 'N/A'}</code></small>
                            </div>
                        </div>
                    </td>
                    <td>${s.on_hand} buc</td>
                    <td>${formatMetric(metrics.average_stock_level, 1)} buc</td>
                    <td>${formatMetric(metrics.min_stock_level, 0)} / ${formatMetric(metrics.max_stock_level, 0)} buc</td>
                    <td>${formatMetric(metrics.stock_range, 0)} buc</td>
                    <td>${metrics.days_out_of_stock}</td>
                    <td>${formatMetric(metrics.stockout_rate, 2, '%')}</td>
                    <td>${formatMetric(metrics.stock_turnover, 2)}</td>
                    <td>${formatMetric(metrics.avg_days_in_inventory, 1)}</td>
                    <td>${metrics.dead_stock_days}</td>
                    <td>${formatMetric(metrics.dead_stock_ratio, 2, '%')}</td>
                    <td>${formatMetric(metrics.avg_inventory_value, 2, ' RON')}</td>
                    <td style="font-weight: bold;">${formatMetric(metrics.stock_health_index * 100, 1, '%')}</td>
                </tr>
            `;
        }).join('');

        elements.container.innerHTML = `
            <figure>
                <table role="grid">${tableHead}<tbody>${tableRows}</tbody></table>
            </figure>
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

    const setupInitialDates = () => {
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(endDate.getDate() - 30);

        elements.endDateFilter.value = endDate.toISOString().split('T')[0];
        elements.startDateFilter.value = startDate.toISOString().split('T')[0];
        state.endDate = elements.endDateFilter.value;
        state.startDate = elements.startDateFilter.value;
    };

    elements.startDateFilter.addEventListener('change', debounce(() => {
        state.startDate = elements.startDateFilter.value;
        state.page = 1;
        fetchSnapshots();
    }, 400));
    
    elements.endDateFilter.addEventListener('change', debounce(() => {
        state.endDate = elements.endDateFilter.value;
        state.page = 1;
        fetchSnapshots();
    }, 400));

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
        if (!confirm('Sunteți sigur că doriți să declanșați manual un snapshot acum?')) return;
        elements.triggerBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch('/api/snapshots/trigger', { method: 'POST' });
            if (!response.ok) throw new Error('Failed to trigger snapshot.');
            alert('Procesul de snapshot a început. Acesta va rula în fundal.');
        } catch (error) {
            alert(`Eroare: ${error.message}`);
        } finally {
            elements.triggerBtn.removeAttribute('aria-busy');
        }
    });

    loadStores();
    setupInitialDates();
    fetchSnapshots();
});