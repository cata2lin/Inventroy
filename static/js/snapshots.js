document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('snapshots-container'),
        table: document.getElementById('snapshots-table'),
        startDateFilter: document.getElementById('start-date-filter'),
        endDateFilter: document.getElementById('end-date-filter'),
        storeFilter: document.getElementById('store-filter'),
        triggerBtn: document.getElementById('trigger-snapshot-btn'),
        prevButton: document.getElementById('prev-button'),
        nextButton: document.getElementById('next-button'),
        pageIndicator: document.getElementById('page-indicator'),
        metricFilters: document.getElementById('metric-filters'),
    };

    let state = {
        page: 1,
        limit: 25,
        totalCount: 0,
        storeId: '',
        startDate: '',
        endDate: '',
        sortField: 'date',
        sortOrder: 'desc',
        metricFilters: {}
    };

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const fetchSnapshots = async () => {
        elements.container.innerHTML = `<tr><td colspan="13" class="text-center">Loading...</td></tr>`;

        const params = new URLSearchParams({
            skip: (state.page - 1) * state.limit,
            limit: state.limit,
            sort_field: state.sortField,
            sort_order: state.sortOrder,
        });
        if (state.storeId) params.set('store_id', state.storeId);
        if (state.startDate) params.set('start_date', state.startDate);
        if (state.endDate) params.set('end_date', state.endDate);
        Object.entries(state.metricFilters).forEach(([key, val]) => {
            if (val.min !== undefined) params.set(`${key}_min`, val.min);
            if (val.max !== undefined) params.set(`${key}_max`, val.max);
        });

        try {
            const response = await fetch(`/api/snapshots/?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch snapshots.');
            const data = await response.json();
            state.totalCount = data.total_count;
            renderTable(data.snapshots);
            updatePagination();
        } catch (err) {
            elements.container.innerHTML = `<tr><td colspan="13" class="text-center text-red-600">${err.message}</td></tr>`;
        }
    };

    const formatMetric = (value, decimals = 2, unit = '') => {
        if (value === null || value === undefined) return 'N/A';
        return `${parseFloat(value).toFixed(decimals)}${unit}`;
    };

    const renderTable = (snapshots) => {
        if (!snapshots.length) {
            elements.container.innerHTML = `<tr><td colspan="13" class="text-center">No data found.</td></tr>`;
            return;
        }

        const rows = snapshots.map(s => {
            const variant = s.product_variant;
            const product = variant ? variant.product : null;
            const imageUrl = product?.image_url || '/static/img/placeholder.png';
            const title = product?.title || '[Produs Șters]';
            const sku = variant?.sku || 'N/A';
            const m = s.metrics || {};

            return `
                <tr>
                    <td>
                        <div class="flex items-center gap-2">
                            <img src="${imageUrl}" class="w-12 h-12 object-cover rounded" alt="${title}">
                            <div>
                                <strong>${title}</strong><br>
                                <small>SKU: ${sku}</small>
                            </div>
                        </div>
                    </td>
                    <td>${s.on_hand} buc</td>
                    <td>${formatMetric(m.average_stock_level, 1)} buc</td>
                    <td>${formatMetric(m.min_stock_level, 0)} / ${formatMetric(m.max_stock_level, 0)}</td>
                    <td>${formatMetric(m.stock_range, 0)}</td>
                    <td>${m.days_out_of_stock}</td>
                    <td>${formatMetric(m.stockout_rate, 2, '%')}</td>
                    <td>${formatMetric(m.stock_turnover, 2)}</td>
                    <td>${formatMetric(m.avg_days_in_inventory, 1)}</td>
                    <td>${m.dead_stock_days}</td>
                    <td>${formatMetric(m.dead_stock_ratio, 2, '%')}</td>
                    <td>${formatMetric(m.avg_inventory_value, 2, ' RON')}</td>
                    <td>${formatMetric(m.stock_health_index * 100, 1, '%')}</td>
                </tr>
            `;
        }).join('');

        elements.container.innerHTML = rows;
    };

    const updatePagination = () => {
        const totalPages = Math.ceil(state.totalCount / state.limit) || 1;
        elements.pageIndicator.textContent = `Page ${state.page} of ${totalPages}`;
        elements.prevButton.disabled = state.page <= 1;
        elements.nextButton.disabled = state.page >= totalPages;
    };

    // --------------------
    // Event Listeners
    // --------------------
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
        if (!confirm('Trigger snapshot now?')) return;
        elements.triggerBtn.setAttribute('aria-busy', 'true');
        try {
            const res = await fetch('/api/snapshots/trigger', { method: 'POST' });
            if (!res.ok) throw new Error('Failed to trigger snapshot.');
            alert('Snapshot process started in background.');
        } catch (err) {
            alert(err.message);
        } finally {
            elements.triggerBtn.removeAttribute('aria-busy');
        }
    });

    // --------------------
    // Sorting by header click
    // --------------------
    document.querySelectorAll('.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const field = th.dataset.sort;
            if (state.sortField === field) {
                state.sortOrder = state.sortOrder === 'asc' ? 'desc' : 'asc';
            } else {
                state.sortField = field;
                state.sortOrder = 'asc';
            }
            fetchSnapshots();
        });
    });

    // --------------------
    // Initial load
    // --------------------
    const initDates = () => {
        const today = new Date();
        const past = new Date();
        past.setDate(today.getDate() - 30);
        elements.endDateFilter.value = today.toISOString().split('T')[0];
        elements.startDateFilter.value = past.toISOString().split('T')[0];
        state.startDate = elements.startDateFilter.value;
        state.endDate = elements.endDateFilter.value;
    };

    const loadStores = async () => {
        try {
            const res = await fetch('/api/config/stores');
            const stores = await res.json();
            stores.forEach(s => {
                elements.storeFilter.add(new Option(s.name, s.id));
            });
        } catch {
            console.error('Failed to load stores.');
        }
    };

    initDates();
    loadStores();
    fetchSnapshots();
});
