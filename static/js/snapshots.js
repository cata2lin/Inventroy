// static/js/snapshots.js
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
        thead: document.getElementById('table-head'),
    };

    let state = {
        page: 1,
        limit: 25,
        totalCount: 0,
        storeId: '',
        startDate: '',
        endDate: '',
        sortField: 'on_hand',
        sortOrder: 'desc',
        metricFilters: {} // { key: {min, max} }
    };

    const METRICS_FOR_FILTERS = [
        ['on_hand', 'Current Stock'],
        ['average_stock_level', 'Avg. Stock Level'],
        ['stockout_rate', 'Stockout Rate (%)'],
        ['dead_stock_ratio', 'Dead Stock Ratio (%)'],
        ['stock_turnover', 'Stock Turnover'],
        ['avg_days_in_inventory', 'Avg. Days in Inventory'],
        ['avg_inventory_value', 'Avg. Inventory Value'],
        ['stock_health_index', 'Health Index (0-1)'],
    ];

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(null, args), delay);
        };
    };

    const fetchSnapshots = async () => {
        elements.container.innerHTML = `<tr><td colspan="7" class="text-center" aria-busy="true">Loading analytics...</td></tr>`;

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
            if (val.min !== undefined && val.min !== '') params.set(`${key}_min`, val.min);
            if (val.max !== undefined && val.max !== '') params.set(`${key}_max`, val.max);
        });

        try {
            const response = await fetch(`/api/snapshots/?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch snapshots.');
            const data = await response.json();
            state.totalCount = data.total_count;
            renderTable(data.snapshots);
            updatePagination();
        } catch (err) {
            elements.container.innerHTML = `<tr><td colspan="7" class="text-center" style="color: var(--pico-color-red-500);">${err.message}</td></tr>`;
        }
    };

    const formatMetric = (value, decimals = 2, unit = '') => {
        if (value === null || value === undefined) return 'N/A';
        const num = parseFloat(value);
        return isNaN(num) ? 'N/A' : `${num.toFixed(decimals)}${unit}`;
    };

    const renderTable = (snapshots) => {
        if (!snapshots || snapshots.length === 0) {
            elements.container.innerHTML = `<tr><td colspan="7" class="text-center">No data found for the selected filters.</td></tr>`;
            return;
        }

        const rows = snapshots.map(s => {
            const m = s.metrics || {};
            const variant = s.product_variant || {};
            const product = variant.product || {};
            const imageUrl = product.image_url || 'https://via.placeholder.com/48';
            const title = product.title || '—';
            const sku = variant.sku || '—';

            return `
                <tr>
                    <td>
                        <div style="display: flex; align-items: center; gap: 0.75rem;">
                            <img src="${imageUrl}" style="width: 48px; height: 48px; object-fit: cover; border-radius: var(--pico-border-radius);" alt="${title}">
                            <div>
                                <strong>${title}</strong><br>
                                <small>SKU: ${sku}</small>
                            </div>
                        </div>
                    </td>
                    <td data-label="Current Stock">${s.on_hand ?? '—'} units</td>
                    <td data-label="Avg. Inv. Value">${formatMetric(m.avg_inventory_value, 2, ' RON')}</td>
                    <td data-label="Stockout Rate">${formatMetric(m.stockout_rate, 2, '%')}</td>
                    <td data-label="Dead Stock Ratio">${formatMetric(m.dead_stock_ratio, 2, '%')}</td>
                    <td data-label="Turnover">${formatMetric(m.stock_turnover, 2)}</td>
                    <td data-label="Health Index">${m.stock_health_index != null ? formatMetric((m.stock_health_index * 100), 1, '%') : 'N/A'}</td>
                </tr>
            `;
        }).join('');

        elements.container.innerHTML = rows;
    };

    const updatePagination = () => {
        const totalPages = Math.max(1, Math.ceil(state.totalCount / state.limit));
        elements.pageIndicator.textContent = `Page ${state.page} of ${totalPages}`;
        elements.prevButton.disabled = state.page <= 1;
        elements.nextButton.disabled = state.page >= totalPages;
    };

    const attachSortHandlers = () => {
        elements.thead.querySelectorAll('th.sortable').forEach(th => {
            th.style.cursor = 'pointer';
            th.addEventListener('click', () => {
                const field = th.getAttribute('data-sort');
                if (state.sortField === field) {
                    state.sortOrder = state.sortOrder === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sortField = field;
                    state.sortOrder = 'desc';
                }
                // Update visual indicator
                elements.thead.querySelectorAll('th.sortable').forEach(header => {
                    header.classList.remove('sorted-asc', 'sorted-desc');
                });
                th.classList.add(state.sortOrder === 'asc' ? 'sorted-asc' : 'sorted-desc');
                
                state.page = 1;
                fetchSnapshots();
            });
        });
    };

    // --- THIS IS THE FIX ---
    // The HTML for the metric filters is updated for a cleaner layout.
    const renderMetricFilters = () => {
        elements.metricFilters.innerHTML = METRICS_FOR_FILTERS.map(([key, label]) => `
            <div class="metric-filter-group">
                <label for="${key}-min">${label}</label>
                <div class="grid">
                    <input id="${key}-min" type="number" placeholder="Min" data-key="${key}" class="metric-filter-input">
                    <input id="${key}-max" type="number" placeholder="Max" data-key="${key}" class="metric-filter-input">
                </div>
            </div>
        `).join('');

        elements.metricFilters.querySelectorAll('.metric-filter-input').forEach(input => {
            input.addEventListener('input', debounce((e) => {
                const key = e.target.dataset.key;
                const minVal = document.getElementById(`${key}-min`).value;
                const maxVal = document.getElementById(`${key}-max`).value;
                state.metricFilters[key] = { min: minVal, max: maxVal };
                state.page = 1;
                fetchSnapshots();
            }, 400));
        });
    };
    
    const setupEventListeners = () => {
        elements.startDateFilter.addEventListener('change', () => {
            state.startDate = elements.startDateFilter.value;
            state.page = 1;
            fetchSnapshots();
        });
        elements.endDateFilter.addEventListener('change', () => {
            state.endDate = elements.endDateFilter.value;
            state.page = 1;
            fetchSnapshots();
        });
        elements.storeFilter.addEventListener('change', () => {
            state.storeId = elements.storeFilter.value;
            state.page = 1;
            fetchSnapshots();
        });
        elements.triggerBtn.addEventListener('click', async () => {
            if (!state.storeId) {
                alert('Please select a store before triggering a snapshot.');
                return;
            }
            elements.triggerBtn.setAttribute('aria-busy', 'true');
            try {
                const res = await fetch(`/api/snapshots/trigger?store_id=${state.storeId}`, { method: 'POST' });
                if (!res.ok) throw new Error('Failed to trigger snapshot.');
                alert('Snapshot triggered successfully! The data will be updated shortly.');
                setTimeout(fetchSnapshots, 2000); // Refresh data after a delay
            } catch (e) {
                alert(e.message);
            } finally {
                elements.triggerBtn.removeAttribute('aria-busy', 'false');
            }
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
    };

    const init = async () => {
        const today = new Date();
        const past = new Date();
        past.setDate(today.getDate() - 30);
        elements.endDateFilter.value = today.toISOString().split('T')[0];
        elements.startDateFilter.value = past.toISOString().split('T')[0];
        state.startDate = elements.startDateFilter.value;
        state.endDate = elements.endDateFilter.value;

        try {
            const res = await fetch('/api/config/stores');
            const stores = await res.json();
            stores.forEach(s => elements.storeFilter.add(new Option(s.name, s.id)));

            if (stores.length > 0) {
                state.storeId = stores[0].id;
                elements.storeFilter.value = stores[0].id;
            }

        } catch {
            console.error('Failed to load stores.');
            elements.container.innerHTML = `<tr><td colspan="7" class="text-center" style="color: var(--pico-color-red-500);">Failed to load stores. Cannot fetch analytics.</td></tr>`;
            return;
        }

        renderMetricFilters();
        attachSortHandlers();
        setupEventListeners();

        fetchSnapshots();
    };

    init();
});