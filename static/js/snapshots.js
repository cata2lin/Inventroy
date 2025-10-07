
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
        sortField: 'date',
        sortOrder: 'desc',
        metricFilters: {} // { key: {min, max} }
    };

    const METRICS = [
        ['average_stock_level','Stoc Mediu'],
        ['min_stock_level','Stoc Min'],
        ['max_stock_level','Stoc Max'],
        ['stock_range','Variație Stoc'],
        ['stock_stddev','StdDev Stoc'],
        ['days_out_of_stock','Zile Fără Stoc'],
        ['stockout_rate','Rată Epuizare %'],
        ['replenishment_days','Zile Realimentare'],
        ['depletion_days','Zile Epuizare'],
        ['total_outflow','Ieșiri Totale'],
        ['stock_turnover','Rulaj'],
        ['avg_days_in_inventory','Zile Medii Stoc'],
        ['dead_stock_days','Zile Stoc Mort'],
        ['dead_stock_ratio','Rată Stoc Mort %'],
        ['avg_inventory_value','Valoare Medie'],
        ['stock_health_index','Index Sănătate (0-1)'],
    ];

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(null, args), delay);
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
            elements.container.innerHTML = `<tr><td colspan="13" class="text-center text-red-600">${err.message}</td></tr>`;
        }
    };

    const formatMetric = (value, decimals = 2, unit = '') => {
        if (value === null || value === undefined) return 'N/A';
        return `${parseFloat(value).toFixed(decimals)}${unit}`;
    };

    const renderTable = (snapshots) => {
        if (!snapshots || snapshots.length === 0) {
            elements.container.innerHTML = `<tr><td colspan="13" class="text-center">No data.</td></tr>`;
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
                        <div class="flex items-center gap-2">
                            <img src="${imageUrl}" class="w-12 h-12 object-cover rounded" alt="${title}">
                            <div>
                                <strong>${title}</strong><br>
                                <small>SKU: ${sku}</small>
                            </div>
                        </div>
                    </td>
                    <td>${s.on_hand ?? '—'} buc</td>
                    <td>${formatMetric(m.average_stock_level, 1)} buc</td>
                    <td>${formatMetric(m.min_stock_level, 0)} / ${formatMetric(m.max_stock_level, 0)}</td>
                    <td>${formatMetric(m.stock_range, 0)}</td>
                    <td>${m.days_out_of_stock ?? '—'}</td>
                    <td>${formatMetric(m.stockout_rate, 2, '%')}</td>
                    <td>${formatMetric(m.stock_turnover, 2)}</td>
                    <td>${formatMetric(m.avg_days_in_inventory, 1)}</td>
                    <td>${m.dead_stock_days ?? '—'}</td>
                    <td>${formatMetric(m.dead_stock_ratio, 2, '%')}</td>
                    <td>${formatMetric(m.avg_inventory_value, 2, ' RON')}</td>
                    <td>${m.stock_health_index != null ? formatMetric((m.stock_health_index * 100), 1, '%') : 'N/A'}</td>
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

    // Sorting by clicking table headers
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
                state.page = 1;
                fetchSnapshots();
            });
        });
    };

    // Metric filter inputs
    const renderMetricFilters = () => {
        elements.metricFilters.innerHTML = METRICS.map(([key, label]) => {
            const idMin = `${key}-min`;
            const idMax = `${key}-max`;
            return `
                <div class="flex flex-col">
                    <label class="text-sm font-medium">${label}</label>
                    <div class="flex gap-2">
                        <input id="${idMin}" type="number" inputmode="decimal" placeholder="min" class="w-full">
                        <input id="${idMax}" type="number" inputmode="decimal" placeholder="max" class="w-full">
                    </div>
                </div>
            `;
        }).join('');

        METRICS.forEach(([key]) => {
            const minEl = document.getElementById(`${key}-min`);
            const maxEl = document.getElementById(`${key}-max`);
            const handler = debounce(() => {
                state.metricFilters[key] = {
                    min: minEl.value,
                    max: maxEl.value,
                };
                state.page = 1;
                fetchSnapshots();
            }, 350);
            minEl.addEventListener('input', handler);
            maxEl.addEventListener('input', handler);
        });
    };

    // Date and store filters
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

    // Trigger snapshot
    elements.triggerBtn.addEventListener('click', async () => {
        if (!state.storeId) {
            alert('Selectați un magazin înainte de a rula snapshot.');
            return;
        }
        elements.triggerBtn.disabled = true;
        try {
            const res = await fetch(`/api/snapshots/trigger?store_id=${encodeURIComponent(state.storeId)}`, {
                method: 'POST'
            });
            if (!res.ok) throw new Error('Eroare la declanșarea snapshot-ului');
            await fetchSnapshots();
        } catch (e) {
            alert(e.message);
        } finally {
            elements.triggerBtn.disabled = false;
        }
    });

    // Pagination controls
    elements.prevButton.addEventListener('click', () => {
        if (state.page > 1) {
            state.page -= 1;
            fetchSnapshots();
        }
    });
    elements.nextButton.addEventListener('click', () => {
        const totalPages = Math.max(1, Math.ceil(state.totalCount / state.limit));
        if (state.page < totalPages) {
            state.page += 1;
            fetchSnapshots();
        }
    });

    // Init dates
    const initDates = () => {
        const today = new Date();
        const past = new Date();
        past.setDate(today.getDate() - 30);
        elements.endDateFilter.value = today.toISOString().split('T')[0];
        elements.startDateFilter.value = past.toISOString().split('T')[0];
        state.startDate = elements.startDateFilter.value;
        state.endDate = elements.endDateFilter.value;
    };

    // Load stores for dropdown
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

    // Boot
    renderMetricFilters();
    attachSortHandlers();
    initDates();
    loadStores();
    fetchSnapshots();
});
