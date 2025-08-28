// static/js/forecasting.js

document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('forecasting-table-container'),
        searchInput: document.getElementById('search-input'),
        leadTime: document.getElementById('lead-time'),
        coveragePeriod: document.getElementById('coverage-period'),
        storeFilter: document.getElementById('store-filter-list'),
        typeFilter: document.getElementById('type-filter-list'),
        statusFilter: document.getElementById('status-filter-list'),
        reorderDateStart: document.getElementById('reorder-date-start'),
        reorderDateEnd: document.getElementById('reorder-date-end'),
        useCustomVelocity: document.getElementById('use-custom-velocity'),
        customVelocityDates: document.getElementById('custom-velocity-dates'),
        velocityStartDate: document.getElementById('velocity-start-date'),
        velocityEndDate: document.getElementById('velocity-end-date'),
        activeVelocityMetric: document.getElementById('active-velocity-metric'),
        exportBtn: document.getElementById('export-button'),
    };

    let forecastingData = [];
    let state = {};

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const updateUrl = () => {
        const params = new URLSearchParams();
        Object.entries(state).forEach(([key, value]) => {
            if (Array.isArray(value) && value.length > 0) {
                value.forEach(v => params.append(key, v));
            } else if (value && !Array.isArray(value)) {
                params.set(key, value);
            }
        });
        window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
    };
    
    const loadStateFromUrl = () => {
        const params = new URLSearchParams(window.location.search);
        state = {
            search: params.get('search') || '',
            lead_time: params.get('lead_time') || '30',
            coverage_period: params.get('coverage_period') || '60',
            store_ids: params.getAll('store_ids'),
            product_types: params.getAll('product_types'),
            stock_statuses: params.getAll('stock_statuses'),
            reorder_start_date: params.get('reorder_start_date') || '',
            reorder_end_date: params.get('reorder_end_date') || '',
            use_custom_velocity: params.get('use_custom_velocity') === 'true',
            velocity_start_date: params.get('velocity_start_date') || '',
            velocity_end_date: params.get('velocity_end_date') || '',
            active_velocity_metric: params.get('active_velocity_metric') || 'velocity_30d',
            sort_by: params.get('sort_by') || 'days_of_stock',
            sort_order: params.get('sort_order') || 'asc',
        };

        // Update UI from state
        elements.searchInput.value = state.search;
        elements.leadTime.value = state.lead_time;
        elements.coveragePeriod.value = state.coverage_period;
        elements.reorderDateStart.value = state.reorder_start_date;
        elements.reorderDateEnd.value = state.reorder_end_date;
        elements.useCustomVelocity.checked = state.use_custom_velocity;
        elements.velocityStartDate.value = state.velocity_start_date;
        elements.velocityEndDate.value = state.velocity_end_date;
        elements.customVelocityDates.style.display = state.use_custom_velocity ? 'grid' : 'none';
        elements.activeVelocityMetric.value = state.active_velocity_metric;
    };

    const loadForecastingData = async () => {
        elements.container.setAttribute('aria-busy', 'true');
        updateUrl();
        
        const params = new URLSearchParams();
        for (const [key, value] of Object.entries(state)) {
            if (Array.isArray(value)) {
                value.forEach(v => params.append(key, v));
            } else if (value) {
                params.set(key, value);
            }
        }
        
        try {
            const response = await fetch(`/api/forecasting/report?${params.toString()}`);
            if (!response.ok) throw new Error('Failed to fetch forecasting data.');
            forecastingData = await response.json();
            renderTable();
        } catch (error) {
            elements.container.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            elements.container.removeAttribute('aria-busy');
        }
    };

    const renderTable = () => {
        forecastingData.sort((a, b) => {
            const valA = a[state.sort_by];
            const valB = b[state.sort_by];
            if (valA === null || valA === undefined) return 1;
            if (valB === null || valB === undefined) return -1;
            
            if (typeof valA === 'string' && valA.includes('-')) {
                return state.sort_order === 'asc' 
                    ? new Date(valA) - new Date(valB) 
                    : new Date(valB) - new Date(valA);
            }
            if (typeof valA === 'string') {
                return state.sort_order === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
            } else {
                return state.sort_order === 'asc' ? valA - valB : valB - valA;
            }
        });

        const headers = [
            { key: 'product_title', label: 'Product' },
            { key: 'total_stock', label: 'Total Stock' },
            { key: 'velocity_7d', label: 'Velocity (7d)' },
            { key: 'velocity_30d', label: 'Velocity (30d)' },
        ];
        if(state.use_custom_velocity){
            headers.push({ key: 'velocity_period', label: 'Velocity (Period)' });
        }
        headers.push({ key: 'velocity_lifetime', label: 'Velocity (Lifetime)' });
        headers.push(
            { key: 'days_of_stock', label: 'Days of Stock' },
            { key: 'stock_status', label: 'Stock Status' },
            { key: 'reorder_date', label: 'Reorder Date' },
            { key: 'reorder_qty', label: 'Reorder Qty' }
        );

        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        headers.forEach(h => {
            let sortIndicator = '';
            if (state.sort_by === h.key) {
                sortIndicator = state.sort_order === 'asc' ? ' ▲' : ' ▼';
            }
            tableHtml += `<th data-sort-key="${h.key}">${h.label}${sortIndicator}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        if (forecastingData.length === 0) {
            tableHtml += `<tr><td colspan="${headers.length}">No products match the current filters.</td></tr>`;
        } else {
            const today = new Date();
            const leadTime = parseInt(state.lead_time, 10);
            
            forecastingData.forEach(item => {
                const statusClass = item.stock_status.replace('_', '-');
                let reorderDateClass = '';
                if (item.reorder_date) {
                    const reorderDate = new Date(item.reorder_date);
                    const diffDays = (reorderDate - today) / (1000 * 3600 * 24);
                    if (diffDays < 0) reorderDateClass = 'status-urgent';
                    else if (diffDays < leadTime) reorderDateClass = 'status-warning';
                    else if (diffDays < leadTime + 7) reorderDateClass = 'status-watch';
                    else if (diffDays < leadTime + 14) reorderDateClass = 'status-healthy-light';
                    else reorderDateClass = 'status-healthy';
                }

                let rowHtml = `
                    <tr>
                        <td>
                            <div class="product-cell">
                                <img src="${item.image_url || '/static/img/placeholder.png'}" alt="${item.product_title}" style="width: 50px; height: 50px; object-fit: cover;">
                                <div>
                                    <strong>${item.product_title}</strong><br>
                                    <small>${item.sku}</small>
                                </div>
                            </div>
                        </td>
                        <td>${item.total_stock}</td>
                        <td>${item.velocity_7d.toFixed(2)}</td>
                        <td>${item.velocity_30d.toFixed(2)}</td>
                `;
                if(state.use_custom_velocity){
                    rowHtml += `<td>${item.velocity_period.toFixed(2)}</td>`;
                }
                rowHtml += `<td>${item.velocity_lifetime.toFixed(2)}</td>`;
                rowHtml += `
                        <td>${item.days_of_stock === null ? '0' : item.days_of_stock}</td>
                        <td><span class="status-badge status-${statusClass}">${item.stock_status.replace('_', ' ')}</span></td>
                        <td class="${reorderDateClass}">${item.reorder_date || 'N/A'}</td>
                        <td>${item.reorder_qty}</td>
                    </tr>
                `;
                tableHtml += rowHtml;
            });
        }
        tableHtml += '</tbody></table></div>';
        elements.container.innerHTML = tableHtml;
        addTableEventListeners();
    };

    const addTableEventListeners = () => {
        elements.container.querySelectorAll('th[data-sort-key]').forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sortKey;
                if (state.sort_by === key) {
                    state.sort_order = state.sort_order === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sort_by = key;
                    state.sort_order = 'asc';
                }
                loadForecastingData();
            });
        });
    };
    
    const loadFilters = async () => {
        try {
            const response = await fetch('/api/forecasting/filters');
            if (!response.ok) throw new Error('Filter options could not be loaded.');
            const data = await response.json();

            elements.storeFilter.innerHTML = data.stores.map(store => `<li><label><input type="checkbox" name="store_ids" value="${store.id}" ${state.store_ids.includes(String(store.id)) ? 'checked' : ''}> ${store.name}</label></li>`).join('');
            
            elements.typeFilter.innerHTML = data.product_types.map(pt => `<li><label><input type="checkbox" name="product_types" value="${pt}" ${state.product_types.includes(pt) ? 'checked' : ''}> ${pt}</label></li>`).join('');

        } catch (error) {
            console.error('Failed to load filters:', error);
            elements.storeFilter.innerHTML = `<li>Error loading filters.</li>`;
            elements.typeFilter.innerHTML = `<li>Error loading filters.</li>`;
        }
    };
    
    const handleExport = () => {
        const params = new URLSearchParams();
        for (const [key, value] of Object.entries(state)) {
            if (Array.isArray(value)) {
                value.forEach(v => params.append(key, v));
            } else if (value) {
                params.set(key, value);
            }
        }
        window.location.href = `/api/forecasting/export?${params.toString()}`;
    };

    // --- START OF FIX ---
    elements.searchInput.addEventListener('input', debounce(() => {
        state.search = elements.searchInput.value;
        loadForecastingData();
    }, 400));
    
    [elements.leadTime, elements.coveragePeriod, elements.activeVelocityMetric].forEach(input => {
        input.addEventListener('change', () => {
            state[input.id.replace(/-/g, '_')] = input.value;
            loadForecastingData();
        });
    });

    [elements.reorderDateStart, elements.reorderDateEnd, elements.velocityStartDate, elements.velocityEndDate].forEach(dateInput => {
        dateInput.addEventListener('change', () => {
            state[dateInput.id.replace(/-/g, '_')] = dateInput.value;
            loadForecastingData();
        });
    });
    // --- END OF FIX ---

    elements.useCustomVelocity.addEventListener('change', () => {
        state.use_custom_velocity = elements.useCustomVelocity.checked;
        elements.customVelocityDates.style.display = state.use_custom_velocity ? 'grid' : 'none';
        loadForecastingData();
    });
    
    elements.storeFilter.addEventListener('change', () => {
        state.store_ids = Array.from(elements.storeFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        loadForecastingData();
    });
    elements.typeFilter.addEventListener('change', () => {
        state.product_types = Array.from(elements.typeFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        loadForecastingData();
    });
    elements.statusFilter.addEventListener('change', () => {
        state.stock_statuses = Array.from(elements.statusFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        loadForecastingData();
    });
    
    elements.exportBtn.addEventListener('click', handleExport);

    // Initial Load
    loadStateFromUrl();
    loadFilters();
    loadForecastingData();
});