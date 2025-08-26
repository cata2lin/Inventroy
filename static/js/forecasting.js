// static/js/forecasting.js

document.addEventListener('DOMContentLoaded', () => {
    const elements = {
        container: document.getElementById('forecasting-table-container'),
        leadTime: document.getElementById('lead-time'),
        coveragePeriod: document.getElementById('coverage-period'),
        storeFilter: document.getElementById('store-filter-list'),
        typeFilter: document.getElementById('type-filter-list'),
        vendorFilter: document.getElementById('vendor-filter-list'),
        statusFilter: document.getElementById('status-filter-list'),
        exportBtn: document.getElementById('export-button'),
    };

    let forecastingData = [];
    let sortState = { key: 'days_of_stock', order: 'asc' };

    const debounce = (func, delay) => {
        let timeout;
        return (...args) => {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    const getSelectedFilters = () => {
        const selectedStores = Array.from(elements.storeFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        const selectedTypes = Array.from(elements.typeFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        const selectedVendors = Array.from(elements.vendorFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        const selectedStatuses = Array.from(elements.statusFilter.querySelectorAll('input:checked')).map(cb => cb.value);
        return {
            store_ids: selectedStores,
            product_types: selectedTypes,
            vendors: selectedVendors,
            stock_statuses: selectedStatuses,
        };
    };

    const loadForecastingData = async () => {
        elements.container.setAttribute('aria-busy', 'true');
        const params = new URLSearchParams();
        const filters = getSelectedFilters();

        params.set('lead_time', elements.leadTime.value);
        params.set('coverage_period', elements.coveragePeriod.value);

        Object.entries(filters).forEach(([key, values]) => {
            values.forEach(value => params.append(key, value));
        });

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
        // Sort data
        forecastingData.sort((a, b) => {
            const valA = a[sortState.key];
            const valB = b[sortState.key];
            if (valA === null || valA === undefined) return 1;
            if (valB === null || valB === undefined) return -1;
            return sortState.order === 'asc' ? valA - valB : valB - valA;
        });

        const headers = [
            { key: 'product', label: 'Product' },
            { key: 'total_stock', label: 'Total Stock' },
            { key: 'velocity_7d', label: 'Velocity (7d)' },
            { key: 'velocity_30d', label: 'Velocity (30d)' },
            { key: 'days_of_stock', label: 'Days of Stock' },
            { key: 'stock_status', label: 'Stock Status' },
            { key: 'reorder_date', label: 'Reorder Date' },
            { key: 'reorder_qty', label: 'Reorder Qty' },
        ];

        let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
        headers.forEach(h => {
            const sortClass = sortState.key === h.key ? `class="${sortState.order}"` : '';
            tableHtml += `<th data-sort-key="${h.key}" ${sortClass}>${h.label}</th>`;
        });
        tableHtml += '</tr></thead><tbody>';

        if (forecastingData.length === 0) {
            tableHtml += '<tr><td colspan="8">No products match the current filters.</td></tr>';
        } else {
            forecastingData.forEach(item => {
                tableHtml += `
                    <tr>
                        <td>
                            <div class="product-cell">
                                <img src="${item.image_url || '/static/img/placeholder.png'}" alt="${item.product_title}">
                                <div>
                                    <strong>${item.product_title}</strong><br>
                                    <small>${item.sku}</small>
                                </div>
                            </div>
                        </td>
                        <td>${item.total_stock}</td>
                        <td>${item.velocity_7d.toFixed(2)}</td>
                        <td>${item.velocity_30d.toFixed(2)}</td>
                        <td>${item.days_of_stock === null ? '∞' : item.days_of_stock}</td>
                        <td><span class="status-badge ${item.stock_status}">${item.stock_status.replace('_', ' ')}</span></td>
                        <td>${item.reorder_date || 'N/A'}</td>
                        <td>${item.reorder_qty}</td>
                    </tr>
                `;
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
                if (sortState.key === key) {
                    sortState.order = sortState.order === 'asc' ? 'desc' : 'asc';
                } else {
                    sortState.key = key;
                    sortState.order = 'asc';
                }
                renderTable();
            });
        });
    };
    
    const loadFilters = async () => {
        try {
            const response = await fetch('/api/forecasting/filters');
            const data = await response.json();

            const populateList = (element, items) => {
                element.innerHTML = items.map(item => `<li><label><input type="checkbox" name="${element.id}" value="${item}"> ${item}</label></li>`).join('');
            };
            
            populateList(elements.storeFilter, data.stores);
            populateList(elements.typeFilter, data.product_types);
            populateList(elements.vendorFilter, data.vendors);

        } catch (error) {
            console.error('Failed to load filters:', error);
        }
    };
    
    const handleExport = () => {
        const params = new URLSearchParams();
        const filters = getSelectedFilters();

        params.set('lead_time', elements.leadTime.value);
        params.set('coverage_period', elements.coveragePeriod.value);
        Object.entries(filters).forEach(([key, values]) => {
            values.forEach(value => params.append(key, value));
        });

        window.location.href = `/api/forecasting/export?${params.toString()}`;
    };

    // Event Listeners
    [elements.leadTime, elements.coveragePeriod].forEach(input => {
        input.addEventListener('change', debounce(loadForecastingData, 400));
    });

    [elements.storeFilter, elements.typeFilter, elements.vendorFilter, elements.statusFilter].forEach(filter => {
        filter.addEventListener('change', loadForecastingData);
    });
    
    elements.exportBtn.addEventListener('click', handleExport);

    // Initial Load
    loadFilters();
    loadForecastingData();
});