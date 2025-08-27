// static/js/sales_analytics.js

document.addEventListener('DOMContentLoaded', function () {
    const state = {
        page: 1,
        limit: 50,
        filters: {
            start: new Date(new Date().setDate(new Date().getDate() - 29)).toISOString().split('T')[0],
            end: new Date().toISOString().split('T')[0],
            stores: [],
            only_paid: false,
            exclude_canceled: false,
            search: ''
        },
        data: []
    };

    const elements = {
        kpiContainer: document.getElementById('kpi-container'),
        startDate: document.getElementById('start-date'),
        endDate: document.getElementById('end-date'),
        searchInput: document.getElementById('search-input'),
        storeFilterList: document.getElementById('store-filter-list'),
        onlyPaid: document.getElementById('only-paid'),
        excludeCanceled: document.getElementById('exclude-canceled'),
        exportCsv: document.getElementById('export-csv'),
        tableContainer: document.getElementById('sales-table-container'),
        prevButton: document.getElementById('prev-button'),
        nextButton: document.getElementById('next-button'),
        pageIndicator: document.getElementById('page-indicator'),
        topProductsChartCtx: document.getElementById('top-products-chart').getContext('2d'),
        refundRateChartCtx: document.getElementById('refund-rate-chart').getContext('2d')
    };

    let topProductsChart = null;
    let refundRateChart = null;

    async function initialize() {
        elements.startDate.value = state.filters.start;
        elements.endDate.value = state.filters.end;
        
        await loadStores();
        addEventListeners();
        fetchData();
    }

    async function loadStores() {
        try {
            const response = await fetch('/api/config/stores');
            const stores = await response.json();
            elements.storeFilterList.innerHTML = stores.map(s => `<li><label><input type="checkbox" name="store" value="${s.id}"> ${s.name}</label></li>`).join('');
            document.querySelectorAll('input[name="store"]').forEach(el => el.addEventListener('change', handleFilterChange));
        } catch (error) {
            elements.storeFilterList.innerHTML = '<li>Error loading stores</li>';
        }
    }

    function addEventListeners() {
        elements.startDate.addEventListener('change', handleFilterChange);
        elements.endDate.addEventListener('change', handleFilterChange);
        elements.searchInput.addEventListener('input', debounce(handleFilterChange, 300));
        elements.onlyPaid.addEventListener('change', handleFilterChange);
        elements.excludeCanceled.addEventListener('change', handleFilterChange);
        elements.exportCsv.addEventListener('click', exportCSV);
        elements.prevButton.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchData(); } });
        elements.nextButton.addEventListener('click', () => { state.page++; fetchData(); });
    }

    function handleFilterChange() {
        state.filters.start = elements.startDate.value;
        state.filters.end = elements.endDate.value;
        state.filters.search = elements.searchInput.value;
        state.filters.stores = Array.from(document.querySelectorAll('input[name="store"]:checked')).map(cb => cb.value);
        state.filters.only_paid = elements.onlyPaid.checked;
        state.filters.exclude_canceled = elements.excludeCanceled.checked;
        state.page = 1;
        fetchData();
    }

    async function fetchData() {
        elements.tableContainer.setAttribute('aria-busy', 'true');
        const params = new URLSearchParams({
            start: state.filters.start,
            end: state.filters.end,
            only_paid: state.filters.only_paid,
            exclude_canceled: state.filters.exclude_canceled,
            search: state.filters.search,
            limit: state.limit,
            offset: (state.page - 1) * state.limit,
        });
        state.filters.stores.forEach(id => params.append('stores', id));

        try {
            const response = await fetch(`/api/analytics/sales-by-product?${params.toString()}`);
            state.data = await response.json();
            renderTable();
            updateKPIs();
            renderCharts();
            updatePagination();
        } catch (error) {
            elements.tableContainer.innerHTML = `<p>Error fetching data: ${error.message}</p>`;
        } finally {
            elements.tableContainer.removeAttribute('aria-busy');
        }
    }

    function renderTable() {
        const headers = [
            {key: 'product_title', label: 'Product'},
            {key: 'barcode', label: 'Barcode'},
            {key: 'orders_count', label: 'Orders'},
            {key: 'units_sold', label: 'Units Sold'},
            {key: 'refunded_units', label: 'Refunded Units'},
            {key: 'refund_rate_qty', label: 'Refund Rate (%)'},
            {key: 'gross_sales', label: 'Gross Sales'},
            {key: 'discounts', label: 'Discounts'},
            {key: 'returns_value', label: 'Returns'},
            {key: 'net_sales', label: 'Net Sales'},
            {key: 'velocity_units_per_day', label: 'Velocity (u/day)'},
            {key: 'asp', label: 'ASP'}
        ];

        let tableHTML = '<table><thead><tr>';
        headers.forEach(h => tableHTML += `<th>${h.label}</th>`);
        tableHTML += '</tr></thead><tbody>';

        state.data.forEach(row => {
            tableHTML += '<tr>';
            headers.forEach(h => {
                let value = row[h.key];
                if (h.key === 'refund_rate_qty') value = (value * 100).toFixed(2);
                if (['gross_sales', 'discounts', 'returns_value', 'net_sales', 'asp'].includes(h.key)) value = parseFloat(value).toFixed(2);
                tableHTML += `<td>${value}</td>`;
            });
            tableHTML += '</tr>';
        });

        tableHTML += '</tbody></table>';
        elements.tableContainer.innerHTML = tableHTML;
    }

    function updateKPIs() {
        const totalNet = state.data.reduce((sum, row) => sum + parseFloat(row.net_sales), 0);
        const totalOrders = state.data.reduce((sum, row) => sum + row.orders_count, 0);
        const totalRefunded = state.data.reduce((sum, row) => sum + row.refunded_units, 0);
        const totalSold = state.data.reduce((sum, row) => sum + row.units_sold, 0);
        const avgRefundRate = totalSold > 0 ? (totalRefunded / totalSold) * 100 : 0;
        const avgVelocity = state.data.reduce((sum, row) => sum + row.velocity_units_per_day, 0) / state.data.length || 0;

        elements.kpiContainer.innerHTML = `
            <div class="metric"><h4>${totalNet.toFixed(2)}</h4><p>Total Net Sales</p></div>
            <div class="metric"><h4>${totalOrders}</h4><p>Total Orders</p></div>
            <div class="metric"><h4>${avgRefundRate.toFixed(2)}%</h4><p>Avg. Refund Rate</p></div>
            <div class="metric"><h4>${avgVelocity.toFixed(2)}</h4><p>Avg. Velocity</p></div>
        `;
    }
    
    function renderCharts() {
        const top20 = state.data.slice(0, 20);
        
        if (topProductsChart) topProductsChart.destroy();
        topProductsChart = new Chart(elements.topProductsChartCtx, {
            type: 'bar',
            data: {
                labels: top20.map(p => p.product_title),
                datasets: [{
                    label: 'Net Sales',
                    data: top20.map(p => p.net_sales),
                    backgroundColor: 'rgba(78, 161, 255, 0.5)'
                }]
            }
        });

        if (refundRateChart) refundRateChart.destroy();
        refundRateChart = new Chart(elements.refundRateChartCtx, {
            type: 'scatter',
            data: {
                datasets: [{
                    label: 'Refund Rate vs. Net Sales',
                    data: state.data.map(p => ({
                        x: p.net_sales,
                        y: p.refund_rate_qty,
                        r: p.velocity_units_per_day * 5
                    })),
                    backgroundColor: 'rgba(255, 99, 132, 0.5)'
                }]
            }
        });
    }

    function updatePagination() {
        elements.pageIndicator.textContent = `Page ${state.page}`;
        elements.prevButton.disabled = state.page === 1;
        elements.nextButton.disabled = state.data.length < state.limit;
    }

    function exportCSV() {
        const headers = [
            "Product", "Barcode", "Orders", "Units Sold", "Refunded Units", 
            "Refund Rate (%)", "Gross Sales", "Discounts", "Returns", "Net Sales", 
            "Velocity (u/day)", "ASP"
        ];
        const rows = state.data.map(row => [
            `"${row.product_title.replace(/"/g, '""')}"`,
            row.barcode,
            row.orders_count,
            row.units_sold,
            row.refunded_units,
            (row.refund_rate_qty * 100).toFixed(2),
            row.gross_sales,
            row.discounts,
            row.returns_value,
            row.net_sales,
            row.velocity_units_per_day,
            parseFloat(row.asp).toFixed(2)
        ]);

        const csvContent = "data:text/csv;charset=utf-8," 
            + headers.join(",") + "\n" 
            + rows.map(r => r.join(",")).join("\n");
        
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", "sales_by_product.csv");
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    function debounce(func, delay) {
        let timeout;
        return function(...args) {
            const context = this;
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(context, args), delay);
        };
    }

    initialize();
});