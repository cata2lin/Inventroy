// static/js/sales_analytics.js

document.addEventListener('DOMContentLoaded', function () {
    const state = {
        page: 1,
        limit: 50,
        sort_by: 'net_sales',
        sort_order: 'desc',
        filters: {
            start: new Date(new Date().setDate(new Date().getDate() - 30)).toISOString().split('T')[0],
            end: new Date().toISOString().split('T')[0],
            stores: [],
            only_paid: false,
            exclude_canceled: false,
            search: ''
        }
    };

    const elements = {
        kpiContainer: document.getElementById('kpi-container'),
        controlsContainer: document.querySelector('.controls-grid'),
        tableContainer: document.getElementById('sales-table-container'),
        prevButton: document.getElementById('prev-button'),
        nextButton: document.getElementById('next-button'),
        pageIndicator: document.getElementById('page-indicator'),
        topProductsChartContainer: document.getElementById('top-products-chart-container'),
        refundRateChartContainer: document.getElementById('refund-rate-chart-container')
    };

    let topProductsChart = null;
    let refundRateChart = null;

    function renderControls() {
        // In a real app, you'd fetch stores from an API
        const stores = [{id: 1, name: 'Main Store'}, {id: 2, name: 'Outlet'}];

        elements.controlsContainer.innerHTML = `
            <input type="date" id="start-date" value="${state.filters.start}">
            <input type="date" id="end-date" value="${state.filters.end}">
            <input type="search" id="search-input" placeholder="Search..." value="${state.filters.search}">
            <div>${stores.map(s => `<label><input type="checkbox" name="store" value="${s.id}">${s.name}</label>`).join('')}</div>
            <label><input type="checkbox" id="only-paid">Only Paid</label>
            <label><input type="checkbox" id="exclude-canceled">Exclude Canceled</label>
            <button id="export-csv">Export CSV</button>
        `;

        document.getElementById('start-date').addEventListener('change', e => { state.filters.start = e.target.value; fetchData(); });
        document.getElementById('end-date').addEventListener('change', e => { state.filters.end = e.target.value; fetchData(); });
        document.getElementById('search-input').addEventListener('input', e => { state.filters.search = e.target.value; fetchData(); });
        document.querySelectorAll('input[name="store"]').forEach(el => el.addEventListener('change', () => {
            state.filters.stores = Array.from(document.querySelectorAll('input[name="store"]:checked')).map(cb => cb.value);
            fetchData();
        }));
        document.getElementById('only-paid').addEventListener('change', e => { state.filters.only_paid = e.target.checked; fetchData(); });
        document.getElementById('exclude-canceled').addEventListener('change', e => { state.filters.exclude_canceled = e.target.checked; fetchData(); });
        document.getElementById('export-csv').addEventListener('click', exportCSV);
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
            sort_by: state.sort_by,
            sort_order: state.sort_order
        });
        state.filters.stores.forEach(id => params.append('stores', id));

        try {
            const response = await fetch(`/api/analytics/sales-by-product?${params.toString()}`);
            const data = await response.json();
            renderTable(data);
            updateKPIs(data);
            renderCharts(data);
        } catch (error) {
            elements.tableContainer.innerHTML = `<p>Error fetching data: ${error.message}</p>`;
        } finally {
            elements.tableContainer.removeAttribute('aria-busy');
        }
    }

    function renderTable(data) {
        const headers = [
            {key: 'product_title', label: 'Product'},
            {key: 'barcode', label: 'Barcode'},
            {key: 'orders_count', label: 'Orders'},
            {key: 'units_sold', label: 'Units Sold'},
            {key: 'refunded_units', label: 'Refunded Units'},
            {key: 'refund_rate_qty', label: 'Refund Rate'},
            {key: 'gross_sales', label: 'Gross Sales'},
            {key: 'discounts', label: 'Discounts'},
            {key: 'returns_value', label: 'Returns'},
            {key: 'net_sales', label: 'Net Sales'},
            {key: 'velocity_units_per_day', label: 'Velocity'},
            {key: 'asp', label: 'ASP'}
        ];

        let tableHTML = '<table><thead><tr>';
        headers.forEach(h => tableHTML += `<th>${h.label}</th>`);
        tableHTML += '</tr></thead><tbody>';

        data.forEach(row => {
            tableHTML += '<tr>';
            headers.forEach(h => tableHTML += `<td>${row[h.key]}</td>`);
            tableHTML += '</tr>';
        });

        tableHTML += '</tbody></table>';
        elements.tableContainer.innerHTML = tableHTML;
    }
    
    function updateKPIs(data) {
        const totalNet = data.reduce((sum, row) => sum + parseFloat(row.net_sales), 0);
        const totalOrders = data.reduce((sum, row) => sum + row.orders_count, 0);
        const avgRefundRate = data.reduce((sum, row) => sum + row.refund_rate_qty, 0) / data.length;
        const avgVelocity = data.reduce((sum, row) => sum + row.velocity_units_per_day, 0) / data.length;

        elements.kpiContainer.innerHTML = `
            <div class="metric"><h4>${totalNet.toFixed(2)}</h4><p>Total Net Sales</p></div>
            <div class="metric"><h4>${totalOrders}</h4><p>Total Orders</p></div>
            <div class="metric"><h4>${(avgRefundRate * 100).toFixed(2)}%</h4><p>Avg. Refund Rate</p></div>
            <div class="metric"><h4>${avgVelocity.toFixed(2)}</h4><p>Avg. Velocity</p></div>
        `;
    }

    function renderCharts(data) {
        const top20 = data.slice(0, 20);
        
        if (topProductsChart) topProductsChart.destroy();
        topProductsChart = new Chart(elements.topProductsChartContainer, {
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
        refundRateChart = new Chart(elements.refundRateChartContainer, {
            type: 'scatter',
            data: {
                datasets: [{
                    label: 'Refund Rate vs. Net Sales',
                    data: data.map(p => ({
                        x: p.net_sales,
                        y: p.refund_rate_qty,
                        r: p.velocity_units_per_day * 5 // Scale point size by velocity
                    })),
                    backgroundColor: 'rgba(255, 99, 132, 0.5)'
                }]
            }
        });
    }
    
    function exportCSV() {
        // Implement CSV export logic here
        alert('CSV export not yet implemented.');
    }

    elements.prevButton.addEventListener('click', () => { if (state.page > 1) { state.page--; fetchData(); } });
    elements.nextButton.addEventListener('click', () => { state.page++; fetchData(); });

    renderControls();
    fetchData();
});