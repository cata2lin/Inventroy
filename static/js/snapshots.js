document.addEventListener('DOMContentLoaded', () => {
  const el = {
    container: document.getElementById('snapshots-container'),
    thead: document.getElementById('table-head'),
    startDate: document.getElementById('start-date-filter'),
    endDate: document.getElementById('end-date-filter'),
    store: document.getElementById('store-filter'),
    trigger: document.getElementById('trigger-snapshot-btn'),
    prev: document.getElementById('prev-button'),
    next: document.getElementById('next-button'),
    page: document.getElementById('page-indicator'),
    metricFilters: document.getElementById('metric-filters'),
  };

  const state = {
    page: 1,
    limit: 25,
    total: 0,
    storeId: '',
    startDate: '',
    endDate: '',
    sortField: 'on_hand',
    sortOrder: 'desc',
    metricFilters: {}, // key -> {min, max}
  };

  const METRICS = [
    ['on_hand', 'Current Stock'],
    ['average_stock_level', 'Avg. Stock Level'],
    ['avg_inventory_value', 'Avg. Inventory Value'],
    ['stockout_rate', 'Stockout Rate (%)'],
    ['dead_stock_ratio', 'Dead Stock Ratio (%)'],
    ['stock_turnover', 'Stock Turnover'],
    ['avg_days_in_inventory', 'Avg. Days in Inventory'],
    ['stock_health_index', 'Health Index (0–1)'],
  ];

  const formatMetric = (val, digits = 2, suffix = '') => {
    if (val === null || val === undefined) return '—';
    const n = Number(val);
    if (Number.isNaN(n)) return '—';
    return `${n.toFixed(digits)}${suffix}`;
  };

  const buildHead = () => {
    el.thead.innerHTML = `
      <tr>
        <th>Product</th>
        <th class="sortable" data-sort="on_hand">Current Stock</th>
        <th class="sortable" data-sort="avg_inventory_value">Avg. Inv. Value</th>
        <th class="sortable" data-sort="stockout_rate">Stockout Rate</th>
        <th class="sortable" data-sort="stock_turnover">Turnover</th>
        <th class="sortable" data-sort="avg_days_in_inventory">Avg. Days in Inv.</th>
        <th class="sortable" data-sort="stock_health_index">Health</th>
      </tr>`;
  };

  const renderTable = (rows) => {
    if (!rows || rows.length === 0) {
      el.container.innerHTML = `<tr><td colspan="7" class="text-center">No data found for the selected filters.</td></tr>`;
      return;
    }
    el.container.innerHTML = rows.map(s => {
      const m = s.metrics || {};
      const variant = s.product_variant || {};
      const product = variant.product || {};
      const imageUrl = product.image_url || 'https://via.placeholder.com/48';
      const title = product.title || '—';
      const sku = variant.sku || '—';
      return `
        <tr>
          <td>
            <div style="display:flex;align-items:center;gap:.75rem;">
              <img src="${imageUrl}" style="width:48px;height:48px;object-fit:cover;border-radius:8px;" alt="${title}">
              <div>
                <strong>${title}</strong><br>
                <small>SKU: ${sku}</small>
              </div>
            </div>
          </td>
          <td>${s.on_hand ?? '—'} units</td>
          <td>${formatMetric(m.avg_inventory_value, 2, ' RON')}</td>
          <td>${formatMetric(m.stockout_rate, 2, '%')}</td>
          <td>${formatMetric(m.stock_turnover, 2)}</td>
          <td>${formatMetric(m.avg_days_in_inventory, 2)}</td>
          <td>${formatMetric(m.stock_health_index, 2)}</td>
        </tr>`;
    }).join('');
  };

  const updatePagination = () => {
    const pages = Math.max(1, Math.ceil(state.total / state.limit));
    el.page.textContent = `Page ${state.page} of ${pages}`;
    el.prev.disabled = state.page <= 1;
    el.next.disabled = state.page >= pages;
  };

  const debounce = (fn, ms) => {
    let t;
    return (...args) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...args), ms);
    };
  };

  const fetchSnapshots = async () => {
    el.container.innerHTML = `<tr><td colspan="7" class="text-center" aria-busy="true">Loading analytics...</td></tr>`;
    const params = new URLSearchParams({
      skip: String((state.page - 1) * state.limit),
      limit: String(state.limit),
      sort_field: state.sortField,
      sort_order: state.sortOrder,
    });
    if (state.storeId) params.set('store_id', state.storeId);
    if (state.startDate) params.set('start_date', state.startDate);
    if (state.endDate) params.set('end_date', state.endDate);
    Object.entries(state.metricFilters).forEach(([k, v]) => {
      if (v.min !== undefined && v.min !== '') params.set(`${k}_min`, v.min);
      if (v.max !== undefined && v.max !== '') params.set(`${k}_max`, v.max);
    });

    try {
      const res = await fetch(`/api/snapshots/?${params.toString()}`);
      if (!res.ok) throw new Error('fetch failed');
      const data = await res.json();
      state.total = data.total_count || 0;
      renderTable(data.snapshots || []);
      updatePagination();
    } catch (e) {
      console.error(e);
      el.container.innerHTML = `<tr><td colspan="7" class="text-center">Failed to load data.</td></tr>`;
    }
  };

  const attachSortHandlers = () => {
    el.thead.querySelectorAll('th.sortable').forEach(th => {
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => {
        const field = th.getAttribute('data-sort');
        if (state.sortField === field) {
          state.sortOrder = state.sortOrder === 'asc' ? 'desc' : 'asc';
        } else {
          state.sortField = field;
          state.sortOrder = 'desc';
        }
        el.thead.querySelectorAll('th.sortable').forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
        th.classList.add(state.sortOrder === 'asc' ? 'sorted-asc' : 'sorted-desc');
        state.page = 1;
        fetchSnapshots();
      });
    });
  };

  const renderMetricFilters = () => {
    el.metricFilters.innerHTML = METRICS.map(([key, label]) => `
      <div class="metric-filter-group">
        <label for="${key}-min">${label}</label>
        <div class="grid">
          <input id="${key}-min" type="number" placeholder="Min" data-key="${key}" class="metric-filter-input">
          <input id="${key}-max" type="number" placeholder="Max" data-key="${key}" class="metric-filter-input">
        </div>
      </div>
    `).join('');

    const debounced = debounce(() => {
      state.page = 1;
      fetchSnapshots();
    }, 400);

    el.metricFilters.querySelectorAll('.metric-filter-input').forEach(input => {
      input.addEventListener('input', (e) => {
        const key = e.target.dataset.key;
        const minVal = document.getElementById(`${key}-min`).value;
        const maxVal = document.getElementById(`${key}-max`).value;
        state.metricFilters[key] = { min: minVal, max: maxVal };
        debounced();
      });
    });
  };

  const setupEvents = () => {
    el.startDate.addEventListener('change', () => {
      state.startDate = el.startDate.value;
      state.page = 1;
      fetchSnapshots();
    });
    el.endDate.addEventListener('change', () => {
      state.endDate = el.endDate.value;
      state.page = 1;
      fetchSnapshots();
    });
    el.store.addEventListener('change', () => {
      state.storeId = el.store.value; // '' means All Stores
      state.page = 1;
      fetchSnapshots();
    });
    el.trigger.addEventListener('click', async () => {
      if (!state.storeId) {
        alert('Select a store to trigger a snapshot.');
        return;
      }
      el.trigger.setAttribute('aria-busy', 'true');
      try {
        const res = await fetch(`/api/snapshots/trigger?store_id=${encodeURIComponent(state.storeId)}`, { method: 'POST' });
        if (!res.ok) throw new Error();
        await fetchSnapshots();
      } catch {
        alert('Failed to trigger snapshot.');
      } finally {
        el.trigger.removeAttribute('aria-busy');
      }
    });
    el.prev.addEventListener('click', () => {
      if (state.page > 1) {
        state.page--;
        fetchSnapshots();
      }
    });
    el.next.addEventListener('click', () => {
      const pages = Math.max(1, Math.ceil(state.total / state.limit));
      if (state.page < pages) {
        state.page++;
        fetchSnapshots();
      }
    });
  };

  const init = async () => {
    buildHead();

    // Load stores from snapshots router
    try {
      const res = await fetch('/api/snapshots/stores');
      if (!res.ok) throw new Error('stores failed');
      const stores = await res.json();
      // keep default "All Stores" option present in HTML
      stores.forEach(s => el.store.add(new Option(s.name, s.id)));
      el.store.value = ''; // All Stores
      state.storeId = '';
    } catch (e) {
      console.error(e);
      el.container.innerHTML = `<tr><td colspan="7" class="text-center">Failed to load stores. Cannot fetch analytics.</td></tr>`;
      return;
    }

    renderMetricFilters();
    attachSortHandlers();
    setupEvents();

    fetchSnapshots();
  };

  init();
});
