// static/js/inventory.js
// Robust, null-safe inventory UI (individual & grouped views)

document.addEventListener('DOMContentLoaded', () => {
  // ---- Element refs (some may be missing; we guard all uses) ----
  const elements = {
    metrics: document.getElementById('metrics-container'),
    filters: {
      search: document.getElementById('search-input'),
      store: document.getElementById('store-filter'),
      type: document.getElementById('type-filter'),
      status: document.getElementById('status-filter'),
      view: document.getElementById('view-toggle'),
    },
    tableContainer: document.getElementById('inventory-table-container'),
    pagination: {
      prev: document.getElementById('prev-button'),
      next: document.getElementById('next-button'),
      indicator: document.getElementById('page-indicator'),
    },
  };

  // ---- State ----
  const getURLView = () => {
    const m = /[?&]view=([^&]+)/.exec(location.search);
    return m ? decodeURIComponent(m[1]) : null;
  };
  const initialView = (elements.filters.view && elements.filters.view.value) || getURLView() || 'individual';
  const state = {
    page: 1,
    pageSize: 50,
    sortBy: 'on_hand',
    sortOrder: 'desc',
    view: initialView,
    search: '',
    store: '',
    status: '',
    type: '',
    totalCount: 0,
  };

  // ---- Helpers ----
  const qs = (obj) =>
    Object.entries(obj)
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&');

  const fetchJSON = async (url) => {
    const res = await fetch(url);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  };

  const setBusy = (busy) => {
    if (!elements.tableContainer) return;
    if (busy) {
      elements.tableContainer.setAttribute('aria-busy', 'true');
    } else {
      elements.tableContainer.removeAttribute('aria-busy');
    }
  };

  // ---- Filters data ----
  const loadFilters = async () => {
    try {
      const data = await fetchJSON('/api/v2/inventory/filters/');
      if (elements.filters.store) {
        (data.stores || []).forEach(s => {
          const opt = document.createElement('option');
          opt.value = s.id;
          opt.textContent = s.name;
          elements.filters.store.appendChild(opt);
        });
      }
      if (elements.filters.type) {
        (data.types || []).forEach(t => {
          const opt = document.createElement('option');
          opt.value = t;
          opt.textContent = t;
          elements.filters.type.appendChild(opt);
        });
      }
      if (elements.filters.status) {
        (data.statuses || []).forEach(s => {
          const opt = document.createElement('option');
          opt.value = s;
          opt.textContent = s;
          elements.filters.status.appendChild(opt);
        });
      }
    } catch (e) {
      console.error('Failed loading filters', e);
    }
  };

  // ---- Page load ----
  const loadPage = async () => {
    if (!elements.tableContainer) return;
    setBusy(true);
    const skip = (state.page - 1) * state.pageSize;
    const params = {
      skip,
      limit: state.pageSize,
      sort_by: state.sortBy,
      sort_order: state.sortOrder,
      view: state.view,
      search: state.search,
      stores: state.store,
      statuses: state.status,
      types: state.type,
    };
    const url = `/api/v2/inventory/report/?${qs(params)}`;
    try {
      const data = await fetchJSON(url);
      state.totalCount = data.total_count || 0;
      renderAll(data);
    } catch (e) {
      console.error(e);
      elements.tableContainer.innerHTML = `<p class="error">Failed to load inventory.</p>`;
    } finally {
      setBusy(false);
    }
  };

  const fmtCurrency = (v) =>
    (Number(v) || 0).toLocaleString('ro-RO', { maximumFractionDigits: 2 }) + ' RON';

  const renderMetrics = (data) => {
    if (!elements.metrics) return;
    elements.metrics.innerHTML = `
      <div class="metric"><h4>${fmtCurrency(data.total_retail_value)}</h4><p>Total Retail Value</p></div>
      <div class="metric"><h4>${fmtCurrency(data.total_inventory_value)}</h4><p>Total Inventory Value</p></div>
      <div class="metric"><h4>${(data.total_on_hand || 0).toLocaleString()}</h4><p>Total Products On Hand</p></div>`;
  };

  const updatePagination = () => {
    const totalPages = Math.max(1, Math.ceil((state.totalCount || 0) / state.pageSize));
    if (elements.pagination.indicator) {
      elements.pagination.indicator.textContent = `Page ${state.page} / ${totalPages}`;
    }
    if (elements.pagination.prev) elements.pagination.prev.disabled = state.page <= 1;
    if (elements.pagination.next) elements.pagination.next.disabled = state.page >= totalPages;
  };

  const openDetailsPage = (groupKey) => {
    if (!groupKey) return;
    window.location.href = `/inventory/product/${encodeURIComponent(groupKey)}`;
  };
  window.__openDetails = openDetailsPage;

  window.__makePrimary = async (groupKey, variantId) => {
    try {
      await fetch('/api/v2/inventory/set-primary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ barcode: groupKey, variant_id: variantId }),
      });
      loadPage();
    } catch (e) {
      console.error('Failed to set primary', e);
    }
  };

  // ---- Renderers ----
  const renderAll = (data) => {
    renderMetrics(data);
    const items = Array.isArray(data.inventory) ? data.inventory : [];
    if (state.view === 'grouped') {
      renderGroupedView(items);
    } else {
      renderIndividualView(items);
    }
    updatePagination();
  };

  const renderIndividualView = (inventory) => {
    const headers = [
      { key: 'image_url', label: 'Image', sortable: false },
      { key: 'product_title', label: 'Product' },
      { key: 'store_name', label: 'Store' },
      { key: 'sku', label: 'SKU' },
      { key: 'barcode', label: 'Barcode' },
      { key: 'price', label: 'Price' },
      { key: 'cost', label: 'Cost' },
      { key: 'on_hand', label: 'On Hand' },
      { key: 'committed', label: 'Committed' },
      { key: 'available', label: 'Available' },
      { key: 'retail_value', label: 'Retail Value' },
      { key: 'inventory_value', label: 'Inv. Value' },
    ];

    let html = '<div class="overflow-auto"><table><thead><tr>';
    headers.forEach(h => {
      const sortClass = state.sortBy === h.key ? `class="${state.sortOrder}"` : '';
      const sortable = h.sortable !== false ? `data-sort-by="${h.key}"` : '';
      html += `<th ${sortable} ${sortClass}>${h.label}</th>`;
    });
    html += '</tr></thead><tbody>';

    (inventory || []).forEach(item => {
      const onHand = item.on_hand || 0;
      const price = Number(item.price) || 0;
      const cost = Number(item.cost) || 0;
      const rv = onHand * price;
      const iv = onHand * cost;
      const groupKey = item.group_id || item.barcode || item.sku || '';
      html += `
        <tr>
          <td>
            <img src="${item.image_url || '/static/img/placeholder.png'}" alt="${item.product_title || ''}"
                 style="width:48px;height:48px;cursor:pointer;border-radius:6px"
                 onclick="window.__openDetails('${groupKey}')">
          </td>
          <td>${item.product_title || ''}<br><small>${item.variant_title || ''}</small></td>
          <td>${item.store_name || ''}</td>
          <td>${item.sku || ''}</td>
          <td>${item.barcode || ''}</td>
          <td>${price.toFixed(2)}</td>
          <td>${cost.toFixed(2)}</td>
          <td>${onHand}</td>
          <td>${item.committed || 0}</td>
          <td>${item.available || 0}</td>
          <td>${rv.toFixed(2)}</td>
          <td>${iv.toFixed(2)}</td>
        </tr>`;
    });

    html += '</tbody></table></div>';
    elements.tableContainer.innerHTML = html;

    // header sorting
    elements.tableContainer.querySelectorAll('th[data-sort-by]').forEach(th => {
      th.addEventListener('click', () => {
        const k = th.getAttribute('data-sort-by');
        if (state.sortBy === k) state.sortOrder = state.sortOrder === 'asc' ? 'desc' : 'asc';
        else { state.sortBy = k; state.sortOrder = 'desc'; }
        loadPage();
      });
    });
  };

  const renderGroupedView = (inventory) => {
    if (!Array.isArray(inventory)) inventory = [];
    let html = '';
    inventory.forEach(group => {
      const onHand = group.on_hand || 0;
      const committed = group.committed || 0;
      const available = group.available || 0;
      const groupKey = group.group_id || group.barcode || 'UNKNOWN';
      const members = group.variants_json || group.variants || group.members || [];
      html += `
        <details class="grouped-item">
          <summary>
            <div class="grid">
              <img src="${group.primary_image_url || '/static/img/placeholder.png'}"
                   alt="${group.primary_title || ''}"
                   style="cursor:pointer;width:60px;height:60px;border-radius:6px"
                   onclick="window.__openDetails('${groupKey}')">
              <div class="product-info">
                <strong>${group.primary_title || group.product_title || ''}</strong>
                <small>Group: ${groupKey} &nbsp; • &nbsp; Primary store: ${group.primary_store || ''}</small>
              </div>
              <div class="quantity-display"><h2>${onHand}</h2><p>On Hand</p></div>
              <div class="quantity-display"><h2>${committed}</h2><p>Committed</p></div>
              <div class="quantity-display"><h2>${available}</h2><p>Available</p></div>
            </div>
          </summary>
          <div class="variant-details">
            <table>
              <thead><tr><th>SKU</th><th>Store</th><th>Status</th><th>Action</th></tr></thead>
              <tbody>
                ${(members || []).map(v => `
                  <tr>
                    <td>${v.sku || ''}</td>
                    <td>${v.store_name || ''}</td>
                    <td>${v.status || ''}</td>
                    <td>${
                      !v.is_primary
                        ? `<button class="outline" onclick="window.__makePrimary('${groupKey}', ${v.variant_id})">Make Primary</button>`
                        : '<strong>Primary</strong>'
                    }</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </details>`;
    });
    elements.tableContainer.innerHTML = html;
  };

  // ---- Events (ALL NULL-SAFE) ----
  if (elements.filters.view) {
    elements.filters.view.value = state.view;
    elements.filters.view.addEventListener('change', () => {
      state.view = elements.filters.view.value || 'individual';
      state.page = 1;
      loadPage();
    });
  }
  if (elements.filters.search) {
    elements.filters.search.addEventListener('input', (e) => {
      state.search = e.target.value.trim();
      state.page = 1;
      loadPage();
    });
  }
  if (elements.filters.store) {
    elements.filters.store.addEventListener('change', () => {
      state.store = elements.filters.store.value;
      state.page = 1;
      loadPage();
    });
  }
  if (elements.filters.type) {
    elements.filters.type.addEventListener('change', () => {
      state.type = elements.filters.type.value;
      state.page = 1;
      loadPage();
    });
  }
  if (elements.filters.status) {
    elements.filters.status.addEventListener('change', () => {
      state.status = elements.filters.status.value;
      state.page = 1;
      loadPage();
    });
  }
  if (elements.pagination.prev) {
    elements.pagination.prev.addEventListener('click', () => {
      if (state.page > 1) { state.page -= 1; loadPage(); }
    });
  }
  if (elements.pagination.next) {
    elements.pagination.next.addEventListener('click', () => {
      const totalPages = Math.max(1, Math.ceil((state.totalCount || 0) / state.pageSize));
      if (state.page < totalPages) { state.page += 1; loadPage(); }
    });
  }

  // ---- Init ----
  loadFilters().then(loadPage);
});
