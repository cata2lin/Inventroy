// static/js/inventory.js
document.addEventListener('DOMContentLoaded', () => {
  // --- Element References ---
  const elements = {
    metrics: document.getElementById('metrics-container'),
    filters: {
      search: document.getElementById('search-input'),
      store: document.getElementById('store-filter'),
      type: document.getElementById('type-filter'),
      category: document.getElementById('category-filter'),
      status: document.getElementById('status-filter'),
      minRetail: document.getElementById('min-retail-input'),
      maxRetail: document.getElementById('max-retail-input'),
      minInv: document.getElementById('min-inv-input'),
      maxInv: document.getElementById('max-inv-input'),
      viewSelect: document.getElementById('view-toggle'),
      groupToggle: document.getElementById('group-toggle'),
      reset: document.getElementById('reset-filters'),
    },
    tableContainer: document.getElementById('inventory-table-container'),
    pagination: {
      prev: document.getElementById('prev-button'),
      next: document.getElementById('next-button'),
      indicator: document.getElementById('page-indicator'),
    },
    groupedSort: {
      wrapper: document.getElementById('group-sort-controls'),
      by: document.getElementById('group-sort-by'),
      order: document.getElementById('group-sort-order'),
      apply: document.getElementById('apply-group-sort'),
    },
  };

  // --- Helpers ---
  const qsEncode = (obj) =>
    Object.entries(obj)
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&');

  const fetchJSON = async (url) => {
    const res = await fetch(url);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  };

  // FIX: New helper function to format currency for consistent display
  const formatCurrency = (value) => {
    if (value === null || isNaN(value)) return 'N/A';
    return Number(value).toLocaleString('ro-RO', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  };

  // Parse existing query to support back/forward navigation and deep links
  const urlParams = new URLSearchParams(window.location.search);

  const state = {
    page: Number(urlParams.get('page') || 1),
    pageSize: 50,
    sortBy: (urlParams.get('sortBy') || 'on_hand').toLowerCase(),
    sortOrder: (urlParams.get('sortOrder') || 'desc').toLowerCase(),
    view: (urlParams.get('view') || (elements.groupToggle?.checked ? 'grouped' : 'individual') || 'individual').toLowerCase(),
    search: urlParams.get('search') || '',
    store: urlParams.get('store') || '',
    status: urlParams.get('statuses') || '',
    type: urlParams.get('types') || '',
  };

  // reflect state into controls if they exist
  if (elements.filters.search) elements.filters.search.value = state.search;
  if (elements.filters.store) elements.filters.store.value = state.store;
  if (elements.filters.type) elements.filters.type.value = state.type;
  if (elements.filters.status) elements.filters.status.value = state.status;
  if (elements.filters.groupToggle) elements.filters.groupToggle.checked = state.view === 'grouped';
  if (elements.groupedSort.by) elements.groupedSort.by.value = state.sortBy;
  if (elements.groupedSort.order) elements.groupedSort.order.value = state.sortOrder;
  if (elements.groupedSort.wrapper) elements.groupedSort.wrapper.style.display = (state.view === 'grouped' ? 'flex' : 'none');

  // --- Load filters ---
  const loadFilters = async () => {
    try {
      const data = await fetchJSON('/api/v2/inventory/filters/');
      (data.stores || []).forEach(s => {
        const opt = document.createElement('option');
        opt.value = s.id; opt.textContent = s.name;
        elements.filters.store.appendChild(opt);
      });
      (data.types || []).forEach(t => {
        const opt = document.createElement('option');
        opt.value = t; opt.textContent = t;
        elements.filters.type.appendChild(opt);
      });
      (data.statuses || []).forEach(s => {
        const opt = document.createElement('option');
        opt.value = s; opt.textContent = s;
        elements.filters.status.appendChild(opt);
      });
    } catch (e) {
      console.error('Failed loading filters', e);
    }
  };

  // --- URL sync ---
  const pushURL = () => {
    const params = new URLSearchParams(window.location.search);
    params.set('page', String(state.page));
    params.set('sortBy', state.sortBy);
    params.set('sortOrder', state.sortOrder);
    params.set('view', state.view);
    if (state.search) params.set('search', state.search); else params.delete('search');
    if (state.store) params.set('store', state.store); else params.delete('store');
    if (state.status) params.set('statuses', state.status); else params.delete('statuses');
    if (state.type) params.set('types', state.type); else params.delete('types');
    history.replaceState(null, '', `?${params.toString()}`);
  };

  // --- Load page ---
  const loadPage = async () => {
    pushURL();
    elements.tableContainer.setAttribute('aria-busy', 'true');
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
    const url = `/api/v2/inventory/report/?${qsEncode(params)}`;
    try {
      const data = await fetchJSON(url);
      renderAll(data);
    } catch (e) {
      console.error(e);
      elements.tableContainer.innerHTML = `<p class="error">Failed to load inventory.</p>`;
    } finally {
      elements.tableContainer.removeAttribute('aria-busy');
    }
  };

  const renderAll = (data) => {
    renderMetrics(data);
    if (state.view === 'grouped') {
      renderGroupedView(data.inventory || []);
    } else {
      renderIndividualView(data.inventory || []);
    }
    updatePagination(data.total_count || 0);
  };

  const renderMetrics = (data) => {
    // FIX: Re-implement the currency formatter here for consistency
    const fmtCurrency = (v) => formatCurrency(v) + ' RON';
    elements.metrics.innerHTML = `
      <div class="metric"><h4>${fmtCurrency(data.total_retail_value)}</h4><p>Total Retail Value</p></div>
      <div class="metric"><h4>${fmtCurrency(data.total_inventory_value)}</h4><p>Total Inventory Value</p></div>
      <div class="metric"><h4>${(data.total_on_hand || 0).toLocaleString()}</h4><p>Total Products On Hand</p></div>`;
  };

  // Navigate to product details page (on image click only)
  const openDetailsPage = (groupKey) => {
    if (!groupKey) return;
    window.location.href = `/inventory/product/${encodeURIComponent(groupKey)}`;
  };

  // --- INDIVIDUAL VIEW ---
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
    let tableHtml = '<div class="overflow-auto"><table><thead><tr>';
    headers.forEach(h => {
      const sortClass = state.sortBy === h.key ? `class="${state.sortOrder}"` : '';
      const sortable = h.sortable !== false ? `data-sort-by="${h.key}"` : '';
      tableHtml += `<th ${sortable} ${sortClass}>${h.label}</th>`;
    });
    tableHtml += '</tr></thead><tbody>';

    (inventory || []).forEach(item => {
      const onHand = item.on_hand || 0;
      const price = Number(item.price) || 0;
      const cost = Number(item.cost) || 0;
      const retailValue = item.retail_value ?? (onHand * price);
      const invValue = item.inventory_value ?? (onHand * cost);
      const groupKey = item.group_id || item.barcode || item.sku || '';
      tableHtml += `
        <tr>
          <td>
            <img src="${item.image_url || '/static/img/placeholder.png'}" alt="${item.product_title || ''}" style="width:48px;height:48px;cursor:pointer;border-radius:6px"
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
          <td>${formatCurrency(retailValue)}</td>
          <td>${formatCurrency(invValue)}</td>
        </tr>`;
    });

    tableHtml += '</tbody></table></div>';
    elements.tableContainer.innerHTML = tableHtml;

    // column sorting
    elements.tableContainer.querySelectorAll('th[data-sort-by]').forEach(th => {
      th.onclick = () => {
        const k = th.getAttribute('data-sort-by');
        if (state.sortBy === k) {
          state.sortOrder = state.sortOrder === 'asc' ? 'desc' : 'asc';
        } else {
          state.sortBy = k;
          state.sortOrder = 'desc';
        }
        state.page = 1;
        loadPage();
      };
    });
  };

  // --- GROUPED VIEW ---
  const renderGroupedView = (inventory) => {
    if (!Array.isArray(inventory)) inventory = [];
    let html = '';
    inventory.forEach(group => {
      const available = group.available || 0;               // dedup across stores
      const committed = group.committed || group.committed_total || 0; // total across stores
      const totalStock = group.total_stock != null ? group.total_stock : (available + committed);
      const groupKey = group.group_id || group.barcode || 'UNKNOWN';
      const members = group.variants_json || group.variants || group.members || [];

      const retailValue = Number(group.retail_value || 0);
      const invValue = Number(group.inventory_value || 0);

      html += `
        <details class="grouped-item">
          <summary>
            <div class="group-summary" style="display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap;">
              <div class="left" style="display:flex;align-items:center;gap:.75rem;min-width:260px;">
                <img src="${group.primary_image_url || '/static/img/placeholder.png'}"
                     alt="${group.primary_title || ''}"
                     style="cursor:pointer;width:60px;height:60px;border-radius:6px;flex:0 0 auto"
                     onclick="window.__openDetails('${groupKey}')">
                <div class="product-info" style="min-width:160px;">
                  <strong>${group.primary_title || group.product_title || ''}</strong><br>
                  <small>Group: ${groupKey} • ${group.primary_store || ''}</small>
                </div>
              </div>
              <div class="metrics-row" style="display:flex;gap:1rem;align-items:center;flex-wrap:nowrap;overflow:auto;white-space:nowrap;">
                <div class="metric-pill" title="Available (deduped across stores)" style="display:flex;flex-direction:column;align-items:center;min-width:90px;">
                  <span class="value" style="font-weight:700">${available}</span>
                  <span class="label">Available</span>
                </div>
                <div class="metric-pill" title="Committed across all stores" style="display:flex;flex-direction:column;align-items:center;min-width:90px;">
                  <span class="value" style="font-weight:700">${committed}</span>
                  <span class="label">Committed</span>
                </div>
                <div class="metric-pill" title="Available + Committed" style="display:flex;flex-direction:column;align-items:center;min-width:90px;">
                  <span class="value" style="font-weight:700">${totalStock}</span>
                  <span class="label">Total stock</span>
                </div>
                <div class="metric-pill" title="Available × Price" style="display:flex;flex-direction:column;align-items:center;min-width:110px;">
                  <span class="value" style="font-weight:700">${formatCurrency(retailValue)}</span>
                  <span class="label">Retail value</span>
                </div>
                <div class="metric-pill" title="Available × Cost" style="display:flex;flex-direction:column;align-items:center;min-width:110px;">
                  <span class="value" style="font-weight:700">${formatCurrency(invValue)}</span>
                  <span class="label">Inv. value</span>
                </div>
              </div>
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
                    <td>${!v.is_primary
                      ? `<button class="outline" onclick="window.__makePrimary('${groupKey}', ${v.variant_id})">Make Primary</button>`
                      : '<strong>Primary</strong>'}
                    </td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </details>`;
    });
    elements.tableContainer.innerHTML = html;
  };

  // Global helpers for onclick in markup
  window.__openDetails = (groupKey) => openDetailsPage(groupKey);
  window.__makePrimary = async (groupKey, variantId) => {
    try {
      await fetch('/api/v2/inventory/set-primary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ barcode: groupKey, variant_id: variantId }),
      });
      loadPage(); // refresh list
    } catch (e) {
      console.error('Failed to set primary', e);
    }
  };

  // --- Pagination ---
  const updatePagination = (totalCount) => {
    const totalPages = Math.max(1, Math.ceil((totalCount || 0) / state.pageSize));
    elements.pagination.indicator.textContent = `Page ${state.page} / ${totalPages}`;
    elements.pagination.prev.disabled = state.page <= 1;
    elements.pagination.next.disabled = state.page >= totalPages;
  };
  elements.pagination.prev.onclick = () => {
    if (state.page > 1) { state.page -= 1; loadPage(); }
  };
  elements.pagination.next.onclick = () => {
    state.page += 1; loadPage();
  };

  // --- Filters / events ---
  if (elements.filters.groupToggle) {
    elements.filters.groupToggle.onchange = () => {
      state.view = elements.filters.groupToggle.checked ? 'grouped' : 'individual';
      if (elements.groupedSort.wrapper) elements.groupedSort.wrapper.style.display = (state.view === 'grouped' ? 'flex' : 'none');
      state.page = 1;
      loadPage();
    };
  }

  if (elements.filters.search) elements.filters.search.oninput = (e) => { state.search = e.target.value.trim(); state.page = 1; loadPage(); };
  if (elements.filters.store) elements.filters.store.onchange = () => { state.store = elements.filters.store.value; state.page = 1; loadPage(); };
  if (elements.filters.type) elements.filters.type.onchange = () => { state.type = elements.filters.type.value; state.page = 1; loadPage(); };
  if (elements.filters.status) elements.filters.status.onchange = () => { state.status = elements.filters.status.value; state.page = 1; loadPage(); };
  if (elements.filters.reset) elements.filters.reset.onclick = () => {
    state.search = '';
    state.store = '';
    state.type = '';
    state.status = '';
    state.sortBy = 'on_hand';
    state.sortOrder = 'desc';
    state.view = (elements.filters.groupToggle?.checked ? 'grouped' : 'individual');
    state.page = 1;
    if (elements.filters.search) elements.filters.search.value = '';
    if (elements.filters.store) elements.filters.store.value = '';
    if (elements.filters.type) elements.filters.type.value = '';
    if (elements.filters.status) elements.filters.status.value = '';
    if (elements.groupedSort.by) elements.groupedSort.by.value = 'on_hand';
    if (elements.groupedSort.order) elements.groupedSort.order.value = 'desc';
    loadPage();
  };

  // grouped sorting controls
  if (elements.groupedSort.apply) {
    elements.groupedSort.apply.onclick = () => {
      state.view = 'grouped';
      state.sortBy = elements.groupedSort.by?.value || 'on_hand';
      state.sortOrder = elements.groupedSort.order?.value || 'desc';
      state.page = 1;
      if (elements.filters.groupToggle) elements.filters.groupToggle.checked = true;
      loadPage();
    };
  }

  // Init
  loadFilters().then(loadPage);
});