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
      view: document.getElementById('view-toggle'),
    },
    tableContainer: document.getElementById('inventory-table-container'),
    pagination: {
      prev: document.getElementById('prev-button'),
      next: document.getElementById('next-button'),
      indicator: document.getElementById('page-indicator'),
    },
  };

  // --- State ---
  const state = {
    page: 1,
    pageSize: 50,
    sortBy: 'on_hand',
    sortOrder: 'desc',
    view: (elements.filters.view?.value || 'individual'),
    search: '',
    store: '',
    status: '',
    type: '',
  };

  // --- Helpers ---
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

  // --- Load page ---
  const loadPage = async () => {
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
    const url = `/api/v2/inventory/report/?${qs(params)}`;
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
    const fmtCurrency = (v) => (v || 0).toLocaleString('ro-RO', { maximumFractionDigits: 2 }) + ' RON';
    elements.metrics.innerHTML = `
      <div class="metric"><h4>${fmtCurrency(data.total_retail_value)}</h4><p>Total Retail Value</p></div>
      <div class="metric"><h4>${fmtCurrency(data.total_inventory_value)}</h4><p>Total Inventory Value</p></div>
      <div class="metric"><h4>${(data.total_on_hand || 0).toLocaleString()}</h4><p>Total Products On Hand</p></div>`;
  };

  // --- Views ---
  const openDetailsPage = (groupKey) => {
    if (!groupKey) return;
    window.location.href = `/inventory/product/${encodeURIComponent(groupKey)}`;
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
      const retailValue = onHand * price;
      const invValue = onHand * cost;
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
          <td>${retailValue.toFixed(2)}</td>
          <td>${invValue.toFixed(2)}</td>
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
        loadPage();
      };
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
  elements.filters.view.onchange = () => { state.view = elements.filters.view.value; state.page = 1; loadPage(); };
  elements.filters.search.oninput = (e) => { state.search = e.target.value.trim(); state.page = 1; loadPage(); };
  elements.filters.store.onchange = () => { state.store = elements.filters.store.value; state.page = 1; loadPage(); };
  elements.filters.type.onchange = () => { state.type = elements.filters.type.value; state.page = 1; loadPage(); };
  elements.filters.status.onchange = () => { state.status = elements.filters.status.value; state.page = 1; loadPage(); };

  // Init
  loadFilters().then(loadPage);
});
