document.addEventListener('DOMContentLoaded', () => {
  const el = {
    container: document.getElementById('products-container'),
    thead: document.getElementById('table-head'),
    store: document.getElementById('store-filter'),
    search: document.getElementById('search-input'),
    velocityDays: document.getElementById('velocity-days'),
    trigger: document.getElementById('trigger-snapshot-btn'),
    prev: document.getElementById('prev-button'),
    next: document.getElementById('next-button'),
    page: document.getElementById('page-indicator'),
  };

  const state = {
    page: 1,
    limit: 25,
    total: 0,
    storeId: '',
    q: '',
    sortField: 'days_left',
    sortOrder: 'asc',
    velocityDays: 7,
  };

  // Format days left with color coding
  const formatDaysLeft = (days) => {
    if (days === null || days === undefined) return '—';
    const d = Number(days);
    if (Number.isNaN(d)) return '—';

    let emoji, color;
    if (d < 7) {
      emoji = '🔴';
      color = 'var(--pico-del-color, #c62828)';
    } else if (d <= 30) {
      emoji = '🟡';
      color = 'var(--pico-mark-background-color, #ff9800)';
    } else {
      emoji = '🟢';
      color = 'var(--pico-ins-color, #2e7d32)';
    }
    return `<span style="color: ${color}; font-weight: 600;">${emoji} ${d.toFixed(0)} days</span>`;
  };

  const formatVelocity = (vel) => {
    if (vel === null || vel === undefined || vel === 0) return '—';
    const v = Number(vel);
    if (Number.isNaN(v)) return '—';
    return `${v.toFixed(2)}/day`;
  };

  const renderTable = (rows) => {
    if (!rows || rows.length === 0) {
      el.container.innerHTML = `<tr><td colspan="5" class="text-center">No products found. Try adjusting your filters or trigger a snapshot first.</td></tr>`;
      return;
    }
    el.container.innerHTML = rows.map(p => {
      const imageUrl = p.image_url || 'https://via.placeholder.com/48';
      const title = p.title || '—';
      const sku = p.sku || '—';
      const barcode = p.barcode || '';
      const barcodeDisplay = barcode ? `<br><small style="opacity:0.6">Barcode: ${barcode}</small>` : '';

      return `
        <tr>
          <td>
            <div style="display:flex;align-items:center;gap:.75rem;">
              <img src="${imageUrl}" style="width:48px;height:48px;object-fit:cover;border-radius:8px;" alt="${title}">
              <div>
                <strong>${title}</strong><br>
                <small>SKU: ${sku}</small>${barcodeDisplay}
              </div>
            </div>
          </td>
          <td><strong>${p.total_stock ?? 0}</strong> units</td>
          <td>${formatVelocity(p.velocity)}</td>
          <td>${formatDaysLeft(p.days_left)}</td>
          <td><strong>${p.store_count ?? 0}</strong> stores</td>
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

  const fetchProducts = async () => {
    el.container.innerHTML = `<tr><td colspan="5" class="text-center" aria-busy="true">Loading...</td></tr>`;

    const params = new URLSearchParams({
      skip: String((state.page - 1) * state.limit),
      limit: String(state.limit),
      sort_field: state.sortField,
      sort_order: state.sortOrder,
      velocity_days: String(state.velocityDays),
    });

    if (state.storeId) params.set('store_id', state.storeId);
    if (state.q) params.set('q', state.q);

    try {
      const res = await fetch(`/api/snapshots/?${params.toString()}`);
      if (!res.ok) throw new Error('fetch failed');
      const data = await res.json();
      state.total = data.total_count || 0;
      renderTable(data.products || []);
      updatePagination();
    } catch (e) {
      console.error(e);
      el.container.innerHTML = `<tr><td colspan="5" class="text-center" style="color: var(--pico-del-color);">Failed to load data. Please try again.</td></tr>`;
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
          // Default to ASC for days_left (urgent first), DESC for others (highest first)
          state.sortOrder = field === 'days_left' ? 'asc' : 'desc';
        }
        el.thead.querySelectorAll('th.sortable').forEach(h => h.classList.remove('sorted-asc', 'sorted-desc'));
        th.classList.add(state.sortOrder === 'asc' ? 'sorted-asc' : 'sorted-desc');
        state.page = 1;
        fetchProducts();
      });
    });
  };

  const setupEvents = () => {
    // Store filter
    el.store.addEventListener('change', () => {
      state.storeId = el.store.value;
      state.page = 1;
      fetchProducts();
    });

    // Search with debounce
    const debouncedSearch = debounce(() => {
      state.q = el.search.value;
      state.page = 1;
      fetchProducts();
    }, 400);
    el.search.addEventListener('input', debouncedSearch);

    // Velocity period
    el.velocityDays.addEventListener('change', () => {
      state.velocityDays = parseInt(el.velocityDays.value, 10) || 7;
      state.page = 1;
      fetchProducts();
    });

    // Trigger snapshot
    el.trigger.addEventListener('click', async () => {
      if (!state.storeId) {
        alert('Select a store to trigger a snapshot.');
        return;
      }
      el.trigger.setAttribute('aria-busy', 'true');
      try {
        const res = await fetch(`/api/snapshots/trigger?store_id=${encodeURIComponent(state.storeId)}`, { method: 'POST' });
        if (!res.ok) throw new Error();
        alert('Snapshot triggered successfully!');
        await fetchProducts();
      } catch {
        alert('Failed to trigger snapshot.');
      } finally {
        el.trigger.removeAttribute('aria-busy');
      }
    });

    // Pagination
    el.prev.addEventListener('click', () => {
      if (state.page > 1) {
        state.page--;
        fetchProducts();
      }
    });
    el.next.addEventListener('click', () => {
      const pages = Math.max(1, Math.ceil(state.total / state.limit));
      if (state.page < pages) {
        state.page++;
        fetchProducts();
      }
    });
  };

  const init = async () => {
    // Load stores
    try {
      const res = await fetch('/api/snapshots/stores');
      if (!res.ok) throw new Error('stores failed');
      const stores = await res.json();
      stores.forEach(s => el.store.add(new Option(s.name, s.id)));
    } catch (e) {
      console.error(e);
      el.container.innerHTML = `<tr><td colspan="5" class="text-center">Failed to load stores.</td></tr>`;
      return;
    }

    attachSortHandlers();
    setupEvents();
    fetchProducts();
  };

  init();
});
