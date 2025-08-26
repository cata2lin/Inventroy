// static/js/product_details.js

document.addEventListener('DOMContentLoaded', () => {
  const groupKey = window.__GROUP_KEY__;
  const els = {
    title: document.getElementById('pd-title'),
    subtitle: document.getElementById('pd-subtitle'),
    start: document.getElementById('pd-start'),
    end: document.getElementById('pd-end'),
    store: document.getElementById('pd-store'),
    apply: document.getElementById('pd-apply'),
    quick7: document.getElementById('pd-7'),
    quick30: document.getElementById('pd-30'),
    quick90: document.getElementById('pd-90'),
    snapshot: document.getElementById('pd-snapshot'),
    velocity: document.getElementById('pd-velocity'),
    salesCanvas: document.getElementById('pd-sales-canvas'),
    stockCanvas: document.getElementById('pd-stock-canvas'),
    smTableBody: document.querySelector('#pd-sm-table tbody'),
    committedBody: document.querySelector('#pd-committed tbody'),
    ordersBody: document.querySelector('#pd-orders tbody'),
  };

  const todayISO = () => new Date().toISOString().slice(0,10);
  const addDays = (d, days) => {
    const dt = new Date(d);
    dt.setDate(dt.getDate() + days);
    return dt.toISOString().slice(0,10);
  };

  // Defaults: last 90 days
  els.end.value = todayISO();
  els.start.value = addDays(els.end.value, -89);

  const fetchJSON = async (url) => {
    const res = await fetch(url);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  };

  const qs = (obj) =>
    Object.entries(obj)
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&');

  const loadStores = async () => {
    try {
      const data = await fetchJSON('/api/v2/inventory/filters/');
      (data.stores || []).forEach(s => {
        const opt = document.createElement('option');
        opt.value = s.id; opt.textContent = s.name;
        els.store.appendChild(opt);
      });
    } catch (e) { console.error(e); }
  };

  // Sales chart (vanilla canvas)
  const drawSalesChart = (series) => {
    const ctx = els.salesCanvas.getContext('2d');
    const W = els.salesCanvas.width = els.salesCanvas.clientWidth;
    const H = els.salesCanvas.height = 220;

    ctx.clearRect(0,0,W,H);
    if (!series || !series.length) {
      ctx.fillText('No sales in selected period.', 10, 20);
      return;
    }
    // Padding
    const padL = 40, padR = 10, padT = 10, padB = 24;
    const xmin = 0, xmax = series.length - 1;
    const ymax = Math.max(...series.map(p => p.units));
    const scaleX = (x) => padL + (x - xmin) * ((W - padL - padR) / (xmax - xmin || 1));
    const scaleY = (y) => H - padB - (y) * ((H - padT - padB) / (ymax || 1));

    // Axes
    ctx.strokeStyle = '#ccc';
    ctx.beginPath(); ctx.moveTo(padL, padT); ctx.lineTo(padL, H - padB); ctx.lineTo(W - padR, H - padB); ctx.stroke();

    // Line
    ctx.beginPath();
    series.forEach((p, i) => {
      const x = scaleX(i), y = scaleY(p.units);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#1f77b4';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Dots
    ctx.fillStyle = '#1f77b4';
    series.forEach((p, i) => {
      const x = scaleX(i), y = scaleY(p.units);
      ctx.beginPath(); ctx.arc(x, y, 2.5, 0, Math.PI*2); ctx.fill();
    });

    // X labels (sparse)
    ctx.fillStyle = '#666';
    ctx.font = '10px sans-serif';
    const stride = Math.ceil(series.length / 10);
    series.forEach((p, i) => {
      if (i % stride === 0 || i === series.length - 1) {
        ctx.fillText(p.day.slice(5), scaleX(i) - 10, H - 6);
      }
    });

    // Y labels
    const yTicks = 4;
    for (let i=0;i<=yTicks;i++){
      const v = Math.round((ymax / yTicks)*i);
      ctx.fillText(String(v), 6, scaleY(v)+3);
    }
  };

    const drawStockChart = (series) => {
        const ctx = els.stockCanvas.getContext('2d');
        const W = els.stockCanvas.width = els.stockCanvas.clientWidth;
        const H = els.stockCanvas.height = 220;

        ctx.clearRect(0, 0, W, H);
        if (!series || !series.length) {
            ctx.fillText('No stock movements in selected period.', 10, 20);
            return;
        }

        // Padding
        const padL = 40, padR = 10, padT = 10, padB = 24;
        const xmin = 0, xmax = series.length - 1;
        const ymax = Math.max(...series.map(p => p.new_quantity));
        const ymin = Math.min(...series.map(p => p.new_quantity));
        const scaleX = (x) => padL + (x - xmin) * ((W - padL - padR) / (xmax - xmin || 1));
        const scaleY = (y) => H - padB - (y - ymin) * ((H - padT - padB) / (ymax - ymin || 1));

        // Axes
        ctx.strokeStyle = '#ccc';
        ctx.beginPath();
        ctx.moveTo(padL, padT);
        ctx.lineTo(padL, H - padB);
        ctx.lineTo(W - padR, H - padB);
        ctx.stroke();

        // Line
        ctx.beginPath();
        series.forEach((p, i) => {
            const x = scaleX(i),
                y = scaleY(p.new_quantity);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.strokeStyle = '#2ca02c';
        ctx.lineWidth = 2;
        ctx.stroke();

        // Dots
        ctx.fillStyle = '#2ca02c';
        series.forEach((p, i) => {
            const x = scaleX(i),
                y = scaleY(p.new_quantity);
            ctx.beginPath();
            ctx.arc(x, y, 2.5, 0, Math.PI * 2);
            ctx.fill();
        });

        // X labels (sparse)
        ctx.fillStyle = '#666';
        ctx.font = '10px sans-serif';
        const stride = Math.ceil(series.length / 10);
        series.forEach((p, i) => {
            if (i % stride === 0 || i === series.length - 1) {
                ctx.fillText(new Date(p.created_at).toLocaleDateString(), scaleX(i) - 10, H - 6);
            }
        });

        // Y labels
        const yTicks = 4;
        for (let i = 0; i <= yTicks; i++) {
            const v = Math.round(ymin + (ymax - ymin) / yTicks * i);
            ctx.fillText(String(v), 6, scaleY(v) + 3);
        }
    };
  const render = (analytics, details) => {
    // Title
    els.title.textContent = analytics.header.title || 'Product Details';
    els.subtitle.textContent = `Group: ${analytics.header.group_key}`;
    // Snapshot
    const inv = analytics.inventory_snapshot;
    els.snapshot.innerHTML = `
      <div class="metric"><h4>${(inv.on_hand || 0).toLocaleString()}</h4><p>On Hand (deduped)</p></div>
      <div class="metric"><h4>${(inv.committed || 0).toLocaleString()}</h4><p>Committed</p></div>
      <div class="metric"><h4>${(inv.available || 0).toLocaleString()}</h4><p>Available</p></div>
      <div class="metric"><h4>${(analytics.metrics.life_on_shelf_days || 0).toLocaleString()}</h4><p>Life on Shelf (days)</p></div>
    `;

    // Velocity block
    const m = analytics.metrics;
    const dec = (x) => (x ?? 0).toFixed(2);
    els.velocity.innerHTML = `
      <div class="metric"><h4>${dec(m.avg_daily_sales)}</h4><p>Avg Daily (period)</p></div>
      <div class="metric"><h4>${dec(m.velocity_7)}</h4><p>Velocity 7d</p></div>
      <div class="metric"><h4>${dec(m.velocity_30)}</h4><p>Velocity 30d</p></div>
    `;

    // Chart
    drawSalesChart(analytics.sales_by_day);
    drawStockChart(details.stock_movements);
    // Stock movements
    els.smTableBody.innerHTML = (analytics.stock_movements_by_day || []).map(r =>
      `<tr><td>${r.day}</td><td>${r.change}</td></tr>`
    ).join('');

    // Committed orders
    els.committedBody.innerHTML = (details.committed_orders || []).map(o =>
      `<tr><td>${(o.created_at || '').slice(0,10)}</td><td>${o.name}</td><td>${o.quantity}</td><td>${o.fulfillment_status || ''}</td></tr>`
    ).join('');

    // All orders
    els.ordersBody.innerHTML = (details.all_orders || []).map(o =>
      `<tr><td>${(o.created_at || '').slice(0,10)}</td><td>${o.name}</td><td>${o.quantity}</td><td>${o.financial_status || ''}</td><td>${o.fulfillment_status || ''}</td></tr>`
    ).join('');
  };

  const loadAll = async () => {
    const params = {
      start: els.start.value,
      end: els.end.value,
      stores: els.store.value,
    };
    const analyticsUrl = `/api/v2/inventory/product-analytics/${encodeURIComponent(groupKey)}?${qs(params)}`;
    const detailsUrl = `/api/v2/inventory/product-details/${encodeURIComponent(groupKey)}`;

    try {
      const [analytics, details] = await Promise.all([
        fetchJSON(analyticsUrl),
        fetchJSON(detailsUrl),
      ]);
      render(analytics, details);
    } catch (e) {
      console.error(e);
      alert('Failed to load product analytics');
    }
  };

  // Events
  els.apply.onclick = loadAll;
  els.quick7.onclick = () => { els.end.value = todayISO(); els.start.value = addDays(els.end.value, -6); loadAll(); };
  els.quick30.onclick = () => { els.end.value = todayISO(); els.start.value = addDays(els.end.value, -29); loadAll(); };
  els.quick90.onclick = () => { els.end.value = todayISO(); els.start.value = addDays(els.end.value, -89); loadAll(); };

  // Init
  loadStores().then(loadAll);
});