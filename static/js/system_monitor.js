// static/js/system_monitor.js
document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const tabs = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');
    const modal = document.getElementById('detail-modal');
    const modalTitle = document.getElementById('detail-modal-title');
    const modalBody = document.getElementById('detail-modal-body');

    const filters = {
        search: document.getElementById('monitor-search'),
        category: document.getElementById('monitor-category'),
        severity: document.getElementById('monitor-severity'),
        store: document.getElementById('monitor-store'),
    };

    // --- State ---
    const state = {
        activeTab: 'audit-log',
        audit: { page: 1, limit: 50, total: 0 },
        errors: { page: 1, limit: 50, total: 0 },
        webhooks: { page: 1, limit: 50, total: 0 },
    };

    // --- Utility ---
    const debounce = (fn, ms) => {
        let t;
        return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
    };

    const formatTime = (isoStr) => {
        if (!isoStr) return '—';
        const d = new Date(isoStr);
        const now = new Date();
        const diffMs = now - d;
        const diffMin = Math.floor(diffMs / 60000);
        const diffHr = Math.floor(diffMs / 3600000);

        if (diffMin < 1) return 'just now';
        if (diffMin < 60) return `${diffMin}m ago`;
        if (diffHr < 24) return `${diffHr}h ago`;
        return d.toLocaleString('ro-RO', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    };

    const severityDot = (sev) => `<span class="severity-dot ${sev}"></span>${sev}`;
    const categoryPill = (cat) => `<span class="category-pill ${cat}">${cat}</span>`;

    // --- Tab Switching ---
    tabs.forEach(btn => {
        btn.addEventListener('click', () => {
            tabs.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const tabId = btn.dataset.tab;
            state.activeTab = tabId;
            tabContents.forEach(tc => tc.style.display = 'none');
            document.getElementById(`tab-${tabId}`).style.display = '';

            // Show/hide category filter for non-audit tabs
            const catWrapper = document.getElementById('category-filter-wrapper');
            if (tabId === 'audit-log') {
                catWrapper.style.display = '';
            } else {
                catWrapper.style.display = 'none';
            }

            loadActiveTab();
        });
    });

    // --- Stats Dashboard ---
    const loadStats = async () => {
        try {
            const res = await fetch('/api/system-monitor/stats');
            const data = await res.json();

            document.getElementById('stat-total-events').textContent = (data.total_events_24h || 0).toLocaleString();
            document.getElementById('stat-errors').textContent = data.errors_24h || 0;
            document.getElementById('stat-webhooks').textContent = (data.webhooks_24h || 0).toLocaleString();
            document.getElementById('stat-webhooks-rate').textContent = `${data.webhooks_per_hour || 0}/hour`;
            document.getElementById('stat-avg-webhook').textContent = data.avg_webhook_ms ? `${data.avg_webhook_ms}ms` : '—';
            document.getElementById('stat-stock-changes').textContent = (data.stock_changes_24h || 0).toLocaleString();
            document.getElementById('stat-unresolved').textContent = data.unresolved_errors || 0;

            // Color the error count
            const errEl = document.getElementById('stat-errors');
            errEl.style.color = data.errors_24h > 0 ? 'var(--color-danger)' : 'var(--color-success)';
        } catch (e) {
            console.error('Failed to load stats:', e);
        }
    };

    // --- Audit Log Tab ---
    const loadAuditLogs = async () => {
        const container = document.getElementById('audit-log-container');
        container.setAttribute('aria-busy', 'true');
        container.innerHTML = '';

        const params = new URLSearchParams({
            skip: (state.audit.page - 1) * state.audit.limit,
            limit: state.audit.limit,
        });
        if (filters.search.value) params.set('search', filters.search.value);
        if (filters.category.value) params.set('category', filters.category.value);
        if (filters.severity.value) params.set('severity', filters.severity.value);
        if (filters.store.value) params.set('store_id', filters.store.value);

        try {
            const res = await fetch(`/api/system-monitor/audit-logs?${params}`);
            const data = await res.json();
            state.audit.total = data.total_count;

            if (!data.logs || data.logs.length === 0) {
                container.innerHTML = '<p style="padding: 2rem; text-align: center;">No audit logs found.</p>';
            } else {
                container.innerHTML = `
                    <table>
                        <thead>
                            <tr>
                                <th style="width:40px;">Sev</th>
                                <th style="width:120px;">Category</th>
                                <th>Message</th>
                                <th style="width:100px;">Target</th>
                                <th style="width:90px;">Duration</th>
                                <th style="width:110px;">Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.logs.map(l => `
                                <tr class="log-row" data-log='${JSON.stringify(l).replace(/'/g, "&#39;")}'>
                                    <td>${severityDot(l.severity)}</td>
                                    <td>${categoryPill(l.category)}</td>
                                    <td>
                                        <strong style="font-size: 0.85rem;">${escapeHtml(l.message).substring(0, 100)}${l.message.length > 100 ? '…' : ''}</strong>
                                        ${l.store_name ? '<br><small style="color: var(--color-text-muted);">' + escapeHtml(l.store_name) + '</small>' : ''}
                                    </td>
                                    <td>${l.target ? '<code>' + escapeHtml(l.target) + '</code>' : '—'}</td>
                                    <td>${l.duration_ms !== null ? l.duration_ms + 'ms' : '—'}</td>
                                    <td><small>${formatTime(l.timestamp)}</small></td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                `;
            }
            updatePagination('audit', state.audit);
        } catch (e) {
            container.innerHTML = `<p style="color: var(--color-danger); padding: 1rem;">Failed to load audit logs.</p>`;
        } finally {
            container.removeAttribute('aria-busy');
        }
    };

    // --- Errors Tab ---
    const loadErrors = async () => {
        const container = document.getElementById('errors-container');
        container.setAttribute('aria-busy', 'true');
        container.innerHTML = '';

        const params = new URLSearchParams({
            skip: (state.errors.page - 1) * state.errors.limit,
            limit: state.errors.limit,
            resolved: 'false',
        });
        if (filters.search.value) params.set('source', filters.search.value);

        try {
            const res = await fetch(`/api/system-monitor/errors?${params}`);
            const data = await res.json();
            state.errors.total = data.total_count;

            if (!data.events || data.events.length === 0) {
                container.innerHTML = '<p style="padding: 2rem; text-align: center; color: var(--color-success);">No unresolved errors. System is healthy.</p>';
            } else {
                container.innerHTML = `
                    <table>
                        <thead>
                            <tr>
                                <th style="width:40px;">Level</th>
                                <th style="width:180px;">Source</th>
                                <th>Message</th>
                                <th style="width:80px;">Status</th>
                                <th style="width:110px;">Time</th>
                                <th style="width:80px;">Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.events.map(e => `
                                <tr class="log-row" data-error='${JSON.stringify(e).replace(/'/g, "&#39;")}'>
                                    <td>${severityDot(e.level)}</td>
                                    <td><code style="font-size: 0.75rem;">${escapeHtml(e.source)}</code></td>
                                    <td><strong style="font-size: 0.85rem;">${escapeHtml(e.message).substring(0, 120)}${e.message.length > 120 ? '…' : ''}</strong></td>
                                    <td>${e.resolved ? '<span class="badge badge-success">Resolved</span>' : '<span class="badge badge-danger">Open</span>'}</td>
                                    <td><small>${formatTime(e.timestamp)}</small></td>
                                    <td>${!e.resolved ? `<button class="resolve-btn" data-id="${e.id}" style="padding:0.25rem 0.5rem;font-size:0.7rem;">Resolve</button>` : '—'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                `;
            }
            updatePagination('errors', state.errors);
        } catch (e) {
            container.innerHTML = `<p style="color: var(--color-danger); padding: 1rem;">Failed to load errors.</p>`;
        } finally {
            container.removeAttribute('aria-busy');
        }
    };

    // --- Webhooks Tab ---
    const loadWebhooks = async () => {
        const container = document.getElementById('webhooks-container');
        container.setAttribute('aria-busy', 'true');
        container.innerHTML = '';

        const params = new URLSearchParams({
            skip: (state.webhooks.page - 1) * state.webhooks.limit,
            limit: state.webhooks.limit,
        });
        if (filters.store.value) params.set('store_id', filters.store.value);
        if (filters.search.value) params.set('topic', filters.search.value);

        try {
            const res = await fetch(`/api/system-monitor/webhook-history?${params}`);
            const data = await res.json();
            state.webhooks.total = data.total_count;

            if (!data.webhooks || data.webhooks.length === 0) {
                container.innerHTML = '<p style="padding: 2rem; text-align: center;">No webhook history found.</p>';
            } else {
                container.innerHTML = `
                    <table>
                        <thead>
                            <tr>
                                <th>Topic</th>
                                <th>Store</th>
                                <th>Result</th>
                                <th>Duration</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.webhooks.map(w => {
                                const isError = w.error_message || w.action.includes('rejected');
                                const statusBadge = isError
                                    ? '<span class="badge badge-danger">Rejected</span>'
                                    : w.action.includes('unhandled')
                                    ? '<span class="badge badge-warning">Unhandled</span>'
                                    : '<span class="badge badge-success">Accepted</span>';
                                return `
                                    <tr class="log-row" data-log='${JSON.stringify(w).replace(/'/g, "&#39;")}'>
                                        <td><code>${escapeHtml(w.topic || '—')}</code></td>
                                        <td>${escapeHtml(w.store_name || '—')}</td>
                                        <td>${statusBadge}</td>
                                        <td>${w.duration_ms !== null ? w.duration_ms + 'ms' : '—'}</td>
                                        <td><small>${formatTime(w.timestamp)}</small></td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                `;
            }
            updatePagination('webhooks', state.webhooks);
        } catch (e) {
            container.innerHTML = `<p style="color: var(--color-danger); padding: 1rem;">Failed to load webhook history.</p>`;
        } finally {
            container.removeAttribute('aria-busy');
        }
    };

    // --- Pagination ---
    const updatePagination = (prefix, tabState) => {
        const totalPages = Math.max(1, Math.ceil(tabState.total / tabState.limit));
        document.getElementById(`${prefix}-page-info`).textContent = `Page ${tabState.page} of ${totalPages} (${tabState.total} entries)`;
        document.getElementById(`${prefix}-prev`).disabled = tabState.page <= 1;
        document.getElementById(`${prefix}-next`).disabled = tabState.page >= totalPages;
    };

    // Pagination buttons
    ['audit', 'errors', 'webhooks'].forEach(prefix => {
        document.getElementById(`${prefix}-prev`).addEventListener('click', () => {
            if (state[prefix].page > 1) {
                state[prefix].page--;
                loadActiveTab();
            }
        });
        document.getElementById(`${prefix}-next`).addEventListener('click', () => {
            const totalPages = Math.ceil(state[prefix].total / state[prefix].limit);
            if (state[prefix].page < totalPages) {
                state[prefix].page++;
                loadActiveTab();
            }
        });
    });

    // --- Detail Modal ---
    const showDetail = (data, title) => {
        modalTitle.textContent = title || 'Event Details';
        let html = '<div style="max-height: 60vh; overflow-y: auto;">';

        // Build key-value pairs
        const pairs = Object.entries(data).filter(([k, v]) => v !== null && v !== undefined);
        html += '<table style="font-size: 0.85rem;">';
        for (const [key, value] of pairs) {
            if (key === 'details' || key === 'stack_trace') continue;
            html += `<tr><td style="padding: 0.35rem 0.75rem; font-weight: 600; white-space: nowrap; color: var(--color-text-secondary);">${key}</td>`;
            html += `<td style="padding: 0.35rem 0.75rem; word-break: break-all;">${typeof value === 'string' ? escapeHtml(value) : JSON.stringify(value)}</td></tr>`;
        }
        html += '</table>';

        // Details (JSONB)
        if (data.details) {
            html += '<h4 style="margin-top: 1rem; font-size: 0.85rem;">Details (JSON)</h4>';
            html += `<pre style="background: var(--zinc-800); padding: 0.75rem; border-radius: var(--radius-md); font-size: 0.75rem; overflow-x: auto; max-height: 200px;"><code>${escapeHtml(JSON.stringify(data.details, null, 2))}</code></pre>`;
        }

        // Stack trace
        if (data.stack_trace) {
            html += '<h4 style="margin-top: 1rem; font-size: 0.85rem; color: var(--color-danger);">Stack Trace</h4>';
            html += `<pre style="background: rgba(239,68,68,0.08); padding: 0.75rem; border-radius: var(--radius-md); font-size: 0.7rem; overflow-x: auto; max-height: 300px; color: var(--color-danger);"><code>${escapeHtml(data.stack_trace)}</code></pre>`;
        }

        html += '</div>';
        modalBody.innerHTML = html;
        modal.showModal();
    };

    // Row click handlers
    document.addEventListener('click', (e) => {
        // Log row click → show detail
        const row = e.target.closest('.log-row');
        if (row) {
            const logData = row.dataset.log ? JSON.parse(row.dataset.log) : (row.dataset.error ? JSON.parse(row.dataset.error) : null);
            if (logData) showDetail(logData, logData.message ? logData.message.substring(0, 60) : 'Event Details');
            return;
        }

        // Resolve button
        if (e.target.classList.contains('resolve-btn')) {
            e.stopPropagation();
            resolveError(e.target.dataset.id);
            return;
        }

        // Modal close
        if (e.target.matches('.close') || e.target === modal) {
            modal.close();
        }
    });

    const resolveError = async (id) => {
        try {
            const res = await fetch(`/api/system-monitor/errors/${id}/resolve`, { method: 'POST' });
            if (res.ok) {
                loadErrors();
                loadStats();
            }
        } catch (e) {
            console.error('Failed to resolve error:', e);
        }
    };

    // --- Load Active Tab ---
    const loadActiveTab = () => {
        if (state.activeTab === 'audit-log') loadAuditLogs();
        else if (state.activeTab === 'errors') loadErrors();
        else if (state.activeTab === 'webhooks') loadWebhooks();
    };

    // --- Filter Events ---
    const debouncedLoad = debounce(() => {
        // Reset all pages on filter change
        state.audit.page = 1;
        state.errors.page = 1;
        state.webhooks.page = 1;
        loadActiveTab();
    }, 400);

    filters.search.addEventListener('input', debouncedLoad);
    filters.category.addEventListener('change', debouncedLoad);
    filters.severity.addEventListener('change', debouncedLoad);
    filters.store.addEventListener('change', debouncedLoad);

    // --- Load Stores ---
    const loadStores = async () => {
        try {
            const res = await fetch('/api/config/stores');
            const stores = await res.json();
            stores.forEach(s => filters.store.add(new Option(s.name, s.id)));
        } catch (e) {
            console.error('Failed to load stores:', e);
        }
    };

    // --- Escape HTML ---
    function escapeHtml(str) {
        if (!str) return '';
        return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // --- Init ---
    loadStores();
    loadStats();
    loadActiveTab();

    // Auto-refresh stats every 30 seconds
    setInterval(loadStats, 30000);
});
