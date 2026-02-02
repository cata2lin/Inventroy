// static/js/data_quality.js
document.addEventListener('DOMContentLoaded', () => {
    const el = {
        container: document.getElementById('issues-container'),
        store: document.getElementById('store-filter'),
        issueType: document.getElementById('issue-filter'),
        search: document.getElementById('search-input'),
        prev: document.getElementById('prev-button'),
        next: document.getElementById('next-button'),
        page: document.getElementById('page-indicator'),
        cards: document.querySelectorAll('.issue-card'),
        countNoBarcode: document.getElementById('count-no-barcode'),
        countNoSku: document.getElementById('count-no-sku'),
        countSkuMismatch: document.getElementById('count-sku-mismatch'),
        countBarcodeMismatch: document.getElementById('count-barcode-mismatch'),
    };

    const state = {
        page: 1,
        limit: 50,
        total: 0,
        storeId: '',
        issueType: '',
        search: '',
    };

    const issueLabels = {
        no_barcode: '🏷️ Missing Barcode',
        no_sku: '🔖 Missing SKU',
        sku_mismatch: '⚠️ SKU Mismatch',
        barcode_mismatch: '🔀 Barcode Mismatch',
    };

    const debounce = (fn, ms) => {
        let t;
        return (...args) => {
            clearTimeout(t);
            t = setTimeout(() => fn(...args), ms);
        };
    };

    const renderTable = (issues) => {
        if (!issues || issues.length === 0) {
            el.container.innerHTML = `<tr><td colspan="5" class="text-center">✅ No issues found! All data looks good.</td></tr>`;
            return;
        }
        el.container.innerHTML = issues.map(issue => {
            const imageUrl = issue.image_url || 'https://via.placeholder.com/48';
            const title = issue.title || '—';
            const sku = issue.sku || '<em style="opacity:0.5">empty</em>';
            const barcode = issue.barcode || '<em style="opacity:0.5">empty</em>';
            const label = issueLabels[issue.issue_type] || issue.issue_type;

            return `
        <tr>
          <td>
            <div style="display:flex;align-items:center;gap:.75rem;">
              <img src="${imageUrl}" style="width:40px;height:40px;object-fit:cover;border-radius:6px;" alt="${title}">
              <div>
                <strong>${title}</strong>
              </div>
            </div>
          </td>
          <td>${sku}</td>
          <td>${barcode}</td>
          <td><small>${issue.store_name || '—'}</small></td>
          <td><span class="issue-badge ${issue.issue_type}">${label}</span></td>
        </tr>`;
        }).join('');
    };

    const updateSummary = (summary) => {
        el.countNoBarcode.textContent = summary.no_barcode || 0;
        el.countNoSku.textContent = summary.no_sku || 0;
        el.countSkuMismatch.textContent = summary.sku_mismatch || 0;
        el.countBarcodeMismatch.textContent = summary.barcode_mismatch || 0;

        // Color-code: green if 0, red if > 0
        el.countNoBarcode.style.color = summary.no_barcode > 0 ? 'var(--pico-del-color)' : 'var(--pico-ins-color)';
        el.countNoSku.style.color = summary.no_sku > 0 ? 'var(--pico-del-color)' : 'var(--pico-ins-color)';
        el.countSkuMismatch.style.color = summary.sku_mismatch > 0 ? 'var(--pico-del-color)' : 'var(--pico-ins-color)';
        el.countBarcodeMismatch.style.color = summary.barcode_mismatch > 0 ? 'var(--pico-del-color)' : 'var(--pico-ins-color)';
    };

    const updatePagination = () => {
        const pages = Math.max(1, Math.ceil(state.total / state.limit));
        el.page.textContent = `Page ${state.page} of ${pages}`;
        el.prev.disabled = state.page <= 1;
        el.next.disabled = state.page >= pages;
    };

    const fetchIssues = async () => {
        el.container.innerHTML = `<tr><td colspan="5" class="text-center" aria-busy="true">Loading...</td></tr>`;

        const params = new URLSearchParams({
            skip: String((state.page - 1) * state.limit),
            limit: String(state.limit),
        });

        if (state.storeId) params.set('store_id', state.storeId);
        if (state.issueType) params.set('issue_type', state.issueType);
        if (state.search) params.set('search', state.search);

        try {
            const res = await fetch(`/api/data-quality/issues?${params.toString()}`);
            if (!res.ok) throw new Error('fetch failed');
            const data = await res.json();
            state.total = data.total_count || 0;
            updateSummary(data.summary || {});
            renderTable(data.issues || []);
            updatePagination();
        } catch (e) {
            console.error(e);
            el.container.innerHTML = `<tr><td colspan="5" class="text-center" style="color: var(--pico-del-color);">Failed to load data. Please try again.</td></tr>`;
        }
    };

    const setupEvents = () => {
        // Store filter
        el.store.addEventListener('change', () => {
            state.storeId = el.store.value;
            state.page = 1;
            fetchIssues();
        });

        // Issue type filter
        el.issueType.addEventListener('change', () => {
            state.issueType = el.issueType.value;
            // Update selected card
            el.cards.forEach(card => card.classList.remove('selected'));
            if (state.issueType) {
                const selectedCard = document.querySelector(`.issue-card[data-issue="${state.issueType}"]`);
                if (selectedCard) selectedCard.classList.add('selected');
            }
            state.page = 1;
            fetchIssues();
        });

        // Search with debounce
        const debouncedSearch = debounce(() => {
            state.search = el.search.value;
            state.page = 1;
            fetchIssues();
        }, 400);
        el.search.addEventListener('input', debouncedSearch);

        // Click on summary cards to filter
        el.cards.forEach(card => {
            card.addEventListener('click', () => {
                const issueType = card.getAttribute('data-issue');
                el.issueType.value = issueType;
                el.issueType.dispatchEvent(new Event('change'));
            });
        });

        // Pagination
        el.prev.addEventListener('click', () => {
            if (state.page > 1) {
                state.page--;
                fetchIssues();
            }
        });
        el.next.addEventListener('click', () => {
            const pages = Math.max(1, Math.ceil(state.total / state.limit));
            if (state.page < pages) {
                state.page++;
                fetchIssues();
            }
        });
    };

    const init = async () => {
        // Load stores
        try {
            const res = await fetch('/api/data-quality/stores');
            if (!res.ok) throw new Error('stores failed');
            const stores = await res.json();
            stores.forEach(s => el.store.add(new Option(s.name, s.id)));
        } catch (e) {
            console.error(e);
        }

        setupEvents();
        fetchIssues();
    };

    init();
});
