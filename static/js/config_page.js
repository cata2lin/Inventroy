// static/js/config_page.js
document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const addStoreForm = document.getElementById('add-store-form');
    const storesListContainer = document.getElementById('stores-list-container');
    const addStoreBtn = document.getElementById('add-store-btn');
    const deleteAllWebhooksBtn = document.getElementById('delete-all-webhooks-btn');
    
    // Modal elements
    const editStoreModal = document.getElementById('edit-store-modal');
    const modalStoreName = document.getElementById('modal-title-store-name');
    
    // Store Settings section
    const storeSettingsForm = document.getElementById('store-settings-form');
    const syncLocationSelect = document.getElementById('sync-location-select');
    const saveSettingsBtn = document.getElementById('save-settings-btn');

    // Webhook section
    const webhookManagementSection = document.getElementById('webhook-management-section');
    const webhookUrlDisplay = document.getElementById('webhook-url-display');
    const createAllWebhooksBtn = document.getElementById('create-all-webhooks-btn');
    const webhooksListContainer = document.getElementById('webhooks-list-container');

    let currentStoreId = null;

    // --- Data Fetching & Rendering ---
    const loadStores = async () => {
        storesListContainer.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch('/api/config/stores');
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            renderStores(stores);
        } catch (error) {
            storesListContainer.innerHTML = `<p style="color: red;">${error.message}</p>`;
        } finally {
            storesListContainer.removeAttribute('aria-busy');
        }
    };

    const renderStores = (stores) => {
        if (stores.length === 0) {
            storesListContainer.innerHTML = '<p>No stores have been added yet.</p>';
            return;
        }
        let tableHtml = '<table><thead><tr><th>Name</th><th>Shopify URL</th><th>Currency</th><th>Actions</th></tr></thead><tbody>';
        stores.forEach(store => {
            tableHtml += `
                <tr>
                    <td>${store.name}</td>
                    <td>${store.shopify_url}</td>
                    <td>${store.currency}</td>
                    <td><button class="outline" data-store-id="${store.id}">Manage</button></td>
                </tr>`;
        });
        tableHtml += '</tbody></table>';
        storesListContainer.innerHTML = tableHtml;
    };

    const openEditModal = async (storeId) => {
        currentStoreId = storeId;
        const storeResponse = await fetch(`/api/config/stores/${storeId}`);
        const storeData = await storeResponse.json();
        
        modalStoreName.textContent = `Manage: ${storeData.name}`;
        
        document.getElementById('store-settings-section').style.display = 'block';
        webhookManagementSection.style.display = 'block';

        await populateLocations(storeId, storeData.sync_location_id);
        webhookUrlDisplay.textContent = `${window.location.origin}/api/webhooks/${storeId}`;
        await loadWebhooks();
        editStoreModal.showModal();
    };

    const populateLocations = async (storeId, currentLocationId) => {
        syncLocationSelect.innerHTML = '<option>Loading locations...</option>';
        syncLocationSelect.disabled = true;
        try {
            const response = await fetch(`/api/config/stores/${storeId}/locations`);
            if (!response.ok) throw new Error('Failed to fetch locations.');
            const data = await response.json();

            if (data.locations && data.locations.length > 0) {
                syncLocationSelect.innerHTML = '<option value="">-- Select a location --</option>';
                data.locations.forEach(loc => {
                    const option = new Option(loc.name, loc.id);
                    if (loc.id === currentLocationId) {
                        option.selected = true;
                    }
                    syncLocationSelect.add(option);
                });
                syncLocationSelect.disabled = false;
            } else {
                syncLocationSelect.innerHTML = '<option>No locations found</option>';
            }
        } catch (error) {
            syncLocationSelect.innerHTML = `<option>${error.message}</option>`;
        }
    };
    
    const loadWebhooks = async () => {
        if (!currentStoreId) return;
        webhooksListContainer.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(`/api/config/stores/${currentStoreId}/webhooks`);
            if (!response.ok) throw new Error('Failed to load webhooks.');
            renderWebhooks(await response.json());
        } catch (error) {
            webhooksListContainer.innerHTML = `<p style="color: red;">${error.message}</p>`;
        } finally {
            webhooksListContainer.removeAttribute('aria-busy');
        }
    };

    const renderWebhooks = (webhooks) => {
        if (webhooks.length === 0) {
            webhooksListContainer.innerHTML = '<p>No webhooks registered for this store.</p>';
            return;
        }
        let tableHtml = '<table><thead><tr><th>Topic</th><th>Action</th></tr></thead><tbody>';
        webhooks.forEach(wh => {
            tableHtml += `
                <tr>
                    <td><code>${wh.topic}</code></td>
                    <td><button class="secondary outline" data-webhook-id="${wh.shopify_webhook_id}">Delete</button></td>
                </tr>`;
        });
        tableHtml += '</tbody></table>';
        webhooksListContainer.innerHTML = tableHtml;
    };

    // --- Event Handlers ---
    addStoreForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        addStoreBtn.setAttribute('aria-busy', 'true');
        const formData = new FormData(addStoreForm);
        const payload = Object.fromEntries(formData.entries());

        try {
            const response = await fetch('/api/config/stores', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (!response.ok) {
                const result = await response.json();
                throw new Error(result.detail || 'Failed to add store.');
            }
            addStoreForm.reset();
            await loadStores();
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            addStoreBtn.removeAttribute('aria-busy');
        }
    });

    storeSettingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (!currentStoreId) return;
        const locationId = syncLocationSelect.value;
        if (!locationId) {
            alert('Please select a sync location.');
            return;
        }
        saveSettingsBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(`/api/config/stores/${currentStoreId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sync_location_id: parseInt(locationId) })
            });
            if (!response.ok) {
                const result = await response.json();
                throw new Error(result.detail || 'Failed to save settings.');
            }
            alert('Sync location saved successfully!');
            editStoreModal.close();
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            saveSettingsBtn.removeAttribute('aria-busy');
        }
    });
    
    storesListContainer.addEventListener('click', (e) => {
        if (e.target.matches('button[data-store-id]')) {
            openEditModal(e.target.dataset.storeId);
        }
    });

    editStoreModal.addEventListener('click', (e) => {
        if (e.target.matches('.close')) {
            editStoreModal.close();
            currentStoreId = null;
        }
    });

    createAllWebhooksBtn.addEventListener('click', async () => {
        if (!currentStoreId) return;
        createAllWebhooksBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(`/api/config/stores/${currentStoreId}/webhooks/create-all`, { method: 'POST' });
            const result = await response.json();
            if (!response.ok) throw new Error(result.detail || 'Failed to create webhooks.');
            alert(result.message);
            await loadWebhooks();
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            createAllWebhooksBtn.removeAttribute('aria-busy');
        }
    });
    
    webhooksListContainer.addEventListener('click', async (e) => {
        if (e.target.matches('button[data-webhook-id]')) {
            const webhookId = e.target.dataset.webhookId;
            if (!confirm('Are you sure you want to delete this webhook?')) return;
            try {
                const response = await fetch(`/api/config/stores/${currentStoreId}/webhooks/${webhookId}`, { method: 'DELETE' });
                if (!response.ok) {
                    const result = await response.json();
                    throw new Error(result.detail || 'Failed to delete webhook.');
                }
                await loadWebhooks();
            } catch (error) {
                alert(`Error: ${error.message}`);
            }
        }
    });
    
    deleteAllWebhooksBtn.addEventListener('click', async () => {
        if (!confirm('Are you sure you want to delete ALL webhooks for ALL stores? This action cannot be undone.')) return;
        deleteAllWebhooksBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch('/api/config/webhooks/delete-all', { method: 'DELETE' });
            const result = await response.json();
            if (!response.ok) {
                const errorDetails = result.errors ? `\\n\\nDetails:\\n${result.errors.join('\\n')}` : '';
                throw new Error(result.message + errorDetails);
            }
            alert(result.message);
            if (editStoreModal.open) {
                await loadWebhooks();
            }
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            deleteAllWebhooksBtn.removeAttribute('aria-busy');
        }
    });

    loadStores();
});