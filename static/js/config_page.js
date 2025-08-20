// static/js/config_page.js

document.addEventListener('DOMContentLoaded', () => {
    // --- Element References ---
    const addStoreForm = document.getElementById('add-store-form');
    const storesListContainer = document.getElementById('stores-list-container');
    const addStoreBtn = document.getElementById('add-store-btn');
    const editStoreModal = document.getElementById('edit-store-modal');
    const editStoreForm = document.getElementById('edit-store-form');
    const updateStoreBtn = document.getElementById('update-store-btn');

    // --- Webhook Management Elements ---
    const webhookManagementSection = document.getElementById('webhook-management-section');
    const webhookUrlDisplay = document.getElementById('webhook-url-display');
    // MODIFIED: Replaced form with a single button
    const createAllWebhooksBtn = document.getElementById('create-all-webhooks-btn');
    const webhooksListContainer = document.getElementById('webhooks-list-container');
    
    let currentStoreId = null;

    // --- Store Management Functions ---
    const loadStores = async () => {
        storesListContainer.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
            if (!response.ok) throw new Error('Failed to load stores.');
            const stores = await response.json();
            renderStores(stores);
        } catch (error) {
            storesListContainer.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            storesListContainer.removeAttribute('aria-busy');
        }
    };

    const renderStores = (stores) => {
        if (stores.length === 0) {
            storesListContainer.innerHTML = '<p>No stores have been added yet.</p>';
            return;
        }
        let tableHtml = '<table><thead><tr><th>Name</th><th>Shopify URL</th><th>Created At</th><th>Actions</th></tr></thead><tbody>';
        stores.forEach(store => {
            tableHtml += `
                <tr>
                    <td>${store.name}</td>
                    <td>${store.shopify_url}</td>
                    <td>${new Date(store.created_at).toLocaleDateString()}</td>
                    <td><button class="outline" data-store-id="${store.id}" onclick="openEditModal(this)">Edit</button></td>
                </tr>`;
        });
        tableHtml += '</tbody></table>';
        storesListContainer.innerHTML = tableHtml;
    };

    window.openEditModal = async (button) => {
        currentStoreId = button.dataset.storeId;
        try {
            const response = await fetch(API_ENDPOINTS.getStore(currentStoreId));
            if (!response.ok) throw new Error('Failed to fetch store details.');
            const store = await response.json();
            
            document.getElementById('edit-store-id').value = store.id;
            document.getElementById('edit-name').value = store.name;
            document.getElementById('edit-shopify_url').value = store.shopify_url;
            
            webhookManagementSection.style.display = 'block';
            webhookUrlDisplay.textContent = `${window.location.origin}/api/webhooks/${store.id}`;
            await loadWebhooks();

            editStoreModal.showModal();
        } catch (error) {
            alert(`Error: ${error.message}`);
        }
    };
    
    editStoreModal.querySelector('.close').addEventListener('click', () => {
        editStoreModal.close();
        currentStoreId = null;
    });

    addStoreForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        addStoreBtn.setAttribute('aria-busy', 'true');
        addStoreBtn.disabled = true;

        const formData = new FormData(addStoreForm);
        const payload = {
            name: formData.get('name'),
            shopify_url: formData.get('shopify_url'),
            api_token: formData.get('api_token'),
            api_secret: formData.get('api_secret') || null,
            webhook_secret: formData.get('webhook_secret') || null,
        };

        try {
            const response = await fetch(API_ENDPOINTS.addStore, {
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
            addStoreBtn.disabled = false;
        }
    });
    
    editStoreForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        updateStoreBtn.setAttribute('aria-busy', 'true');
        updateStoreBtn.disabled = true;

        const storeId = document.getElementById('edit-store-id').value;
        const formData = new FormData(editStoreForm);
        const payload = {
            name: formData.get('name'),
            shopify_url: formData.get('shopify_url'),
        };

        const apiToken = formData.get('api_token');
        if (apiToken) payload.api_token = apiToken;
        const apiSecret = formData.get('api_secret');
        if (apiSecret) payload.api_secret = apiSecret;
        const webhookSecret = formData.get('edit-webhook_secret');
        if (webhookSecret) payload.webhook_secret = webhookSecret;
        
        try {
            const response = await fetch(API_ENDPOINTS.updateStore(storeId), {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (!response.ok) {
                const result = await response.json();
                throw new Error(result.detail || 'Failed to update store.');
            }
            alert('Store details updated successfully!');
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            updateStoreBtn.removeAttribute('aria-busy');
            updateStoreBtn.disabled = false;
        }
    });

    // --- Webhook Management Functions ---
    const loadWebhooks = async () => {
        if (!currentStoreId) return;
        webhooksListContainer.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(API_ENDPOINTS.getWebhooks(currentStoreId));
            if (!response.ok) throw new Error('Failed to load webhooks.');
            const webhooks = await response.json();
            renderWebhooks(webhooks);
        } catch (error) {
            webhooksListContainer.innerHTML = `<p style="color: var(--pico-color-red-500);">${error.message}</p>`;
        } finally {
            webhooksListContainer.removeAttribute('aria-busy');
        }
    };

    const renderWebhooks = (webhooks) => {
        if (webhooks.length === 0) {
            webhooksListContainer.innerHTML = '<p>No webhooks are registered for this store.</p>';
            return;
        }
        let tableHtml = '<table><thead><tr><th>Topic</th><th>Address</th><th>Action</th></tr></thead><tbody>';
        webhooks.forEach(wh => {
            tableHtml += `
                <tr>
                    <td><code>${wh.topic}</code></td>
                    <td><code>${wh.address}</code></td>
                    <td><button class="secondary outline" data-webhook-id="${wh.shopify_webhook_id}" onclick="deleteWebhook(this)">Delete</button></td>
                </tr>`;
        });
        tableHtml += '</tbody></table>';
        webhooksListContainer.innerHTML = tableHtml;
    };

    // MODIFIED: Event listener for the new automatic creation button
    createAllWebhooksBtn.addEventListener('click', async () => {
        if (!currentStoreId) return;
        
        createAllWebhooksBtn.setAttribute('aria-busy', 'true');
        createAllWebhooksBtn.disabled = true;

        try {
            const response = await fetch(API_ENDPOINTS.createAllWebhooks(currentStoreId), {
                method: 'POST'
            });
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || 'Failed to create webhooks.');
            }
            alert(result.message);
            await loadWebhooks(); // Refresh the list
        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            createAllWebhooksBtn.removeAttribute('aria-busy');
            createAllWebhooksBtn.disabled = false;
        }
    });

    window.deleteWebhook = async (button) => {
        if (!confirm('Are you sure you want to delete this webhook subscription?')) return;
        
        const webhookId = button.dataset.webhookId;
        button.setAttribute('aria-busy', 'true');

        try {
            const response = await fetch(API_ENDPOINTS.deleteWebhook(currentStoreId, webhookId), {
                method: 'DELETE'
            });
            if (!response.ok) {
                const result = await response.json();
                throw new Error(result.detail || 'Failed to delete webhook.');
            }
            await loadWebhooks();
        } catch (error) {
            alert(`Error: ${error.message}`);
        }
    };

    // --- Initial Load ---
    loadStores();
});
