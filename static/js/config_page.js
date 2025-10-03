// static/js/config_page.js

document.addEventListener('DOMContentLoaded', () => {
    // --- (Element references remain the same, with one addition) ---
    const addStoreForm = document.getElementById('add-store-form');
    const storesListContainer = document.getElementById('stores-list-container');
    const addStoreBtn = document.getElementById('add-store-btn');
    const editStoreModal = document.getElementById('edit-store-modal');
    
    const webhookManagementSection = document.getElementById('webhook-management-section');
    const webhookUrlDisplay = document.getElementById('webhook-url-display');
    const createAllWebhooksBtn = document.getElementById('create-all-webhooks-btn');
    const webhooksListContainer = document.getElementById('webhooks-list-container');
    
    // NEW ELEMENT REFERENCE
    const deleteAllWebhooksBtn = document.getElementById('delete-all-webhooks-btn');

    let currentStoreId = null;

    // --- (Existing functions remain the same) ---
    const loadStores = async () => {
        storesListContainer.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(API_ENDPOINTS.getStores);
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
        let tableHtml = '<table><thead><tr><th>Name</th><th>Shopify URL</th><th>Actions</th></tr></thead><tbody>';
        stores.forEach(store => {
            tableHtml += `
                <tr>
                    <td>${store.name}</td>
                    <td>${store.shopify_url}</td>
                    <td><button class="outline" data-store-id="${store.id}">Manage</button></td>
                </tr>`;
        });
        tableHtml += '</tbody></table>';
        storesListContainer.innerHTML = tableHtml;
        
        storesListContainer.querySelectorAll('button[data-store-id]').forEach(button => {
            button.addEventListener('click', () => openEditModal(button.dataset.storeId));
        });
    };

    const openEditModal = async (storeId) => {
        currentStoreId = storeId;
        webhookManagementSection.style.display = 'block';
        webhookUrlDisplay.textContent = `${window.location.origin}/api/webhooks/${storeId}`;
        await loadWebhooks();
        editStoreModal.showModal();
    };
    
    editStoreModal.querySelector('.close').addEventListener('click', () => {
        editStoreModal.close();
        currentStoreId = null;
    });

    addStoreForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        addStoreBtn.setAttribute('aria-busy', 'true');
        const formData = new FormData(addStoreForm);
        const payload = Object.fromEntries(formData.entries());

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
        }
    });

    const loadWebhooks = async () => {
        if (!currentStoreId) return;
        webhooksListContainer.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(API_ENDPOINTS.getWebhooks(currentStoreId));
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
        let tableHtml = '<table><thead><tr><th>Topic</th><th>Address</th><th>Action</th></tr></thead><tbody>';
        webhooks.forEach(wh => {
            tableHtml += `
                <tr>
                    <td><code>${wh.topic}</code></td>
                    <td><code>${wh.address}</code></td>
                    <td><button class="secondary outline" data-webhook-id="${wh.shopify_webhook_id}">Delete</button></td>
                </tr>`;
        });
        tableHtml += '</tbody></table>';
        webhooksListContainer.innerHTML = tableHtml;

        webhooksListContainer.querySelectorAll('button[data-webhook-id]').forEach(button => {
            button.addEventListener('click', () => deleteWebhook(button.dataset.webhookId));
        });
    };

    createAllWebhooksBtn.addEventListener('click', async () => {
        if (!currentStoreId) return;
        createAllWebhooksBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(API_ENDPOINTS.createAllWebhooks(currentStoreId), { method: 'POST' });
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

    const deleteWebhook = async (webhookId) => {
        if (!confirm('Are you sure you want to delete this webhook subscription?')) return;
        try {
            const response = await fetch(API_ENDPOINTS.deleteWebhook(currentStoreId, webhookId), { method: 'DELETE' });
            if (!response.ok) {
                const result = await response.json();
                throw new Error(result.detail || 'Failed to delete webhook.');
            }
            await loadWebhooks();
        } catch (error) {
            alert(`Error: ${error.message}`);
        }
    };
    
    // --- NEW EVENT LISTENER ---
    deleteAllWebhooksBtn.addEventListener('click', async () => {
        if (!confirm('Are you sure you want to delete ALL webhooks for ALL stores? This action cannot be undone.')) return;
        
        deleteAllWebhooksBtn.setAttribute('aria-busy', 'true');
        try {
            const response = await fetch(API_ENDPOINTS.deleteAllWebhooks, { method: 'DELETE' });
            const result = await response.json();
            if (!response.ok) {
                const errorDetails = result.detail.errors ? `\n\nDetails:\n${result.detail.errors.join('\n')}` : '';
                throw new Error(result.detail.message + errorDetails);
            }
            alert(result.message);
            // If the modal is open, refresh its content
            if (currentStoreId) {
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