// static/js/config_page.js

document.addEventListener('DOMContentLoaded', () => {
    const addStoreForm = document.getElementById('add-store-form');
    const storesListContainer = document.getElementById('stores-list-container');
    const addStoreBtn = document.getElementById('add-store-btn');
    const editStoreModal = document.getElementById('edit-store-modal');
    const editStoreForm = document.getElementById('edit-store-form');
    const updateStoreBtn = document.getElementById('update-store-btn');

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
                </tr>
            `;
        });
        tableHtml += '</tbody></table>';
        storesListContainer.innerHTML = tableHtml;
    };

    window.openEditModal = async (button) => {
        const storeId = button.dataset.storeId;
        try {
            const response = await fetch(API_ENDPOINTS.getStore(storeId));
            if (!response.ok) throw new Error('Failed to fetch store details.');
            const store = await response.json();
            
            document.getElementById('edit-store-id').value = store.id;
            document.getElementById('edit-name').value = store.name;
            document.getElementById('edit-shopify_url').value = store.shopify_url;
            
            editStoreModal.showModal();
        } catch (error) {
            alert(`Error: ${error.message}`);
        }
    };
    
    editStoreModal.querySelector('.close').addEventListener('click', () => {
        editStoreModal.close();
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

            editStoreModal.close();
            await loadStores();

        } catch (error) {
            alert(`Error: ${error.message}`);
        } finally {
            updateStoreBtn.removeAttribute('aria-busy');
            updateStoreBtn.disabled = false;
        }
    });

    loadStores();
});