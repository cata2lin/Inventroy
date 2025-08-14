// static/js/mutations.js

document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('product-edit-form');
    const saveButton = document.getElementById('saveButton');
    const productTitleHeader = document.getElementById('product-title-header');
    const toast = document.getElementById('toast');

    let productId = null;

    const showToast = (message, type = 'info', duration = 4000) => {
        toast.textContent = message;
        toast.className = `show ${type}`;
        setTimeout(() => { toast.className = ''; }, duration);
    };

    const getProductIdFromUrl = () => {
        const params = new URLSearchParams(window.location.search);
        return params.get('id');
    };

    const loadProductData = async () => {
        productId = getProductIdFromUrl();
        if (!productId) {
            document.getElementById('edit-form-container').innerHTML = '<p>Error: No product ID specified.</p>';
            return;
        }

        try {
            const response = await fetch(API_ENDPOINTS.getProduct(productId));
            if (!response.ok) {
                throw new Error('Failed to fetch product details.');
            }
            const product = await response.json();
            populateForm(product);
        } catch (error) {
            showToast(error.message, 'error');
            productTitleHeader.textContent = 'Error loading product';
        }
    };

    const populateForm = (product) => {
        productTitleHeader.textContent = `Editing: ${product.title}`;
        form.elements.title.value = product.title || '';
        form.elements.vendor.value = product.vendor || '';
        // CORECTAT: Populează câmpul corect (răspunsul API folosește 'body_html')
        form.elements.descriptionHtml.value = product.body_html || '';
        form.elements.productType.value = product.product_type || '';
        form.elements.status.value = product.status || 'ACTIVE';
        form.elements.tags.value = product.tags || '';
    };

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        saveButton.setAttribute('aria-busy', 'true');
        saveButton.disabled = true;

        const formData = new FormData(form);
        // CORECTAT: Trimite 'descriptionHtml' în loc de 'bodyHtml'
        const payload = {
            title: formData.get('title'),
            vendor: formData.get('vendor'),
            descriptionHtml: formData.get('descriptionHtml'),
            productType: formData.get('productType'),
            status: formData.get('status'),
            tags: formData.get('tags'),
        };

        try {
            const response = await fetch(API_ENDPOINTS.updateProduct(productId), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            
            const result = await response.json();
            if (!response.ok) {
                throw new Error(result.detail || 'Failed to update product.');
            }
            showToast('Product updated successfully!', 'success');
            productTitleHeader.textContent = `Editing: ${payload.title}`;
        } catch (error)
        {
            showToast(error.message, 'error');
        } finally {
            saveButton.removeAttribute('aria-busy');
            saveButton.disabled = false;
        }
    });

    loadProductData();
});