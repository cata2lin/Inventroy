// static/js/config.js

const API_ENDPOINTS = {
    // Stores
    getStores: '/api/config/stores',
    addStore: '/api/config/stores',

    // Webhook Endpoints
    getWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks`,
    createAllWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks/create-all`,
    deleteWebhook: (storeId, webhookId) => `/api/config/stores/${storeId}/webhooks/${webhookId}`,
    deleteAllWebhooks: '/api/config/webhooks/delete-all',

    // Sync Control
    syncProducts: '/api/sync-control/products',
    getSyncStatus: '/api/sync-control/status',

    // Products Endpoint
    getProducts: '/api/products/',
    getProduct: (productId) => `/api/products/${productId}`, // NEW

    // Mutations Endpoints
    executeMutation: (storeId) => `/api/mutations/execute/${storeId}`,
    findCategories: (storeId) => `/api/mutations/find-categories/${storeId}`,
};