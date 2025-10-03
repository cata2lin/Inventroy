// static/js/config.js

const API_ENDPOINTS = {
    // Stores
    getStores: '/api/config/stores',
    addStore: '/api/config/stores',

    // Webhook Endpoints
    getWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks`,
    createAllWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks/create-all`,
    deleteWebhook: (storeId, webhookId) => `/api/config/stores/${storeId}/webhooks/${webhookId}`,
    deleteAllWebhooks: '/api/config/webhooks/delete-all', // NEW ENDPOINT

    // Sync Control
    syncProducts: '/api/sync-control/products',
    getSyncStatus: '/api/sync-control/status',
};