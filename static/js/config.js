// static/js/config.js

const API_ENDPOINTS = {
    // Stores
    getStores: '/api/config/stores',
    getStore: (storeId) => `/api/config/stores/${storeId}`,
    addStore: '/api/config/stores',
    updateStore: (storeId) => `/api/config/stores/${storeId}`,

    // --- ADDED: Webhook Endpoints ---
    getWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks`,
    createWebhook: (storeId) => `/api/config/stores/${storeId}/webhooks`,
    deleteWebhook: (storeId, webhookId) => `/api/config/stores/${storeId}/webhooks/${webhookId}`,

    // Dashboard V2 (Orders Report)
    getDashboardOrders: (params) => `/api/v2/dashboard/orders/?${params.toString()}`,
    exportDashboardOrders: (params) => `/api/v2/dashboard/export/?${params.toString()}`,

    // Inventory V2 (Inventory Report)
    getInventoryReport: (params) => `/api/v2/inventory/report/?${params.toString()}`,
    getInventoryFilters: '/api/v2/inventory/filters/',
    setPrimaryVariant: '/api/v2/inventory/set-primary-variant/',

    // Product Mutations Page
    getProduct: (productId) => `/api/mutations/product/${productId}`,
    updateProduct: (productId) => `/api/mutations/product/${productId}`,

    // Sync Control
    syncOrders: '/api/sync-control/orders',
    syncProducts: '/api/sync-control/products',
    getSyncStatus: '/api/sync-control/status',

    // Bulk Update Endpoints
    getAllVariantsForBulkEdit: '/api/bulk-update/variants/',
    processBulkUpdates: '/api/bulk-update/variants/',
    generateBarcodes: '/api/bulk-update/generate-barcode/',
    uploadExcel: '/api/bulk-update/upload-excel/',
    
    // LEGACY/DEPRECATED (Kept for reference)
    getVariants: (storeId) => `/api/products/variants/${storeId}`,
    updateVariantField: (storeId) => `/api/products/variants/update-field/${storeId}`,
    syncInventory: (storeId) => `/api/inventory/sync/${storeId}`
};
