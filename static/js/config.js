// static/js/config.js

const API_ENDPOINTS = {
    // Stores
    getStores: '/api/config/stores',
    getStore: (storeId) => `/api/config/stores/${storeId}`,
    addStore: '/api/config/stores',
    updateStore: (storeId) => `/api/config/stores/${storeId}`,

    // Webhook Endpoints
    getWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks`,
    createAllWebhooks: (storeId) => `/api/config/stores/${storeId}/webhooks/create-all`,
    deleteWebhook: (storeId, webhookId) => `/api/config/stores/${storeId}/webhooks/${webhookId}`,

    // Dashboard V2 (Orders Report)
    getDashboardOrders: (params) => `/api/v2/dashboard/orders/?${params.toString()}`,
    exportDashboardOrders: (params) => `/api/v2/dashboard/export/?${params.toString()}`,
    // ADDED: New endpoint for dynamic filters
    getDashboardFilters: '/api/v2/dashboard/filters/',

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
    reconcileStock: '/api/sync-control/reconcile-stock',

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
