// static/js/config.js

const API_ENDPOINTS = {
    // Stores
    getStores: '/api/dashboard/stores',

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
};