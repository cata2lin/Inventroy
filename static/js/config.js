// static/js/config.js

const API_ENDPOINTS = {
    // Original Dashboard (Legacy)
    getStores: '/api/dashboard/stores',
    getOrders: (storeId) => `/api/dashboard/orders/${storeId}`,
    getFulfillments: (storeId) => `/api/dashboard/fulfillments/${storeId}`,
    getInventoryDashboard: (storeId) => `/api/dashboard/inventory/${storeId}`,
    syncOrders: (storeId) => `/api/orders/sync/${storeId}`,
    
    // Products Page & Variant Editing
    getVariants: (storeId) => `/api/inventory/variants/${storeId}`,
    syncInventory: (storeId) => `/api/inventory/sync/${storeId}`,
    updateVariantField: (storeId) => `/api/inventory/variants/update-field/${storeId}`,

    // Product Mutations Page
    getProduct: (productId) => `/api/mutations/product/${productId}`,
    updateProduct: (productId) => `/api/mutations/product/${productId}`,

    // Dashboard V2 (Orders Report)
    getDashboardOrders: (params) => `/api/v2/dashboard/orders/?${params.toString()}`,

    // Inventory V2 (Inventory Report)
    getInventoryReport: (params) => `/api/v2/inventory/report/?${params.toString()}`,
    getInventoryFilters: '/api/v2/inventory/filters/'
};