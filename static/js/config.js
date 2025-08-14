// static/js/config.js

const API_ENDPOINTS = {
    getStores: '/api/dashboard/stores',
    getOrders: (storeId) => `/api/dashboard/orders/${storeId}`,
    getFulfillments: (storeId) => `/api/dashboard/fulfillments/${storeId}`,
    getInventoryDashboard: (storeId) => `/api/dashboard/inventory/${storeId}`,
    syncOrders: (storeId) => `/api/orders/sync/${storeId}`,
    
    getVariants: (storeId) => `/api/inventory/variants/${storeId}`,
    syncInventory: (storeId) => `/api/inventory/sync/${storeId}`,
    updateVariantField: (storeId) => `/api/inventory/variants/update-field/${storeId}`,

    getGroupedInventory: (params) => `/api/inventory/grouped/?${params.toString()}`,
    setPrimaryVariant: '/api/inventory/set-primary-variant',
    setInventoryQuantity: '/api/inventory/set_quantity',
    addInventoryQuantity: '/api/inventory/add_quantity',
    subtractInventoryQuantity: '/api/inventory/subtract_quantity',

    // ADDED for mutations page
    getProduct: (productId) => `/api/mutations/product/${productId}`,
    updateProduct: (productId) => `/api/mutations/product/${productId}`
};