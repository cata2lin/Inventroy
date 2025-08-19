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

    // Bulk Update Page -- ADDED
    getAllVariantsForBulkEdit: '/api/bulk-update/variants/',
    processBulkUpdates: '/api/bulk-update/variants/',
    generateBarcodes: '/api/bulk-update/generate-barcode/', // ADDED
    uploadExcel: '/api/bulk-update/upload-excel/', // ADDED

    // Legacy Endpoints (can be removed if not used elsewhere)
    syncOrders: (storeId) => `/api/orders/sync/${storeId}`,
    getVariants: (storeId) => `/api/inventory/variants/${storeId}`,
    syncInventory: (storeId) => `/api/inventory/sync/${storeId}`,
    updateVariantField: (storeId) => `/api/inventory/variants/update-field/${storeId}`,
};