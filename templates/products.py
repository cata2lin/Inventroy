{% extends "base.html" %}

{% block content %}
<div class="container">
    <hgroup>
        <h1>Synced Products</h1>
        <h2>View all products synchronized from your connected stores.</h2>
    </hgroup>

    <article>
        <div class="grid">
            <input type="search" id="search-input" placeholder="Search by name, SKU, or barcode...">
            <select id="store-filter">
                <option value="">All Stores</option>
            </select>
        </div>
    </article>

    <article id="products-container" aria-busy="true">
        <p>Loading products...</p>
    </article>

    <div class="pagination-controls grid">
        <button id="prev-button" disabled>Previous</button>
        <span id="page-indicator">Page 1 of 1</span>
        <button id="next-button" disabled>Next</button>
    </div>
</div>
{% endblock %}

{% block scripts %}
<script src="/static/js/config.js"></script>
<script src="/static/js/products.js"></script>
{% endblock %}