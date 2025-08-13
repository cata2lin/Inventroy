# Inventory Intelligence Platform

This is a web application for managing and synchronizing product, order, and inventory data from Shopify stores, built with FastAPI and PostgreSQL.

## Setup and Installation

1.  **Clone the Repository**
    ```bash
    git clone <your-repo-url>
    cd <your-project-directory>
    ```

2.  **Set Up Virtual Environment**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment**
    Copy the example template to create your own environment file.
    ```bash
    cp .env.example .env
    ```
    Now, edit the `.env` file with your new, secure credentials.

5.  **Run the Application**
    ```bash
    uvicorn main:app --reload
    ```