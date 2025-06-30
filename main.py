import os
from fastapi import FastAPI, HTTPException, BackgroundTasks, status
from pydantic import BaseModel
from google.cloud.sql.connector import Connector, IPTypes
import pg8000.dbapi
import sqlalchemy
from sqlalchemy.pool import NullPool # Recommended for serverless environments
from sqlalchemy import text # Import text for raw SQL execution

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Product Search API",
    description="API for searching products by text and embedding matches.",
    version="1.0.0",
)

# --- Configuration (from Environment Variables) ---
ENV_CLOUD_SQL_CONNECTION_NAME_KEY = "CLOUD_SQL_CONNECTION_NAME"
ENV_DB_USER_KEY = "DB_USER"
ENV_DB_PASS_KEY = "DB_PASS"
ENV_DB_NAME_KEY = "DB_NAME"
ENV_USE_PRIVATE_IP_KEY = "USE_PRIVATE_IP"

CLOUD_SQL_CONNECTION_NAME = os.environ.get(ENV_CLOUD_SQL_CONNECTION_NAME_KEY)
DB_USER = os.environ.get(ENV_DB_USER_KEY)
DB_PASS = os.environ.get(ENV_DB_PASS_KEY)
DB_NAME = os.environ.get(ENV_DB_NAME_KEY)
USE_PRIVATE_IP = os.environ.get(ENV_USE_PRIVATE_IP_KEY, "false").lower() == "true"

# --- Validate essential environment variables ---
if not all([CLOUD_SQL_CONNECTION_NAME, DB_USER, DB_PASS, DB_NAME]):
    missing_vars = []
    if CLOUD_SQL_CONNECTION_NAME is None: missing_vars.append(ENV_CLOUD_SQL_CONNECTION_NAME_KEY)
    if DB_USER is None: missing_vars.append(ENV_DB_USER_KEY)
    if DB_PASS is None: missing_vars.append(ENV_DB_PASS_KEY)
    if DB_NAME is None: missing_vars.append(ENV_DB_NAME_KEY)
    raise ValueError(f"Missing one or more essential environment variables for database connection: {', '.join(missing_vars)}")

# --- Cloud SQL Connector and SQLAlchemy Setup ---
connector = Connector()

def getconn() -> pg8000.dbapi.Connection:
    """Function to establish a new database connection."""
    try:
        conn: pg8000.dbapi.Connection = connector.connect(
            CLOUD_SQL_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            ip_type=IPTypes.PRIVATE if USE_PRIVATE_IP else IPTypes.PUBLIC,
        )
        return conn
    except Exception as e:
        print(f"Error establishing database connection: {e}")
        raise RuntimeError(f"Could not connect to database: {e}")

db_pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
    poolclass=NullPool
)

# --- Pydantic Models for Request/Response Validation ---
class SearchRequest(BaseModel):
    query_text: str

class SearchResponse(BaseModel):
    ids: list[str]

class GenerateEmbeddingsResponse(BaseModel):
    message: str

# --- Background Task for Embedding Generation ---

def generate_embeddings_sync(BATCH_SIZE: int = 10):
    """
    Synchronous function to perform the embedding generation in batches.
    This runs in a background thread managed by FastAPI.
    """
    total_products_embedded = 0
    print("Starting background embedding generation...")

    while True:
        try:
            with db_pool.connect() as connection:
                # The SQL statement to update products in batches
                sql_query = text(f"""
                    UPDATE products
                    SET abstract_embeddings = embedding('gemini-embedding-001', name)::vector
                    WHERE id IN (
                        SELECT id
                        FROM products
                        WHERE abstract_embeddings IS NULL
                        ORDER BY id ASC
                        LIMIT {BATCH_SIZE}
                    )
                    RETURNING id; -- Return IDs of updated rows to check progress
                """)

                # Execute the update
                result = connection.execute(sql_query)
                updated_ids = result.scalars().all() # Get list of updated IDs

                if not updated_ids:
                    print("Embedding generation complete: No more products to process.")
                    break # Exit loop if no rows were updated

                num_updated = len(updated_ids)
                total_products_embedded += num_updated
                print(f"Embedded {num_updated} products in this batch. Total embedded: {total_products_embedded}")

                # No explicit commit needed here with `with db_pool.connect() as connection:`
                # as SQLAlchemy handles transactions for simple statements automatically,
                # committing on success or rolling back on error within the `with` block.
                # If you need multi-statement transactions, you'd use `connection.begin()`

        except Exception as e:
            print(f"Error during embedding generation batch processing: {e}")
            # Log the specific error for debugging
            # Consider more robust error handling / retry logic here in a real app
            break # Stop on error for now

        # Optional: Add a small delay between batches to avoid overwhelming resources
        # This is a synchronous sleep, it will block this background thread.
        # time.sleep(0.5) # Import time if using this. For now, rely on API call latency.

# --- FastAPI Routes ---

@app.get("/healthok", status_code=200)
async def health_check():
    """Simple health check endpoint that also pings the database."""
    try:
        with db_pool.connect() as connection:
            connection.execute(sqlalchemy.text("SELECT 1")) # Simple query to check connectivity
        return {"status": "ok", "db_connection": "successful"}
    except Exception as e:
        print(f"Health check failed due to database error: {e}")
        raise HTTPException(status_code=503, detail=f"Service Unavailable: Database connection failed: {str(e)}")

@app.post("/search-products", response_model=SearchResponse, status_code=200)
async def search_products(request_body: SearchRequest):
    """
    Searches for product IDs based on the provided query text.
    Combines text-based and embedding-based matches, prioritizing text matches.
    """
    query_text = request_body.query_text

    sql_query = """
    SELECT external_id
    FROM (
      SELECT DISTINCT ON (external_id) *
      FROM (
        (
          SELECT 'text_match' AS source, external_id
          FROM products
          WHERE name ILIKE :query_text_pattern
          LIMIT 100
        )
        UNION ALL
        (
          SELECT 'embedding_match' AS source, external_id
          FROM products
          ORDER BY abstract_embeddings <=> embedding('gemini-embedding-001', :query_text_embedding)::vector
          LIMIT 100
        )
      ) combined
      ORDER BY external_id, source DESC -- IMPORTANT: This prioritizes 'text_match' for deduplication
    ) deduped
    ORDER BY source DESC; -- This orders the final output by source (text_match first)
    """

    ids = []
    try:
        with db_pool.connect() as connection:
            result = connection.execute(
                text(sql_query), # Use text() for raw SQL
                {
                    "query_text_pattern": f"%{query_text}%",
                    "query_text_embedding": query_text
                }
            )
            ids = [row.external_id for row in result.fetchall()]
        return SearchResponse(ids=ids)
    except Exception as e:
        print(f"Database query error in search_products: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve IDs: {str(e)}")

@app.post("/products/generate-embeddings", response_model=GenerateEmbeddingsResponse, status_code=status.HTTP_202_ACCEPTED)
async def trigger_embedding_generation(
    background_tasks: BackgroundTasks
):
    """
    Triggers the generation of embeddings for product names in the background.
    This endpoint returns immediately with a 202 Accepted status.
    """
    # The batch size for the SQL update
    BATCH_SIZE = 10 # Your requested limit

    # Add the synchronous function to FastAPI's background tasks
    # FastAPI will run this function in a separate thread,
    # preventing the main event loop from blocking.
    background_tasks.add_task(generate_embeddings_sync, BATCH_SIZE)

    return {"message": f"Embedding generation started in the background with batch size {BATCH_SIZE}. Check service logs for progress."}


# This block is for local development only.
# Cloud Run will use gunicorn/uvicorn to run the app.
if __name__ == "__main__":
    import uvicorn
    # Use the PORT environment variable provided by Cloud Run
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# --- Cleanup for Cloud Run instance shutdown ---
@app.on_event("shutdown")
def shutdown_event():
    """Closes the Cloud SQL Connector when the application shuts down."""
    print("Shutting down: Closing Cloud SQL Connector...")
    connector.close()
    print("Cloud SQL Connector closed.")
