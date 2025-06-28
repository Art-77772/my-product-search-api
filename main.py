# main.py

import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud.sql.connector import Connector, IPTypes
import pg8000.dbapi
import sqlalchemy
from sqlalchemy.pool import NullPool # Recommended for serverless environments

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Product Search API",
    description="API for searching products by text and embedding matches.",
    version="1.0.0",
)

# --- Configuration (from Environment Variables) ---
# IMPORTANT: These must be the NAMES of the environment variables
# that you will set in your Cloud Run service configuration.
ENV_CLOUD_SQL_CONNECTION_NAME = "meilsearchproduct2106:europe-central2:testdata1"
ENV_DB_USER = "postgres"
ENV_DB_PASS = "testDatap"
ENV_DB_NAME = "test_data"
ENV_USE_PRIVATE_IP = "10.30.145.3" # Set this to "true" or "false"

# Retrieve values using the defined environment variable names
CLOUD_SQL_CONNECTION_NAME = os.environ.get(ENV_CLOUD_SQL_CONNECTION_NAME)
DB_USER = os.environ.get(ENV_DB_USER)
DB_PASS = os.environ.get(ENV_DB_PASS)
DB_NAME = os.environ.get(ENV_DB_NAME)
USE_PRIVATE_IP = os.environ.get(ENV_USE_PRIVATE_IP, "false").lower() == "true" # Default to public if not set

# --- Validate essential environment variables ---
if not all([CLOUD_SQL_CONNECTION_NAME, DB_USER, DB_PASS, DB_NAME]):
    missing_vars = []
    if CLOUD_SQL_CONNECTION_NAME is None: missing_vars.append(ENV_CLOUD_SQL_CONNECTION_NAME)
    if DB_USER is None: missing_vars.append(ENV_DB_USER)
    if DB_PASS is None: missing_vars.append(ENV_DB_PASS)
    if DB_NAME is None: missing_vars.append(ENV_DB_NAME)
    raise ValueError(f"Missing one or more essential environment variables for database connection: {', '.join(missing_vars)}")


# --- Cloud SQL Connector and SQLAlchemy Setup ---
# Initialize Connector once globally
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
        # Re-raise to prevent the app from starting without a DB connection
        raise RuntimeError(f"Could not connect to database: {e}")


# Create a SQLAlchemy engine using NullPool for serverless environments.
db_pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
    poolclass=NullPool
)


# --- Pydantic Models for Request/Response Validation ---
class SearchRequest(BaseModel):
    query_text: str

class SearchResponse(BaseModel):
    ids: list[str] # Assuming IDs are strings (UUIDs or text-based)

# --- FastAPI Routes ---

@app.get("/healthz", status_code=200)
async def health_check():
    """Simple health check endpoint that also pings the database."""
    try:
        with db_pool.connect() as connection:
            connection.execute(sqlalchemy.text("SELECT 1")) # Simple query to check connectivity
        return {"status": "ok", "db_connection": "successful"}
    except Exception as e:
        print(f"Health check failed due to database error: {e}")
        # Return 503 Service Unavailable if database is not reachable
        raise HTTPException(status_code=503, detail=f"Service Unavailable: Database connection failed: {str(e)}")


@app.post("/search-products", response_model=SearchResponse, status_code=200)
async def search_products(request_body: SearchRequest):
    """
    Searches for product IDs based on the provided query text.
    Combines text-based and embedding-based matches, prioritizing text matches.
    """
    query_text = request_body.query_text

    # The SQL query, parameterized for security.
    # Note the `:param_name` syntax for SQLAlchemy's text() construct.
    # This query assumes your PostgreSQL instance has the 'embedding' function
    # provided by Cloud SQL's AI features (e.g., pg_embedding extension).
    sql_query = """
    SELECT id
    FROM (
      SELECT DISTINCT ON (id) *
      FROM (
        (
          SELECT 'text_match' AS source, id, name
          FROM products
          WHERE name ILIKE :query_text_pattern
          LIMIT 10
        )
        UNION ALL
        (
          SELECT 'embedding_match' AS source, id, name
          FROM products
          ORDER BY abstract_embeddings <=> embedding('text-embedding-005', :query_text_embedding)::vector
          LIMIT 10
        )
      ) combined
      ORDER BY id, source DESC -- IMPORTANT: This prioritizes 'text_match' for deduplication
    ) deduped
    ORDER BY source DESC; -- This orders the final output by source (text_match first)
    """

    ids = []
    try:
        # Use SQLAlchemy's connection context manager for automatic close
        with db_pool.connect() as connection:
            # Execute the query with parameters
            # This prevents SQL injection and is the recommended way.
            result = connection.execute(
                sqlalchemy.text(sql_query),
                {
                    "query_text_pattern": f"%{query_text}%",
                    "query_text_embedding": query_text # This text is passed to the embedding() function in SQL
                }
            )
            ids = [row.id for row in result.fetchall()] # Access by column name
        return SearchResponse(ids=ids) # Return the Pydantic model
    except Exception as e:
        print(f"Database query error: {e}")
        # Raise an HTTPException for FastAPI to handle with appropriate status code
        raise HTTPException(status_code=500, detail=f"Failed to retrieve IDs: {str(e)}")

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