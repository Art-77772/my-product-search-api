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
CLOUD_SQL_CONNECTION_NAME = os.environ.get("meilsearchproduct2106:europe-central2:testdata1")
DB_USER = os.environ.get("postgres")
DB_PASS = os.environ.get("testDatap")
DB_NAME = os.environ.get("test_data")
USE_PRIVATE_IP = os.environ.get("10.30.145.3", "false").lower() == "true"

# --- Cloud SQL Connector and SQLAlchemy Setup ---
connector = Connector()

def getconn() -> pg8000.dbapi.Connection:
    """Function to establish a new database connection."""
    conn: pg8000.dbapi.Connection = connector.connect(
        CLOUD_SQL_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        ip_type=IPTypes.PRIVATE if USE_PRIVATE_IP else IPTypes.PUBLIC,
    )
    return conn

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
    """Simple health check endpoint."""
    return {"status": "ok"}

@app.post("/search-products", response_model=SearchResponse, status_code=200)
async def search_products(request_body: SearchRequest):
    """
    Searches for product IDs based on the provided query text.
    Combines text-based and embedding-based matches, prioritizing text matches.
    """
    query_text = request_body.query_text

    # The SQL query, parameterized for security.
    # Note the `:param_name` syntax for SQLAlchemy's text() construct.
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
                    "query_text_embedding": query_text
                }
            )
            ids = [row[0] for row in result.fetchall()] # Fetch all IDs
        return SearchResponse(ids=ids) # Return the Pydantic model
    except Exception as e:
        print(f"Database error: {e}")
        # Raise an HTTPException for FastAPI to handle with appropriate status code
        raise HTTPException(status_code=500, detail=f"Failed to retrieve IDs: {str(e)}")

# This block is for local development only.
# Cloud Run will use uvicorn to run the app.
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))