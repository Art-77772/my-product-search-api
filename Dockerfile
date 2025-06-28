# Dockerfile
FROM python:3.10-slim-buster

WORKDIR /app

# Install system dependencies needed for building many Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN adduser --system --group appuser

COPY requirements.txt .

# Upgrade pip, setuptools, and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install your application dependencies
RUN pip install --no-cache-dir --verbose -r requirements.txt # This will now read the modified requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Change ownership of the /app directory to the non-root user
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

ENV PORT 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "$PORT", "--workers", "1", "--timeout", "0"]