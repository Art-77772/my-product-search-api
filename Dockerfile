# Use the official Python image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Command to run the application with Gunicorn and Uvicorn workers
# Gunicorn manages workers, Uvicorn serves the ASGI app
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8080"]
# -w 4: 4 Uvicorn workers (adjust based on CPU/memory)
# -k uvicorn.workers.UvicornWorker: use Uvicorn for ASGI apps
# main:app: refers to the 'app' object in 'main.py'
# --bind 0.0.0.0:8080: listens on the port Cloud Run expects