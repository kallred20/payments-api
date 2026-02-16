FROM python:3.11-slim

# Create app directory
WORKDIR /app

# Install Python dependencies first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app ./app

# Cloud Run listens on PORT env var (default 8080)
ENV PORT=8080

# Start FastAPI
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]



