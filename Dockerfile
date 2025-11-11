FROM python:3.11-slim

WORKDIR /app

# Copy application code
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY . .

# Expose ports
EXPOSE 8000 8501

# Default command (can be overridden in docker-compose)
CMD ["streamlit", "run", "app/Dashboard.py"]
