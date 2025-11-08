FROM python:3.11-slim

WORKDIR /app

# Copy application code
COPY pyproject.toml .
COPY . .
RUN pip install --no-cache-dir -e .

# Expose ports
EXPOSE 8000 8501

# Default command (can be overridden in docker-compose)
CMD ["streamlit", "run", "app/Dashboard.py"]
