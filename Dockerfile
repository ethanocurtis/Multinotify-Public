# syntax=docker/dockerfile:1

FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -ms /bin/bash appuser

WORKDIR /app

# Copy only requirements first (better layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy app code
COPY bot.py /app/bot.py

# Ensure a persistent place for the DB; we symlink bot.db -> /app/data/bot.db
RUN mkdir -p /app/data && ln -sf /app/data/bot.db /app/bot.db && chown -R appuser:appuser /app

# Use Tini as init to handle signals properly
ENTRYPOINT ["/usr/bin/tini", "--"]

# Run as non-root
USER appuser

# Default command
CMD ["python", "-u", "/app/bot.py"]
