# Home Assistant MQTT Publisher for Anker Solix Devices
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ha_mqtt_publisher.py .

# No need for entrypoint script - environment variables are already available

# Create non-root user for security
RUN useradd -r -s /bin/false anker && \
    chown -R anker:anker /app
USER anker

# Set default environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import socket; socket.create_connection(('$MQTT_HOST', ${MQTT_PORT:-1883}), timeout=5)" || exit 1

CMD ["python", "ha_mqtt_publisher.py", "--live", "--mqtt", "--rt"]