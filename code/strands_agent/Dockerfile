FROM --platform=linux/arm64 python:3.12-slim

WORKDIR /app

# Install system dependencies that might be needed
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Upgrade pip and install requirements
RUN pip install --upgrade pip
RUN python -m pip install --no-cache-dir aws_opentelemetry_distro_genai_beta>=0.1.2
RUN pip install --no-cache-dir -r requirements.txt

COPY agent.py .

# Create non-root user
RUN useradd -m -u 1000 bedrock_agentcore
USER bedrock_agentcore
EXPOSE 8080
CMD ["opentelemetry-instrument", "python", "agent.py"]