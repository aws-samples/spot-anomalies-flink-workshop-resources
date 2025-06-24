# Fragmentation Anomaly Producer

Lambda function that generates unusual packet fragmentation anomaly events.

## Setup Python Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows

# Install dependencies
pip install -r requirements.txt
```

## Local Testing

```bash
# Activate virtual environment first
source venv/bin/activate

# Test core functions (no dependencies required)
python3 test_functions.py

# Test with mocked dependencies (requires activated venv)
python3 test_with_env.py

# Deactivate when done
deactivate
```

## Environment Variables

Set these before running:
- `BUCKET_NAME`: S3 bucket name
- `FILE_KEY`: CSV file key in S3
- `BOOTSTRAP_SERVER`: Kafka bootstrap server
- `TOPIC_NAME`: Kafka topic name
- `MESSAGE_COUNT`: Number of messages to generate
- `ANOMALY`: "True" to enable anomaly generation
- `CY`: Number of cycles to run
- `AWS_REGION`: AWS region

## Deactivate Environment

```bash
deactivate
```