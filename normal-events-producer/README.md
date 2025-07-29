# Normal Events Producer

## Setup Virtual Environment

### Override externally managed restriction:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt --break-system-packages
```

## Running Tests

### Test producer functions:
```bash
python test_functions.py
```

### Test with mocked Kafka:
```bash
python test_with_env.py
```

## Running Producer

### Set environment variables:
```bash
export BOOTSTRAP_SERVER="your-kafka-server:9092"
export TOPIC_NAME="your-topic-name"
```

### Run producer:
```bash
python normal_events_producer.py
```

## Docker

### Build container:
```bash
docker build -t normal-flow-log-producer .
```

### Run container:
```bash
docker run -i -e BOOTSTRAP_SERVER="your-kafka-server:9092" -e TOPIC_NAME="your-topic" normal-flow-log-producer:latest 
```

## Deactivate Virtual Environment
```bash
deactivate
```