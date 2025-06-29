FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY normal_events_producer.py .

CMD ["python", "normal_events_producer.py"]