FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY normal_events_producer.py .

CMD ["python3", "normal_events_producer.py"]