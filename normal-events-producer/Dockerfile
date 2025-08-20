FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    telnet \
    dnsutils \
    awscli \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY normal_events_producer.py .
COPY startup.sh .

RUN pip install -r requirements.txt
RUN chmod +x startup.sh

CMD ["./startup.sh"]