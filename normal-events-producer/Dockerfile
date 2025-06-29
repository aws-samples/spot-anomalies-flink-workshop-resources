FROM --platform=linux/amd64 python:3.9-slim

WORKDIR /app

# Install Python 3.9 and debugging tools
RUN yum update -y && \
    yum install -y python3 python3-pip \
    curl \
    telnet \
    nc \
    bind-utils \
    iputils \
    vim \
    && yum clean all

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY normal_events_producer.py .

CMD ["python3", "normal_events_producer.py"]