FROM public.ecr.aws/amazonlinux/amazonlinux:2

WORKDIR /app

RUN yum update -y && \
    yum install -y python3 python3-pip \
    curl \
    git \
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