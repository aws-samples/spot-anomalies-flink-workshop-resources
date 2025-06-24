# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Import dependencies
import json
import datetime
import ipaddress
import random
import boto3
import io
import os
import socket
import pandas as pd
import uuid

from faker import Faker
from faker.providers import internet

from kafka import KafkaProducer
from kafka.errors import KafkaError

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Constants
FILE_KEY = os.environ["FILE_KEY"]

# Base data template
BASE_DATA = {
    "event_type": "GET",
    "ip_src": "127.0.0.1",
    "ip_dst": "10.124.7.1",
    "port_src": 80,
    "port_dst": 8080,
    "ip_proto": "UDP",
    "timestamp_start": "2021-07-15 19:35:23.000000",
    "timestamp_end": "2021-11-25 20:20:12.382551",
    "packets": 1,
    "bytes": 80,
    "writer_id": "nflw-avroec01",
    "text": '84.55.41.57- - [14/Apr/2023:08:22:13 0100] "GET /wordpress/wp- content/plugins/custom_plugin/check_user.php?userid=1 AND (SELECT 6810 FROM(SELECT COUNT(*),CONCAT(0x7171787671,(SELECT (ELT(6810=6810,1))),0x71707a7871,FLOOR(RAND(0)*2))x FROM INFORMATION_SCHEMA CHARACTER_SETS GROUP BY x)a) HTTP/1.1" 200 166 "-" "Mozilla/5.0 (Windows; U; Windows NT 6.1; ru; rv:1.9.2.3) Gecko/20100401 Firefox/4.0 (NET CLR 3.5.30729)" 84.55.41.57- - [14/Apr/2023:08:22:13 0100] "GET /wordpress/wp- content/plugins/custom_plugin/check_user.php?userid=(SELECT 7505 FROM(SELECT COUNT(*),CONCAT(0x7171787671,(SELECT (ELT(7505=7505,1))),0×71707a7871,FLOOR(RAND(0)*2))x FROM INFORMATION_SCHEMA CHARACTER_SETS GROUP BY x)a) HTTP/1.1" 200 166 "-** "Mozilla/5.0 (Windows; U; Windows NT 6.1; ru; rv:1.9.2.3) Gecko/20100401 Firefox/4.0 (NET CLR 3.5.30729)" 84.55.41.57- - [14/Apr/2023:08:22:13 0100] "GET /wordpress/wp- content/plugins/custom_plugin/check_user.php?userid=(SELECT CONCAT(0x7171787671, (SELECT (ELT(1399=1399,1))),0×71707a7871)) HTTP/11" 200 166 "_" "Mozilla/5.0 (Windows; U; Windows NT 6.1; ru; rv:1.9.2.3) Gecko/20100401 Firefox/4.0 (.NET CLR 3.5.30729)" 84.55.41.57- - [14/Apr/2023:08:22:27 0100] "GET /wordpress/wp- content/plugins/custom_plugin/check_user.php?userid=1 UNION ALL SELECT CONCAT(0x7171787671,0x537653544175467a724f,0x71707a7871),NULL,NULL-- HTTP/1.1" 200 182 "-" "Mozilla/5.0 (Windows; U; Windows NT 6.1; ru; rv:1.9.2.3) Gecko/20100401 Firefox/4.0 (NET CL R3.5.30729)"',
}

INTERNAL_IP_RANGES = [
    "10.34.0.0/16",
    "10.24.25.0/24",
    "11.64.0.0/15",
    "10.115.19.144/28",
    "172.17.129.48/30",
    "172.23.129.224/28",
    "172.20.193.96/28",
]

EXTERNAL_IP_RANGES = [
    "1.46.0.0/19",
    "23.221.80.0/20",
    "58.8.0.0/14",
    "1.186.0.0/15",
    "2.16.89.0/24",
]

PROTOCOLS = ["TCP", "UDP", "ICMP"]

EVENT_TYPES = ["GET", "POST", "DELETE", "PATCH", "PUT"]

PORTS = ["80", "443", "22", "53", "25", "8080", "3306", "1433", "27017"]

fake = Faker()
fake.add_provider(internet)


# For IAM auth
class MSKTokenProvider:

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(
            os.environ["AWS_REGION"])
        return token


def generate_fake_text(event_type: str, timestamp: str, anomalous: bool) -> str:

    ip_address = fake.ipv4_public()

    # Generate a random URL path
    url_path = "/".join([fake.word(), fake.word(),
                        fake.word(), fake.word() + ".php"])

    # Generate a random user ID
    user_id = uuid.uuid4()

    # Generate random HTTP version string
    http_version = random.choice(
        ["HTTP/1.0", "HTTP/1.1", "HTTP/2", "HTTP/1.1", "HTTP/1.1", "HTTP/3"]
    )

    # Generate a random HTTP status code
    status_code = random.choice([200, 404, 500])

    # Generate a random SQL injection payload
    payload = random.choice(
        [
            f"AND (SELECT {random.randint(1000, 9999)} FROM(SELECT COUNT(*),CONCAT(0x7171787671,(SELECT (ELT({random.randint(1000, 9999)}={random.randint(1000, 9999)},1))),0x71707a7871,FLOOR(RAND(0)*2))x FROM INFORMATION_SCHEMA.CHARACTER_SETS GROUP BY x)a)",
            f"(SELECT {random.randint(1000, 9999)} FROM(SELECT COUNT(*),CONCAT(0x7171787671,(SELECT (ELT({random.randint(1000, 9999)}={random.randint(1000, 9999)},1))),0x71707a7871,FLOOR(RAND(0)*2))x FROM INFORMATION_SCHEMA.CHARACTER_SETS GROUP BY x)a)",
            f"(SELECT CONCAT(0x7171787671, (SELECT (ELT({random.randint(1000, 9999)}={random.randint(1000, 9999)},1))),0x71707a7871))",
            f"UNION ALL SELECT CONCAT(0x7171787671,0x{fake.hexify(text='SQLinjectionPayload')[:16]},0x71707a7871),NULL,NULL--",
        ]
    )

    # Generate a random response size
    response_size = random.randint(100, 1000)

    # Generate a random user agent
    user_agent = fake.user_agent()

    # Construct the log entry
    if anomalous:
        return f'{ip_address} - - [{timestamp}] "{event_type} {url_path}?userid={user_id} {payload} {http_version}" {status_code} {response_size} "-" "{user_agent}"'
    else:
        return f'{ip_address} - - [{timestamp}] "{event_type} {url_path}?userid={user_id} {http_version}" {status_code} {response_size} "-" "{user_agent}"'


def load_csv_data(bucket_name: str) -> pd.DataFrame:
    """Reads CSV dataset from a S3 bucket."""
    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=bucket_name, Key=FILE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df


def generate_external_ip_data() -> list:
    """Generates list of external IPs."""
    external_ip_data = []
    for iprange in EXTERNAL_IP_RANGES:
        external_ip_data += [
            str(ip) for ip in list(ipaddress.ip_network(iprange).hosts())
        ]
    return external_ip_data


def generate_ip_data() -> list:
    """Generates list of external and internal IPs."""
    ip_data = []
    for iprange in INTERNAL_IP_RANGES + EXTERNAL_IP_RANGES:
        ip_data += [str(ip)
                    for ip in list(ipaddress.ip_network(iprange).hosts())]
    return ip_data


def get_random_ip(ip_data: list) -> str:
    """Returns random item from a given list."""
    return random.choice(ip_data)


def lambda_handler(event, context):
    """Main function."""

    print("Starting...")

    tp = MSKTokenProvider()

    # Load CSV data
    df = load_csv_data(os.environ["BUCKET_NAME"])
    num_rows = df.shape[0]

    time_delay = datetime.timedelta(0, 1)
    external_ip_data = generate_external_ip_data()
    ip_data = generate_ip_data()

    producer = KafkaProducer(
        bootstrap_servers=os.environ["BOOTSTRAP_SERVER"],
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=10,
        batch_size=262144,
    )

    topic = os.environ["TOPIC_NAME"]
    msg_count = os.environ["MESSAGE_COUNT"]
    messages = []

    print("Start of data gen - ", str(datetime.datetime.now()))

    # Generate the events and convert into snappy object and create an array
    for i in range(int(msg_count)):

        data = BASE_DATA.copy()
        event_type = random.choice(EVENT_TYPES)
        data["event_type"] = event_type

        random_index = random.randint(0, num_rows - 1)
        random_sample = df.iloc[random_index]
        data["writer_id"] = random_sample.iloc[0]
        time_data = datetime.datetime.now()

        if os.environ["ANOMALY"] == "True" and random.random() < 0.001:
            # Probability of 0.1% to generate a anomalous log
            data["ip_src"] = get_random_ip(external_ip_data)
            data["text"] = generate_fake_text(
                event_type, str(time_data), True)
        else:
            data["ip_src"] = random_sample.iloc[1]
            data["text"] = generate_fake_text(
                event_type, str(time_data), False)

        data["ip_dst"] = get_random_ip(ip_data)
        data["port_src"] = random.choice(PORTS)
        data["port_dst"] = random.choice(PORTS)
        data["ip_proto"] = random.choice(PROTOCOLS)
        data["timestamp_start"] = str(time_data - time_delay)
        data["timestamp_end"] = str(time_data)
        data["bytes"] = random.randint(1, 1000)
        data["packets"] = random.randint(1, 100)
        messages.append(data)

    print("End of data gen - ",
          str(datetime.datetime.now()), f"{len(messages)}")

    cy = int(os.environ["CY"])
    print(
        f"Start of data pub: topic={topic} time={str(datetime.datetime.now())}")

    cycled = 0

    while True:
        if cy != 0 and cycled >= cy:
            print("End of data pub - ", str(datetime.datetime.now()))
            producer.close()
            break

        cycled += 1
        i = 0
        for idx, msg in enumerate(messages, start=1):
            try:
                producer.send(topic, key=str(idx), value=msg)
                i += 1
                if i % 100000 == 0:
                    producer.flush()
            except KafkaError as e:
                producer.send(topic, key=str(idx), value=msg)

        producer.flush()

    return {"statusCode": 200}
