# License: MIT-0

import json
import datetime
import ipaddress
import random
import os
import socket
import hashlib
import time

from faker import Faker
from faker.providers import internet

from kafka import KafkaProducer
from kafka.errors import KafkaError

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(os.environ["AWS_REGION"])
        return token

def generate_8char_hash():
    """Generate 8-character hash for unique identifiers"""
    return hashlib.md5(str(random.random()).encode()).hexdigest()[:8]

def generate_internal_ips() -> list:
    """Generate list of internal target IPs"""
    internal_ips = []
    for ip_range in ["10.34.0.0/16", "10.24.25.0/24", "11.64.0.0/15"]:
        network = ipaddress.ip_network(ip_range)
        internal_ips.extend([str(ip) for ip in list(network.hosts())[:50]])
    return internal_ips

def produce_normal_events():
    """Produce normal traffic events continuously"""
    # Log environment variables
    print(f"BOOTSTRAP_SERVER: {os.environ.get('BOOTSTRAP_SERVER', 'NOT_SET')}")
    print(f"TOPIC_NAME: {os.environ.get('TOPIC_NAME', 'NOT_SET')}")
    print(f"AWS_REGION: {os.environ.get('AWS_REGION', 'NOT_SET')}")
    
    fake = Faker()
    fake.add_provider(internet)
    tp = MSKTokenProvider()

    internal_ips = generate_internal_ips()
    protocols = ["UDP", "TCP", "ICMP"]
    ports = ["53", "80", "443", "8080", "1433"]
    event_types = ["GET", "POST", "DELETE", "PATCH", "PUT"]
    
    # Log producer configuration
    producer_config = {
        "bootstrap_servers": os.environ["BOOTSTRAP_SERVER"],
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "client_id": socket.gethostname(),
        "api_version": (2, 0, 0)
    }
    print(f"Kafka Producer Config: {producer_config}")
    
    producer = KafkaProducer(
        bootstrap_servers=os.environ["BOOTSTRAP_SERVER"],
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    topic = os.environ["TOPIC_NAME"]
    
    while True:
        time_data = datetime.datetime.now()
        
        data = {
            "event_type": random.choice(event_types),
            "ip_src": fake.ipv4_private(),
            "ip_dst": random.choice(internal_ips),
            "port_src": random.choice(ports),
            "port_dst": random.choice(ports),
            "ip_proto": random.choice(protocols),
            "timestamp_start": str(time_data - datetime.timedelta(seconds=1)),
            "timestamp_end": str(time_data),
            "packets": random.randint(10, 500),
            "bytes": random.randint(64, 1500),
            "writer_id": f"ENI-{generate_8char_hash()}-x{random.randint(1, 5)}",
            "text": f"Normal traffic from {fake.ipv4_private()} to {random.choice(internal_ips)}"
        }
        
        producer.send(topic, key=str(random.randint(1, 10000)), value=data)
        time.sleep(0.1)  # 10 events per second

if __name__ == "__main__":
    produce_normal_events()