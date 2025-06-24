# License: MIT-0

import json
import datetime
import ipaddress
import random
import boto3
import io
import os
import socket
import pandas as pd

from faker import Faker
from faker.providers import internet

from kafka import KafkaProducer
from kafka.errors import KafkaError

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Constants
FILE_KEY = os.environ["FILE_KEY"]

# Base data template
BASE_DATA = {
    "event_type": "FRAGMENT",
    "ip_src": "127.0.0.1",
    "ip_dst": "10.124.7.1",
    "port_src": 80,
    "port_dst": 8080,
    "ip_proto": "UDP",
    "timestamp_start": "2021-07-15 19:35:23.000000",
    "timestamp_end": "2021-11-25 20:20:12.382551",
    "packets": 1,
    "bytes": 80,
    "writer_id": "nflw-fragmentation",
    "text": "",
}

# Suspicious IP ranges for fragmentation attacks
SUSPICIOUS_IP_RANGES = [
    "192.168.1.0/24",
    "10.0.0.0/24",
    "172.16.0.0/24",
]

INTERNAL_IP_RANGES = [
    "10.34.0.0/16",
    "10.24.25.0/24",
    "11.64.0.0/15",
]

PROTOCOLS = ["UDP", "TCP", "ICMP"]
PORTS = ["53", "80", "443", "8080", "1433"]

fake = Faker()
fake.add_provider(internet)

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(os.environ["AWS_REGION"])
        return token

def generate_fragmentation_text(ip_src: str, ip_dst: str, fragment_id: int, fragment_offset: int, more_fragments: bool) -> str:
    """Generate fragmentation-specific log text"""
    flags = "MF" if more_fragments else "DF"
    return f"IP {ip_src} > {ip_dst}: frag {fragment_id}:{fragment_offset}@ {flags} proto UDP"

def generate_suspicious_ips() -> list:
    """Generate list of suspicious IPs for fragmentation attacks"""
    suspicious_ips = []
    for ip_range in SUSPICIOUS_IP_RANGES:
        network = ipaddress.ip_network(ip_range)
        suspicious_ips.extend([str(ip) for ip in list(network.hosts())[:10]])
    return suspicious_ips

def generate_internal_ips() -> list:
    """Generate list of internal target IPs"""
    internal_ips = []
    for ip_range in INTERNAL_IP_RANGES:
        network = ipaddress.ip_network(ip_range)
        internal_ips.extend([str(ip) for ip in list(network.hosts())[:50]])
    return internal_ips

def load_csv_data(bucket_name: str) -> pd.DataFrame:
    """Reads CSV dataset from S3 bucket"""
    s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=bucket_name, Key=FILE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df

def lambda_handler(event, context):
    """Main function to generate fragmentation anomaly events"""
    
    print("Starting fragmentation anomaly generator...")
    
    tp = MSKTokenProvider()
    
    # Load CSV data
    df = load_csv_data(os.environ["BUCKET_NAME"])
    
    suspicious_ips = generate_suspicious_ips()
    internal_ips = generate_internal_ips()
    
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
    msg_count = int(os.environ["MESSAGE_COUNT"])
    messages = []
    
    print("Start of fragmentation data generation -", str(datetime.datetime.now()))
    
    # Generate fragmentation anomaly events
    for i in range(msg_count):
        data = BASE_DATA.copy()
        time_data = datetime.datetime.now()
        
        # Determine if this should be an anomalous event
        is_anomaly = os.environ.get("ANOMALY", "False") == "True" and random.random() < 0.01
        
        if is_anomaly:
            # Generate high fragmentation from single/range of IPs
            attacker_ip = random.choice(suspicious_ips)
            target_ip = random.choice(internal_ips)
            
            # Generate multiple fragments from same source
            fragment_id = random.randint(1000, 9999)
            fragment_count = random.randint(50, 200)  # High fragmentation
            
            for frag_num in range(fragment_count):
                frag_data = data.copy()
                frag_data["ip_src"] = attacker_ip
                frag_data["ip_dst"] = target_ip
                frag_data["port_src"] = random.choice(PORTS)
                frag_data["port_dst"] = random.choice(PORTS)
                frag_data["ip_proto"] = "UDP"
                frag_data["timestamp_start"] = str(time_data - datetime.timedelta(seconds=1))
                frag_data["timestamp_end"] = str(time_data)
                frag_data["packets"] = 1
                frag_data["bytes"] = random.randint(8, 64)  # Small fragment sizes
                frag_data["writer_id"] = f"frag-detector-{random.randint(1, 5)}"
                
                fragment_offset = frag_num * 8
                more_fragments = frag_num < fragment_count - 1
                frag_data["text"] = generate_fragmentation_text(
                    attacker_ip, target_ip, fragment_id, fragment_offset, more_fragments
                )
                
                messages.append(frag_data)
        else:
            # Generate normal traffic
            data["ip_src"] = fake.ipv4_private()
            data["ip_dst"] = random.choice(internal_ips)
            data["port_src"] = random.choice(PORTS)
            data["port_dst"] = random.choice(PORTS)
            data["ip_proto"] = random.choice(PROTOCOLS)
            data["timestamp_start"] = str(time_data - datetime.timedelta(seconds=1))
            data["timestamp_end"] = str(time_data)
            data["packets"] = random.randint(10, 500)
            data["bytes"] = random.randint(64, 1500)
            data["writer_id"] = f"normal-traffic-{random.randint(1, 10)}"
            data["text"] = f"Normal traffic from {data['ip_src']} to {data['ip_dst']}"
            
            messages.append(data)
    
    print(f"Generated {len(messages)} fragmentation events")
    
    # Publish messages
    cy = int(os.environ.get("CY", "1"))
    cycled = 0
    
    while True:
        if cy != 0 and cycled >= cy:
            print("End of data publication -", str(datetime.datetime.now()))
            producer.close()
            break
        
        cycled += 1
        for idx, msg in enumerate(messages, start=1):
            try:
                producer.send(topic, key=str(idx), value=msg)
                if idx % 1000 == 0:
                    producer.flush()
            except KafkaError:
                producer.send(topic, key=str(idx), value=msg)
        
        producer.flush()
    
    return {"statusCode": 200}