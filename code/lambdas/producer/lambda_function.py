# License: MIT-0

import json
import time
import ipaddress
import random
import os
import socket
import hashlib

from kafka import KafkaProducer
from kafka.errors import KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

def generate_8char_hash():
    """Generate 8-character hash for unique identifiers"""
    return hashlib.md5(str(random.random()).encode()).hexdigest()[:8]

def generate_fragmentation_text(ip_src: str, ip_dst: str, fragment_id: int, fragment_offset: int, more_fragments: bool) -> str:
    """Generate fragmentation-specific log text"""
    flags = "MF" if more_fragments else "DF"
    payload = random.choice(
        [
            f"AND (SELECT {random.randint(1000, 9999)} FROM(SELECT COUNT(*),CONCAT(0x7171787671,(SELECT (ELT({random.randint(1000, 9999)}={random.randint(1000, 9999)},1))),0x71707a7871,FLOOR(RAND(0)*2))x FROM INFORMATION_SCHEMA.CHARACTER_SETS GROUP BY x)a)",
            f"(SELECT {random.randint(1000, 9999)} FROM(SELECT COUNT(*),CONCAT(0x7171787671,(SELECT (ELT({random.randint(1000, 9999)}={random.randint(1000, 9999)},1))),0x71707a7871,FLOOR(RAND(0)*2))x FROM INFORMATION_SCHEMA.CHARACTER_SETS GROUP BY x)a)",
            f"(SELECT CONCAT(0x7171787671, (SELECT (ELT({random.randint(1000, 9999)}={random.randint(1000, 9999)},1))),0x71707a7871))",
        ]
    )
    return f"IP {ip_src} > {ip_dst}: frag {fragment_id}:{fragment_offset}@ {flags} proto UDP: {payload}"

def generate_suspicious_ips() -> list:
    """Generate list of suspicious IPs for fragmentation attacks"""
    suspicious_ips = []
    for ip_range in ["192.168.1.0/24", "10.0.0.0/24", "172.16.0.0/24"]:
        network = ipaddress.ip_network(ip_range)
        suspicious_ips.extend([str(ip) for ip in list(network.hosts())[:10]])
    return suspicious_ips

def generate_internal_ips() -> list:
    """Generate list of internal target IPs"""
    internal_ips = []
    for ip_range in ["10.34.0.0/16", "10.24.25.0/24", "11.64.0.0/15"]:
        network = ipaddress.ip_network(ip_range)
        internal_ips.extend([str(ip) for ip in list(network.hosts())[:50]])
    return internal_ips

class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(os.environ["AWS_REGION"])
        return token

def lambda_handler(event, context):
    """Generate 30 fragmentation anomaly events and return"""
    
    # Log environment variables
    print(f"BOOTSTRAP_SERVER: {os.environ.get('BOOTSTRAP_SERVER', 'NOT_SET')}")
    print(f"TOPIC_NAME: {os.environ.get('TOPIC_NAME', 'NOT_SET')}")
    print(f"AWS_REGION: {os.environ.get('AWS_REGION', 'NOT_SET')}")
    
    tp = MSKTokenProvider()
    suspicious_ips = generate_suspicious_ips()
    internal_ips = generate_internal_ips()
    
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
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        api_version=(2, 0, 0)
    )
    
    topic = os.environ["TOPIC_NAME"]
    
    
    # Generate 30 fragmentation anomaly events
    attacker_ip = random.choice(suspicious_ips)
    target_ip = random.choice(internal_ips)
    fragment_id = random.randint(1000, 9999)
    
    for frag_num in range(30):
        current_time_ms = int(time.time() * 1000)
        data = {
            "event_type": random.choice(["GET", "POST", "DELETE"]),
            "ip_src": attacker_ip,
            "ip_dst": target_ip,
            "port_src": random.choice(["53", "80", "443"]),
            "port_dst": random.choice(["8080", "1433"]),
            "ip_proto": "UDP",
            "timestamp_start": current_time_ms - 10,
            "timestamp_end": current_time_ms,
            "packets": 1,
            "bytes": random.randint(8, 64),
            "writer_id": f"ENI{generate_8char_hash()}-x{random.randint(1, 5)}",
            "text": generate_fragmentation_text(
                attacker_ip, target_ip, fragment_id, frag_num * 8, frag_num < 29
            )
        }
        
        producer.send(topic, key=str(frag_num), value=data)
    
    producer.flush()
    producer.close()
    
    return {"statusCode": 200, "body": "Generated 30 fragmentation anomaly events"}