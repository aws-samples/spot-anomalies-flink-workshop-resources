#!/usr/bin/env python3

import sys
sys.path.append('.')

from normal_events_producer import generate_internal_ips, generate_8char_hash
import random
import datetime
from faker import Faker
from faker.providers import internet

# Test the functions
print("Testing normal events producer functions:")

fake = Faker()
fake.add_provider(internet)

internal_ips = generate_internal_ips()
print(f"Generated {len(internal_ips)} internal IPs: {internal_ips[:5]}")

print("\nTesting normal event generation:")
for i in range(3):
    time_data = datetime.datetime.now()
    data = {
        "event_type": random.choice(["GET", "POST", "DELETE"]),
        "ip_src": fake.ipv4_private(),
        "ip_dst": random.choice(internal_ips),
        "packets": random.randint(10, 500),
        "bytes": random.randint(64, 1500),
        "writer_id": f"ENI-{generate_8char_hash()}-x{random.randint(1, 5)}",
        "timestamp_start": str(time_data)
    }
    print(f"Normal event {i+1}: {data}")

print("\nNormal events producer test completed successfully!")