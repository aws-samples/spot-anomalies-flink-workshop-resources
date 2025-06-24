#!/usr/bin/env python3

import ipaddress
import random

# Copy the functions to test without imports
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

def generate_suspicious_ips():
    suspicious_ips = []
    for ip_range in SUSPICIOUS_IP_RANGES:
        network = ipaddress.ip_network(ip_range)
        suspicious_ips.extend([str(ip) for ip in list(network.hosts())[:10]])
    return suspicious_ips

def generate_internal_ips():
    internal_ips = []
    for ip_range in INTERNAL_IP_RANGES:
        network = ipaddress.ip_network(ip_range)
        internal_ips.extend([str(ip) for ip in list(network.hosts())[:50]])
    return internal_ips

def generate_fragmentation_text(ip_src, ip_dst, fragment_id, fragment_offset, more_fragments):
    flags = "MF" if more_fragments else "DF"
    return f"IP {ip_src} > {ip_dst}: frag {fragment_id}:{fragment_offset}@ {flags} proto UDP"

# Test the functions
print("Testing fragmentation functions:")

suspicious_ips = generate_suspicious_ips()
print(f"Generated {len(suspicious_ips)} suspicious IPs: {suspicious_ips[:5]}")

internal_ips = generate_internal_ips()
print(f"Generated {len(internal_ips)} internal IPs: {internal_ips[:5]}")

print("\nTesting fragmentation text generation:")
attacker_ip = random.choice(suspicious_ips)
target_ip = random.choice(internal_ips)
fragment_id = 1234

for i in range(5):
    frag_text = generate_fragmentation_text(
        attacker_ip, target_ip, fragment_id, i*8, i < 4
    )
    print(f"Fragment {i}: {frag_text}")

print(f"\nSimulating fragmentation attack from {attacker_ip} to {target_ip}")
print("This would generate 50-200 fragments in the actual function")
print("Test completed successfully!")