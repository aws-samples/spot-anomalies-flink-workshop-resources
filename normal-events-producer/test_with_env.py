#!/usr/bin/env python3

import os
import json
from unittest.mock import Mock, patch
import time

# Set required environment variables
os.environ.update({
    "BOOTSTRAP_SERVER": "localhost:9092",
    "TOPIC_NAME": "test-topic"
})

# Mock Kafka producer
with patch('normal_events_producer.KafkaProducer') as mock_producer:
    mock_producer_instance = Mock()
    mock_producer.return_value = mock_producer_instance
    
    # Import and test for a short duration
    import normal_events_producer
    
    print("Testing normal events producer with mocked Kafka...")
    
    # Run producer for 2 seconds
    import threading
    import signal
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Test timeout")
    
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(2)  # 2 second timeout
    
    try:
        normal_events_producer.produce_normal_events()
    except (TimeoutError, KeyboardInterrupt):
        print("\nStopped producer after test duration")
    
    print(f"Producer send called: {mock_producer_instance.send.called}")
    print(f"Total send calls: {mock_producer_instance.send.call_count}")
    
    if mock_producer_instance.send.called:
        # Show sample messages
        calls = mock_producer_instance.send.call_args_list
        for i, call in enumerate(calls[:3]):
            args, kwargs = call
            message = json.loads(kwargs['value']) if isinstance(kwargs['value'], str) else kwargs['value']
            print(f"Message {i+1}: {message['ip_src']} -> {message['ip_dst']}")
            print(f"  Event: {message['event_type']}, Bytes: {message['bytes']}, Packets: {message['packets']}")
    
    print("Normal events producer test completed successfully!")