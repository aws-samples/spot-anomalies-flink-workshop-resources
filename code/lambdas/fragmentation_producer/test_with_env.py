#!/usr/bin/env python3

import os
import json
from unittest.mock import Mock, patch
import pandas as pd

# Set required environment variables
os.environ.update({
    "FILE_KEY": "test.csv",
    "BUCKET_NAME": "test-bucket", 
    "BOOTSTRAP_SERVER": "localhost:9092",
    "TOPIC_NAME": "test-topic",
    "MESSAGE_COUNT": "5000",
    "ANOMALY": "True",
    "CY": "1",
    "AWS_REGION": "us-east-1"
})

# Mock CSV data
mock_df = pd.DataFrame({
    'writer_id': ['writer1', 'writer2'],
    'ip_src': ['10.0.1.1', '10.0.1.2']
})

# Mock AWS and Kafka
with patch('lambda_function.load_csv_data', return_value=mock_df), \
     patch('lambda_function.KafkaProducer') as mock_producer, \
     patch('aws_msk_iam_sasl_signer.MSKAuthTokenProvider.generate_auth_token', return_value=('token', None)):
    
    mock_producer_instance = Mock()
    mock_producer.return_value = mock_producer_instance
    
    from lambda_function import lambda_handler
    
    print("Testing fragmentation producer with mocked dependencies...")
    result = lambda_handler({}, {})
    
    print(f"Lambda result: {result}")
    print(f"Producer send called: {mock_producer_instance.send.called}")
    print(f"Total send calls: {mock_producer_instance.send.call_count}")
    
    if mock_producer_instance.send.called:
        # Show sample messages
        calls = mock_producer_instance.send.call_args_list
        for i, call in enumerate(calls[:500]):
            args, kwargs = call
            message = kwargs['value'] if isinstance(kwargs['value'], dict) else json.loads(kwargs['value'])
            print(f"Message {i+1}: {message['ip_src']} -> {message['ip_dst']}")
            print(f"  Event: {message['event_type']}, Bytes: {message['bytes']}, Packets: {message['packets']}, Text: {message['text'][:50]}...")
    
    print("Test completed successfully!")