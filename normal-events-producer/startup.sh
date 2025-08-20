#!/bin/bash
echo "Testing AWS STS GetCallerIdentity..."
aws sts get-caller-identity || echo "STS call failed - check IAM permissions"

echo "Starting DNS troubleshooting..."
echo "BOOTSTRAP_SERVER: ${BOOTSTRAP_SERVER}"

echo "Available environment variables:"
env | sort

if [ -n "$BOOTSTRAP_SERVER" ]; then
    BROKER_HOST=${BOOTSTRAP_SERVER%:*}
    echo "Running nslookup on $BROKER_HOST"
    nslookup $BROKER_HOST
    echo "DNS lookup completed"
else
    echo "BOOTSTRAP_SERVER environment variable not set"
fi

echo "Keeping container running..."
python3 ./normal_events_producer.py