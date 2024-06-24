# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import base64
import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    firehose_client = boto3.client('firehose')
    # print ("Lambda Handler is initiated.... {}".format(event))

    firehoseStreamName = os.environ['FIREHOSE_STREAM_NAME']
    records = event.get("records")
    # print(records)

    for topic_key in records.keys():

        messages = records.get(topic_key)
        for msg in messages:
            data = json.loads(base64.b64decode(
                msg["value"]).decode("utf-8"), strict=False)
            firehose_client.put_record(DeliveryStreamName=firehoseStreamName, Record={
                                       'Data': json.dumps(data)})

    return {
        'statusCode': 200,
        'body': json.dumps('Data streamed from Kafka to Firehose successfully')
    }
