# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import re
import boto3
import json
from aws_lambda_powertools import Logger, Tracer, Metrics
from langchain_community.chat_models import BedrockChat
from botocore.client import Config

tracer = Tracer()
logger = Logger(log_uncaught_exceptions=True, serialize_stacktrace=True)
metrics = Metrics()


class Connections:

    region_name = os.environ["AWS_REGION"]

    config = Config(read_timeout=60)
    bedrock_client = boto3.client(
        "bedrock-runtime", region_name=region_name, config=config
    )

    @staticmethod
    def get_bedrock_llm(model_name="ClaudeInstant", max_tokens=256, cache=True):

        MODELID_MAPPING = {
            "Titan": "amazon.titan-tg1-large",
            "Claude2": "anthropic.claude-v2",
            "ClaudeInstant": "anthropic.claude-instant-v1",
            "Claude3": "anthropic.claude-3-sonnet-20240229-v1:0",
        }

        MODEL_KWARGS_MAPPING = {
            "Titan": {
                "maxTokenCount": max_tokens,
                "temperature": 0,
                "topP": 1,
            },
            "Claude2": {
                "max_tokens": max_tokens,
                "temperature": 0,
                "top_p": 1,
                "top_k": 50,
                "stop_sequences": ["\n\nHuman"],
            },
            "Claude3": {
                "max_tokens": max_tokens,
                "temperature": 0,
                "top_p": 1,
                "top_k": 50,
                "stop_sequences": ["\n\nHuman"],
            },
            "ClaudeInstant": {
                "max_tokens": max_tokens,
                "temperature": 0,
                "top_p": 1,
                "top_k": 50,
                "stop_sequences": ["\n\nHuman"],
            },
        }

        model = model_name
        llm = BedrockChat(
            client=Connections.bedrock_client,
            model_id=MODELID_MAPPING[model],
            model_kwargs=MODEL_KWARGS_MAPPING[model],
            cache=cache,
        )
        return llm
