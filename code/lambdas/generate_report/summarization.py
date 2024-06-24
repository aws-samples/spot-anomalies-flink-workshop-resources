# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from langchain.output_parsers.json import SimpleJsonOutputParser
from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from connections import Connections, tracer, logger, metrics
from aws_lambda_powertools.metrics import MetricUnit
from prompt_templates import SYSTEM_PROMPT, SUMMARIZATION_TEMPLATE_PARAGRAPH
from connections import Connections
import boto3
import os
import base64
import json

# Get env variable
TOPIC_ARN = os.environ["TOPIC_ARN"]
REGION_NAME = os.environ["REGION_NAME"]

sns_client = boto3.client("sns", region_name=REGION_NAME)


@logger.inject_lambda_context(log_event=True, clear_state=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event, context):

    metrics.add_metric(name="TotalInvocations", unit=MetricUnit.Count, value=1)

    # logger.info(f"Received event: {event}")

    records = event.get("records")

    model_name = "Claude3"
    # Initialize output parser object, specifying the tags (to be consistent with the prompt)
    parser = SimpleJsonOutputParser()
    # Prompt from a template & parser
    system_message_template = SystemMessagePromptTemplate.from_template(
        SYSTEM_PROMPT)
    human_message_template = HumanMessagePromptTemplate.from_template(
        SUMMARIZATION_TEMPLATE_PARAGRAPH
    )
    prompt = ChatPromptTemplate.from_messages(
        [system_message_template, human_message_template]
    )

    logger.info(f"Getting LLM: {model_name}")
    # LLM object
    llm = Connections.get_bedrock_llm(
        model_name=model_name, max_tokens=2048, cache=False
    )

    # Chain the elements together
    chain = prompt | llm | parser
    responses = []
    for topic_key in records.keys():

        messages = records.get(topic_key)

        for msg in messages:
            jsg_msg = json.loads(
                base64.b64decode(msg["value"]).decode("utf-8"), strict=False
            )
            text = jsg_msg.get("text")
            ip_src = jsg_msg.get("ip_src")
            fraud = jsg_msg.get("fraud")

            # logger.info(f"raw event: {jsg_msg}")
            # logger.info(f"is it fraud? {fraud}")

            if fraud == True:

                # Define input dict
                input_dict = {
                    "format_instructions": parser.get_format_instructions(),
                    "input_event": text,
                }

                # Invoke the chain
                ans = chain.invoke(input_dict)
                logger.info(f"Chain response: {ans}")
                logger.info(f"Publishing to sns topic: {TOPIC_ARN}")
                email_body = Message = ans.get("incident_report") + """
                
Original input event:
""" + text

                sns_client.publish(TopicArn=TOPIC_ARN, Message=email_body)
                input_dict["llm_response"] = ans
                responses.append(input_dict)
                continue
    return responses
