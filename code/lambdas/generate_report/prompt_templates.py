# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

SYSTEM_PROMPT = """
    You are an AI language model assistant specialized in sreviewing security log events.
    Your job is to determine the severity level, and summarise detail in a formatted incident report.
"""


SUMMARIZATION_TEMPLATE_PARAGRAPH = """
   
    ## Severity levels can take on the following values:
    0: No indication of malicious intent
    1: Possible indication of malicious intent.
    2: Direct evidence of malicious intent.


    ## Follow this incident report template
    Incident Report
    
    Summary:
    High level summary of what can be important. List severity level 0-2.

    Analysis:
    A few sentences about the scope of the problem.

    Remediation:
    Suggested next steps.
    

    ## Please provide in json format with the following keys - the values should just be a text string:
    
    "incident_report": (your carefully filled out incident report template, formatted as html markdown for easy reading),
    "severity": (severity level)
    "ip_address": (ip address of the inspected event)
    

    ## Event to summarize:

    {input_event}

    Your json response (do not add anything - your response should start and end with json object ):
    """
