import os
from mcp_lambda.client.streamable_http_sigv4 import streamablehttp_client_with_sigv4
from strands.tools.mcp.mcp_client import MCPClient
import boto3
import json

# Environment variables
MCP_RUNTIME_ARN = os.environ["MCP_RUNTIME_ARN"]
REGION_NAME = os.environ["AWS_REGION"]

# Setup MCP client
encoded_arn = MCP_RUNTIME_ARN.replace(':', '%3A').replace('/', '%2F')
mcp_url = f"https://bedrock-agentcore.{REGION_NAME}.amazonaws.com/runtimes/{encoded_arn}/invocations?qualifier=DEFAULT"

streamable_http_mcp_client = MCPClient(
    lambda: streamablehttp_client_with_sigv4(
        mcp_url, 
        service="bedrock-agentcore", 
        region=REGION_NAME, 
        credentials=boto3.Session().get_credentials().get_frozen_credentials()
    )
)
streamable_http_mcp_client.start()
tools = streamable_http_mcp_client.list_tools_sync()
for i in tools:
    print(json.dumps(i.tool_spec))