from strands import Agent
import os
from datetime import timedelta
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands.models import BedrockModel
from mcp_lambda.client.streamable_http_sigv4 import streamablehttp_client_with_sigv4
from strands.tools.mcp.mcp_client import MCPClient
import boto3
import logging 
logging.basicConfig(level=logging.INFO)
app = BedrockAgentCoreApp()

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
        # timeout=timedelta(seconds=120), 
        # terminate_on_close=False,
        credentials=boto3.Session().get_credentials().get_frozen_credentials()
    )
)

model_id = "global.anthropic.claude-sonnet-4-20250514-v1:0"
model = BedrockModel(model_id=model_id)


@app.entrypoint
def strands_agent_bedrock(payload):
    """Invoke the agent with a payload"""
    print(f"User input: {payload}")
    user_input = payload.get("prompt")
    
    # Create agent with MCP tools
    with streamable_http_mcp_client:
        tools = streamable_http_mcp_client.list_tools_sync()
        
        agent = Agent(
            model=model,
            tools=tools,
            system_prompt="""You are a network security analyst specializing in anomaly detection. Your job is to analyze network traffic events and provide expert analysis.

Always maintain a professional, analytical tone and avoid making assumptions beyond the data provided.
## Overall handling process:

When presented with a network event:
1. Analyze the event details (timing, metrics, patterns)
2. Determine the likely event type and severity
3. Assess potential impact and risk level
4. Provide recommendations for response
5. Send all of this information as a notification 

## Severity rating guidelines:

Rate the severity of events on a scale of 1-3:
1 = Low (likely normal activity or minimal impact)
2 = Medium (unusual activity requiring investigation)
3 = High (clear anomaly pattern with potential for impact)

## Notification guidelines:

For severity 2 or higher events, use the send_notification tool with:
- Subject line: "CRITICAL: Network Anomaly Detected"
- Must follow given template 

## Notification template:

All notifications must follow this pattern:

### Subject
"CRITICAL: Network Anomaly Detected"

### Body
Event Details: 

- Attack Type: {AttackType}
- Start Time: {StartTime}
- End Time: {EndTime}
- Source IP: {SourceIP}
- Target IP: {TargetIP}
- Fragment Count: {FragmentCount}
- Average Packets: {AvgPackets}
- Average Fragment Size: {AvgSize}
- Size Reduction Percentage: {SizePct}
- Attack Duration: {Duration}

Recommendations:
{Recommendations}
"""
        )
        
        response = agent(user_input)
        print(f"Agent response: {response}")
        return response.message['content'][0]['text']

if __name__ == "__main__":
    app.run()
