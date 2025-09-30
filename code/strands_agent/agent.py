from strands import Agent, tool
import json
import boto3
import os
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands.models import BedrockModel

app = BedrockAgentCoreApp()

# Environment variables
TOPIC_ARN = os.environ.get("TOPIC_ARN", "")
REGION_NAME = os.environ.get("REGION_NAME", "us-west-2")

sns_client = boto3.client("sns", region_name=REGION_NAME)

@tool
def send_notification(severity: str, message: str, subject: str = "CRITICAL: Network Anomaly Detected") -> str:
    """Send SNS notification for security events with severity 2 or higher"""
    
    if severity in ['2', '3']:
        if TOPIC_ARN:
            try:
                sns_client.publish(
                    TopicArn=TOPIC_ARN,
                    Message=message,
                    Subject=subject
                )
                return "notification sent successfully"
            except Exception as e:
                return f"notification failed: {str(e)}"
        else:
            return "notification failed: no topic ARN configured"
    else:
        return "notification skipped - severity below threshold"

model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
model = BedrockModel(model_id=model_id)

agent = Agent(
    model=model,
    tools=[send_notification],
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

@app.entrypoint
def strands_agent_bedrock(payload):
    """Invoke the agent with a payload"""
    print(f"User input: {payload}")
    user_input = payload.get("prompt")
    response = agent(user_input)
    print(f"Agent response: {response}")
    return response.message['content'][0]['text']

if __name__ == "__main__":
    app.run()


# from strands import Agent, tool
# import json
# import boto3
# import os
# import re
# from bedrock_agentcore.runtime import BedrockAgentCoreApp
# from strands.models import BedrockModel

# app = BedrockAgentCoreApp()

# # Environment variables
# TOPIC_ARN = os.environ.get("TOPIC_ARN", "")
# REGION_NAME = os.environ.get("REGION_NAME", "us-west-2")

# sns_client = boto3.client("sns", region_name=REGION_NAME)

# @tool
# def generateTemplate(eventData: str) -> str:
#     """Analyzes network event data and generates a detailed security assessment report"""
    
#     # Extract key information from event data
#     ip_match = "Unknown"
#     target_ip = "Unknown"
#     fragment_count = 0
    
#     # Parse event data for key metrics
#     if 'IP: ' in eventData:
#         ip_match = eventData.split('IP: ')[1].split(',')[0].strip()
#     if 'Target IP: ' in eventData:
#         target_ip = eventData.split('Target IP: ')[1].split('\n')[0].strip()
#     if 'Fragment Count: ' in eventData:
#         try:
#             fragment_count = int(eventData.split('Fragment Count: ')[1].split('\n')[0].strip())
#         except:
#             fragment_count = 0
    
#     # Determine severity based on fragment count (matching original logic)
#     severity = 2 if fragment_count > 20 else 1
    
#     # Create incident report
#     incident_report = f"""Security Alert: Network Event

# A security event was detected in the network traffic.

# Details:
# - Source IP: {ip_match}
# - Target IP: {target_ip}
# - Fragment Count: {fragment_count}

# Please investigate and take appropriate action."""
    
#     # Return structured JSON response
#     result = {
#         "incident_report": incident_report,
#         "severity": severity,
#         "ip_address": ip_match
#     }
    
#     return json.dumps(result)

# @tool
# def sendNotification(severity: str, subject: str, message: str) -> str:
#     """Sends security alerts to the response team based on event severity and details"""
    
#     if severity in ['2', '3']:
#         if TOPIC_ARN:
#             try:
#                 sns_client.publish(
#                     TopicArn=TOPIC_ARN,
#                     Message=message,
#                     Subject=subject
#                 )
#                 return "notification sent successfully"
#             except Exception as e:
#                 return f"notification failed: {str(e)}"
#         else:
#             return "notification failed: no topic ARN configured"
#     else:
#         return "notification skipped - severity below threshold"

# model_id = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
# model = BedrockModel(model_id=model_id)

# agent = Agent(
#     model=model,
#     tools=[generateTemplate, sendNotification],
#     system_prompt="""You are a network security analyst specializing in anomaly detection. Your job is to analyze network traffic events and provide expert analysis.

# When presented with a network event:
# 1. Use generateTemplate tool to analyze the event details and get structured assessment
# 2. Based on the severity from the analysis, decide if notification is needed  
# 3. For severity 2 or higher events, use sendNotification tool with appropriate subject and message

# For network anomalies, pay attention to:
# - Size metrics (unusual size patterns may indicate network issues)
# - Count metrics (higher counts may suggest unusual activity - fragment count > 20 indicates severity 2)
# - Duration (timing patterns can reveal important information)
# - Source and destination addresses (look for patterns)

# Rate the severity of events on a scale of 1-3:
# 1 = Low (likely normal activity or minimal impact)
# 2 = Medium (unusual activity requiring investigation)
# 3 = High (clear anomaly pattern with potential for impact)

# For severity 2 or higher events, create notifications with:
# - Subject line: "CRITICAL: Network Anomaly Detected"
# - Summary of the event in plain language
# - Original event details for reference
# - A request to investigate and take appropriate action

# Always maintain a professional, analytical tone and provide specific, practical recommendations. Keep alerts brief and use only basic ASCII characters."""
# )

# @app.entrypoint
# def strands_agent_bedrock(payload):
#     """Invoke the agent with a payload"""
#     user_input = payload.get("prompt")
#     print("User input:", user_input)
    
#     # Enhanced prompt for security analysis
#     enhanced_prompt = f"Analyze this network security event and determine the appropriate response: {user_input}"
    
#     response = agent(enhanced_prompt)
#     return response.message['content'][0]['text']

# if __name__ == "__main__":
#     app.run()
