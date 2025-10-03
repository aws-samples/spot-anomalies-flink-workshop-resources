from mcp.server.fastmcp import FastMCP
import boto3
import os
import logging 
logging.basicConfig(level=logging.INFO)

mcp = FastMCP(host="0.0.0.0", stateless_http=True)

# Environment variables
TOPIC_ARN = os.environ.get("TOPIC_ARN", "")
REGION_NAME = os.environ.get("REGION_NAME", "us-west-2")

sns_client = boto3.client("sns", region_name=REGION_NAME)

@mcp.tool()
def send_notification(severity: str, subject: str, message: str) -> str:
    """Sends security alerts to the response team based on event severity and details"""
    
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

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
