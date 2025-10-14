from mcp.server.fastmcp import FastMCP
import boto3
import os
import logging 
logging.basicConfig(level=logging.INFO)

mcp = FastMCP(host="0.0.0.0", stateless_http=True)

# Environment variables
REGION_NAME = os.environ["AWS_REGION"]
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
TOPIC_ARN = f"arn:aws:sns:{REGION_NAME}:{ACCOUNT_ID}:AnomalyReportSNSTopic"

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