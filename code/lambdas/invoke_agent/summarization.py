import boto3
import os
import base64
import json
import time
import uuid
from datetime import datetime
from connections import tracer, logger, metrics
from aws_lambda_powertools.metrics import MetricUnit
from botocore.exceptions import ClientError

AGENT_RUNTIME_ARN = os.environ["AGENT_RUNTIME_ARN"]
REGION_NAME = os.environ["REGION_NAME"]

agent_core_client = boto3.client("bedrock-agentcore", region_name=REGION_NAME)

def format_event_data(jsg_msg):
    """Format anomaly data into readable event description"""
    return f"""Fragment Attack Detection:
- Attack Start Time: {datetime.fromtimestamp(jsg_msg['attack_start_time']).isoformat()}Z
- Attack End Time: {datetime.fromtimestamp(jsg_msg['attack_end_time']).isoformat()}Z  
- Attacker IP: {jsg_msg['attacker_id']}
- Target IP: {jsg_msg['target_ip']}
- Fragment Count: {jsg_msg['fragment_count']}
- Average Packets: {jsg_msg['avg_packets']}
- Average Fragment Size: {jsg_msg['avg_fragment_size']:.2f}
- Size Reduction Percentage: {jsg_msg['size_reduction_percent']:.1f}%
- Attack Duration: {jsg_msg['attack_end_time'] - jsg_msg['attack_start_time']:.3f} seconds"""

def process_agent_response(response):
    """Process AgentCore response based on content type"""
    agent_response = ""
    
    if "text/event-stream" in response.get("contentType", ""):
        # Handle streaming response
        for line in response["response"].iter_lines(chunk_size=10):
            if line:
                line = line.decode("utf-8")
                if line.startswith("data: "):
                    line = line[6:]
                    agent_response += line
    
    elif response.get("contentType") == "application/json":
        # Handle standard JSON response
        content = []
        for chunk in response.get("response", []):
            content.append(chunk.decode('utf-8'))
        agent_response = ''.join(content)
    
    else:
        # Handle other content types
        agent_response = str(response)
    
    return agent_response

def invoke_agent_with_retry(event_data, max_retries=3):
    """Invoke AgentCore agent with retry logic for throttling"""
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Calling AgentCore runtime {AGENT_RUNTIME_ARN} - Attempt {attempt + 1}")
            
            # Prepare payload for AgentCore
            payload = json.dumps({
                "prompt": f"Analyze this network security event. Send a notification if severe, and ensure responses are formatted so they are easily readible. If you run into any errors, return 'ERROR' with a description somewhere in the response body. \n\nEvent:\n{event_data}"
            }).encode()
                        
            response = agent_core_client.invoke_agent_runtime(
                agentRuntimeArn=AGENT_RUNTIME_ARN,
                payload=payload
            )
            
            agent_response = process_agent_response(response)
            logger.info(f"SUCCESS: AgentCore completed: {agent_response}")
            if "ERROR" in agent_response:
                logger.error(f"ERROR: AgentCore completed with error: {agent_response}")
                metrics.add_metric(name="AgentCoreInternalErrors", unit=MetricUnit.Count, value=1)
                raise RuntimeError(agent_response)
            return agent_response
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'throttlingException':
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1  # Exponential backoff
                    logger.warning(f"Throttled, retrying in {wait_time}s (attempt {attempt + 1})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries exceeded for throttling")
                    metrics.add_metric(name="ThrottlingErrors", unit=MetricUnit.Count, value=1)
                    raise Exception("throttled")
            else:
                raise e
                
        except Exception as agent_error:
            logger.error(f"AgentCore error (attempt {attempt + 1}): {str(agent_error)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise agent_error

def process_anomaly_message(msg):
    """Process a single anomaly message from Kafka"""
    jsg_msg = json.loads(base64.b64decode(msg["value"]).decode("utf-8"), strict=False)
    
    logger.info(f"Processing anomaly from IP: {jsg_msg['attacker_id']} at {datetime.fromtimestamp(jsg_msg['attack_start_time']).isoformat()}")
    
    event_data = format_event_data(jsg_msg)
    
    try:
        agent_response = invoke_agent_with_retry(event_data)
        logger.info(f"Agent response: {agent_response}")
        return {"anomaly": jsg_msg, "agent_response": agent_response}
    except Exception as e:
        logger.error(f"Error processing anomaly: {str(e)}")
        return {"anomaly": jsg_msg, "error": str(e)}

@logger.inject_lambda_context(log_event=True, clear_state=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event, context):
    
    metrics.add_metric(name="TotalInvocations", unit=MetricUnit.Count, value=1)
    
    records = event.get("records")
    responses = []
    
    for topic_key in records.keys():
        messages = records.get(topic_key)
        
        for msg in messages:
            response = process_anomaly_message(msg)
            responses.append(response)
    
    return responses
