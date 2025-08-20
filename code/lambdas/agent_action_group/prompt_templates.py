SYSTEM_PROMPT = """
    You are a cybersecurity analyst specializing in network security events.
    Your job is to analyze network security incidents and create clear, actionable alert emails for security teams.
    Always maintain a professional tone and provide specific, practical recommendations.
"""


SUMMARIZATION_TEMPLATE_PARAGRAPH = """
Analyze this network data: {input_event}

Create a very simple security alert email. Keep it brief and use only basic ASCII characters.

Return ONLY a JSON object with these fields:
"incident_report": a plain text email
"severity": a number (2 if fragment count > 20, otherwise 1)
"ip_address": the source IP address

Format example:
{{"incident_report": "Security Alert: Network Event\n\nA security event was detected.\n\nDetails:\n- Source: 10.0.0.1\n- Target: 192.168.1.1\n\nPlease investigate.", "severity": 2, "ip_address": "10.0.0.1"}}

DO NOT include any markdown formatting, code blocks, or explanations. Return ONLY the JSON object.
"""
