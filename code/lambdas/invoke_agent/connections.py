import os
import boto3
from aws_lambda_powertools import Logger, Tracer, Metrics

tracer = Tracer()
logger = Logger(log_uncaught_exceptions=True, serialize_stacktrace=True)
metrics = Metrics()
