"""Small helpers for Amazon SQS (or LocalStack, which imitates SQS locally)."""
import json

import boto3
from botocore.config import Config

from . import config


def client():
    return boto3.client(
        "sqs",
        endpoint_url=config.SQS_ENDPOINT,
        region_name=config.AWS_REGION,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}, max_pool_connections=20),
    )


def ensure_queues(c) -> str:
    """Create the order queue and its dead-letter queue (safe to call many times).
    A message that fails 5 times moves to the DLQ instead of retrying forever."""
    dlq_url = c.create_queue(QueueName=config.SQS_QUEUE_NAME + "-dlq")["QueueUrl"]
    dlq_arn = c.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    url = c.create_queue(QueueName=config.SQS_QUEUE_NAME, Attributes={"VisibilityTimeout": "30"})["QueueUrl"]
    c.set_queue_attributes(QueueUrl=url, Attributes={
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}),
    })
    return url


def depth(c, url: str) -> dict:
    a = c.get_queue_attributes(QueueUrl=url, AttributeNames=[
        "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    return {"visible": int(a.get("ApproximateNumberOfMessages", 0)),
            "inFlight": int(a.get("ApproximateNumberOfMessagesNotVisible", 0))}
