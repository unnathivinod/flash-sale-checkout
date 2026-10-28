// Small helpers for Amazon SQS (or LocalStack, which imitates SQS locally).
import {
  SQSClient, CreateQueueCommand, GetQueueAttributesCommand, SetQueueAttributesCommand, GetQueueUrlCommand,
} from '@aws-sdk/client-sqs';
import * as C from './config.js';

export function client() {
  return new SQSClient({ region: C.AWS_REGION, endpoint: C.SQS_ENDPOINT, maxAttempts: 5 });
}

// Create the order queue and its dead-letter queue (safe to call many times).
// A message that fails 5 times moves to the DLQ instead of retrying forever.
export async function ensureQueues(sqs) {
  const dlq = await sqs.send(new CreateQueueCommand({ QueueName: `${C.SQS_QUEUE_NAME}-dlq` }));
  const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }));
  const q = await sqs.send(new CreateQueueCommand({ QueueName: C.SQS_QUEUE_NAME, Attributes: { VisibilityTimeout: '30' } }));
  await sqs.send(new SetQueueAttributesCommand({
    QueueUrl: q.QueueUrl,
    Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: Attributes.QueueArn, maxReceiveCount: '5' }) },
  }));
  return q.QueueUrl;
}

export async function queueUrl(sqs) {
  return (await sqs.send(new GetQueueUrlCommand({ QueueName: C.SQS_QUEUE_NAME }))).QueueUrl;
}

export async function depth(sqs, url) {
  const { Attributes: a } = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: url, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
  }));
  return { visible: Number(a.ApproximateNumberOfMessages || 0), inFlight: Number(a.ApproximateNumberOfMessagesNotVisible || 0) };
}
