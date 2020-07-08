import boto3
import json
import os

TABLE_NAME = os.environ['TABLE_NAME']

dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    print('EVENT: {}'.format(json.dumps(event)))

    for record in event['Records']:
        eventType = record['messageAttributes']['eventType']['stringValue']
        jobEvent = json.loads(record['body'])

        if 'JobCreated' == eventType:
            create_job_event(jobEvent)

        elif 'JobDeleted' == eventType:
            delete_job_event(jobEvent)

        else:
            raise Exception('Don\'t know the event type {}'.format(eventType))

    return


def create_job_event(jobEvent):
    response = dynamodb.put_item(
        TableName=TABLE_NAME,
        Item={
            'id': {
                'S': jobEvent['jobId'],
            },
            'eventCreated': {
                'S': jobEvent['eventCreated'],
            },
            'eventSource': {
                'S': jobEvent['eventSource'],
            },
            'eventDetails': {
                'S': json.dumps(jobEvent['eventDetails']),
            }
        }
    )
    print('put item and received response: {}'.format(response))

    return


def delete_job_event(jobEvent):
    response = dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={
            'id': {
                'S': jobEvent['jobId'],
            }
        },
        UpdateExpression='SET markAsDeleted = :m',
        ExpressionAttributeValues={
            ':m': { 'BOOL': True }
        }
    )
    print('put item and received response: {}'.format(response))

    return