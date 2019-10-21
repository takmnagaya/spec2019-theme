import json
import os
import uuid
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key
import requests


def user_create(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    body = json.loads(event['body'])
    user_wallet_table.put_item(
        Item={
            'userId': body['id'],
            'userName': body['name'],
            'walletId': str(uuid.uuid4()),
            'amount': 0,
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps({'result': 'ok'})
    }


def wallet_charge(event, context):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WalletCharge')
    queue.send_message(MessageBody=event['body'])
    return {
            'statusCode': 202,
            'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }


def wallet_charge_receiver(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])

    for record in event['Records']:
        body = json.loads(record['body'])
        user_wallet = user_wallet_table.update_item(
                ExpressionAttributeNames={
                    '#A': 'amount',
                },
                ExpressionAttributeValues={
                    ':a': body['chargeAmount'],
                },
                Key={
                    'userId': body['userId'],
                },
                ReturnValues='ALL_NEW',
                UpdateExpression='SET #A = #A + :a',
            )
        print(user_wallet)
        history_table.put_item(
            Item={
                'walletId': user_wallet['Attributes']['walletId'],
                'transactionId': body['transactionId'],
                'chargeAmount': body['chargeAmount'],
                'locationId': body['locationId'],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
            'transactionId': body['transactionId'],
            'userId': body['userId'],
            'chargeAmount': body['chargeAmount'],
            'totalAmount': int(user_wallet['Attributes']['amount'])
        })

        return {
            'statusCode': 202,
            'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
        }


def wallet_use(event, context):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WalletUse')
    queue.send_message(MessageBody=event['body'])
    return {
            'statusCode': 202,
            'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }

def wallet_use_receiver(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])

    for record in event['Records']:
        body = json.loads(record['body'])
        try:
            usage_result = user_wallet_table.update_item(
                ExpressionAttributeNames={
                    '#A': 'amount',
                },
                ExpressionAttributeValues={
                    ':a': body['useAmount'],
                },
                Key={
                    'userId': body['userId'],
                },
                ReturnValues='ALL_NEW',
                UpdateExpression='SET #A = #A - :a',
                ConditionExpression=Attr('amount').gte(0),
            )
            # ConditionalCheckFailed
        except Exception as e:
            print(e)
            return {
                'statusCode': 400,
                'body': json.dumps({'errorMessage': 'There was not enough money.'})
            }

        history_table.put_item(
            Item={
                'walletId': usage_result['Attributes']['walletId'],
                'transactionId': body['transactionId'],
                'useAmount': body['useAmount'],
                'locationId': body['locationId'],
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        # TODO: SQS
        requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
            'transactionId': body['transactionId'],
            'userId': body['userId'],
            'useAmount': body['useAmount'],
            'totalAmount': int(usage_result['Attributes']['amount'])
        })

        return {
            'statusCode': 202,
            'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
        }


def wallet_transfer(event, context):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='WalletTransfer')
    queue.send_message(MessageBody=event['body'])
    return {
            'statusCode': 202,
            'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }

def wallet_transfer_receiver(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])

    for record in event['Records']:
        body = json.loads(record['body'])
        try:
            from_wallet = user_wallet_table.update_item(
                ExpressionAttributeNames={
                    '#A': 'amount',
                },
                ExpressionAttributeValues={
                    ':a': body['transferAmount'],
                },
                Key={
                    'userId': body['fromUserId'],
                },
                ReturnValues='ALL_NEW',
                UpdateExpression='SET #A = #A - :a',
                ConditionExpression=Attr('amount').gte(0),
            )
            # ConditionalCheckFailed
        except Exception as e:
            print(e)
            return {
                'statusCode': 400,
                'body': json.dumps({'errorMessage': 'There was not enough money.'})
            }

    to_wallet = user_wallet_table.update_item(
        ExpressionAttributeNames={
            '#A': 'amount',
        },
        ExpressionAttributeValues={
            ':a': body['transferAmount'],
        },
        Key={
            'userId': body['toUserId'],
        },
        ReturnValues='ALL_NEW',
        UpdateExpression='SET #A = #A + :a',
    )

    history_table.put_item(
        Item={
            'walletId': from_wallet['Attributes']['walletId'],
            'transactionId': body['transactionId'],
            'useAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    history_table.put_item(
        Item={
            'walletId': to_wallet['Attributes']['walletId'],
            'transactionId': body['transactionId'],
            'chargeAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['fromUserId'],
        'useAmount': body['transferAmount'],
        'totalAmount': int(from_wallet['Attributes']['amount']),
        'transferTo': body['toUserId']
    })
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['toUserId'],
        'chargeAmount': body['transferAmount'],
        'totalAmount': int(to_wallet['Attributes']['amount']),
        'transferFrom': body['fromUserId']
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }


def get_user_summary(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    params = event['pathParameters']
    user_wallet = user_wallet_table.get_item(
        Key={'userId': params['userId']}
    )
    payment_history = history_table.query(
        KeyConditionExpression=Key('walletId').eq(user_wallet['Item']['walletId'])
    )
    sum_charge = 0
    sum_payment = 0
    times_per_location = {}
    for item in payment_history['Items']:
        sum_charge += item.get('chargeAmount', 0)
        sum_payment += item.get('useAmount', 0)
        location_name = _get_location_name(item['locationId'])
        if location_name not in times_per_location:
            times_per_location[location_name] = 1
        else:
            times_per_location[location_name] += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'userName': user_wallet['Item']['userName'],
            'currentAmount': int(user_wallet['Item']['amount']),
            'totalChargeAmount': int(sum_charge),
            'totalUseAmount': int(sum_payment),
            'timesPerLocation': times_per_location
        })
    }


def get_payment_history(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    params = event['pathParameters']
    user_wallet = user_wallet_table.get_item(
        Key={'userId': params['userId']}
    )
    payment_history_result = history_table.query(
        KeyConditionExpression=Key('walletId').eq(user_wallet['Item']['walletId'])
    )
    payment_history = []
    for p in payment_history_result['Items']:
        if 'chargeAmount' in p:
            p['chargeAmount'] = int(p['chargeAmount'])
        if 'useAmount' in p:
            p['useAmount'] = int(p['useAmount'])
        p['locationName'] = _get_location_name(p['locationId'])
        del p['locationId']
        payment_history.append(p)

    sorted_payment_history = list(sorted(
        payment_history,
        key=lambda x:x['timestamp'],
        reverse=True))

    return {
        'statusCode': 200,
        'body': json.dumps(sorted_payment_history)
    }


def _get_location_name(location_id):
    locations = requests.get(os.environ['LOCATION_ENDPOINT']).json()
    return locations[str(location_id)]
