import json
import os
import uuid
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key
import requests


def user_create(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    # user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    # wallet_table = boto3.resource('dynamodb').Table(os.environ['WALLET_TABLE'])
    body = json.loads(event['body'])
    user_wallet_table.put_item(
        Item={
            'userId': body['id'],
            'userName': body['name'],
            'walletId': str(uuid.uuid4()),
            # 'userId': body['id'],
            'amount': 0,
        }
    )
    # wallet_table.put_item(
    #     Item={
    #         'id': str(uuid.uuid4()),
    #         'userId': body['id'],
    #         'amount': 0
    #     }
    # )
    return {
        'statusCode': 200,
        'body': json.dumps({'result': 'ok'})
    }


def wallet_charge(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    # wallet_table = boto3.resource('dynamodb').Table(os.environ['WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    body = json.loads(event['body'])
    user_wallet = user_wallet_table.get_item(
        Key={'userId': body['userId']}
    )
    # result = wallet_table.scan(
    #     ScanFilter={
    #         'userId': {
    #             'AttributeValueList': [
    #                 body['userId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # )
    # user_wallet = result['Items'].pop()
    total_amount = user_wallet['Item']['amount'] + body['chargeAmount']
    user_wallet_table.update_item(
        Key={
            'userId': user_wallet['Item']['walletId']
        },
        AttributeUpdates={
            'amount': {
                'Value': total_amount,
                'Action': 'PUT'
            }
        }
    )
    history_table.put_item(
        Item={
            'walletId': user_wallet['Item']['walletId'],
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
        'totalAmount': int(total_amount)
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }


def wallet_use(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    # wallet_table = boto3.resource('dynamodb').Table(os.environ['WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    body = json.loads(event['body'])
    user_wallet = user_wallet_table.get_item(
        Key={'userId': body['userId']}
    )
    # result = wallet_table.scan(
    #     ScanFilter={
    #         'userId': {
    #             'AttributeValueList': [
    #                 body['userId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # )
    # user_wallet = result['Items'].pop()
    total_amount = user_wallet['Item']['amount'] - body['useAmount']
    if total_amount < 0:
        return {
            'statusCode': 400,
            'body': json.dumps({'errorMessage': 'There was not enough money.'})
        }

    user_wallet_table.update_item(
        Key={
            'userId': user_wallet['Item']['walletId']
        },
        AttributeUpdates={
            'amount': {
                'Value': total_amount,
                'Action': 'PUT'
            }
        }
    )
    history_table.put_item(
        Item={
            'walletId': user_wallet['Item']['walletId'],
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
        'totalAmount': int(total_amount)
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }


def wallet_transfer(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    # wallet_table = boto3.resource('dynamodb').Table(os.environ['WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    body = json.loads(event['body'])
    # from_wallet = user_wallet_table.get_item(
    #     Key={'userId': body['fromUserId']}
    # )
    # from_wallet = wallet_table.scan(
    #     ScanFilter={
    #         'userId': {
    #             'AttributeValueList': [
    #                 body['fromUserId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # ).get('Items').pop()
    # to_wallet = wallet_table.scan(
    #     ScanFilter={
    #         'userId': {
    #             'AttributeValueList': [
    #                 body['toUserId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # ).get('Items').pop()



    try:
        from_wallet = user_wallet_table.update_item(
            ExpressionAttributeNames={
                '#A': 'amount',
            },
            ExpressionAttributeValues={
                ':a': {
                    'N': body['transferAmount'],
                },
            },
            Key={
                'userId': {
                    'S': body['fromUserId'],
                },
            },
            ReturnValues='ALL_NEW',
            # TableName=user_wallet_table,
            UpdateExpression='SET #A = #A - :a',
            ConditionExpression=Attr('amount').ge(0),
        )
        # ConditionalCheckFailed
    except Exception:
        return {
            'statusCode': 400,
            'body': json.dumps({'errorMessage': 'There was not enough money.'})
        }

    to_wallet = user_wallet_table.update_item(
        ExpressionAttributeNames={
            '#A': 'amount',
        },
        ExpressionAttributeValues={
            ':a': {
                'N': body['transferAmount'],
            },
        },
        Key={
            'userId': {
                'S': body['toUserId'],
            },
        },
        ReturnValues='ALL_NEW',
        # TableName=user_wallet_table,
        UpdateExpression='SET #A = #A + :a',
    )


    # to_wallet = user_wallet_table.get_item(
    #     Key={'userId': body['toUserId']}
    # )

    # from_total_amount = from_wallet['Item']['amount'] - body['transferAmount']
    # to_total_amount = from_wallet['Item']['amount'] + body['transferAmount']
    # if from_total_amount < 0:
    #     return {
    #         'statusCode': 400,
    #         'body': json.dumps({'errorMessage': 'There was not enough money.'})
    #     }

    # user_wallet_table.update_item(
    #     Key={
    #         'userId': from_wallet['Item']['walletId']
    #     },
    #     AttributeUpdates={
    #         'amount': {
    #             'Value': from_total_amount,
    #             'Action': 'PUT'
    #         }
    #     }
    # )
    # user_wallet_table.update_item(
    #     Key={
    #         'userId': to_wallet['Item']['walletId']
    #     },
    #     AttributeUpdates={
    #         'amount': {
    #             'Value': to_total_amount,
    #             'Action': 'PUT'
    #         }
    #     }
    # )
    history_table.put_item(
        Item={
            'walletId': from_wallet['Item']['walletId'],
            'transactionId': body['transactionId'],
            'useAmount': body['transferAmount'],
            'locationId': body['locationId'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    history_table.put_item(
        Item={
            'walletId': from_wallet['Item']['walletId'],
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
        'totalAmount': int(from_wallet['Item']['amount']),
        'transferTo': body['toUserId']
    })
    requests.post(os.environ['NOTIFICATION_ENDPOINT'], json={
        'transactionId': body['transactionId'],
        'userId': body['toUserId'],
        'chargeAmount': body['transferAmount'],
        'totalAmount': int(to_wallet['Item']['amount']),
        'transferFrom': body['fromUserId']
    })

    return {
        'statusCode': 202,
        'body': json.dumps({'result': 'Accepted. Please wait for the notification.'})
    }


def get_user_summary(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    # wallet_table = boto3.resource('dynamodb').Table(os.environ['WALLET_TABLE'])
    # user_table = boto3.resource('dynamodb').Table(os.environ['USER_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    params = event['pathParameters']
    user_wallet = user_wallet_table.get_item(
        Key={'userId': params['userId']}
    )
    # wallet = wallet_table.scan(
    #     ScanFilter={
    #         'userId': {
    #             'AttributeValueList': [
    #                 params['userId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # ).get('Items').pop()
    payment_history = history_table.query(
        KeyConditionExpression=Key('userId').eq(params['userId'])
    )
    # payment_history = history_table.scan(
    #     ScanFilter={
    #         'walletId': {
    #             'AttributeValueList': [
    #                 user_wallet['Item']['walletId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # )
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
            # 'userName': user['Item']['name'],
            'userName': user_wallet['Item']['userName'],
            'currentAmount': int(user_wallet['Item']['amount']),
            'totalChargeAmount': int(sum_charge),
            'totalUseAmount': int(sum_payment),
            'timesPerLocation': times_per_location
        })
    }


def get_payment_history(event, context):
    user_wallet_table = boto3.resource('dynamodb').Table(os.environ['USER_WALLET_TABLE'])
    # wallet_table = boto3.resource('dynamodb').Table(os.environ['WALLET_TABLE'])
    history_table = boto3.resource('dynamodb').Table(os.environ['PAYMENT_HISTORY_TABLE'])
    params = event['pathParameters']
    # wallet = wallet_table.scan(
    #     ScanFilter={
    #         'userId': {
    #             'AttributeValueList': [
    #                 params['userId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # ).get('Items').pop()
    # user_wallet = user_wallet_table.get_item(
    #     Key={'userId': params['userId']}
    # )
    payment_history_result = history_table.query(
        KeyConditionExpression=Key('userId').eq(params['userId'])
    )
    # payment_history_result = history_table.scan(
    #     ScanFilter={
    #         'walletId': {
    #             'AttributeValueList': [
    #                 user_wallet['Item']['walletId']
    #             ],
    #             'ComparisonOperator': 'EQ'
    #         }
    #     }
    # )

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
