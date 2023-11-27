import json
import datetime
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

def upsert_item(user_key,date_time_str,question,answer):
    item_object = {}
    item_object['userKey'] = user_key
    item_object['dateTime'] = date_time_str
    item_object['question'] = question
    item_object['answer'] = answer
    dynamoDb = boto3.resource('dynamodb')
    table = dynamoDb.Table('Video-000200-UserConversation')
    response = table.put_item( Item=item_object )
    return response

def get_item(user_key,date_time_str):
    dynamoDb = boto3.resource("dynamodb")
    table = dynamoDb.Table('Video-000200-UserConversation')
    response = table.get_item( Key={ "userKey": user_key, "dateTime": date_time_str } )
    item_object = response['Item']
    return item_object

def delete_item(user_key,date_time_str):
    dynamoDb = boto3.resource("dynamodb")
    table = dynamoDb.Table('Video-000200-UserConversation')
    response = table.delete_item( Key={ "userKey": user_key, "dateTime": date_time_str } )
    return response

def query_items(user_key):
    dynamoDb = boto3.resource('dynamodb')
    table = dynamoDb.Table('Video-000200-UserConversation')
    response = table.query(
        KeyConditionExpression=Key('userKey').eq(user_key)
    )
    return response['Items']

def scan_items():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Video-000200-UserConversation')
    response = table.scan()
    results = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        results.extend(response['Items'])
    return results

def lambda_handler(event, context):
    # Constants
    USER_KEY = "demoUser1"
    
    # Get now
    date_time = datetime.datetime.now()
    date_time_str = date_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Insert new record
    response = upsert_item(USER_KEY,date_time_str,'What is your favorite season?','Fall is best')
    print( "UPSERT_ITEM RESPONSE--->" + str(response) )
    
    # Get record
    item_object = get_item(USER_KEY,date_time_str)
    print( "ITEM_OBJECT--->" + str(item_object) )
    
    # Update existing record
    response = upsert_item(USER_KEY,date_time_str,'What is your favorite season?','Spring is best')
    print( "UPSERT_ITEM RESPONSE--->" + str(response) )
    
    # Delete record
    response = delete_item(USER_KEY,date_time_str)
    print( "DELETE_ITEM RESPONSE--->" + str(response) )
    
    # Query records within a partition
    results = query_items(USER_KEY)
    for item in results:
        print( "QUERY: ITEM_OBJECT--->" + str(item) )
    
    # Scan records in table
    results = scan_items()
    for item in results:
        print( "SCAN: ITEM_OBJECT--->" + str(item) )
    
    # Return success
    return {
        'statusCode': 200,
        'body': json.dumps('Successful Lambda Execution!')
    }