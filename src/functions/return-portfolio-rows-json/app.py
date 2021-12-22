import json
import boto3
import os

REGION_NAME = os.environ.get('region_id')

dynamodb_resource = boto3.resource('dynamodb',region_name=REGION_NAME)

def lambda_handler(event, context):

    table = dynamodb_resource.Table('CurrentPortfolioRows')
    scan = table.scan(ConsistentRead=True)

    return {
        'statusCode': 200,
        'body': json.dumps(scan['Items'])
    }