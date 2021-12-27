import json
import boto3
import os

REGION_NAME = os.environ.get('region_id')

dynamodb_resource = boto3.resource('dynamodb',region_name=REGION_NAME)

def lambda_handler(event, context):

    try:
        table = dynamodb_resource.Table('CurrentPortfolioRows')
        scan = table.scan(ConsistentRead=True)
    except Exception as e:
        return {
            'statusCode': 400,
            'body': str(e)
        }

    return {
        'statusCode': 200,
        'body': json.dumps(scan['Items'])
    }