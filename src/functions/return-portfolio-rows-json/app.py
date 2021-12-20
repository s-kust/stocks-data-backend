import json
import pickle
import boto3

BUCKET_NAME = 'kust-pics'
CURRENT_PORTFOLIO_ROWS_FILE = 'current_portfolio_rows.pkl'

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    try:
        s3.download_file(BUCKET_NAME, CURRENT_PORTFOLIO_ROWS_FILE, '/tmp/'+CURRENT_PORTFOLIO_ROWS_FILE)
    except Exception as e:
        print('Problem with downloading CURRENT_PORTFOLIO_ROWS_FILE from S3, terminate execution')
        raise e
    
    with open('/tmp/'+CURRENT_PORTFOLIO_ROWS_FILE, "rb") as input_file:
        portfolio_rows = pickle.load(input_file)
    if isinstance(portfolio_rows, list):
        print("SUCCESS_IMPORTS list downloaded from S3 - OK")
    else:
        raise TypeError("SUCCESS_IMPORTS_FILE must contain a list of dictionaries")
    
    return {
        'statusCode': 200,
        'body': json.dumps(portfolio_rows)
    }