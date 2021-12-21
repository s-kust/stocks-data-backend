import json
import boto3
from botocore.exceptions import ClientError

ses_client = boto3.client('ses',region_name="us-east-1")
SENDER = "send2kust@gmail.com"
RECIPIENT = "send2kust@gmail.com"
CHARSET = "UTF-8"

def lambda_handler(event, context):
    
    try:
        email_subject = "Trading App: problem with ticker " + event['failed_ticker']
        body_html = "<html><head></head><body><p>Trading App: problem with ticker - %s</p></body></html>" %  (event['failed_ticker'])
        body_text = "Trading App: problem with ticker: %s" %  (event['failed_ticker'])
    except Exception as e:
        email_subject = "Trading App: problem "
        body_html = "<html><head></head><body><p>Trading App: problem, failed_ticker not specified</p><p>Execution Id - %s</p>" % (event['executionId'])
        body_html = body_html + "<p>%s</p></body></html>" %  (event['Payload'])
        body_text = "Trading App: problem, failed_ticker not specified. Execution Id - %s." %  (event['executionId'])
        body_text = body_text + "Event Payload - %s." %  (event['Payload'])
    
    try:
        response = ses_client.send_email(
            Destination={'ToAddresses': [RECIPIENT,],},
            Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': body_html,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': body_text,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': email_subject,
            },
        },
        Source=SENDER,
    )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID: ", response['MessageId'])
                
    return {
        'statusCode': 200,
        'body': {'email_message_id': response['MessageId']}
    }
