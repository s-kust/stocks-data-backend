import json
import boto3
from botocore.exceptions import ClientError
import os

SENDER = os.environ.get('email_from')	
RECIPIENT = os.environ.get('email_to')	
REGION_ID = os.environ.get('region_id')	
CHARSET = "UTF-8"

ses_client = boto3.client('ses',region_name=REGION_ID)

def lambda_handler(event, context):
    # print("Incoming event:")
    # print(event)
    if event['failed_ticker']:
        email_subject = "Trading App: problem with ticker " + event['failed_ticker']
        body_html = "<html><head></head><body><p>Trading App: problem with ticker - %s</p></body></html>" %  (event['failed_ticker'])
        body_text = "Trading App: problem with ticker: %s" %  (event['failed_ticker'])
    else:
        email_subject = "Trading App: problem "
        body_html = "<html><head></head><body><p>Trading App: problem, failed_ticker not specified</p><p>Execution Id - %s</p>" % (event['Payload']['executionId'])
        body_html = body_html + "<p>%s</p></body></html>" %  (event['Payload']['Payload'])
        body_text = "Trading App: problem, failed_ticker not specified. Execution Id - %s." %  (event['Payload']['executionId'])
        body_text = body_text + "Event Payload - %s." %  (event['Payload']['Payload'])
    
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
        print("Failed to send e-mail with problem notification!")
        print(e.response['Error']['Message'])
        raise e
    else:
        print("Email sent! Message ID: ", response['MessageId'])
                
    return {
        'statusCode': 200,
        'body': {'email_message_id': response['MessageId']}
    }
