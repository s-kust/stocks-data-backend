import json
import os.path
import boto3
import email
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication

BUCKET_NAME = "charts-public"
LOCAL_FOLDER = "/tmp/"
EMAIL_FROM = "send2kust@gmail.com"
EMAIL_TO = "send2kust@gmail.com"

s3 = boto3.client("s3")
ses_client = boto3.client('ses',region_name="us-east-1")

class DataStore:
    def __init__(self, subject_text, note_text, from_text=EMAIL_FROM, to_text=EMAIL_TO):
        self.subject = subject_text
        self.note = note_text
        self.from_text = from_text
        self.to_text = to_text

def _add_image_to_msg_root(message_root, image_path, header):
    image_add_success_indicator = False
    try:
        file_bytes = open(image_path, "rb")
        msg_image = MIMEImage(file_bytes.read())
        msg_image.add_header("Content-ID", header)
        message_root.attach(msg_image)
        file_bytes.close()
    except Exception as exception_msg:
        print(exception_msg)
        print("Problem with file", image_path)
    else:
        image_add_success_indicator = True
    return message_root, image_add_success_indicator

def _create_data_for_email(data_store_object, filename_1, filename_2=None):
    msg_root = MIMEMultipart("related")
    msg_root["Subject"] = data_store_object.subject
    msg_root["From"] = data_store_object.from_text
    msg_root["To"] = data_store_object.to_text
    msg_root.preamble = "This is a multi-part message in MIME format, with pictures."

    # Encapsulate the plain and HTML versions of the message body in an
    # 'alternative' part, so message agents can decide which they want to display.
    msg_alternative = MIMEMultipart("alternative")
    msg_root.attach(msg_alternative)

    image_1_add_success = False
    image_2_add_success = False
    if filename_2 is not None:
        msg_root, image_2_add_success = _add_image_to_msg_root(
            msg_root, filename_2, "<image2>")
    if filename_1 is not None:
        msg_root, image_1_add_success = _add_image_to_msg_root(
            msg_root, filename_1, "<image1>")

    msg_text = MIMEText('{0}<br>Have a nice day!'.format(data_store_object.note), "html")
    if image_1_add_success and not image_2_add_success:
        msg_text = MIMEText('<img src="cid:image1"><br>{0}<br>Have a nice day!'.format(data_store_object.note), "html")
    if image_1_add_success and image_2_add_success:
        msg_text = MIMEText(
            '<img src="cid:image1"><br><img src="cid:image2"><br>{0}<br>Bye!'.format(data_store_object.note), "html")
    plain_text_msg = MIMEText("See the charts attaced to this e-mail. Have a nice day!", _charset='utf-8')
    msg_alternative.attach(plain_text_msg)
    msg_alternative.attach(msg_text)
    return msg_root

def lambda_handler(event, context):
    try:
        if event['type'] == "Stocks_relative_two":
            subject_str = "Relative " + event['ticker_1'] + "-" + event['ticker_2']
            local_file_name = LOCAL_FOLDER + event['chart_filename']
            s3.download_file(BUCKET_NAME, event['chart_filename'], local_file_name)
            mail_imputs_data_store = DataStore(subject_str, event['note'])
            msg = _create_data_for_email(mail_imputs_data_store, local_file_name)
        if (event['type'] == "Stocks_single") or (event['type'] == "FX"):
            subject_str = "Idea: " + event['ticker']
            local_line_plot_file_name = LOCAL_FOLDER + event['filename_line']
            s3.download_file(BUCKET_NAME, event['filename_line'], local_line_plot_file_name)
            local_candle_chart_file_name = LOCAL_FOLDER + event['filename_candle']
            s3.download_file(BUCKET_NAME, event['filename_candle'], local_candle_chart_file_name)
            mail_imputs_data_store = DataStore(subject_str, event['note'])
            msg = _create_data_for_email(mail_imputs_data_store, local_line_plot_file_name, local_candle_chart_file_name)
    except Exception as e:
        print("Exception occured during data preparation for sending e-mail:")
        print(e)        
        print('event: ', event)
        raise e
    try:
        response = ses_client.send_raw_email(
            Source=mail_imputs_data_store.from_text,
            Destinations=[
                mail_imputs_data_store.to_text
            ],
            RawMessage={
                'Data':msg.as_string(),
            },
        )
    # Display an error if something goes wrong. 
    except ClientError as e:
        print(e.response['Error']['Message'])
        print('event: ', event)
        raise e
    except Exception as e:
        print("Other exception occured during e-mail sending:")
        print(e)        
        print('event: ', event)
        raise e
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    
    return {
        'statusCode': 200,
        'body': {'email_message_id': response['MessageId']}
    }
    