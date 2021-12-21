import json
import os
import pandas as pd
import mplfinance as mpf
import pickle
import boto3
from botocore.exceptions import ClientError

class DataPickleDownloadException(Exception): pass
class ChartUploadException(Exception): pass

BUCKET_NAME_SOURCE = 'kust-pics'
BUCKET_NAME_DEST = 'charts-public'
LOCAL_FOLDER = '/tmp/'
FOLDER_IN_BUCKET = 'charts-public/'
HORIZONTAL_SIZE = 794
VERTICAL_SIZE = 512
MY_PLOTS_DPI = 100 # determined by trial and error
LONG_PERIOD_DAYS = 350
SHORT_PERIOD_DAYS = 50

s3 = boto3.client('s3')

def _prepare_last_date_text(df):
    last_index = df.tail(1).index.item().date()
    last_date_text = 'Last day: ' + str(last_index)
    return last_date_text

def _create_relative_chart(ts_1, ts_2, symbol_1, symbol_2):
    print('Function _create_relative_chart started...')
    filename = 'rel_' + symbol_1 + "_" + symbol_2 + ".png"
    data = ts_1 / ts_2
    data = data.tail(LONG_PERIOD_DAYS)
    last_date_text = _prepare_last_date_text(data)
    plot_title = "Rel " + symbol_1 + "-" + symbol_2 + ", " + last_date_text
    mpf.plot(data, type="line", style="yahoo", title=plot_title, 
            figsize=(HORIZONTAL_SIZE / MY_PLOTS_DPI, VERTICAL_SIZE / MY_PLOTS_DPI), savefig=LOCAL_FOLDER+filename, )
    print('Function _create_relative_chart end - OK')
    return filename

def _create_line_chart(ts, symbol):
    print('Function _create_line_chart started...')
    filename = "long_period_" + symbol + ".png"
    data = ts.tail(LONG_PERIOD_DAYS)
    last_date_text = _prepare_last_date_text(data)
    plot_title = symbol + " last 1.5 years, " + last_date_text
    mpf.plot(data, type="line", style="yahoo", title=plot_title, 
            figsize=(HORIZONTAL_SIZE / MY_PLOTS_DPI, VERTICAL_SIZE / MY_PLOTS_DPI), savefig=LOCAL_FOLDER+filename, )
    print('Function _create_line_chart end - OK')
    return filename

def _create_candlestick_chart(ts, symbol):
    print('Function _create_candlestick_chart started...')
    filename = "short_period_" + symbol + ".png"
    data = ts.tail(SHORT_PERIOD_DAYS)
    last_date_text = _prepare_last_date_text(data)
    plot_title = symbol + " 2.5 months, " + last_date_text
    mpf.plot(data, type="candle", style="yahoo", title=plot_title, \
                volume=True, figsize=(HORIZONTAL_SIZE / MY_PLOTS_DPI, VERTICAL_SIZE / MY_PLOTS_DPI), savefig=LOCAL_FOLDER+filename, )
    print('Function _create_candlestick_chart end - OK')
    return filename
    
    
def _upload_file_to_s3(file_name, bucket=BUCKET_NAME_DEST):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :return: True if file was uploaded, else False
    """
    print('Function _upload_file_to_s3 started...')
    # object_name = FOLDER_IN_BUCKET + os.path.basename(file_name)
    object_name = os.path.basename(file_name)
    try:
        response = s3.upload_file(LOCAL_FOLDER+file_name, BUCKET_NAME_DEST, object_name)
        # response = s3.put_object(ACL='public-read', Body=LOCAL_FOLDER+file_name, Bucket=BUCKET_NAME_DEST, Key=object_name, )
    except ClientError as e:
        print(e)
        return False
    print('Function _upload_file_to_s3 finished - OK')        
    return True

def _download_pickle_data_from_s3(file_name):
    try:
        s3.download_file(BUCKET_NAME_SOURCE, file_name, LOCAL_FOLDER+file_name)
        with open(LOCAL_FOLDER+file_name, "rb") as input_file:
            data = pickle.load(input_file)
    except Exception as e:
        print(file_name, " - failed to download from S3. Strange error, so raise exception, terminate.")
        print(e)
        raise e
    if not isinstance(data, pd.DataFrame):
        exception_message = file_name + '- is not a pandas DataFrame, strange, raise Exception'
        print(exception_message)
        raise DataPickleDownloadException(exception_message)
    if not all(item in data.columns for item in ['Open','High','Low','Close']):
        exception_message = file_name + '- not all required OHLC columns found, strange, raise Exception'
        print(exception_message)
        raise DataPickleDownloadException(exception_message)
    print(file_name, ' - downloaded, passed checks for pd.Dataframe, OHLC columns')
    return data
            
def lambda_handler(event, context):
    data_1 = _download_pickle_data_from_s3(event['file_1'])
    if event['type'] == 'Stocks_relative_two':
        data_2 = _download_pickle_data_from_s3(event['file_2'])
        chart_filename = _create_relative_chart(data_1, data_2, event['ticker_1'], event['ticker_2'])
        upload_result = _upload_file_to_s3(chart_filename)
        if not upload_result:
            exception_message = chart_filename + '- upload to S3 failed, strange, raise Exception'
            print(exception_message)
            raise ChartUploadException(exception_message)
        response_dict = {'type': event['type'], 'note': event['note'], 'ticker_1': event['ticker_1'], 'ticker_2': event['ticker_2'], 'chart_filename': chart_filename }
        response_dict['api_call_count'] = event['api_call_count']
        return {
        'statusCode': 200,
        'body': response_dict
    }
    if event['type'] == 'FX':
        ticker_single_ts = event['ticker_1'] + '_' + event['ticker_2']
    else:
        ticker_single_ts = event['ticker_1']
    chart_filename_line = _create_line_chart(data_1, ticker_single_ts)
    upload_result = _upload_file_to_s3(chart_filename_line)
    if not upload_result:
        exception_message = chart_filename_line + '- upload to S3 failed, strange, raise Exception'
        print(exception_message)
        raise ChartUploadException(exception_message)
    chart_filename_candle = _create_candlestick_chart(data_1, ticker_single_ts)
    upload_result = _upload_file_to_s3(chart_filename_candle)
    if not upload_result:
        exception_message = chart_filename_candle + '- upload to S3 failed, strange, raise Exception'
        print(exception_message)
        raise ChartUploadException(exception_message)
    try:
        print('event_file_2 - ', event['file_2'])
    except KeyError:
        print('event_file_2 - NO')
    response_dict = {'type': event['type'], 'note': event['note'], 'ticker': ticker_single_ts, 'filename_line': chart_filename_line, 'filename_candle': chart_filename_candle}
    response_dict['api_call_count'] = event['api_call_count']
    return {
        'statusCode': 200,
        'body': response_dict
    }