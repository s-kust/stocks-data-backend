import json
import os
import pandas as pd
import mplfinance as mpf
import pickle
import boto3
from botocore.exceptions import ClientError

class DataPickleDownloadException(Exception): pass
class ChartUploadException(Exception): pass

BUCKET_NAME_SOURCE = os.environ.get('s3_main_bucket')	
BUCKET_NAME_DEST = os.environ.get('s3_bucket_charts')
REGION_NAME = os.environ.get('region_id')
LOCAL_FOLDER = '/tmp/'
FOLDER_IN_BUCKET = 'charts-public/'
HORIZONTAL_SIZE = 794
VERTICAL_SIZE = 512
MY_PLOTS_DPI = 100 # determined by trial and error
LONG_PERIOD_DAYS = 350
SHORT_PERIOD_DAYS = 50

s3 = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb_resource.Table('CurrentPortfolioRows')

def _upload_file_to_s3(file_name, bucket=BUCKET_NAME_DEST):
    print('Function _upload_file_to_s3 started...')
    object_name = os.path.basename(file_name)
    try:
        _ = s3.upload_file(LOCAL_FOLDER+file_name, BUCKET_NAME_DEST, object_name)
    except Exception as e:
        exception_message = file_name + '- upload to S3 failed, strange, raise Exception'
        print(exception_message)
        raise ChartUploadException(exception_message)

def _prepare_last_date_text(df):
    last_index = df.tail(1).index.item().date()
    last_date_text = 'Last day: ' + str(last_index)
    return last_date_text

def _create_relative_chart(ts_1, ts_2, symbol_1, symbol_2):
    print('Function _create_relative_chart started - ', symbol_1, '-', symbol_2)
    filename = 'rel_' + symbol_1 + "_" + symbol_2 + ".png"
    data = ts_1 / ts_2
    data_relative = data.tail(LONG_PERIOD_DAYS)
    last_date_text = _prepare_last_date_text(data_relative)
    title_relative = "Rel " + symbol_1 + "-" + symbol_2 + ", " + last_date_text
    mpf.plot(data_relative, type="line", style="yahoo", title=title_relative, 
            figsize=(HORIZONTAL_SIZE / MY_PLOTS_DPI, VERTICAL_SIZE / MY_PLOTS_DPI), savefig=LOCAL_FOLDER+filename, )
    _upload_file_to_s3(filename)
    print('Function _create_relative_chart end - OK')
    return filename

def _create_line_chart(ts, symbol):
    print('Function _create_line_chart started - ', symbol)
    filename = "long_period_" + symbol + ".png"
    data_line = ts.tail(LONG_PERIOD_DAYS)
    last_date_text = _prepare_last_date_text(data_line)
    title_line = symbol + " last 1.5 years, " + last_date_text
    mpf.plot(data_line, type="line", style="yahoo", title=title_line, 
            figsize=(HORIZONTAL_SIZE / MY_PLOTS_DPI, VERTICAL_SIZE / MY_PLOTS_DPI), savefig=LOCAL_FOLDER+filename, )
    _upload_file_to_s3(filename)
    print('Function _create_line_chart end - OK')
    return filename

def _create_candlestick_chart(ts, symbol):
    print('Function _create_candlestick_chart started - ', symbol)
    filename = "short_period_" + symbol + ".png"
    data = ts.tail(SHORT_PERIOD_DAYS)
    last_date_text = _prepare_last_date_text(data)
    plot_title = symbol + " 2.5 months, " + last_date_text
    mpf.plot(data, type="candle", style="yahoo", title=plot_title, \
                volume=True, figsize=(HORIZONTAL_SIZE / MY_PLOTS_DPI, VERTICAL_SIZE / MY_PLOTS_DPI), savefig=LOCAL_FOLDER+filename, )
    _upload_file_to_s3(filename)
    print('Function _create_candlestick_chart end - OK')
    return filename        

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
            
def process_stocks_relative_two(event_internal):
    print("Function process_stocks_relative_two started - ", event_internal['ticker_1'], "-", event_internal['ticker_2'])
    data_1 = _download_pickle_data_from_s3(event_internal['file_1'])
    data_2 = _download_pickle_data_from_s3(event_internal['file_2'])
    chart_filename = _create_relative_chart(data_1, data_2, event_internal['ticker_1'], event_internal['ticker_2'])
    response_dict = {
        'type': event_internal['type'], 
        'note': event_internal['note'], 
        'ticker_1': event_internal['ticker_1'], 
        'ticker_2': event_internal['ticker_2'], 
        'chart_filename': chart_filename,
        'api_call_count': event_internal['api_call_count']
        }
    # now update successfully processed rows table 
    # and return response_dict
    item_to_put = {
            'ticker_combined': event_internal['ticker_1'] + "-" + event_internal['ticker_2'],
            'type': "Stocks relative",
            'note': event_internal['note'],
            'file_1': chart_filename,
            'file_2': ""
        }
    _ = table.put_item(Item=item_to_put)    
    return response_dict

def _create_charts_line_candle(data_df, ticker_id):
    chart_filename_line = _create_line_chart(data_df, ticker_id)
    chart_filename_candle = _create_candlestick_chart(data_df, ticker_id)
    return chart_filename_line, chart_filename_candle

def process_stocks_single(event_internal):
    print("Function process_stocks_single started - ", event_internal['ticker_1'])
    data_1 = _download_pickle_data_from_s3(event_internal['file_1'])
    filename_line, filename_candle = _create_charts_line_candle(data_1, event_internal['ticker_1'])
    response_dict = {
        'type': event_internal['type'], 
        'note': event_internal['note'], 
        'ticker': event_internal['ticker_1'], 
        'filename_line': filename_line, 
        'filename_candle': filename_candle,
        'api_call_count': event_internal['api_call_count']
        }
    item_to_put = {
                'ticker_combined': event_internal['ticker_1'],
                'type': "Stocks single",
                'note': event_internal['note'],
                'file_1': filename_line,
                'file_2': filename_candle
            }
    _ = table.put_item(Item=item_to_put)
    return response_dict

def process_fx_row(event_internal):
    fx_ticker_id = event_internal['ticker_1'] + '-' + event_internal['ticker_2']
    print("Function process_fx_row started - ", fx_ticker_id)
    data_1 = _download_pickle_data_from_s3(event_internal['file_1'])
    filename_line, filename_candle = _create_charts_line_candle(data_1, fx_ticker_id)
    response_dict = {
        'type': event_internal['type'], 
        'note': event_internal['note'], 
        'ticker': fx_ticker_id, 
        'filename_line': filename_line, 
        'filename_candle': filename_candle,
        'api_call_count': event_internal['api_call_count']
        }
    item_to_put = {
                'ticker_combined': fx_ticker_id,
                'type': "Currency pair",
                'note': event_internal['note'],
                'file_1': filename_line,
                'file_2': filename_candle
            }
    _ = table.put_item(Item=item_to_put)
    return response_dict
    
def lambda_handler(event, context):
    # print("Incoming event:")
    # print(event)
    response_dict = None
    if event['type'] == 'Stocks_relative_two':
        response_dict = process_stocks_relative_two(event)
    if event['type'] == 'FX':
        response_dict = process_fx_row(event)
    if event['type'] == 'Stocks_single':
        response_dict = process_stocks_single(event)
    if response_dict is None:
        raise TypeError("Wrong portfolio row type")
    return {
        'statusCode': 200,
        'body': response_dict
    }