import json
import pandas as pd
from datetime import datetime as dt, timedelta, timezone
import pickle
import boto3
import os

BUCKET_NAME = os.environ.get('s3_main_bucket')		  
FAILED_IMPORTS_FILE_REMOTE = 'failed_imports.pkl'
FAILED_IMPORTS_FILE_LOCAL = '/tmp/' + FAILED_IMPORTS_FILE_REMOTE
SECRET_NAME = os.environ.get('secret_api')		  
REGION_NAME = os.environ.get('region_id')		  

class FileUploadFailureException(Exception): pass

s3 = boto3.client('s3')
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=REGION_NAME
)
get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)
for key in secret:
    secret[key] = secret[key].replace('\\n', '\n')
alpha_vantage_api_key = secret['alpha_vantage_api_key']   
start_date = dt.now() - timedelta(days=1.5 * 365)

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True

def _check_fresh_file_in_bucket(file_key, max_age_minutes=10):
    print('Trying to find the ', file_key, ' file in the S3 bucked and check if it is fresh or outdated')
    try:
        res = s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
    except Exception as e:
        print(file_key, ' file not found in the S3 bucked')
        return False
    file_age =  dt.now(timezone.utc) - res['LastModified']
    if (file_age < timedelta(minutes=max_age_minutes)):
        return True
    else:
        print(file_key, ' file found in the S3 buckedt, but outdated')
        return False

def _prepare_stock_ticker_import_url(stock_ticker):
    url_backbone = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&outputsize=full&datatype=csv&apikey='
    symbol_backbone = '&symbol='
    return(url_backbone + alpha_vantage_api_key + symbol_backbone + stock_ticker)
        
    
def _import_preprocess_ticker_data(alpha_vantage_url):
    print('Starting _import_preprocess_ticker_data function...')
    try:
        data = pd.read_csv(alpha_vantage_url, index_col='timestamp', parse_dates=True)
        if not all(item in data.columns for item in ['open','high','low','close']):
            return False
    except Exception as e:
        print ('Problem with pd.read_csv(alpha_vantage_url) operation')
        print('alpha_vantage_url - ', alpha_vantage_url)
        print(e)
        return False
    try:    
        data.sort_values(by=['timestamp'], inplace=True, ascending=True)
        data = data.loc[start_date:]
        data.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
        data = data.drop(['dividend_amount', 'split_coefficient'], axis=1, errors='ignore')
    except Exception as e:
        print ('Import from AV - OK, but problem with data preprocessing')
        print(e)
        return False
    print('function _import_preprocess_ticker_data executed OK')
    return data

def _upload_pickle_file(data_to_upload, file_name_key):  
    print('Starting _upload_pickle_file function - ', file_name_key)
    try:
        with open('/tmp/'+file_name_key, "wb") as output_file:
            pickle.dump(data_to_upload, output_file)
    except Exception as e:
        print ('Problem with data pickle dump')
        print(e)
        return False
    try:
        upload_result = upload_file('/tmp/'+file_name_key, BUCKET_NAME, file_name_key)
    except Exception as e:
        print ('Problem with uploading data fiile to S3 bucket')
        print(e)
        print('file_name_key - ', file_name_key)
        return False
    print('function _upload_pickle_file executed OK')
    return upload_result

def _import_stock_ticker_data(ticker_code, failed_imports_list, ticker_file_name):
    print ('Fresh ', ticker_code, ' data file NOT found in our S3, so now import it')
    data_import_url = _prepare_stock_ticker_import_url(ticker_code)
    imported_data = _import_preprocess_ticker_data(data_import_url)
    if not isinstance(imported_data, pd.DataFrame):
        print(ticker_code, ' import from AV failed!')
        failed_imports_list.append(ticker_code)
        print('failed_imports -', failed_imports_list)
        upload_res = _upload_pickle_file(failed_imports_list, FAILED_IMPORTS_FILE_REMOTE)
        if not upload_res:
            raise FileUploadFailureException(FAILED_IMPORTS_FILE_REMOTE, ' - failed to upload to S3')
        return False
    imported_data.rename(columns={'volume': 'Volume'}, inplace=True)
    upload_res = _upload_pickle_file(imported_data, ticker_file_name)
    if not upload_res:
        raise FileUploadFailureException(ticker_file_name, ' - failed to upload to S3')
    return True
    
def _process_single_stock_ticker(ticker_code, failed_imports_list):
    ticker_file_name = 'data-daily-' + ticker_code + '.pkl'
    print('ticker_file_name - ', ticker_file_name)
    if ticker_code in failed_imports_list:
        print(ticker_code, ' found in failed_imports list, so process as import failure...')
        return False, ticker_file_name, 0
    fresh_ticker_file_found = _check_fresh_file_in_bucket(ticker_file_name)
    if fresh_ticker_file_found:
        print ('Fresh ', ticker_code, ' data file found in our S3, no need to import again')
        return True, ticker_file_name, 0
    else:
        import_res = _import_stock_ticker_data(ticker_code, failed_imports_list, ticker_file_name)
        return import_res, ticker_file_name, 1

def lambda_handler(event, context):    
    # print('Incoming event:')
    # print(event)    
    failed_imports_file_fresh = _check_fresh_file_in_bucket(FAILED_IMPORTS_FILE_REMOTE)
    if failed_imports_file_fresh:
        try:
            s3.download_file(BUCKET_NAME, FAILED_IMPORTS_FILE_REMOTE, '/tmp/'+FAILED_IMPORTS_FILE_REMOTE)
            with open('/tmp/'+FAILED_IMPORTS_FILE_REMOTE, "rb") as input_file:
                failed_imports = pickle.load(input_file)
            if isinstance(failed_imports, list):
                print("failed_imports list downloaded from S3 - OK")
            else:
                print("failed_imports list downloaded from S3, but not list, so now I reset it as empty list")
                failed_imports = []
        except Exception as e:
            print("The fresh file failed_imports found in S3 bucket, but can't download it. Strange error, so terminate the function.")
            print(e)
            raise e
    else:
        failed_imports = []
        
    if event['Type'] == 'Forex':
        print('row - Forex')
        combined_ticker_fx = event['Ticker1'] + '-' + event['Ticker2']
        if combined_ticker_fx in failed_imports:
            print(combined_ticker_fx, ' found in failed_imports list, so process as import failure')
            response_dict = {'import_success': False, 'failed_ticker': combined_ticker_fx, 'note': event['Note'], 'api_call_count': 0}
            return {'statusCode': 200, 'body': response_dict}
        combined_ticker_fx_file_name = 'data-daily-' + combined_ticker_fx + '.pkl'
        print('combined_ticker_fx_file_name - ', combined_ticker_fx_file_name)
        fresh_ticker_file_found = _check_fresh_file_in_bucket(combined_ticker_fx_file_name)
        if fresh_ticker_file_found:
            print ('Fresh ticker data file found in our S3, no need to import again')
            response_dict = {'import_success': True, 'type': 'FX', 'note': event['Note'], 'ticker_1': event['Ticker1'], 'ticker_2': event['Ticker2'], 'file_1': combined_ticker_fx_file_name, 'api_call_count': 0}
            return {'statusCode': 200, 'body': response_dict}
        print ('Fresh ticker data file NOT found in our S3, so now import')
        url_backbone = 'https://www.alphavantage.co/query?function=FX_DAILY&outputsize=full&datatype=csv&apikey='
        from_symbol = '&from_symbol=' + event['Ticker1']
        to_symbol = '&to_symbol=' + event['Ticker2']
        data_import_url = url_backbone + alpha_vantage_api_key + from_symbol + to_symbol
        imported_data = _import_preprocess_ticker_data(data_import_url)
        if not isinstance(imported_data, pd.DataFrame):
            failed_imports.append(combined_ticker_fx)
            print('failed_imports -', failed_imports)
            upload_res = _upload_pickle_file(failed_imports, FAILED_IMPORTS_FILE_REMOTE)
            if not upload_res:
                raise FileUploadFailureException(FAILED_IMPORTS_FILE_REMOTE, ' - failed to upload to S3')
            response_dict = {'import_success': False, 'failed_ticker': combined_ticker_fx, 'note': event['Note'], 'api_call_count': 1}
            return {'statusCode': 200, 'body': response_dict}
        imported_data['Volume'] = 0.0
        upload_res = _upload_pickle_file(imported_data, combined_ticker_fx_file_name)
        if not upload_res:
            raise FileUploadFailureException(combined_ticker_fx_file_name, ' - failed to upload to S3')
        response_dict = {'import_success': True, 'type': 'FX', 'note': event['Note'], 'ticker_1': event['Ticker1'], 'ticker_2': event['Ticker2'], 'file_1': combined_ticker_fx_file_name, 'api_call_count': 1}
        return {'statusCode': 200, 'body': response_dict}
    else:
        print('row - not FX, so stocks, at least Ticker1')
        stock_ticker_process_res, ticker_1_file_name, api_call_count_1 = _process_single_stock_ticker(event['Ticker1'], failed_imports)
        if not stock_ticker_process_res:
            response_dict = {'import_success': False, 'failed_ticker': event['Ticker1'], 'note': event['Note'], 'api_call_count': api_call_count_1}
            return {'statusCode': 200, 'body': response_dict}
        if event['Ticker2'] == '':
            print('row - Stocks_ETFs, single ticker')
            response_dict = {'import_success': True, 'type': 'Stocks_single', 'note': event['Note'], 'ticker_1': event['Ticker1'], 'file_1': ticker_1_file_name, 'api_call_count': api_call_count_1}
            return {'statusCode': 200, 'body': response_dict}
        print('row - Stocks_ETFs, relative two tickers')    
        stock_ticker_process_res, ticker_2_file_name, api_call_count_2 = _process_single_stock_ticker(event['Ticker2'], failed_imports)
        api_call_count = api_call_count_1 + api_call_count_2
        if not stock_ticker_process_res:
            response_dict = {'import_success': False, 'failed_ticker': event['Ticker2'], 'note': event['Note'], 'api_call_count': api_call_count}
            return {'statusCode': 200, 'body': response_dict}
        response_dict = {'import_success': True, 'type': 'Stocks_relative_two', 'note': event['Note'], 'ticker_1': event['Ticker1'], 'ticker_2': event['Ticker2'], 'file_1': ticker_1_file_name, 'file_2': ticker_2_file_name, 'api_call_count': api_call_count}
        return {'statusCode': 200, 'body': response_dict}