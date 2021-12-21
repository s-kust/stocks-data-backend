import json
import gspread
from datetime import datetime as dt, timedelta, timezone
import pandas as pd
import numpy as np
import boto3
import os

class SecretAccessFailedException(Exception): pass
class NoSecretStringException(Exception): pass
class GoogleSpreadSheetAccessFailedException(Exception): pass 
class PortfolioFileSheetAccessException(Exception): pass
class RequiredColumnsMissingException(Exception): pass
class WrongRowTypeException(Exception): pass
class ForexTickerEmptyException(Exception): pass
class StocksTickerEmptyException(Exception): pass

BUCKET_NAME = os.environ.get('s3_main_bucket')
SECRET_NAME = os.environ.get('secret_id')
REGION_ID = os.environ.get('region_id')
s3 = boto3.client('s3')

def get_secret():
    secret_name = SECRET_NAME
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION_ID
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        print(e)
        raise SecretAccessFailedException("Access to Secrets Manager failed!")
    else:
        if not 'SecretString' in get_secret_value_response:
            raise NoSecretStringException("No SecretString in data obtained from Secrets Manager!")
        secret_data = get_secret_value_response['SecretString']
        secret_data = json.loads(secret_data)
        for key in secret_data:
            secret_data[key] = secret_data[key].replace('\\n', '\n')
        return secret_data

def delete_outdated_pickle_files():
    print('Start check S3 for outdated pickle files to delete')
    list_response = s3.list_objects_v2(Bucket = BUCKET_NAME)
    files_to_delete = []
    date_now = dt.now(timezone.utc)
    for elem in list_response['Contents']:
        condition_suffix = elem['Key'].endswith('.pkl')
        if not condition_suffix:
            continue
        else:
            print('Found .pkl file - ', elem['Key'], ' - now check its age...')
            file_age =  date_now - elem['LastModified']
            condition_outdated = (file_age > timedelta(hours=12))
            print('file_age', file_age)
            print('condition_outdated', condition_outdated)
            if condition_outdated:
                files_to_delete.append({'Key':elem['Key']})
    if files_to_delete:
        print('Found outdated pickle files, now delete them:')
        for obj in files_to_delete:
            print(obj['Key'])
        _ = s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': files_to_delete})
    else:
        print('No outdated pickle files found in S3 bucket')

def get_list_of_dics_from_spreadsheet(secret_json):
    try:
        gc = gspread.service_account_from_dict(secret_json)
        sheet = gc.open("Portfolio")
    except Exception as e:
        raise GoogleSpreadSheetAccessFailedException("Access to Google SpreadSheet Portfolio file failed!")    
    try:
        sheet_0 = sheet.get_worksheet(0)
        data_list_of_dics = sheet_0.get_all_records()
    except Exception as e:
        raise PortfolioFileSheetAccessException("Access to Google SpreadSheet Portfolio file OK, but access to WorkSheet[0] failed!")    
    # leading and lagging spaces often appear when copy-paste ticker codes, remove them
    for row_dict in data_list_of_dics:
        for elem in row_dict:
            row_dict[elem] = row_dict[elem].strip() if isinstance(row_dict[elem], str) else row_dict[elem]    
    ideas_df = pd.DataFrame.from_dict(sheet_0.get_all_records())
    ideas_df.replace("", np.nan, inplace=True)
    return data_list_of_dics, ideas_df

def check_portfolio_rows_validation_conditions(df_to_check_conditions):
    condition_all_and_only_required_columns = ("Ticker1" in df_to_check_conditions.columns) & ("Ticker2" in df_to_check_conditions.columns) \
           & ("Note" in df_to_check_conditions.columns) & ("Type" in df_to_check_conditions.columns) \
           & (len(df_to_check_conditions.columns) == 4)
    if not (condition_all_and_only_required_columns & (not df_to_check_conditions.empty)):
        raise RequiredColumnsMissingException("Allowed and required columns: Ticker1, Ticker2, Type, Note. Ticker2 and Note may be empty. Ticker1 ad Type - not empty.")
    
    all_ideas_types = set(df_to_check_conditions['Type'].unique())
    full_types_set = {'Stocks_ETFs', 'Forex'}
    if not (all_ideas_types == full_types_set):
        raise WrongRowTypeException("In portfolio worksheet, types allowed are Forex and Stocks_ETFs only.")
    
    all_ideas_rows_forex = df_to_check_conditions[df_to_check_conditions['Type'] == 'Forex']
    condition_forex_tickers_both_not_empty = (all_ideas_rows_forex.shape[0] == all_ideas_rows_forex.dropna(subset=['Ticker1', 'Ticker2']).shape[0])
    if not condition_forex_tickers_both_not_empty:
        raise ForexTickerEmptyException("In Forex rows Ticker1 and Ticker2 must be not empty.")
    
    all_ideas_rows_stocks = df_to_check_conditions[df_to_check_conditions['Type'] == 'Stocks_ETFs']
    condition_stocks_ticker_1_not_empty = (all_ideas_rows_stocks.shape[0] == all_ideas_rows_stocks.dropna(subset=['Ticker1']).shape[0])
    if not condition_stocks_ticker_1_not_empty:
        raise StocksTickerEmptyException("In Stocks_ETFs rows Ticker1 must be not empty.")

def lambda_handler(event, context):
    # print("Incoming event:")
    # print(event)
    delete_outdated_pickle_files()
    secret = get_secret()        
    data_to_return, portfolio_rows_df = get_list_of_dics_from_spreadsheet(secret)
    check_portfolio_rows_validation_conditions(portfolio_rows_df)
        
    files_to_delete = []
    files_to_delete.append({'Key':'failed_imports.pkl'})
    files_to_delete.append({'Key':'success_imports.pkl'})
    _ = s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': files_to_delete})
    
    return {
        'statusCode': 200,
        'body': {'data_list_of_dics': data_to_return}
    }


