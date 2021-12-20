import json
import gspread
from datetime import datetime as dt, timedelta, timezone
import pandas as pd
import numpy as np
import boto3

class SecretAccessFailedException(Exception): pass
class NoSecretStringException(Exception): pass
class GoogleSpreadSheetAccessFailedException(Exception): pass 
class PortfolioFileSheetAccessException(Exception): pass
class RequiredColumnsMissingException(Exception): pass
class WrongRowTypeException(Exception): pass
class ForexTickerEmptyException(Exception): pass
class StocksTickerEmptyException(Exception): pass

BUCKET_NAME = 'kust-pics'
s3 = boto3.client('s3')

def get_secret():

    secret_name = "portfolio_spreadsheet"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        print(e)
        raise SecretAccessFailedException("Access to Secrets Manager failed!")
    else:
        return get_secret_value_response

def lambda_handler(event, context):
    
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

    
    secret_obtained = get_secret()
    if 'SecretString' in secret_obtained:
        secret = secret_obtained['SecretString']
        secret = json.loads(secret)
        for key in secret:
            secret[key] = secret[key].replace('\\n', '\n')
    else:
        raise NoSecretStringException("No SecretString in data obtained from Secrets Manager!")
        
    try:
        gc = gspread.service_account_from_dict(secret)
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
    
    all_ideas_rows = pd.DataFrame.from_dict(sheet_0.get_all_records())
    all_ideas_rows.replace("", np.nan, inplace=True)
    
    condition_all_and_only_required_columns = ("Ticker1" in all_ideas_rows.columns) & ("Ticker2" in all_ideas_rows.columns) \
           & ("Note" in all_ideas_rows.columns) & ("Type" in all_ideas_rows.columns) \
           & (len(all_ideas_rows.columns) == 4)
    if not (condition_all_and_only_required_columns & (not all_ideas_rows.empty)):
        raise RequiredColumnsMissingException("Allowed and required columns: Ticker1, Ticker2, Type, Note. Ticker2 and Note may be empty. Ticker1 ad Type - not empty.")
    all_ideas_types = set(all_ideas_rows['Type'].unique())
    full_types_set = {'Stocks_ETFs', 'Forex'}
    if not (all_ideas_types == full_types_set):
        raise WrongRowTypeException("In portfolio worksheet, types allowed are Forex and Stocks_ETFs only.")
    all_ideas_rows_forex = all_ideas_rows[all_ideas_rows['Type'] == 'Forex']
    condition_forex_tickers_both_not_empty = (all_ideas_rows_forex.shape[0] == all_ideas_rows_forex.dropna(subset=['Ticker1', 'Ticker2']).shape[0])
    if not condition_forex_tickers_both_not_empty:
        raise ForexTickerEmptyException("In Forex rows Ticker1 and Ticker2 must be not empty.")
    all_ideas_rows_stocks = all_ideas_rows[all_ideas_rows['Type'] == 'Stocks_ETFs']
    condition_stocks_ticker_1_not_empty = (all_ideas_rows_stocks.shape[0] == all_ideas_rows_stocks.dropna(subset=['Ticker1']).shape[0])
    if not condition_stocks_ticker_1_not_empty:
        raise StocksTickerEmptyException("In Stocks_ETFs rows Ticker1 must be not empty.")
    
    files_to_delete = []
    files_to_delete.append({'Key':'failed_imports.pkl'})
    files_to_delete.append({'Key':'success_imports.pkl'})
    _ = s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': files_to_delete})
    # delete_res = s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': files_to_delete})
    # print('delete_res - ', delete_res)
    
    return {
        'statusCode': 200,
        'body': {'data_list_of_dics': data_list_of_dics}
    }


