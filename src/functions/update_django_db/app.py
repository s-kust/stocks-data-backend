import json
import pickle
from datetime import datetime as dt
import boto3
import sqlite3
import os

BUCKET_NAME = os.environ.get('s3_main_bucket')
SUCCESS_IMPORTS_FILE_REMOTE = 'success_imports.pkl'
CURRENT_PORTFOLIO_ROWS_FILE = 'current_portfolio_rows.pkl'
SQLITE_DB = 'sqlite.db'

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # print("Incoming event:")
    # print(event)
    try:
        s3.download_file(BUCKET_NAME, SUCCESS_IMPORTS_FILE_REMOTE, '/tmp/'+SUCCESS_IMPORTS_FILE_REMOTE)
    except Exception as e:
        print('Problem with downloading SUCCESS_IMPORTS_FILE from S3, terminate execution')
        raise e
    
    with open('/tmp/'+SUCCESS_IMPORTS_FILE_REMOTE, "rb") as input_file:
        success_imports = pickle.load(input_file)
    if isinstance(success_imports, list):
        print("SUCCESS_IMPORTS list downloaded from S3 - OK")
    else:
        raise TypeError("SUCCESS_IMPORTS_FILE must contain a list of dictionaries")
    
    try:
        s3.download_file(BUCKET_NAME, SQLITE_DB, '/tmp/'+SQLITE_DB)
    except Exception as e:
        print('Problem with downloading SUCCESS_IMPORTS_FILE from S3, terminate execution')
        raise e            
    
    success_imports_unique = []
    temporary_keys = set()
    for elem in success_imports:
        temporary_key = elem['type'] + elem['note']
        if temporary_key not in temporary_keys:
            temporary_keys.add(temporary_key)
            success_imports_unique.append(elem)
    
    try:
        conn = sqlite3.connect('/tmp/'+SQLITE_DB)
    except Exception as e:
        print('Problem with connecting to locally downloaded SQLITE DB, terminate execution')
        raise e  
    try:
        with conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM frankie_portfolio;',);
            print('DELETE FROM frankie_portfolio - deleted', cur.rowcount, 'records from the table.')
            cur.execute('DELETE FROM frankie_portfoliorow;',);
            print('DELETE FROM frankie_portfoliorow - deleted', cur.rowcount, 'records from the table.')
    except Exception as e:
        print('Problem with DELETE FROM portfolio and rows in locally downloaded SQLITE DB, terminate execution')
        raise e          
    
    portfolio_rows_data_to_insert = []
    id = 0
    for elem in success_imports_unique:
    # for elem in success_imports:        
        id = id + 1
        if not elem['type'] in ['FX', 'Stocks_relative_two', 'Stocks_single']:
            raise TypeError("Portfolio row type must be FX, Stocks_relative_two or, Stocks_single")
        if elem['type'] == 'FX':
            data_to_insert = (id, elem['ticker'], "Currency pair", elem['note'], elem['filename_line'], elem['filename_candle'], 1)
        if elem['type'] == 'Stocks_relative_two':
            combined_ticker = elem['ticker_1'] + "-" + elem['ticker_2']
            data_to_insert = (id, combined_ticker, "Stocks relative", elem['note'], elem['chart_filename'], "", 1)
        if elem['type'] == 'Stocks_single':
            data_to_insert = (id, elem['ticker'], "Stocks single", elem['note'], elem['filename_line'], elem['filename_candle'], 1)
        portfolio_rows_data_to_insert.append(data_to_insert)
    
    try:
        with conn:
            cur = conn.cursor()
            portfolio_data = (1, dt.now(), dt.now())
            query = ''' INSERT INTO frankie_portfolio(id,created,modified) VALUES(?,?,?) '''
            cur.execute(query, portfolio_data)
            conn.commit()
            query_portfolio_row = ''' INSERT INTO frankie_portfoliorow(id,ticker, row_type,note,file_1,file_2,portfolio_id) VALUES(?,?,?,?,?,?,?) '''
            for row_data in portfolio_rows_data_to_insert:
                cur.execute(query_portfolio_row, row_data)
            conn.commit()
    except Exception as e:
        print('Problem with INSERT INTO portfolio and rows in locally downloaded SQLITE DB, terminate execution')
        raise e       
    
    try:
        _ = s3.upload_file('/tmp/'+SQLITE_DB, BUCKET_NAME, SQLITE_DB)
    except Exception as e:
        print('Problem with uploading SQLITE_DB from S3, terminate execution')
        raise e   
        
    try:
        with open('/tmp/'+CURRENT_PORTFOLIO_ROWS_FILE, "wb") as output_file:
            pickle.dump(portfolio_rows_data_to_insert, output_file)
    except Exception as e:
        print ('Problem with portfolio_rows_data_to_insert pickle dump')
        print(e)
        raise (e)
    
    try:
        _ = s3.upload_file('/tmp/'+CURRENT_PORTFOLIO_ROWS_FILE, BUCKET_NAME, CURRENT_PORTFOLIO_ROWS_FILE)
    except Exception as e:
        print ('Problem with uploading portfolio_rows_data_to_insert pickle fiile to S3 bucket')
        print(e)
        raise (e)
    
    return {
        'statusCode': 200,
        'body': json.dumps(portfolio_rows_data_to_insert)
    }