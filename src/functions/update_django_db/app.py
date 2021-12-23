import json
from datetime import datetime as dt
import boto3
import sqlite3
import os

BUCKET_NAME = os.environ.get('s3_main_bucket')
SQLITE_DB = 'sqlite.db'
REGION_NAME = os.environ.get('region_id')

s3 = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb',region_name=REGION_NAME)
table = dynamodb_resource.Table('CurrentPortfolioRows')

def download_django_sqlite_db():
    try:
        s3.download_file(BUCKET_NAME, SQLITE_DB, '/tmp/'+SQLITE_DB)
    except Exception as e:
        print('Problem with downloading SUCCESS_IMPORTS_FILE from S3, terminate execution')
        raise e            

def django_db_initial_clear():
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
    return conn

def get_list_of_rows_from_dynamodb():
    scan = table.scan(ConsistentRead=True)
    portfolio_rows_data_to_insert = []
    id = 0
    for elem in scan['Items']:   
        id = id + 1        
        data_to_insert = (id, elem['ticker_combined'], elem['type'], elem['note'], elem['file_1'], elem['file_1'], 1)
        portfolio_rows_data_to_insert.append(data_to_insert)
    return portfolio_rows_data_to_insert

def insert_new_rows_into_django_db(portfolio_rows_data_to_insert, conn):
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
    
def upload_new_django_db_to_s3():
    try:
        _ = s3.upload_file('/tmp/'+SQLITE_DB, BUCKET_NAME, SQLITE_DB)
    except Exception as e:
        print('Problem with uploading SQLITE_DB from S3, terminate execution')
        raise e  
    
def lambda_handler(event, context):
    # print("Incoming event:")
    # print(event)
    
    download_django_sqlite_db()
    django_db_connection = django_db_initial_clear()
    portfolio_rows_to_insert = get_list_of_rows_from_dynamodb()
    insert_new_rows_into_django_db(portfolio_rows_to_insert, django_db_connection) 
    upload_new_django_db_to_s3()

    return {
        'statusCode': 200,
        'body': json.dumps(portfolio_rows_to_insert)
    }