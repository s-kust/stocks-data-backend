import json
import pickle
import boto3
import os

BUCKET_NAME = os.environ.get('s3_main_bucket')
SUCCESS_IMPORTS_FILE_REMOTE = 'success_imports.pkl'

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # print("Incoming event:")
    # print(event)
    try:
        s3.download_file(BUCKET_NAME, SUCCESS_IMPORTS_FILE_REMOTE, '/tmp/'+SUCCESS_IMPORTS_FILE_REMOTE)
    except Exception as e:
        success_imports = []
    else:
        with open('/tmp/'+SUCCESS_IMPORTS_FILE_REMOTE, "rb") as input_file:
            success_imports = pickle.load(input_file)
        if isinstance(success_imports, list):
            print("success_imports list downloaded from S3 - OK")
        else:
            print("success_imports list downloaded from S3, but not list, so now I reset it as empty list")
            success_imports = []
    
    success_imports.append(event)
    success_imports_unique = []
    temporary_keys = set()
    for elem in success_imports:
        temporary_key = elem['type'] + elem['note']
        if temporary_key not in temporary_keys:
            temporary_keys.add(temporary_key)
            success_imports_unique.append(elem)
    success_imports = success_imports_unique
            
    try:
        with open('/tmp/'+SUCCESS_IMPORTS_FILE_REMOTE, "wb") as output_file:
            pickle.dump(success_imports, output_file)
    except Exception as e:
        print ('Problem with data pickle dump')
        print(e)
        raise (e)
    try:
        _ = s3.upload_file('/tmp/'+SUCCESS_IMPORTS_FILE_REMOTE, BUCKET_NAME, SUCCESS_IMPORTS_FILE_REMOTE)
    except Exception as e:
        print ('Problem with uploading data fiile to S3 bucket')
        print(e)
        print('file_name_key - ', file_name_key)
        raise (e)
    
    return {
        'statusCode': 200,
        'body': json.dumps(success_imports)
    }