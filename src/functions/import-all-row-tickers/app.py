import json
import pandas as pd
from datetime import datetime as dt, timedelta, timezone
import pickle
import boto3
import os

BUCKET_NAME = os.environ.get("s3_main_bucket")
FAILED_IMPORTS_FILE_REMOTE = "failed_imports.pkl"
FAILED_IMPORTS_FILE_LOCAL = "/tmp/" + FAILED_IMPORTS_FILE_REMOTE
SECRET_NAME = os.environ.get("secret_id")
REGION_NAME = os.environ.get("region_id")


class FileUploadFailureException(Exception):
    pass


s3 = boto3.client("s3")
session = boto3.session.Session()
secrets_client = session.client(service_name="secretsmanager", region_name=REGION_NAME)
get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
secret = get_secret_value_response["SecretString"]
secret = json.loads(secret)
for key in secret:
    secret[key] = secret[key].replace("\\n", "\n")
alpha_vantage_api_key = secret["alpha_vantage_api_key"]
start_date = dt.now() - timedelta(days=1.5 * 365)


def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    try:
        response = s3.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True


def _check_fresh_file_in_bucket(file_key, max_age_minutes=10):
    print(
        "Trying to find the ",
        file_key,
        " file in the S3 bucked and check if it is fresh or outdated",
    )
    try:
        res = s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
    except Exception as e:
        print(file_key, " file not found in the S3 bucked")
        return False
    file_age = dt.now(timezone.utc) - res["LastModified"]
    if file_age < timedelta(minutes=max_age_minutes):
        return True
    else:
        print(file_key, " file found in the S3 buckedt, but outdated")
        return False


def _prepare_stock_ticker_import_url(stock_ticker):
    url_backbone = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&datatype=csv&apikey="
    symbol_backbone = "&symbol="
    return url_backbone + alpha_vantage_api_key + symbol_backbone + stock_ticker


def _import_preprocess_ticker_data(alpha_vantage_url):
    print("Starting Alpha Vantage data import from url:")
    print(alpha_vantage_url)
    try:
        data = pd.read_csv(alpha_vantage_url, index_col="timestamp", parse_dates=True)
    except Exception as e:
        print("Problem with pd.read_csv(alpha_vantage_url) operation")
        print("Alpha Vvantage_url - ", alpha_vantage_url)
        print(e)
        return False
    all_required_columns_present = all(
        item in data.columns for item in ["open", "high", "low", "close"]
    )
    data_is_pd_df = isinstance(data, pd.DataFrame)
    if not all_required_columns_present or not data_is_pd_df:
        return False
    try:
        data.sort_values(by=["timestamp"], inplace=True, ascending=True)
        data = data.loc[start_date:]
        data.rename(
            columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"},
            inplace=True,
        )
        data = data.drop(
            ["dividend_amount", "split_coefficient"], axis=1, errors="ignore"
        )
    except Exception as e:
        print("Import from Alpha Vantage - OK, but problem with data preprocessing")
        print(e)
        return False
    return data


def upload_pickle_file(data_to_upload, file_name_key):
    print("Starting upload_pickle_file function - ", file_name_key)
    with open("/tmp/" + file_name_key, "wb") as output_file:
        pickle.dump(data_to_upload, output_file)
    upload_result = upload_file("/tmp/" + file_name_key, BUCKET_NAME, file_name_key)
    if not upload_result:
        raise FileUploadFailureException(file_name_key, " - failed to upload to S3")


def _failed_imports_new_append_upload(ticker_id, failed_imports_param):
    print(ticker_id, " import from Alpha Vantage failed!")
    failed_imports_param.append(ticker_id)
    print("failed_imports -", failed_imports_param)
    upload_pickle_file(failed_imports_param, FAILED_IMPORTS_FILE_REMOTE)


def _import_stock_ticker_data(ticker_code, failed_imports_list, ticker_file_name):
    print("Fresh ", ticker_code, " data file NOT found in our S3, so now import it")
    data_import_url = _prepare_stock_ticker_import_url(ticker_code)
    imported_data = _import_preprocess_ticker_data(data_import_url)
    if not isinstance(imported_data, pd.DataFrame):
        _failed_imports_new_append_upload(ticker_code, failed_imports_list)
        return False
    imported_data.rename(columns={"volume": "Volume"}, inplace=True)
    upload_pickle_file(imported_data, ticker_file_name)
    # print(ticker_code, " last:")
    # print(imported_data.tail(1).index.item())
    return True


def process_single_stock_ticker(ticker_code, failed_imports_list):
    ticker_file_name = "data-daily-" + ticker_code + ".pkl"
    print("ticker_file_name - ", ticker_file_name)
    if ticker_code in failed_imports_list:
        print(
            ticker_code,
            " found in failed_imports list, so process as import failure...",
        )
        return False, ticker_file_name, 0
    fresh_ticker_file_found = _check_fresh_file_in_bucket(ticker_file_name)
    if fresh_ticker_file_found:
        print(
            "Fresh ", ticker_code, " data file found in our S3, no need to import again"
        )
        return True, ticker_file_name, 0
    else:
        import_res = _import_stock_ticker_data(
            ticker_code, failed_imports_list, ticker_file_name
        )
        return import_res, ticker_file_name, 1


def get_failed_imports_list():
    failed_imports_file_fresh = _check_fresh_file_in_bucket(FAILED_IMPORTS_FILE_REMOTE)
    if failed_imports_file_fresh:
        s3.download_file(
            BUCKET_NAME,
            FAILED_IMPORTS_FILE_REMOTE,
            "/tmp/" + FAILED_IMPORTS_FILE_REMOTE,
        )
        with open("/tmp/" + FAILED_IMPORTS_FILE_REMOTE, "rb") as input_file:
            failed_imports_internal_list = pickle.load(input_file)
        if isinstance(failed_imports_internal_list, list):
            print("failed_imports list downloaded from S3 - OK")
            return failed_imports_internal_list
        else:
            print(
                "failed_imports list downloaded from S3, but not list, so now reset it as empty list"
            )
            failed_imports_internal_list = []
            return failed_imports_internal_list
    else:
        failed_imports_internal_list = []
        return failed_imports_internal_list


def _prepare_fx_success_response_dict(event_internal, fx_file_name, api_call_counter):
    dict_to_return = {
        "import_success": True,
        "type": "FX",
        "note": event_internal["Note"],
        "ticker_1": event_internal["Ticker1"],
        "ticker_2": event_internal["Ticker2"],
        "file_1": fx_file_name,
        "api_call_count": api_call_counter,
    }
    return dict_to_return


def _import_forex_row(event_internal, file_name):
    url_backbone = "https://www.alphavantage.co/query?function=FX_DAILY&outputsize=full&datatype=csv&apikey="
    from_symbol = "&from_symbol=" + event_internal["Ticker1"]
    to_symbol = "&to_symbol=" + event_internal["Ticker2"]
    data_import_url = url_backbone + alpha_vantage_api_key + from_symbol + to_symbol
    imported_data = _import_preprocess_ticker_data(data_import_url)
    if not isinstance(imported_data, pd.DataFrame):
        return False
    imported_data["Volume"] = 0.0
    upload_pickle_file(imported_data, file_name)
    # print(event_internal['Ticker1'] + '-' + event_internal['Ticker2'], " last:")
    # print(imported_data.tail(1).index.item())
    return True


def process_fx_row(event_internal, failed_imports_internal):
    combined_ticker_fx = event_internal["Ticker1"] + "-" + event_internal["Ticker2"]
    print("Row is FX - ", combined_ticker_fx)

    # search FX ticker in failed_imports
    if combined_ticker_fx in failed_imports_internal:
        print(
            combined_ticker_fx,
            " found in failed_imports list, so process as import failure",
        )
        response_dict = {
            "import_success": False,
            "failed_ticker": combined_ticker_fx,
            "note": event_internal["Note"],
            "api_call_count": 0,
        }
        return response_dict

    # search FX ticker in previously imported data pickle files
    combined_ticker_fx_file_name = "data-daily-" + combined_ticker_fx + ".pkl"
    print("combined_ticker_fx_file_name - ", combined_ticker_fx_file_name)
    fresh_ticker_file_found = _check_fresh_file_in_bucket(combined_ticker_fx_file_name)
    if fresh_ticker_file_found:
        print("Fresh ticker data file found in our S3, no need to import again")
        response_dict = _prepare_fx_success_response_dict(
            event_internal, combined_ticker_fx_file_name, 0
        )
        return response_dict

    print("Fresh ticker data file NOT found in our S3, so now import")
    fx_row_import_result = _import_forex_row(
        event_internal, combined_ticker_fx_file_name
    )
    if not fx_row_import_result:
        _failed_imports_new_append_upload(combined_ticker_fx, failed_imports_internal)
        response_dict = {
            "import_success": False,
            "failed_ticker": combined_ticker_fx,
            "note": event_internal["Note"],
            "api_call_count": 1,
        }
    else:
        response_dict = _prepare_fx_success_response_dict(
            event_internal, combined_ticker_fx_file_name, 1
        )
    return response_dict


def _prepare_stock_fail_response_dict(ticker_id, note_text, api_call_counter):
    dict_to_return = {
        "import_success": False,
        "failed_ticker": ticker_id,
        "note": note_text,
        "api_call_count": api_call_counter,
    }
    return dict_to_return


def process_stocks_row(event_internal, failed_imports_internal):
    print("row - not FX, so stocks, at least Ticker1")
    # Ticker1 should be non-empty, guaranteed by previous processing stage, so process it immediately
    process_result, ticker_1_file_name, api_call_count_1 = process_single_stock_ticker(
        event_internal["Ticker1"], failed_imports_internal
    )
    if not process_result:
        response_dict = _prepare_stock_fail_response_dict(
            event_internal["Ticker1"], event_internal["Note"], api_call_count_1
        )
        return response_dict
    if event_internal["Ticker2"] == "":
        print("row - Stocks_ETFs, single ticker")
        response_dict = {
            "import_success": True,
            "type": "Stocks_single",
            "note": event_internal["Note"],
            "ticker_1": event_internal["Ticker1"],
            "file_1": ticker_1_file_name,
            "api_call_count": api_call_count_1,
        }
        return response_dict
    print(
        "row - Stocks_ETFs, relative two tickers",
        event_internal["Ticker1"],
        "-",
        event_internal["Ticker2"],
    )
    process_result, ticker_2_file_name, api_call_count_2 = process_single_stock_ticker(
        event_internal["Ticker2"], failed_imports_internal
    )
    api_call_count_total = api_call_count_1 + api_call_count_2
    if not process_result:
        response_dict = _prepare_stock_fail_response_dict(
            event_internal["Ticker2"], event_internal["Note"], api_call_count_total
        )
        return response_dict
    response_dict = {
        "import_success": True,
        "type": "Stocks_relative_two",
        "note": event_internal["Note"],
        "ticker_1": event_internal["Ticker1"],
        "ticker_2": event_internal["Ticker2"],
        "file_1": ticker_1_file_name,
        "file_2": ticker_2_file_name,
        "api_call_count": api_call_count_total,
    }
    return response_dict


def lambda_handler(event, context):
    # print('Incoming event:')
    # print(event)
    failed_imports = get_failed_imports_list()

    if event["Type"] == "Forex":
        response_dict = process_fx_row(event, failed_imports)
        return {"statusCode": 200, "body": response_dict}
    else:
        # stocks - single or relative two tickers
        response_dict = process_stocks_row(event, failed_imports)
        return {"statusCode": 200, "body": response_dict}
