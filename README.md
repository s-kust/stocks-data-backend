# Daily Portfolio Positions Reports: Backend

The system workflow:

1. Load a list of portfolio rows from Google Spreadsheet. 
2. For each row, import its tickers recent data using the data provider's API.
3. Plot the charts and save them on the hard drive or in the S3 bucket.
3. Generate a report with charts and send it to the email. 

You can run the workflow regularly at suitable intervals. For example, on business days in the morning and again 15 minutes before the end of the trading time. 

In addition to email, you can see the recent charts online using the [frontend](https://github.com/s-kust/amplifyapp/). 

![frontend screenshot](/misc/frontend-screen.png)

The system currently supports the following types of reports:
1. Single stock or ETF.
2. Relative performance of two tickers.
3. FX currencies pair. 

Additional report types can be easily added as needed.

The system consists of the AWS Lambda backend and a React frontend. This repository contains the code of the backend. It has several Python functions coordinated by the state machine. 

![State machine schema](/misc/sm-schema.png) 

All of these resources are integrated into the AWS CloudFormation template for fast and easy deployment.

The frontend is an AWS Amplify React application. Please see [its repository](https://github.com/s-kust/amplifyapp/) with the codebase and setup instructions.

<h2>Deployment manual</h2>

First, prepare the Google spreadsheet with the list of stocks and currency pairs tickers to be traced.

![Watchlist spreadsheet example](/misc/1.PNG) 

To enable the script to work with that spreadsheet, follow [these instructions](https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html). If the page is not available, use the [archived PDF version](/misc/Google_Spreadsheets_Python.pdf).
   1. You'll have to go to the Google APIs Console, create a new project, enable API, etc. 
   1. Note that you must give the spreadsheet editing rights to your function, not just viewing, although in our case it does not perform any editing.
   1. Prepare the values and save the following key-value pairs in the AWS Secrets Manager: `type`, `project_id`, `private_key_id`, `private_key`, `client_email`, `client_id`, `auth_uri`, `token_uri`, `auth_provider_x509_cert_url`, `client_x509_cert_url`. 
   1. The name of the secret may be `portfolio_spreadsheet`, otherwise change it in the AWS CloudFormation template parameter `SecretId`.

Prepare the [Alpha Vantage](https://www.alphavantage.co/) API key and save it in the AWS Secrets Manager secret named `alpha_vantage_api_key` with the same key.

Create two AWS S3 buckets for data and generated charts. Paste their names in the AWS CloudFormation template parameters `BucketMainData` and `BucketCharts`. Note that the objects in the charts bucket must be publicly accessible. 

Now you have to manually load the state machine definition file `/src/state_machine.json` into the data S3 bucket. After that, check out the `DefinitionUri` parameter in the `template.yml` file. Unfortunately, I was unable to simplify this step. Currently, the system does not accept the state machine definition from the local file.

After all there preparations, run the bash script `1-create-bucket.sh`. Make sure that the `bucket-name.txt` file appeared in the root directory and that one more S3 bucket has been created. This step only needs to be done once.

Check carefully all the input parameters in the `template.yml` file and then run the `3-deploy.sh` script. If the deployment was successful, go to the AWS Step Functions console and run the newly created state machine for testing. 

In addition to the real portfolio items, you can add several erroneous tickers to the spreadsheet to see how the system handles errors. Make sure you receive error notification emails. Also, the system should continue processing subsequent portfolio rows after it encounters an erroneous ticker.

![Error email notification](/misc/problem-email.png)

After testing the state machine, go to the AWS API Gateway console and make sure that the newly created API works OK. The frontend will call it and use its data.

In the EventBridge console, create a schedule to automatically run the machine on a regular basis.

Whenever you want to redeploy the system, you just need to run the `3-deploy.sh` script again. The system automatically detects all changes made to the files and deploys them.

<h2>What is useful here for AWS Lambda developers</h2>

This repository contains the following examples:
1. The state machine of medium complexity. It uses `map`. Also, it catches and handles errors that may occur in Lambda functions. See its definition in the `/src/state_machine.json` file.
2. Integration of the state machine into the AWS CloudFormation template.
3. Passing environment variables to AWS Lambda functions through the AWS CloudFormation template.
4. How to filter files in the S3 bucket by name, as well as by the date and time of their last update. See the functions `create-tickers-df-from-spreadsheet` and `import-all-row-tickers`.
5. In the `create-tickers-df-from-spreadsheet` function, working with a Google Spreadsheet document using the `gspread` library.
6. The function `import-all-row-tickers` receives seveal kinds of data from Alpha Vantage through its API. It carefully validates the obtained data before transferring it for further processing.
7. The system pauses in order not to send requests to Alpha Vantage API too often and not to exceed the allowed limit. This logic is implemented in the state machine and not inside the Lambda functions.
8. The function `create_charts` uses the `mplfinance` library to draw the candlestick and line charts. 

When using the AWS Secrets Manager, a problem with the obtained secret value may arise. It is solved by the following code:
```python
get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)
for key in secret:
    secret[key] = secret[key].replace('\\n', '\n')
```	
See the details in the `import-all-row-tickers` function. 
