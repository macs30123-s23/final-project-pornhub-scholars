import requests
import dataset
import re
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import boto3
import json
import zipfile


aws_lambda = boto3.client('lambda', region_name='us-east-1')
iam_client = boto3.client('iam')
role = iam_client.get_role(RoleName='LabRole')


def update_lammbda():

    with zipfile.ZipFile('lamba_deployment.zip', 'a') as zipf:
        zipf.write('lambda_function.py')

    # Open zipped directory
    with open('lamba_deployment.zip', 'rb') as f:
        lambda_zip = f.read()


    try:
        # If function hasn't yet been created, create it
        response = aws_lambda.create_function(
            FunctionName='pornhub_scraper',
            Runtime='python3.9',
            Role=role['Role']['Arn'],
            Handler='lambda_function.lambda_handler',
            Code=dict(ZipFile=lambda_zip),
            Timeout=300
        )
    except aws_lambda.exceptions.ResourceConflictException:
        # If function already exists, update it based on zip
        # file contents
        response = aws_lambda.update_function_code(
            FunctionName='pornhub_scraper',
            ZipFile=lambda_zip
            )

    lambda_arn = response['FunctionArn']

    sfn = boto3.client('stepfunctions', region_name='us-east-1')

    def make_def(lambda_arn):
        definition = {
            "Comment": "My State Machine",
            "StartAt": "Map",
            "States": {
                "Map": {
                    "Type": "Map",
                    "End": True,
                    "Iterator": {
                        "StartAt": "Lambda Invoke",
                        "States": {
                            "Lambda Invoke": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::lambda:invoke",
                                "OutputPath": "$.Payload",
                                "Parameters": {
                                    "Payload.$": "$",
                                    "FunctionName": lambda_arn
                                    },
                            "Retry": [
                                {
                                "ErrorEquals": [
                                    "Lambda.ServiceException",
                                    "Lambda.AWSLambdaException",
                                    "Lambda.SdkClientException",
                                    "Lambda.TooManyRequestsException",
                                    "States.TaskFailed"
                                    ],
                                "IntervalSeconds": 2,
                                "MaxAttempts": 6,
                                "BackoffRate": 2
                                }
                                ],
                            "End": True
                            }
                        }
                    }
                }
            }
        }
        return definition
        
    sf_def = make_def(lambda_arn)

    try:
        response = sfn.create_state_machine(
            name='pornhub_scraper_sm',
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn'],
            type='EXPRESS'
        )
    except sfn.exceptions.StateMachineAlreadyExists:
        response = sfn.list_state_machines()
        state_machine_arn = [sm['stateMachineArn'] 
                                for sm in response['stateMachines'] 
                                if sm['name'] == 'pornhub_scraper_sm'][0]
        response = sfn.update_state_machine(
            stateMachineArn=state_machine_arn,
            definition=json.dumps(sf_def),
            roleArn=role['Role']['Arn']
        )
    print(response)

    return sfn

with open('db_details.txt', 'r') as f:
    ENDPOINT = f.readline().strip()
    PORT = f.readline().strip()
    rdb_name = f.readline().strip()
    USERNAME = f.readline().strip()
    PASSWORD = f.readline().strip()

db_url = "mysql+mysqlconnector://{}:{}@{}:{}/books".format(USERNAME, PASSWORD, ENDPOINT, PORT)
db = dataset.connect(db_url)

lambda_db_url = "mysql+mysqlconnector://{}:{}@{}:{}/book_info".format(USERNAME, PASSWORD, ENDPOINT, PORT)

full_book_data = []
full_book_urls = []
r = None
base_url = 'http://books.toscrape.com/'
url = base_url
while True:
    try:
        r = requests.get(url)
        # break
    except:
        continue
    html_soup = BeautifulSoup(r.text, 'html.parser')
    book_data, book_urls = scrape_books(html_soup, url)
    full_book_data.extend(book_data)
    full_book_urls.extend(book_urls)
    # Is there a next page?
    next_a = html_soup.select('li.next > a')
    if not next_a or not next_a[0].get('href'):
        break
    url = urljoin(url, next_a[0].get('href'))

print('All done! Now inserting data into db and calling Lambda')
# invoke 50 async lambda functions to pull book data from each url
# and insert into db
num_lambda = 50
chunks = len(full_book_urls) // num_lambda

# break up urls into chunks of 50
url_chunks = [full_book_urls[x:x+chunks] for x in range(0, len(full_book_urls), chunks)]

# Get arn for Step Function state machine
response = sfn.list_state_machines()
state_machine_arn = [sm['stateMachineArn']
                        for sm in response['stateMachines'] 
                        if sm['name'] == 'scrape_books_sm'][0]

response = sfn.start_execution(
stateMachineArn=state_machine_arn,
name='async_test',
input=json.dumps(url_chunks)
)

for book in full_book_data:
    book_id, last_seen = book
    db['books'].upsert({'book_id': book_id, 'last_seen': last_seen}, ['book_id'])


if __name__ == '__main__':
    sfn = update_lammbda()

