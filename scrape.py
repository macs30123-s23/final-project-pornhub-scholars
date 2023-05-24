################################################################################
"""
Creates the database tables for the pornhub data hosted on an AWS RDS instance.

Uses argpase for number of lambda functions to create and number of videos to scrape.
--num_lambda default is 10, num_pages default is 10 unless specified.

Calling this script with flag --update will create or update the lambda function.

a command line call of the form:
python scrape.py --update --num_lambda 10 --num_pages 10
will create 10 lambda functions and scrape 10 videos per lambda function.
or
python scrape.py -l 10 -n 10
"""
################################################################################

import boto3
import json
import zipfile
import configparser
import argparse
import os
import shutil
from tqdm import tqdm


aws_lambda = boto3.client('lambda', region_name='us-east-1')
iam_client = boto3.client('iam')
sqs = boto3.client('sqs', region_name='us-east-1')
s3 = boto3.client('s3')
role = iam_client.get_role(RoleName='LabRole')

config = configparser.ConfigParser()
config.read('db_details.ini')

def send_scrape(lambda_package, queue_url):
    """
    Submits a package for lambda to scrape to the SQS queue
    """

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(lambda_package),
    )

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print("Error sending message to queue: {}".format(response))

def update_lambda():
    """
    Lambda function deployment pipeline
    """
    # If zip file does not exist at current directory, download it from S3
    if not os.path.exists('lambda_deployment.zip'):
        print("Downloading zip file from S3...")
        s3.download_file('final-project-pornhub-macss', 'lambda_deployment.zip', 'lambda_deployment.zip')

    # Temporary directory for holding the unzipped files
    temp_dir = 'temp_dir'
    zip_file_name = 'lambda_deployment.zip'
    file_to_replace = 'lambda_function.py'

    # Extract all files to temporary directory
    print("Extracting files from zip...")
    with zipfile.ZipFile(zip_file_name, 'r') as zipf:
        zipf.extractall(path=temp_dir)

    # Remove the zip file
    os.remove(zip_file_name)

    with zipfile.ZipFile(zip_file_name, 'w') as zipf:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file == file_to_replace:
                    continue
                file_path = os.path.join(root, file)
                # Get the relative path from temp_dir
                arcname = os.path.relpath(file_path, temp_dir)
                # Write the file under its relative path
                zipf.write(file_path, arcname=arcname)

    # Add the new file_to_replace
    print("Adding new file to zip...")
    with zipfile.ZipFile(zip_file_name, 'a') as zipf:
        zipf.write(file_to_replace)

    # Delete the temporary directory
    print("Deleting temporary directory...")
    shutil.rmtree(temp_dir)

    # Upload the zip file to S3
    print("Uploading zip file to S3...")
    with open(zip_file_name, 'rb') as data:
        s3.upload_fileobj(data, 'final-project-pornhub-macss', zip_file_name)

    try:
        # If function hasn't yet been created, create it
        response = aws_lambda.create_function(
            FunctionName='pornhub_scraper',
            Runtime='python3.9',
            Role=role['Role']['Arn'],
            Handler='lambda_function.lambda_handler',
            Code={
                'S3Bucket': 'final-project-pornhub-macss',
                'S3Key': zip_file_name
            },
            Timeout=300
        )
    except aws_lambda.exceptions.ResourceConflictException:
        # If function already exists, update it based on zip
        # file contents
        print("Function already exists, updating...")
        response = aws_lambda.update_function_code(
            FunctionName='pornhub_scraper',
            S3Bucket='final-project-pornhub-macss',
            S3Key=zip_file_name
            )
    
    # Set concurrency limit
    try:
        aws_lambda.put_function_concurrency(
            FunctionName='pornhub_scraper',
            ReservedConcurrentExecutions=20
        )
        print("Concurrency limit set successfully")
    except Exception as e:
        print("Failed to set concurrency limit: ", e)

    # Create SQS Queue
    try:
        queue_url = sqs.create_queue(QueueName='scraper_queue')['QueueUrl']
    except sqs.exceptions.QueueNameExists:
        queue_url = [url
                    for url in sqs.list_queues()['QueueUrls']
                    if 'scraper_queue' in url][0]
    
    # Write queue url to config file
    config['QUEUE'] = {
        'queue_url': queue_url
    }
    # write the changes back to the file
    with open('db_details.ini', 'w') as configfile:
        config.write(configfile)

    # Set visibility timeout to 5 minutes (300 seconds) to be longer than the lambda timeout
    sqs.set_queue_attributes(
    QueueUrl=queue_url,
    Attributes={'VisibilityTimeout': '300'} 
    )

    sqs_info = sqs.get_queue_attributes(QueueUrl=queue_url,
                                        AttributeNames=['QueueArn'])
    sqs_arn = sqs_info['Attributes']['QueueArn']

    # Trigger Lambda Function when new messages enter SQS Queue
    try:
        response = aws_lambda.create_event_source_mapping(
            EventSourceArn=sqs_arn,
            FunctionName='pornhub_scraper',
            Enabled=True,
            BatchSize=1,
        )
    except aws_lambda.exceptions.ResourceConflictException:
        es_id = aws_lambda.list_event_source_mappings(
            EventSourceArn=sqs_arn,
            FunctionName='pornhub_scraper'
        )['EventSourceMappings'][0]['UUID']
        
        response = aws_lambda.update_event_source_mapping(
            UUID=es_id,
            FunctionName='pornhub_scraper',
            Enabled=True,
            BatchSize=1,
        )


def read_config():
    print("Reading config file...")
    ENDPOINT = config.get('DATABASE', 'ENDPOINT')
    PORT = config.get('DATABASE', 'PORT')
    rdb_name = config.get('DATABASE', 'rdb_name')
    USERNAME = config.get('DATABASE', 'USERNAME')
    PASSWORD = config.get('DATABASE', 'PASSWORD')
    sqs_url = config.get('QUEUE', 'queue_url')
    return ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD, sqs_url

def scrape(num_lambdas=10, num_pages=10):
    """
    
    """
    ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD, sqs_url = read_config()

    db_url = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USERNAME, PASSWORD, ENDPOINT, PORT, rdb_name)
    print("Using database url: {}".format(db_url))

    # the lambda package to send to the state machine
    print("Creating lambda package...")
    lambda_package = {
        'db_url': db_url,
        'num_pages': num_pages,
    }

    # the number of lambdas to invoke
    lambda_payload = [lambda_package for i in range(num_lambdas)]

    print("Invoking {} lambdas...".format(len(lambda_payload)))
    for payload in tqdm(lambda_payload):
        send_scrape(payload, sqs_url)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Python script that accepts command line arguments.")
    parser.add_argument('-l', '--num_lambdas', default=10, type=int, help="Number of lambdas")
    parser.add_argument('-n', '--num_pages', default=10, type=int, help="Number of pages")
    parser.add_argument('--update', action='store_true', help="Flag to trigger lambda update")

    args = parser.parse_args()
    
    if args.update:
        update_lambda()

    scrape(args.num_lambdas, args.num_pages)