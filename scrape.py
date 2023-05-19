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

aws_lambda = boto3.client('lambda', region_name='us-east-1')
iam_client = boto3.client('iam')
s3 = boto3.client('s3')
role = iam_client.get_role(RoleName='LabRole')

config = configparser.ConfigParser()

# Read existing configuration file, or create an empty one if it doesn't exist
config.read('config.ini')

def update_lambda():
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
        print("Function doesn't exist, creating...")
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

    lambda_arn = response['FunctionArn']

    sfn = boto3.client('stepfunctions', region_name='us-east-1')
        
    sf_def = make_def(lambda_arn)

    print("Creating/updating state machine...")
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

def read_config():
    print("Reading config file...")
    config = configparser.ConfigParser()
    config.read('db_details.ini')
    ENDPOINT = config.get('DATABASE', 'ENDPOINT')
    PORT = config.get('DATABASE', 'PORT')
    rdb_name = config.get('DATABASE', 'rdb_name')
    USERNAME = config.get('DATABASE', 'USERNAME')
    PASSWORD = config.get('DATABASE', 'PASSWORD')
    return ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD

def scrape(num_lambdas=10, num_pages=10):
    """
    
    """
    ENDPOINT, PORT, rdb_name, USERNAME, PASSWORD = read_config()

    db_url = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(USERNAME, PASSWORD, ENDPOINT, PORT, rdb_name)
    print("Using database url: {}".format(db_url))

    # Get arn for Step Function state machine
    sfn = boto3.client('stepfunctions', region_name='us-east-1')
    response = sfn.list_state_machines()
    state_machine_arn = [sm['stateMachineArn']
                            for sm in response['stateMachines'] 
                            if sm['name'] == 'pornhub_scraper_sm'][0]
    print("Using state machine arn: {}".format(state_machine_arn))

    # the lambda package to send to the state machine
    print("Creating lambda package...")
    lambda_package = {
        'db_url': db_url,
        'num_pages': num_pages,
    }

    # the number of lambdas to invoke
    num_lambdas = [lambda_package for i in range(num_lambdas + 1)]
    print("Invoking {} lambdas...".format(len(num_lambdas)))

    response = sfn.start_execution(
    stateMachineArn=state_machine_arn,
    name='async_test',
    input=json.dumps(lambda_package)
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Python script that accepts command line arguments.")
    parser.add_argument('-l', '--num_lambdas', default=10, type=int, help="Number of lambdas")
    parser.add_argument('-n', '--num_pages', default=10, type=int, help="Number of pages")
    parser.add_argument('--update', action='store_true', help="Flag to trigger lambda update")

    args = parser.parse_args()
    
    if args.update:
        update_lambda()

    scrape(args.num_lambdas, args.num_pages)

