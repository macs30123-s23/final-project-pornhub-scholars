import boto3
import json

aws_lambda = boto3.client('lambda', region_name='us-east-1')
iam_client = boto3.client('iam')
role = iam_client.get_role(RoleName='LabRole')

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
