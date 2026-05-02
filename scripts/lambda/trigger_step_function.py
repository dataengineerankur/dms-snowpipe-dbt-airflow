import os

import boto3
from botocore.exceptions import ClientError

SFN_ARN = os.environ.get("STATE_MACHINE_ARN", "")


def lambda_handler(event, context):
    if not SFN_ARN:
        raise ValueError("STATE_MACHINE_ARN must be set.")

    sfn = boto3.client("stepfunctions")
    try:
        sfn.start_execution(stateMachineArn=SFN_ARN, input="{}")
        return {"status": "started"}
    except ClientError as e:
        if e.response['Error']['Code'] == 'ExecutionAlreadyExists':
            return {"status": "already_running"}
        raise
