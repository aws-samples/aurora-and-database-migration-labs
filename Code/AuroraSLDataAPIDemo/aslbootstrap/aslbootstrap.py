import sys
import logging
import pymysql
import boto3
import os
from botocore.exceptions import ClientError
import cfnresponse
import json

#Get variables
db_host = os.environ['DB_HOST']
user_name = os.environ['USER_NAME']
secret_arn = os.environ['SECRET_ARN']
region_name = os.environ['REGION_NAME']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Here cfnresponse.SUCCESS is sent even when the operations fail as we dont want the whole cloudformation stack to rollback because the bootstrap of the DB failed
# You can always inspect(using cloudwatch log stream assosiated with the lambda function) why the bootstrap SQLs failed and manually run the SQLs to bootstrap the newly created Database


def handler(event, context):
    """
    This function bootstraps Aurora MySQL Cluster
    """
    
    responseData = {}
    
    if event['RequestType'] == 'Create':
    
        try:
            #Get Secret
            dbpass = get_secret(secret_arn,region_name)
            logger.info("SUCCESS: Retrieved Secret successfully")
            conn = pymysql.connect(db_host, user=user_name, passwd=dbpass, connect_timeout=60)
            logger.info("SUCCESS: Connection to Aurora MySQL Serverless Cluster succeeded")
        except Exception as e:
            logger.error("ERROR: Unexpected error: Could not connect to Aurora MySQL Serverless Cluster")
            logger.error('Exception: ' + str(e))
            responseData['Data'] = "ERROR: Unexpected error: Couldn't connect to Aurora PostgreSQL instance."
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, "None")
            
        try:
            with conn.cursor() as cur:
                cur.execute("create database Demo")
                cur.execute("create table Demo.Cities(City varchar(255))")
                conn.commit()
            logger.info("SUCCESS: Executed SQL statements successfully.")
            responseData['Data'] = "SUCCESS: Executed SQL statements successfully."
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, "None")
        except Exception as e:
            logger.error("ERROR: Unexpected error: Bootstrap of Aurora MySQL Serverless Cluster failed") 
            logger.error('Exception: ' + str(e))
            responseData['Data'] = "ERROR: Unexpected error: Bootstrap of Aurora MySQL Serverless Cluster failed"
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, "None")
            
    else:
        logger.info("{} is unsupported stack operation for this lambda function.".format(event['RequestType']))
        responseData['Data'] = "{} is unsupported stack operation for this lambda function.".format(event['RequestType'])
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData, "None")
      
def get_secret(secret_arn,region_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_arn
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("Secrets Manager can't decrypt the protected secret text using the provided KMS key")
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("An error occurred on the server side")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("You provided an invalid value for a parameter")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("You provided a parameter value that is not valid for the current state of the resource")
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("We can't find the resource that you asked for")
    else:
        # Decrypts secret using the associated KMS CMK.
        secret = json.loads(get_secret_value_response['SecretString'])['password']
        return secret     