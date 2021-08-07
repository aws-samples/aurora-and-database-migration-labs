#!/usr/bin/env python3

from __future__ import print_function
import boto3
import logging
import argparse
import sys
# import DB-API 2.0 compliant module for PygreSQL 
from pgdb import connect
from botocore.exceptions import ClientError
import json
import os
import subprocess


def getoptions():
    parser = argparse.ArgumentParser(
        description='Load PostgreSQL performance statistics collected by Snapper and exit',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-e",
                        "--endpoint",
                        help="PostgreSQL Instance Endpoint",
                        required=True)

    parser.add_argument("-P",
                        "--port",
                        help="Port",
                        required=True)

    parser.add_argument("-d",
                        "--dbname",
                        help="Database Name",
                        default='postgres')                      
                        
    parser.add_argument("-u",
                        "--user",
                        help="Database UserName",
                        required=True)
                        
    parser.add_argument("-s",
                        "--SecretARN",
                        help="AWS Secrets Manager stored Secret ARN",
                        required=True)
                        
    parser.add_argument("-o",
                        "--stagingdir",
                        help="Directory containing the snapper generated csv files",
                        required=True)                      
                        
    parser.add_argument("-r",
                        "--region",
                        help="AWS region",
                        required=True)

    opts = parser.parse_args()

    return opts
    
def get_secret(secret_arn,region_name):
    
    logger = logging.getLogger("Loader")

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

def runcmd(command):
    
    logger = logging.getLogger("Loader")
    
    #return subprocess.call(command, shell=True)
    try:
        return subprocess.check_call(command, stderr=subprocess.STDOUT, shell=True)
    except Exception as e:
        logger.error('Exception: ' + str(e))
    
    
if __name__ == "__main__":

    #Parse and Validate Arguments
    opts = getoptions()

    # consume arguments 
    DBHOST = opts.endpoint
    DBPORT = opts.port
    DBNAME = opts.dbname
    DBUSER = opts.user
    SECRET_ARN = opts.SecretARN
    REGION_NAME = opts.region
    STAGING_DIR = opts.stagingdir
    
    # Create Log directory
    LOG_DIR=os.path.join(os.path.dirname(__file__),'log')

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # create logger
    logger = logging.getLogger("Loader")
    logger.setLevel(logging.INFO)

    # create console handler and set level to ERROR
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    # create file handler and set level to INFO
    fh = logging.FileHandler(os.path.join(LOG_DIR,'pg_perf_stat_loader.log'))
    fh.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # add formatter to ch and fh
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add ch and fh to logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    # load all config settings
    with open(os.path.join(os.path.dirname(__file__),'config_pg_perf_stat_snapper.json'), 'r') as f:
        config = json.load(f)
    
    logger.info('__________________________________________________________________________________________________________________')
    
    if not os.path.exists(STAGING_DIR):
        logger.error("ERROR: Staging Directory doesn't exist")
        sys.exit()
    
    try:
        DBPASS = get_secret(SECRET_ARN,REGION_NAME)
        logger.info('SUCCESS: Password retrieval for PostgreSQL instance succeeded')
    
    except Exception as e:
        logger.error('Exception: ' + str(e))
        logger.error("ERROR: Unexpected error: Couldn't retrieve password for the PostgreSQL instance.")
        sys.exit()
        
    try:
        ISV_DBNAME = input("Enter Database name to be created for importing PostgreSQL performance statistics:")
    
        logger.info('Setting up Database for importing Snapper related table(s) ...')
        runcmd("PGPASSWORD='" + DBPASS + "'" + " /usr/local/pgsql/bin/psql --host=" + DBHOST + " --port=" + DBPORT + " --username=" + DBUSER + " --dbname=" + DBNAME + " --command='" + "CREATE DATABASE " +  ISV_DBNAME + ";'" + " --quiet" + " --echo-errors" + " 2>>" + os.path.join(LOG_DIR,'pg_perf_stat_loader.log'))
        logger.info('Database ' + ISV_DBNAME + ' created for importing Snapper related table(s) ...')
    
        ddl_file_name = os.path.join(STAGING_DIR,'all_ddls.sql')
    
        logger.info('Creating Snapper related table(s) ...')
        runcmd("PGPASSWORD='" + DBPASS + "'" + " /usr/local/pgsql/bin/psql --host=" + DBHOST + " --port=" + DBPORT + " --username=" + DBUSER + " --dbname=" + ISV_DBNAME + " --file=" + ddl_file_name + " --quiet" + " --echo-errors" + " 2>>" + os.path.join(LOG_DIR,'pg_perf_stat_loader.log'))
        
    except Exception as e:
        logger.error('Exception: ' + str(e))
        logger.error("ERROR: Unexpected error: Couldn't run DCL/DDL statements in the PostgreSQL instance.")
        sys.exit()
    
    try:
        HOSTPORT=DBHOST + ':' + str(DBPORT)
        my_connection = connect(database=ISV_DBNAME, host=HOSTPORT, user=DBUSER, password=DBPASS, connect_timeout = 30)
        logger.info('SUCCESS: Connection to PostgreSQL instance ' + HOSTPORT + '/' + ISV_DBNAME + ' succeeded')
        
    except Exception as e:
        logger.error('Exception: ' + str(e))
        logger.error("ERROR: Unexpected error: Couldn't connect to the PostgreSQL instance.")
        sys.exit()
    
    try:
        load_obj_list=config['SNAP']
        load_query_list=config['PACKAGE']
        load_obj_list.extend(load_query_list)
        
        with my_connection.cursor() as cur:
            
            for filename in os.listdir(STAGING_DIR):
                if filename.endswith(".csv"):
                    if os.path.getsize(os.path.join(STAGING_DIR, filename)) > 0:
                        logger.info('  Loading file ' + filename + ' ...')
                     
                        if filename == 'pg_awr_snapshots.csv':
                            table_name='pg_awr_snapshots_cust'
                        else:
                            try:
                                table_name=next(item for item in load_obj_list if item["filename"] == filename)["target"]
                            except Exception as e:
                                 logger.error('  Target table name for: ' + filename + ' not found in config file')
                                 continue
                        try:
                            with open(os.path.join(STAGING_DIR, filename), 'r') as f:
                                cur.copy_from(f,table_name,'csv')
                                f.close()
                        except Exception as e:
                            logger.error('  Error loading file: ' + filename)
                            logger.error('   Exception: ' + str(e))
                            logger.error('  Abandoning loading ...')
                            f.close()
                            cur.close()
                            my_connection.close()
                            sys.exit()
                    else:
                         logger.info('  Skipping file ' + filename + ' since filesize is 0 ...')
                         
            logger.info('Loading of all Snapper related data completed successfully ...')
            print('Loading of all Snapper related data completed successfully ...')
                
        # Commit transaction and Close cursor, connection
        my_connection.commit()
        cur.close()
        my_connection.close()

    except Exception as e:
        logger.error('Exception: ' + str(e))