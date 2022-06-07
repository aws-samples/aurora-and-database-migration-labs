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
import subprocess #nosec B404
import fileinput


def getoptions():
    parser = argparse.ArgumentParser(
        description='Snap PostgreSQL performance statistics and exit',
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
                        help="Database Name where Application objects are stored",
                        required=True)                     
                        
    parser.add_argument("-u",
                        "--user",
                        help="Database UserName",
                        required=True)
                        
    parser.add_argument("-s",
                        "--SecretARN",
                        help="AWS Secrets Manager stored Secret ARN",
                        required=True)
    
    parser.add_argument("-m",
                        "--mode",
                        help="Mode in which the script will run: Specify either snap or package",
                        required=True)
                        
    parser.add_argument("-o",
                        "--outputdir",
                        help="Output Directory",
                        default=os.path.join(os.path.dirname(__file__),'output'))
                        
    parser.add_argument("-r",
                        "--region",
                        help="AWS region",
                        required=True)

    opts = parser.parse_args()

    return opts
    
def get_secret(secret_arn,region_name):
    
    logger = logging.getLogger("Snapper")

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

def runcmd(command,secret):
    
    logger = logging.getLogger("Snapper")
    
    try:
        return subprocess.check_call(command, stderr=subprocess.STDOUT, shell=True) #nosec B602
    except Exception as e:
        logger.error('Exception: ' + str(e).replace(secret,'*******'))
        sys.exit()
    
    
if __name__ == "__main__":
    
    # Parse and Validate Arguments
    opts = getoptions()

    # consume arguments 
    DBHOST = opts.endpoint
    DBPORT = opts.port
    DBNAME = opts.dbname
    DBUSER = opts.user
    SECRET_ARN = opts.SecretARN
    REGION_NAME = opts.region
    OUTPUT_DIR = opts.outputdir
    MODE = opts.mode
    
    # Create Log directory
    LOG_DIR=os.path.join(os.path.dirname(__file__),'log',DBHOST,DBNAME)

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # Create Output directory
    OUTPUT_DIR=os.path.join(OUTPUT_DIR,DBHOST,DBNAME)
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    # create logger
    logger = logging.getLogger("Snapper")
    logger.setLevel(logging.INFO)

    # create console handler and set level to ERROR
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    # create file handler and set level to INFO    
    fh = logging.FileHandler(os.path.join(LOG_DIR,'pg_perf_stat_snapper.log'))
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
    
    
    try:
        DBPASS = get_secret(SECRET_ARN,REGION_NAME)
        HOSTPORT=DBHOST + ':' + str(DBPORT)
        my_connection = connect(database=DBNAME, host=HOSTPORT, user=DBUSER, password=DBPASS, connect_timeout = 30)
        logger.info('SUCCESS: Connection to PostgreSQL instance ' + HOSTPORT + '/' + DBNAME + ' succeeded')
    
    except Exception as e:
        logger.error('Exception: ' + str(e).replace(DBPASS,'*******'))
        logger.error("ERROR: Unexpected error: Couldn't connect to the PostgreSQL instance.")
        sys.exit()
    
    try:
        with my_connection.cursor() as cur:
            
            drop_view_ddl=""
            
            # Set client_encoding to UTF-8, this prevents possible character encoding errors
            cur.execute("SET client_encoding=utf8")
            
            # Get PostgreSQL major version number
            cur.execute("SHOW server_version")
            pg_ver=int(cur.fetchone()[0].split(".", 1)[0])
            
           
            try:
                cur.execute("savepoint sp")
                cur.execute("SELECT AURORA_VERSION()")
                is_aurora=True
                
            except Exception as e:    
                is_aurora=False
                cur.execute("rollback to savepoint sp")
           
            
            if MODE == 'snap':
                
                # Create PGSnapper running file to prevent concurrent script runs for same host and database combination
                RUNNING_FILE=os.path.join(os.path.dirname(__file__),'.snapper_' + DBHOST + '_' + DBNAME + '.running')
    
                if os.path.exists(RUNNING_FILE):
                    
                    logger.info('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
                    logger.error("ERROR: Another instance of PGSnapper is already running for the same DBHOST and database. Exiting ... ")
                    logger.info('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
                    sys.exit()
                else:
                    runcmd("/bin/touch " + RUNNING_FILE,DBPASS)
                
                snap_md_fname=os.path.join(OUTPUT_DIR,'pg_awr_snapshots.csv')
                
                # Generate Snap ID
                if os.path.exists(snap_md_fname):
                    with open(snap_md_fname) as f:
                        data = f.readlines()
                        last_line = data[-1].split(',')
                        snap_id= int(last_line[0])+1
                else:
                    snap_id = 1
                
                logger.info('Starting SNAP with snap_id=' + str(snap_id))
                
                # Get Snap Begin Time
                cur.execute("select clock_timestamp()")
                snap_begin_time=cur.fetchone()[0]
                
                # Get all the queries to be Snapped
                snap_query_list=config['SNAP']
                
                for query_block in snap_query_list:
        
                    # Prefix query result with Snap ID
                    query_str=query_block["query"].replace('select','select ' + str(snap_id) + ',')
                    dump_file_name=os.path.join(OUTPUT_DIR,query_block["filename"])
                    
                    # For RDS PG, skip aurora_log_report snapping
                    if not is_aurora and "aurora_log_report" in query_str:
                        continue
                    
                    logger.info('  Dumping query output to ' + dump_file_name + ' ...')
                    
                    # Dump Data of SQL Queries in configuration file
                    try:
                        with open(dump_file_name, 'a+') as f:
                            cur.execute("SET client_min_messages TO WARNING")
                            cur.execute("savepoint sp")
                            cur.copy_to(f,query_str,'csv')
                            f.close()
                    except Exception as e:
                        logger.error('  Error While running query: ' + query_str)
                        logger.error('  Exception: ' + str(e))
                        f.close()
                        cur.execute("rollback to savepoint sp")
                        continue
                    
                # Get Snap End Time
                cur.execute("select clock_timestamp()")
                snap_end_time=cur.fetchone()[0]
                
                # Store snapshot metda data in pg_awr_snapshots.csv
                snap_md = str(snap_id) + ',' + str(snap_begin_time) + ',' + str(snap_end_time)
                with open(snap_md_fname,'a+') as f:
                    f.write(snap_md)
                    f.write('\n')
                
                logger.info('Finished SNAP with snap_id=' + str(snap_id))
                
                # Snapping complete. Remove running file.
                os.remove(RUNNING_FILE)
                
            elif MODE == 'package':
                
                logger.info('Starting Packaging ...')
                
                # Get all the queries to be Snapped
                package_query_list=config['PACKAGE']
                
                for query_block in package_query_list:
        
                    # Prefix query result with Snap ID
                    query_str=query_block["query"]
                    dump_file_name=os.path.join(OUTPUT_DIR,query_block["filename"])
                    
                    logger.info('  Dumping query output to ' + dump_file_name + ' ...')
                    
                    # If the query is a CTE, create a view to get around pygresql limitation for copy_to
                    if query_str.lower().startswith('with'):
                        
                        create_view_ddl='CREATE VIEW v_snapper_' + query_block["target"] + ' AS ' + query_str + ";"
                        drop_view_ddl=drop_view_ddl + 'DROP VIEW v_snapper_' + query_block["target"] + ";"
                        logger.info('   Creating temporary View v_snapper_' + query_block["target"] + ' ...')
                        runcmd("PGPASSWORD='" + DBPASS + "'" + " /usr/local/pgsql/bin/psql --host=" + DBHOST + " --port=" + DBPORT + " --username=" + DBUSER + " --dbname=" + DBNAME + " --quiet" + " --echo-errors" + " --command=" + '"' + create_view_ddl + '"' + " 2>>" + os.path.join(LOG_DIR,'pg_perf_stat_snapper.log'),DBPASS)
                        query_str='SELECT * FROM ' + 'v_snapper_' + query_block["target"] #nosec B608
                    
                    # If PG version >= 12 then oid is already included as a visible column for pg_class and pg_namespace
                    if pg_ver >= 12:
                        query_str = query_str.replace("select oid,","select ",1)
                    
                    
                    # Dump Data of SQL Queries in configuration file
                    try:
                        with open(dump_file_name, 'w+') as f:
                            cur.execute("SET client_min_messages TO WARNING")
                            cur.execute("savepoint sp")
                            cur.copy_to(f,query_str,'csv')
                            f.close()
                    except Exception as e:
                        logger.error('  Error While running query: ' + query_str)
                        logger.error('  Exception: ' + str(e))
                        f.close()
                        cur.execute("rollback to savepoint sp")
                        continue
                    
                logger.info('  Generating DDL Extraction Input file ...') 
                
                ddl_gen_file_name=os.path.join(OUTPUT_DIR,'ddl_gen_input.sql')
                
                # generate DDL extraction input file
                with open(ddl_gen_file_name, 'w+') as f:
                        f.write("\pset format unaligned\r\n")
                        f.write("\pset recordsep ','\r\n")
                        f.write("\pset fieldsep ' '\r\n")
                        f.write("\pset tuples_only\r\n")
                        f.write("\set QUIET 1\r\n")
                        f.write("\o " + OUTPUT_DIR + "/all_ddls.sql\r\n")
                        
                # For snapped and packaged tables, generate DDL
                ddl_obj_list=config['SNAP']
                ddl_query_list=config['PACKAGE']
                ddl_obj_list.extend(ddl_query_list)
                
                for obj_block in ddl_obj_list:
                    
                    if "ddl_query" not in obj_block:
                        
                        obj_name=obj_block["object"]
                        target_obj_name=obj_block["target"]
                        add_snap_id=obj_block["add_snap_id"]
                    
                        with open(ddl_gen_file_name, 'a+') as f:
                            if add_snap_id == 1:
                                f.write("\qecho 'create table " + target_obj_name + " (snap_id bigint,'\r\n")
                            else:
                                f.write("\qecho 'create table " + target_obj_name + " ('\r\n")
                            f.write("\d " + obj_name + "\r\n")
                            f.write("\qecho ');'\r\n")
                    else:
                        # For snapped and packaged SQL Queries, create a temp table and then extract the DDL for the temp table
                        query_str=obj_block["ddl_query"]
                        target_obj_name=obj_block["target"]
                        add_snap_id=obj_block["add_snap_id"]
                        
                        # For RDS PG, skip aurora_log_report DDL generation
                        if not is_aurora and "aurora_log_report" in query_str:
                            continue
                        
                        # If PG version >= 12 then oid is already included as a visible column for pg_class and pg_namespace
                        if pg_ver >= 12:
                            query_str = query_str.replace("select oid,","select ", 1)
                        
                        with open(ddl_gen_file_name, 'a+') as f:
                            f.write("create temp table " + target_obj_name + " as " + query_str + ";" + "\r\n")
                        
                            if add_snap_id == 1:
                                f.write("\qecho 'create table " + target_obj_name + " (snap_id bigint,'\r\n")
                            else:
                                f.write("\qecho 'create table " + target_obj_name + " ('\r\n")
                        
                            f.write("\d " + target_obj_name + "\r\n")
                            f.write("\qecho ');'\r\n")
                
                # Extract DDL using the DDL generation input file 
                logger.info('  Extracting all DDLs ...')
                
                runcmd("PGPASSWORD='" + DBPASS + "'" + " /usr/local/pgsql/bin/psql --host=" + DBHOST + " --port=" + DBPORT + " --username=" + DBUSER + " --dbname=" + DBNAME + " --file=" + ddl_gen_file_name + " --quiet" + " --echo-errors" + " 2>>" + os.path.join(LOG_DIR,'pg_perf_stat_snapper.log'),DBPASS)
                
                # delete DDL extraction input file
                os.remove(ddl_gen_file_name)
                
                # Append pg_awr_snapshots DDL to all_ddls.sql
                with open(os.path.join(OUTPUT_DIR, 'all_ddls.sql'),'a+') as f:
                    f.write("create table pg_awr_snapshots_cust (snap_id bigint,sample_start_time timestamp with time zone,sample_end_time timestamp with time zone);")
                    f.write('\n')
                
                # Replace column datatype in all_ddls.sql to support data import later using copy command
                
                # Get all Datatype substitutions that need to be done in all_ddls.sql
                replace_type_list=config['DATA_TYPE_REPLACE']
                
                logger.info('  Doing inplace replace of some column datatypes in the generated DDL ...')
                
                for replace_block in replace_type_list:
                    
                    source_type=replace_block["source"]
                    target_type=replace_block["target"]
                
                    #for line in fileinput.input(os.path.join(OUTPUT_DIR, 'all_ddls.sql'), inplace=True, backup='.bak'):
                    for line in fileinput.input(os.path.join(OUTPUT_DIR, 'all_ddls.sql'), inplace=True):
                        print(line.replace(source_type, target_type), end='')
                        
                logger.info('Packaging Completed Successfully ...')
                print('Packaging Completed Successfully ...')
                
            else:
                logger.error("ERROR: Invalid MODE specified")
            
            # Commit transaction and Close cursor, connection
            my_connection.commit()
            cur.close()
            my_connection.close()
            
            if drop_view_ddl:
                logger.info('   Dropping temporary Views created by PGSnapper' + ' ...')
                runcmd("PGPASSWORD='" + DBPASS + "'" + " /usr/local/pgsql/bin/psql --host=" + DBHOST + " --port=" + DBPORT + " --username=" + DBUSER + " --dbname=" + DBNAME + " --quiet" + " --echo-errors" + " --command=" + '"' + drop_view_ddl + '"' + " 2>>" + os.path.join(LOG_DIR,'pg_perf_stat_snapper.log'),DBPASS)
                    
    except Exception as e:
        logger.error('Exception: ' + str(e).replace(DBPASS,'*******'))