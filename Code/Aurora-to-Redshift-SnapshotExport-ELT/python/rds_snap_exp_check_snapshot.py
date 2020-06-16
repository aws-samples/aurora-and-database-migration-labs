import boto3
from datetime import datetime, timezone

class SnapshotException(Exception):
    pass


def lambda_handler(event, context):
    
    # Input from Cloudwatch event rule
    
    aurora_cluster_id=event["aurora_cluster_id"]
    s3_bucket_for_rds_snap_exp=event["s3_bucket_for_rds_snap_exp"]
    iam_role_for_rds_snap_exp = event["iam_role_for_rds_snap_exp"]
    kms_key_id_for_rds_snap_exp = event["kms_key_id_for_rds_snap_exp"]
    export_list = event["export_list"]
    run_date=event["run_date"]
    
  
    #Get run_date for which snapshot export needs to happen. 
    
    if run_date == "":
        run_date= datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
    print('Run date is:' + run_date)
    
    stsclient = boto3.client('sts')
    
    response = stsclient.assume_role(
        DurationSeconds=3600,
        RoleArn=iam_role_for_rds_snap_exp,
        RoleSessionName='snapshot-export-demo-session'
    )
    
    ACCESS_KEY = response['Credentials']['AccessKeyId']
    SECRET_KEY = response['Credentials']['SecretAccessKey']
    SESSION_TOKEN = response['Credentials']['SessionToken']
    
    session = boto3.session.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN
    )
    
    
    rdsclient = session.client('rds')
    
    
    response = rdsclient.describe_db_cluster_snapshots(
        DBClusterIdentifier=aurora_cluster_id,
        SnapshotType='automated'
    )
    
    DBClusterSnapshots=response['DBClusterSnapshots']
    
    # Find a snapshot matching the run_date
    
    export_snapshot_arn = ''
    
    for DBClusterSnapshot in DBClusterSnapshots:
            
            snapshot_arn = DBClusterSnapshot['DBClusterSnapshotArn']
            snapshot_status = DBClusterSnapshot['Status']
            snapshot_date = datetime.strftime(DBClusterSnapshot['SnapshotCreateTime'], '%Y-%m-%d')
            
            #print (snapshot_arn,snapshot_status,snapshot_date)
            
            if snapshot_status == 'available' and snapshot_date == run_date:
                export_snapshot_arn = snapshot_arn
                print ('A valid snapshot to be exported matching the run date found: ' + snapshot_arn)
                break
    
    if export_snapshot_arn == '':
        
        print ('Valid snapshot to export not found. Exiting...')
        raise SnapshotException("Snapshot Not Found")
        
    else:
        return export_snapshot_arn