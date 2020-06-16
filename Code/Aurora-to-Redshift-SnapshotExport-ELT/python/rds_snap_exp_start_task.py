import boto3
from datetime import datetime, timezone

class TaskException(Exception):
    pass
	
def lambda_handler(event, context):
    
    # Input from Cloudwatch event rule
    
    aurora_cluster_id=event["aurora_cluster_id"]
    s3_bucket_for_rds_snap_exp=event["s3_bucket_for_rds_snap_exp"]
    iam_role_for_rds_snap_exp = event["iam_role_for_rds_snap_exp"]
    kms_key_id_for_rds_snap_exp = event["kms_key_id_for_rds_snap_exp"]
    export_list = event["export_list"]
    run_date=event["run_date"]
    export_snapshot_arn=event["snapshot_arn"]
  
    
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
    
    # check if snapshot export is already running
    
    response = rdsclient.describe_export_tasks(
    SourceArn=export_snapshot_arn
    )
    
    ExportTasksList=response['ExportTasks']
    
    start_export_task_flag = True
    
    for ExportTask in ExportTasksList:
        if ExportTask['Status'] != 'CANCELED':
            start_export_task_flag = False
            export_task_identifier = ExportTask['ExportTaskIdentifier']
            break
        
    if start_export_task_flag == True:
        
        print ('Starting RDS snapshot export task...')
        
        export_task_identifier = aurora_cluster_id + '-' + datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        
        response = rdsclient.start_export_task(
        ExportTaskIdentifier=export_task_identifier,
        SourceArn=export_snapshot_arn,
        S3BucketName=s3_bucket_for_rds_snap_exp,
        IamRoleArn=iam_role_for_rds_snap_exp,
        KmsKeyId=kms_key_id_for_rds_snap_exp,
        ExportOnly=[
            export_list
        ]
        )
        
        return {
            'export_task_started': True,
            'export_task_identifier': export_task_identifier
        }    
    
    else:
	    
        print ('Snapshot Export Task is already initiated...')		
        
        return {
            'export_task_started': False,
            'export_task_identifier': export_task_identifier
        }
    