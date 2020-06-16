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
    export_task_identifier=event["Export_Task_Details"]["export_task_identifier"]
    
    db_name=export_list.split('.')[0]
    
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
    
    
    response = rdsclient.describe_export_tasks(
    ExportTaskIdentifier=export_task_identifier
    )
    
    #Check the last element in the List as there can be multiple snapshot export task runs for the same RDS snapshot
    
    ExportTasksStatus=response['ExportTasks'][-1]['Status']
    PercentProgress=response['ExportTasks'][-1]['PercentProgress']
    
    s3path='s3://' + s3_bucket_for_rds_snap_exp + '/' + export_task_identifier + '/' + db_name
    
    print (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + ' ExportTaskID: ' + export_task_identifier + ' ExportTaskStatus: ' + ExportTasksStatus + ' PercentProgress:' + str(PercentProgress))
    
    if ExportTasksStatus == 'COMPLETE':
        print ('RDS snapshot export task ' + export_task_identifier + ' completed successfully.')	
    
    return {
        'ExportTasksStatus' : ExportTasksStatus,
        's3path' : s3path
    }