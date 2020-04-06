import json
import os
import pg8000

from datetime import datetime

print('Loading function')

PGDATABASE = os.environ['PG_DATABASE']
PGHOST = os.environ['PG_HOST']
PGPORT = int(os.environ['PG_PORT'].strip())
PGUSER = os.environ['PG_USER']
PGPASSWORD = os.environ['PG_PASSWORD']

print ("DB Connection. DataBase= " + PGDATABASE + ", Host= " + PGHOST + ", PORT= " + str(PGPORT) + ", User= " + PGUSER)

# This function filters out the records from the dynamodb stream that represent trip completion i.e. STATUS == Completed
def getTripCompletionRecords(ddbRecords):
    
    completedTripList = []
    
    for record in ddbRecords:
        if (record['eventName'] == "MODIFY"): #The ride completion event is always an update an existed item for the ride entry
            newInfo = record["dynamodb"]["NewImage"] 
            
            print(newInfo)

            if('STATUS' in newInfo and newInfo['STATUS']['S'] == "Completed"):
                
                completedtripInfo = {
                    "rider_mobile": "" if 'RIDER_MOBILE' not in newInfo else newInfo['RIDER_MOBILE']['S'],
                    "pickup_datetime": "" if 'PICKUP_DATETIME' not in newInfo else datetime.strptime(newInfo['PICKUP_DATETIME']['S'], "%Y-%m-%dT%H:%M:%S%z").strftime('%Y-%m-%d %H:%M:%S'),
                    "rider_name": "" if 'RIDER_NAME' not in newInfo else newInfo['RIDER_NAME']['S'],
                    "vendor_id": "" if 'VENDOR_ID' not in newInfo else newInfo['VENDOR_ID']['N'],
                    "pickup_longitude": "" if 'PICKUP_LONGITUDE' not in newInfo else newInfo['PICKUP_LONGITUDE']['N'],
                    "trip_type": "" if 'TRIP_TYPE' not in newInfo else newInfo['TRIP_TYPE']['N'],
                    "store_and_fwd_flag": "" if 'STORE_AND_FWD_FLAG' not in newInfo else newInfo['STORE_AND_FWD_FLAG']['S'],
                    "dropoff_latitude": "" if 'DROPOFF_LATITUDE' not in newInfo else newInfo['DROPOFF_LATITUDE']['N'],
                    "rate_code_id": "" if 'RATE_CODE_ID' not in newInfo else newInfo['RATE_CODE_ID']['N'],
                    "tolls_amount": "" if 'TOLLS_AMOUNT' not in newInfo else newInfo['TOLLS_AMOUNT']['N'],
                    "improvement_surcharge": "" if 'IMPROVEMENT_SURCHARGE' not in newInfo else newInfo['IMPROVEMENT_SURCHARGE']['N'],
                    "tip_amount": "" if 'TIP_AMOUNT' not in newInfo else newInfo['TIP_AMOUNT']['N'],
                    "dropoff_datetime": "" if 'DROPOFF_DATETIME' not in newInfo else datetime.strptime(newInfo['DROPOFF_DATETIME']['S'], "%Y-%m-%dT%H:%M:%S%z").strftime('%Y-%m-%d %H:%M:%S'),
                    "cab_type_id": "" if 'CAB_TYPE_ID' not in newInfo else newInfo['CAB_TYPE_ID']['N'],
                    "driver_name": "" if 'DRIVER_NAME' not in newInfo else newInfo['DRIVER_NAME']['S'],
                    "pickup_latitude": "" if 'PICKUP_LATITUDE' not in newInfo else newInfo['PICKUP_LATITUDE']['N'],
                    "trip_distance": "" if 'TRIP_DISTANCE' not in newInfo else newInfo['TRIP_DISTANCE']['N'],
                    "vehicle_id": "" if 'VEHICLE_ID' not in newInfo else newInfo['VEHICLE_ID']['S'],
                    "total_amount": "" if 'TOTAL_AMOUNT' not in newInfo else newInfo['TOTAL_AMOUNT']['N'],
                    "mta_tax": "" if 'MTA_TAX' not in newInfo else newInfo['MTA_TAX']['N'],
                    "dropoff_longitude": "" if 'DROPOFF_LONGITUDE' not in newInfo else newInfo['DROPOFF_LONGITUDE']['N'],
                    "payment_type": "" if 'PAYMENT_TYPET' not in newInfo else newInfo['PAYMENT_TYPET']['N'],
                    "driver_id": "" if 'DRIVER_ID' not in newInfo else newInfo['DRIVER_ID']['N'],
                    "driver_email": "" if 'DRIVER_EMAIL' not in newInfo else newInfo['DRIVER_EMAIL']['S'],
                    "trip_Info": "" if 'tripinfo' not in newInfo else newInfo['tripinfo']['S'],
                    "riderId": "" if 'RIDER_ID' not in newInfo else newInfo['RIDER_ID']['N'],
                    "extra": "" if 'EXTRA' not in newInfo else newInfo['EXTRA']['N'],
                    "fare_amount": "" if 'FARE_AMOUNT' not in newInfo else newInfo['FARE_AMOUNT']['N'],
                    "passenger_count": "" if 'PASSENGER_COUNT' not in newInfo else newInfo['PASSENGER_COUNT']['N'],
                    "rider_email": "" if 'RIDER_EMAIL' not in newInfo else newInfo['RIDER_EMAIL']['S'],
                    "driver_mobile": "" if 'DRIVER_MOBILE' not in newInfo else newInfo['DRIVER_MOBILE']['S'],
                    "status": "Completed"
                }
                # "<>": "" if 'driverid' not in newInfo else newInfo['driverid']['S'],
                # "<>": "" if 'DriverDetails' not in newInfo else newInfo['DriverDetails']['S'],
                # "<>": "" if 'Vehicle Details' not in newInfo else newInfo['Vehicle Details']['S'],
                # "<>": "" if 'ID' not in newInfo else newInfo['ID']['S'],
                
                print("Completed Trip Informaiton = " + json.dumps(completedtripInfo, indent = 2))

                completedTripList.append(completedtripInfo)

    return  completedTripList
    
def createInsertSQLQueries(completedTripList):
    insertSQLQuries = []
    
    queryStringBase = """INSERT INTO public.trips (
            rider_id, driver_id, rider_name, rider_mobile, rider_email, trip_info, driver_name,
            driver_email, driver_mobile, vehicle_id, cab_type_id, vendor_id, pickup_datetime,
            dropoff_datetime, store_and_fwd_flag, rate_code_id, pickup_longitude, pickup_latitude,
            dropoff_longitude, dropoff_latitude, passenger_count, trip_distance, fare_amount, extra,
            mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type,
            trip_type, pickup_location_id, dropoff_location_id, status)"""
            
    for tripInfo in completedTripList:
        queryString = queryStringBase + " VALUES(" 
        queryString += tripInfo['riderId'] + "," + tripInfo['driver_id'] + ",'" + tripInfo['rider_name'] + "','" + tripInfo['rider_mobile'] + "','" + tripInfo['rider_email'] + "','" + tripInfo['trip_Info'] + "','"
        queryString += tripInfo['driver_name'] + "','" + tripInfo['driver_email'] + "','" + tripInfo['driver_mobile'] + "','" + tripInfo['vehicle_id'] + "'," + tripInfo['cab_type_id'] + ","
        queryString += tripInfo['vendor_id'] + ",'" + tripInfo['pickup_datetime'] + "','" + tripInfo['dropoff_datetime'] + "','" + tripInfo['store_and_fwd_flag'] + "'," + tripInfo['rate_code_id'] + ","
        queryString += tripInfo['pickup_longitude'] + "," + tripInfo['pickup_latitude'] + "," + tripInfo['dropoff_longitude'] + "," + tripInfo['dropoff_latitude'] + "," + tripInfo['passenger_count'] + ","
        queryString += tripInfo['trip_distance'] + "," + tripInfo['fare_amount'] + "," + tripInfo['extra'] + "," + tripInfo['mta_tax'] + "," + tripInfo['tip_amount'] + ","
        queryString += tripInfo['tolls_amount'] + "," + "0" + "," + tripInfo['improvement_surcharge'] + "," + tripInfo['total_amount'] + "," + tripInfo['payment_type'] + ","
        queryString += tripInfo['trip_type'] + "," + "0" + "," + "0" + ",'" + tripInfo['status'] + "');"
        
        print("Trip information =" + json.dumps(tripInfo, indent = 2))
        print ("Insert query string =" + queryString)
        
        insertSQLQuries.append(queryString)
        
    return insertSQLQuries
    
def pusblishTripCompletionInfo(sqlCmds):
    
    conn = pg8000.connect(database=PGDATABASE, host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD)
    cursor = conn.cursor()
    
    for cmd in sqlCmds:
        print("Executing Query:" + cmd)
        cursor.execute(cmd)
        print("Completed Executing Query:" + cmd)
        
    #queryOuts = cursor.fetchall()
    cursor.close()
    conn.commit()
        
    #for out in queryOuts:
    #    print(out)
        
    return
    
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    print("Filtering trip completion records ... InProgress")
    completedTripInfoList = getTripCompletionRecords(event['Records'])
    print("Filtering trip completion records ... Completed")
    
    if(len(completedTripInfoList) > 0 ):
        print("Create insert SQL commands ... InProgress")
        insertSQLQueries = createInsertSQLQueries(completedTripInfoList)
        print("Create insert SQL commands ... Completed")
     
        print("Publish completed trip informaiton ... InProgress")
        pusblishTripCompletionInfo(insertSQLQueries)
        print("Publish completed trip informaiton ... Completed")
    else:
        print("No Trip Completion Records found.")
    
    return 'Successfully processed {} records.'.format(len(event['Records']))