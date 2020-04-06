import boto3
import decimal 
import json
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('aws-db-workshop-trips')


def getTripInfo(infoReq):
    
    try:
        response = table.get_item(
            Key={
                "riderid":  infoReq['riderid'], 
                "tripinfo": infoReq['tripinfo'] 
            }
        )
        
        try:
            print("GetItem succeeded...Response" + json.dumps(response, indent=4))
        except TypeError as te:
            print("!Warning! = " + str(te))
            print("GetItem succeeded...Response =")
            print(response)
            
        if 'Item' in response:
            return response['Item']
        else:
            return "Item not found in table"

    except ClientError as e:
        print("Put Item Error=" + e.response['Error']['Message'])
    

def updateTripInfo(newInfo, expectedTripStatus = None):
    
    
    key = {
        'riderid': newInfo['riderid'],
        'tripinfo':newInfo['tripinfo'],        
    }
    
    updateExp = "SET "
    
    expAttrNames={
        "#st": "STATUS"
    }
    
    updateExp += "#st = :s, "
    newItem = {
        ':s': newInfo['Status']
    }
    
    if(expectedTripStatus is None):
        conditionalExp = "attribute_not_exists(#st)"
    else:
        newItem.update({":ps": expectedTripStatus})
        conditionalExp = "#st=:ps"
        
    if "RIDER_ID" in newInfo:
        updateExp += "RIDER_ID = :rid, "
        newItem.update({":rid": newInfo['RIDER_ID']})
    if 'RIDER_MOBILE' in newInfo:
        updateExp += "RIDER_MOBILE = :rm, "
        newItem.update({":rm": newInfo['RIDER_MOBILE']})
    if 'PICKUP_DATETIME' in newInfo:
        updateExp += "PICKUP_DATETIME = :pdt, "
        newItem.update({":pdt": newInfo['PICKUP_DATETIME']})
    if 'RIDER_NAME' in newInfo:
        updateExp += "RIDER_NAME = :rn, "
        newItem.update({":rn" : newInfo['RIDER_NAME']})
    if 'VENDOR_ID' in newInfo:
        updateExp += "VENDOR_ID = :vid, "
        newItem.update({":vid" : newInfo['VENDOR_ID']})
    if 'PICKUP_LONGITUDE' in newInfo:
        updateExp += "PICKUP_LONGITUDE = :pickLong, "
        newItem.update({":pickLong" : decimal.Decimal(newInfo['PICKUP_LONGITUDE'])})
    if 'TRIP_TYPE' in newInfo:
        updateExp += "TRIP_TYPE = :tt, "
        newItem.update({":tt" : newInfo['TRIP_TYPE']})
    if 'STORE_AND_FWD_FLAG' in newInfo:
        updateExp += "STORE_AND_FWD_FLAG = :saf, "
        newItem.update({":saf" : newInfo['STORE_AND_FWD_FLAG']})
    if 'DROPOFF_LATITUDE' in newInfo:
        updateExp += "DROPOFF_LATITUDE = :dropLat, "
        newItem.update({":dropLat" : decimal.Decimal(newInfo['DROPOFF_LATITUDE'])})
    if 'RATE_CODE_ID' in newInfo:
        updateExp += "RATE_CODE_ID = :rcid, "
        newItem.update({":rcid" : newInfo['RATE_CODE_ID']})
    if 'TOLLS_AMOUNT' in newInfo:
        updateExp += "TOLLS_AMOUNT = :tllamt, "
        newItem.update({":tllamt" : decimal.Decimal(newInfo['TOLLS_AMOUNT'])})
    if 'IMPROVEMENT_SURCHARGE' in newInfo:
        updateExp += "IMPROVEMENT_SURCHARGE = :isc, "
        newItem.update({":isc" : decimal.Decimal(newInfo['IMPROVEMENT_SURCHARGE'])})
    if 'TIP_AMOUNT' in newInfo:
        updateExp += "TIP_AMOUNT = :tpamt, "
        newItem.update({":tpamt" : decimal.Decimal(newInfo['TIP_AMOUNT'])})
    if 'DROPOFF_DATETIME' in newInfo:
        updateExp += "DROPOFF_DATETIME = :drpdt, "
        newItem.update({":drpdt" : newInfo['DROPOFF_DATETIME']})
    if 'CAB_TYPE_ID' in newInfo:
        updateExp += "CAB_TYPE_ID = :ctid, "
        newItem.update({":ctid" : newInfo['CAB_TYPE_ID']})
    if 'DRIVER_NAME' in newInfo:
        updateExp += "DRIVER_NAME = :dn, "
        newItem.update({":dn" : newInfo['DRIVER_NAME']})
    if 'PICKUP_LATITUDE' in newInfo:
        updateExp += "PICKUP_LATITUDE = :pickLat, "
        newItem.update({":pickLat" : decimal.Decimal(newInfo['PICKUP_LATITUDE'])})
    if 'TRIP_DISTANCE' in newInfo:
        updateExp += "TRIP_DISTANCE = :trpDist, "
        newItem.update({":trpDist" : decimal.Decimal(newInfo['TRIP_DISTANCE'])})
    if 'VEHICLE_ID' in newInfo:
        updateExp += "VEHICLE_ID = :vhid, "
        newItem.update({":vhid" : newInfo['VEHICLE_ID']})
    if 'TOTAL_AMOUNT' in newInfo:
        updateExp += "TOTAL_AMOUNT = :tamt, "
        newItem.update({":tamt" : decimal.Decimal(newInfo['TOTAL_AMOUNT'])})
    if 'MTA_TAX' in newInfo:
        updateExp += "MTA_TAX = :mtax, "
        newItem.update({":mtax" : decimal.Decimal(newInfo['MTA_TAX'])})
    if 'DROPOFF_LONGITUDE' in newInfo:
        updateExp += "DROPOFF_LONGITUDE = :dropLong, "
        newItem.update({":dropLong" : decimal.Decimal(newInfo['DROPOFF_LONGITUDE'])})
    if 'PAYMENT_TYPET' in newInfo:
        updateExp += "PAYMENT_TYPET = :pmtt, "
        newItem.update({":pmtt" : newInfo['PAYMENT_TYPET']})
    if 'DRIVER_ID' in newInfo:
        updateExp += "DRIVER_ID = :did, "
        newItem.update({":did" : int(newInfo['DRIVER_ID'])})
    if 'DRIVER_EMAIL' in newInfo:
        updateExp += "DRIVER_EMAIL = :de, "
        newItem.update({":de" : newInfo['DRIVER_EMAIL']})
        updateExp += "driverid = :drid, "
        newItem.update({":drid" : newInfo['DRIVER_EMAIL']})
    if 'EXTRA' in newInfo:
        updateExp += "EXTRA = :ext, "
        newItem.update({":ext" : decimal.Decimal(newInfo['EXTRA'])})
    if 'FARE_AMOUNT' in newInfo:
        updateExp += "FARE_AMOUNT = :famt, "
        newItem.update({":famt" : decimal.Decimal(newInfo['FARE_AMOUNT'])})
    if 'PASSENGER_COUNT' in newInfo:
        updateExp += "PASSENGER_COUNT = :psgCnt, "
        newItem.update({":psgCnt" : newInfo['PASSENGER_COUNT']})
    if 'RIDER_EMAIL' in newInfo:
        updateExp+= "RIDER_EMAIL = :re, " 
        newItem.update({":re" : newInfo['RIDER_EMAIL']})
    if 'DRIVER_MOBILE' in newInfo:
        updateExp+= "DRIVER_MOBILE = :dm, " 
        newItem.update({":dm" : newInfo['DRIVER_MOBILE']})
    if 'tripinfo' in newInfo:
        updateExp += "ID = :id, "
        newItem.update({":id" : int(newInfo['tripinfo'].split(',')[1])})
    if 'DriverDetails' in newInfo:
        updateExp += "DriverDetails = :ddet, "
        newItem.update({":ddet" : json.dumps(newInfo['DriverDetails'], indent=2, cls=DecimalEncoder)})
    
    # Remove last 2 charectors , and space from the update expression
    updateExp = updateExp[:-2]
  
    print("Key= " + json.dumps(key, indent=2))  
    print("UpdateExpression= " + updateExp)
    print ("ConditionExpression = " + conditionalExp)
    print ("ExpressionAttributeValues= " + json.dumps(newItem, indent=2, cls=DecimalEncoder))
    
    try:
        response = table.update_item(
            Key = key,
            UpdateExpression = updateExp,
            ExpressionAttributeValues = newItem,
            ExpressionAttributeNames = expAttrNames,
            ConditionExpression = conditionalExp,
            ReturnValues = "ALL_NEW"
            )
        print("UpdateItem succeeded...Response = " + json.dumps(response, indent=4, cls=DecimalEncoder))
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print("UpdateItem Item Error. Conditional check failed =" + e.response['Error']['Message'])
        raise
    
    
    
    