import datetime
import json
import random

from random import randint

from util import updateTripInfo, getTripInfo, DecimalEncoder

rate_codes = [1, 2, 3, 4, 5, 6, 99]

rider_id = "person69257@example.com"
dropoff_longitude = round(random.uniform(-74,-73),6)
dropoff_latitude = round(random.uniform(40,41),6) 

trip_info = input("Enter your tripinfo : ") 

tripCompletedInfo = {
    "riderid"  : rider_id,
    "tripinfo" : trip_info,
    "DROPOFF_LATITUDE" : str(dropoff_latitude),
    "RATE_CODE_ID" : rate_codes[randint(0, 6)],
    "TOLLS_AMOUNT" : str(round(random.uniform(0,5),2)),
    "IMPROVEMENT_SURCHARGE" : str(round(random.uniform(0,1),1)),
    "TIP_AMOUNT" : str(round(random.uniform(0,10),2)),
    "DROPOFF_DATETIME" : datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "TRIP_DISTANCE" : randint(1, 50),
    "TOTAL_AMOUNT" : str(round(random.uniform(5,150),2)),
    "MTA_TAX" : str(round(random.uniform(0,1),1)),
    "DROPOFF_LONGITUDE" : str(dropoff_longitude),
    "PAYMENT_TYPET" : randint(1, 7),
    "EXTRA" : str(round(random.uniform(0,1),1)),
    "FARE_AMOUNT" : str(round(random.uniform(5,150),2)), 
    "PASSENGER_COUNT": randint(1, 7),
    "Status" : "Completed"
}

print ("Trip complettion information = ", json.dumps(tripCompletedInfo, indent=2))

response = updateTripInfo(tripCompletedInfo, "InProgress")
print("Trip completion information has been updated to Trips table")

print("Driver trip completion informaiton =" + json.dumps(response['Attributes'], indent = 4, cls=DecimalEncoder) )
