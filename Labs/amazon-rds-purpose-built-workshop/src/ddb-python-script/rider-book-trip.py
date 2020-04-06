import datetime
import json

from random import randint
from util import updateTripInfo, getTripInfo, DecimalEncoder

rider_id = 69257

rider_name = "person" + str(rider_id)
print("Rider Name=" + rider_name)

riderid = rider_name + "@example.com"
rider_email = riderid
print("Rider ID= " + riderid)

rider_mobile = "+11609467790"
print("Rider Mobile = " + rider_mobile)

pickUpDateTime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
id =  ''.join(["%s" % randint(0, 9) for num in range(0, 7)])
tripinfo = pickUpDateTime +"," + id 
print("Trip Info= " + tripinfo)

status = "Booked"
print("Status=" + status)

tripInfo = {
    "riderid" : riderid,
    "tripinfo" : tripinfo,
    "RIDER_ID" : rider_id,
    "RIDER_MOBILE" : rider_mobile,
    "PICKUP_DATETIME" : pickUpDateTime,
    "RIDER_NAME" : rider_name,
    "RIDER_EMAIL" : rider_email,
    "Status" : status
}

print("Trip Information =" + json.dumps(tripInfo, indent=2))

response = updateTripInfo(tripInfo)
print("Trip information has been updated to Trips table")

print("Rider Booking Trip Informaiton =" + json.dumps(response['Attributes'], indent = 4, cls=DecimalEncoder) )