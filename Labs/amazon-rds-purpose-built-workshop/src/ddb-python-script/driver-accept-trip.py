import json
import random
from decimal import getcontext, Decimal
from random import randint

from util import updateTripInfo, getTripInfo, DecimalEncoder

driver_details = [
    {"driver_email" : "driver556550@taxi.com", "driver_id": "556550", "driver_name": "driver556550", "driver_mobile": "+11720912154", "vehicle_details": {"id": "QSY353471", "type" : 2}},
    {"driver_email" : "driver541829@taxi.com", "driver_id": "541829", "driver_name": "driver541829", "driver_mobile": "+11264112049", "vehicle_details": {"id": "GTR508161", "type" : 2}},
    {"driver_email" : "driver507977@taxi.com", "driver_id": "507977", "driver_name": "driver507977", "driver_mobile": "+11088418780", "vehicle_details": {"id": "XVJ356159", "type" : 2}},
    {"driver_email" : "driver551153@taxi.com", "driver_id": "551153", "driver_name": "driver551153", "driver_mobile": "+11240868167", "vehicle_details": {"id": "CPX160101", "type" : 2}},
    {"driver_email" : "driver520045@taxi.com", "driver_id": "520045", "driver_name": "driver520045", "driver_mobile": "+11751510159", "vehicle_details": {"id": "HHR298952", "type" : 2}},
    {"driver_email" : "driver514040@taxi.com", "driver_id": "514040", "driver_name": "driver514040", "driver_mobile": "+11661484862", "vehicle_details": {"id": "TLA210480", "type" : 2}},
    {"driver_email" : "driver527336@taxi.com", "driver_id": "527336", "driver_name": "driver527336", "driver_mobile": "+11564984764", "vehicle_details": {"id": "OVY229214", "type" : 2}},
    {"driver_email" : "driver510909@taxi.com", "driver_id": "510909", "driver_name": "driver510909", "driver_mobile": "+11261783124", "vehicle_details": {"id": "UDT200764", "type" : 2}},
    {"driver_email" : "driver549736@taxi.com", "driver_id": "549736", "driver_name": "driver549736", "driver_mobile": "+11561755450", "vehicle_details": {"id": "ORX460076", "type" : 2}},
    {"driver_email" : "driver528204@taxi.com", "driver_id": "528204", "driver_name": "driver528204", "driver_mobile": "+11185992795", "vehicle_details": {"id": "PXX248130", "type" : 2}}
]



    
rider_id = "person69257@example.com"
vendor_id = 2

pickup_longitude = str(round(random.uniform(-74,-73),6))
pickup_latitude = str(round(random.uniform(40,41),6))
driver_info =  driver_details[randint(0, 9)]

trip_info = input("Enter your tripinfo : ") 

tripAcceptInfo = {
    "riderid"  : rider_id,
    "tripinfo" : trip_info,
    "VENDOR_ID" : vendor_id,
    "PICKUP_LONGITUDE" : pickup_longitude,
    "PICKUP_LATITUDE" : pickup_latitude,
    "TRIP_TYPE" : 2,
    "STORE_AND_FWD_FLAG" : "N",
    "CAB_TYPE_ID" : driver_info['vehicle_details']['type'],
    "DRIVER_NAME" : driver_info['driver_name'],
    "VEHICLE_ID" : driver_info['vehicle_details']['id'],
    "DRIVER_ID" : driver_info['driver_id'],
    "DRIVER_EMAIL" : driver_info['driver_email'],
    "DRIVER_MOBILE" : driver_info['driver_mobile'],
    "DriverDetails" : {
        "Name" : driver_info['driver_name'],
        "Vehicle Details" : {
            "id" : driver_info['vehicle_details']['id'],
            "type": driver_info['vehicle_details']['type']
        }
    },
    "Status" : "InProgress"
}

print("Trip accept information ="+ json.dumps(tripAcceptInfo, indent=2))

response = updateTripInfo(tripAcceptInfo, "Booked")
print("Trip accept information has been updated to Trips table")

print("Driver Accept Trip Informaiton =" + json.dumps(response['Attributes'], indent = 4, cls=DecimalEncoder) )