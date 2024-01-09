import json
from datetime import datetime
import pytz
from collections import defaultdict
import pandas as pd
import csv
from statistics import mean, median, mode

#function to load data from json file, return 'trackDetails'
def load_data_from_json():
    f = open('data.json')
    data = json.load(f)
    data_trackdeets = []

    for dat in data:
        data_trackdeets.append(dat['trackDetails'])

    return data_trackdeets

""" 
function to return payment type, using logic - 
payment present in specialhandlings field where type is COD, will use that for logic to find out if it is COD or prepaid 
pass specialHandlings as argument
"""
def compute_payment_type(sph):
    for handling in sph:
        if handling['type'] == 'COD':
            return 'COD'
    
    return 'Prepaid'


""" 
function to return time to deliver,pickup address and time, drop address and time, using logic - 
eventType = "DL" for delivered and "PU" for picked up
pass events as argument
"""
def time_address(events):

    timezone = pytz.timezone('Asia/Kolkata') #IST
    for event in events:
        if event["eventType"] == "DL":
            delivery_date = int(event['timestamp']['$numberLong'])
            delivery_date_obj = datetime.fromtimestamp(delivery_date/1000,timezone)
            drop_pincode = event["address"]["postalCode"]
            drop_state = event["address"]["stateOrProvinceCode"]
            drop_city = event["address"]["city"]
            drop_address = drop_pincode+', '+drop_city+', '+drop_state
        if event["eventType"] == "PU":
            pickup_date = int(event['timestamp']['$numberLong'])
            pickup_date_obj = datetime.fromtimestamp(pickup_date/1000,timezone)
            pickup_pincode = event["address"]["postalCode"]
            pickup_state = event["address"]["stateOrProvinceCode"]
            pickup_city = event["address"]["city"]
            pickup_address = pickup_pincode+', '+pickup_city+', '+pickup_state

    #delivery time computation
    delta = delivery_date_obj - pickup_date_obj

    return delta.days, delivery_date_obj, pickup_date_obj, drop_address, pickup_address

""" 
function to return number of attempts , using logic - 
eventType = "DL" for delivered and "OD" for out for delivery
pass events as argument
"""

def compute_outfordel(events):

    delivery_attempts = defaultdict(int)
    out_for_delivery_datetimes = []
    for event in events:
        timestamp = int(event["timestamp"]["$numberLong"]) // 1000
        #using utcfromtimestamp to avoid timezone stuff  
        event_date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

        if event["eventType"] == "OD":
            #If Out For Delivery, mark it as a delivery attempt for the given date, and add it to the list
            delivery_attempts[event_date] += 1
            out_for_delivery_datetimes.append(datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'))
        elif event["eventType"] == "DL":
            #If Delivered, check if Out For Delivery happened on the same date, and count as a delivery attempt
            out_for_delivery_date = (datetime.utcfromtimestamp(timestamp - 1)).strftime('%Y-%m-%d')
            if delivery_attempts[out_for_delivery_date] > 0:
                delivery_attempts[out_for_delivery_date] -= 1
            else:
                delivery_attempts[event_date] += 1

    total_attempts = sum(delivery_attempts.values())

    #handling edge case where delivery attempt can become 0
    return total_attempts if total_attempts > 0 else 1

#Tracking number and shipment Weight does not need a seperate function

'''
Function where all the data is extracted and put into one csv, pass trackDetails data as argument
'''
def extract_data(trackDetails):

    data_list = [["Tracking number", "Payment Type", "Pickup Date Time in IST", "Delivery Date Time in IST", "Days taken for delivery",
                 "Shipment weight","Pickup Pincode, City, State", "Drop Pincode, City, State", "Number of delivery attempts needed"]]
    
    for data in trackDetails:

        #creating a list for each order
        list_for_order = []
        data = data[0]
        tracking_number = data['trackingNumber']

        #payment type computation
        payment_type = compute_payment_type(data['specialHandlings'])

        #fetching the following data using time_address
        delivery_time,delivery_date,pickup_date,delivery_address,pickup_address = time_address(data['events'])

        #calculating out for delivery attempts
        delivery_attempts = compute_outfordel(data['events'])

        #extracting shipment weight
        shipment_weight = str(data['shipmentWeight']['value']) +' ' + data['shipmentWeight']['units']

        list_for_order.append(tracking_number)
        list_for_order.append(payment_type)
        list_for_order.append(pickup_date)
        list_for_order.append(delivery_date)
        list_for_order.append(delivery_time)
        list_for_order.append(shipment_weight)
        list_for_order.append(pickup_address)
        list_for_order.append(delivery_address)
        list_for_order.append(delivery_attempts)
    
        data_list.append(list_for_order)

    return data_list


def analytics(csv_file_name):

    df = pd.read_csv(csv_file_name)

    #Mean, Median, and Mode for 'Days taken for delivery'
    days_taken_mean = df['Days taken for delivery'].mean()
    days_taken_median = df['Days taken for delivery'].median()
    days_taken_mode = mode(df['Days taken for delivery'])

    #Mean, Median, and Mode for 'Number of delivery attempts needed'
    delivery_attempts_mean = df['Number of delivery attempts needed'].mean()
    delivery_attempts_median = df['Number of delivery attempts needed'].median()
    delivery_attempts_mode = mode(df['Number of delivery attempts needed'])

    stats_data = {
        'Metric': ['Mean', 'Median', 'Mode'],
        'Days taken for delivery': [days_taken_mean, days_taken_median, days_taken_mode],
        'Number of delivery attempts needed': [delivery_attempts_mean, delivery_attempts_median, delivery_attempts_mode]
    }

    stats_df = pd.DataFrame(stats_data)
    stats_file_name = "delivery_stats.csv"
    stats_df.to_csv(stats_file_name, index=False)




if __name__ == "__main__":

    #loading json data
    track_details_data = load_data_from_json()
    
    #getting the output data as a list
    output_data = extract_data(track_details_data)

    #creating the csv
    csv_file_name = "output.csv"

    with open(csv_file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(output_data)

    print(f"CSV file '{csv_file_name}' has been created.")

    #summary statistics
    analytics(csv_file_name)












    
