#!/usr/bin/env python3

#Imports
import pandas as pd
import uuid
import africastalking
import logging
import os
import sys
from datetime import date

#Add current directory to path so that driver can be loaded correctly, then import driver
curr_directory = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_directory)
import driver

with open(curr_directory + "/Paths/logpath.txt", "r") as logpathfile:
    logpath = logpathfile.read().rstrip()
logging.basicConfig(level=logging.INFO, filename=logpath+"morning_caller.log", filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

####Production - get data from database

newvisits_data = driver.df_from_sql(query_file="newvisits_calls_query.sql")
absentees_data = driver.df_from_sql(query_file="absentees_calls_query.sql")

all_data = pd.concat([newvisits_data, absentees_data])

####Testing - get data from file

# all_data = pd.read_excel("C:/Users/graha/Downloads/call_test_file.xlsx", dtype="str")

all_data["uuid"] = all_data["pid"].apply(lambda x: uuid.uuid4())

#Look for duplicate phone numbers. Sort by call type and mark the first 'drop' call_type entry as the 'actual_call' - i.e. will really be called. The others will be logged, but phone number will not be called multiple times.
actual_calls = all_data.sort_values("call_type").drop_duplicates(subset="phone", keep="first")
actual_calls_uuids = actual_calls["uuid"].to_list()
all_data["actual_call"] = all_data["uuid"].apply(lambda x: True if x in actual_calls_uuids else False)

#Pre-process phone numbers
all_data.phone = all_data.phone.apply(lambda x: "+234" + x.lstrip('0'))

#Check if any of these calls are already queued (based on whether PID is in data.calls on this date). Drop those that have already been queued.
already_queued = driver.df_from_sql(query_text="select pid, phone from data.calls cl where cl.date = current_date;")
to_queue = all_data[~all_data["pid"].isin(already_queued["pid"].to_list())].copy()
to_queue.columns = ["pid", "phone", "language", "site", "call_type", "phone_type", "uuid", "actual_call"] #rename 'langpref' to language to match with data.calls table

#Add additional columns needed for table in database
to_queue["date"] = date.today()
to_queue["morning_answer"] = False
to_queue["curr_call"] = "morning"

#Exit if there are no calls to queue
if to_queue.shape[0] == 0:
    logging.info("No calls to queue. Exiting.")
    print("No calls to queue.") #DEBUGGING
    sys.exit()

driver.upload_df_append(to_queue, "calls")
print(to_queue.head()) ####FOR TESTING

logging.info("Logged calls for " + str(to_queue.shape[0]) + " people.")

#Now initiate calls
with open(curr_directory + "/Keys/at_api_key.txt", "r") as keyfile:
    apikey = keyfile.read().strip()
username = "messager"

origin = "+23417008806"
headers = {"apiKey":apikey, "Content-Type":"application/x-www-form-urlencoded", "Accept":"application/json"}

#######FOR TESTING#########
# sys.exit()

africastalking.initialize(username, apikey)
voice = africastalking.Voice #voice service

#Filter to_queue down to only the actual calls
to_queue = to_queue[to_queue["actual_call"] == True]
to_queue["morning_at_queued"] = None #add columns for queue status columns in database table
to_queue["morning_at_queue_status"] = None
to_queue.set_index("phone", inplace=True, append=False, drop=False)

nums = to_queue["phone"].to_list() #Only call API for phone numbers listed as "actual calls"
print(nums)

callsQueued = 0
try:
    r = voice.call(origin, nums)
    print(r)

    #Handle individual errors
    if r["errorMessage"] == "None":
        errorOccurred = False
        for entry in r["entries"]:
            to_queue.loc[entry["phoneNumber"], "morning_at_queue_status"] = entry["status"]
            if entry["status"] != "Queued":
                errorOccurred = True
                to_queue.loc[entry["phoneNumber"], "morning_at_queued"] = False
            else:
                callsQueued = callsQueued + 1
                to_queue.loc[entry["phoneNumber"], "morning_at_queued"] = True
        if errorOccurred:
            if callsQueued > 0:
                print("Problem queueing some calls")
            else:
                print("Problem queueing all calls")

    #Handle bulk errors
    else:
        print("Problem queueing all calls - Catastrophic error from API: " + r["errorMessage"])
        to_queue["morning_at_queued"] = False
        to_queue["morning_at_queue_status"] = "BULK ERROR (from API) - " + r["errorMessage"]

except Exception as e:
    print("Printing exception: " + str(e))
    print("Problem queueing all calls - Catastrophic error from client")
    to_queue["morning_at_queued"] = False
    to_queue["morning_at_queue_status"] = "BULK ERROR (from client) - " + str(e)

#Now log status updates
to_queue = to_queue[["uuid", "morning_at_queued", "morning_at_queue_status"]]
try: 
    res = driver.update_rows(to_queue, "calls")

    if res == False:
        logging.info("ERROR - failed to update database table with correct statuses for right # of rows")
except Exception as e:
    print("Error updating database table with statuses")
    logging.info("ERROR - failed to update database table with correct statuses for all rows")

logging.info("Completed run - successfully queued " + str(callsQueued) + " calls.")