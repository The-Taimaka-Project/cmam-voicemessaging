#!/usr/bin/env python3

#Imports
import pandas as pd
import psycopg2
import africastalking
import logging
import sys
import os

#Add current directory to path so that driver can be loaded correctly, then import driver
curr_directory = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_directory)
import driver

with open(curr_directory + "/Paths/logpath.txt", "r") as logpathfile:
    logpath = logpathfile.read().rstrip()
logging.basicConfig(level=logging.INFO, filename=logpath+"afternoon_caller.log", filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

to_queue = driver.df_from_sql(query_file="afternoon_calls_query.sql")
#Add columns for queue status columns in database table
to_queue["afternoon_at_queued"] = None
to_queue["afternoon_at_queue_status"] = None
to_queue.set_index("phone", inplace=True, append=False, drop=False)

if to_queue.shape[0] == 0:
    print("No calls to queue")
    logging.info("No afternoon calls to queue")
    sys.exit()

#Update curr_call in database to 'afternoon'
update_successful = driver.update_call_status(to_queue.shape[0])
if update_successful == False:
    print("Failure at curr_call update")
    logging.info("Failed to initiate afternoon calls due to failure in curr_call update")
    sys.exit()

#Now initiate calls
with open(curr_directory + "/Keys/at_api_key.txt", "r") as keyfile:
    apikey = keyfile.read().strip()
username = "messager"

origin = "+23417008806"
headers = {"apiKey":apikey, "Content-Type":"application/x-www-form-urlencoded", "Accept":"application/json"}

africastalking.initialize(username, apikey)
voice = africastalking.Voice #voice service

nums = to_queue["phone"].tolist()
logging.info("Initiating afternoon calls for " + str(len(nums)) + " people.")

callsQueued = 0
try:
    r = voice.call(origin, nums)
    print(r)

    #Handle individual errors
    if r["errorMessage"] == "None":
        errorOccurred = False
        for entry in r["entries"]:
            to_queue.loc[entry["phoneNumber"], "afternoon_at_queue_status"] = entry["status"]
            if entry["status"] != "Queued":
                errorOccurred = True
                to_queue.loc[entry["phoneNumber"], "afternoon_at_queued"] = False
            else:
                callsQueued = callsQueued + 1
                to_queue.loc[entry["phoneNumber"], "afternoon_at_queued"] = True
        if errorOccurred:
            if callsQueued > 0:
                print("Problem queueing some calls")
            else:
                print("Problem queueing all calls")

    #Handle bulk errors
    else:
        print("Problem queueing all calls - Catastrophic error from API: " + r["errorMessage"])
        to_queue["afternoon_at_queued"] = False
        to_queue["afternoon_at_queue_status"] = "BULK ERROR (from API) - " + r["errorMessage"]

except Exception as e:
    print("Printing exception: " + str(e))
    print("Problem queueing all calls - Catastrophic error from client")
    to_queue["afternoon_at_queued"] = False
    to_queue["afternoon_at_queue_status"] = "BULK ERROR (from client) - " + str(e)

#Now log status updates
to_queue = to_queue[["uuid", "afternoon_at_queued", "afternoon_at_queue_status"]]
print(to_queue.head())
try: 
    res = driver.update_rows(to_queue, "calls")

    if res == False:
        logging.info("ERROR - failed to update database table with correct statuses for right # of rows")
except Exception as e:
    print("Error updating database table with statuses")
    print(e)
    logging.info("ERROR - failed to update database table with correct statuses for all rows")

logging.info("Completed run - successfully queued " + str(callsQueued) + " calls.")