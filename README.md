This repository contains the code used by [Taimaka](https://taimaka.org) to send automated voice messages to caregivers enrolled in its community management of acute malnutrition (CMAM) program in an attempt to reduce dropout rates.

The findings of an RCT of this initiative can be found [here](https://docs.google.com/document/d/1WklMkADeZAV0YVdFeVA2zhiIiXKp10BbG0px5jq3zOc/edit?usp=sharing). Contact information for technical questions can be found in that write-up.

There are three parts to this code:
1. Python scripts to queue calls through an API provided by [Africa's Talking](https://africastalking.com/). These scripts were run with a cron job, once in the morning (morning.py) and once in the afternoon (afternoon.py). Calls were scheduled based on data stored in Taimaka's in-house case management database, queried using the included SQL files. These SQL files are included for illustration only, your database structure will be different than ours.
2. PHP files deployed to a webserver to answer callbacks from the Africa's Talking API when a call is connected, dictating what audio messages are played. 
3. A SQL file defining the database table we used to log the queuing and completion of calls, read/edited by both the python scripts and PHP files.
