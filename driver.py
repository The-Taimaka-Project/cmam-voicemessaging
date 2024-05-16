#Imports
from pyodk.client import Client
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, insert, MetaData, Table, Column, delete, bindparam, update
from sqlalchemy.types import Text, Date, Boolean, Integer, SmallInteger, Numeric, Uuid, TIMESTAMP
from urllib.parse import quote_plus
from datetime import timedelta, date
import time
import os

#SQlAlchemy Engine Creation
def get_engine():
    curr_directory = os.path.dirname(os.path.abspath(__file__))
    with open(curr_directory + "/Keys/cmam_tester.txt", "r") as keyfile:
        passw = keyfile.read().strip()
    engine = create_engine(f"postgresql+psycopg2://cmam_tester:{quote_plus(passw)}@taimaka-internal.org/cmam")
    return engine

#Pre-processor, finds occurrences of a character in a string
def findOccurrences(s, ch):
    return [i for i, letter in enumerate(s) if letter == ch]

#Pre-processor, returns first element in a list < target, where list is sorted from smallest to largest
def getBelow(ls, target, index):
    if index < 0:
        return -1

    if ls[index] < target:
        return ls[index]
    else:
        return getBelow(ls, target, index-1)

#Creates a list of program weeks with start and end dates. 
#Inputs: none
#Outputs: list, where each item is a list, such that [week #, start date, end date]
def gen_week_list():
    weeks = [[1, pd.Timestamp("2023-05-28T23:59:59+01:00"), pd.Timestamp("2023-06-04T23:59:59+01:00")]]
    for week in range(1,59):
        weeks.append([week+1, weeks[week-1][1] + timedelta(days=7), weeks[week-1][2] + timedelta(days=7)])
    return weeks

#Creates a list of the Monday of each program week
#Inputs: none
#Outputs: list, where each item is the text date of the Monday of that corresponding week index (index 0 is None so that week 1 is at index 1, etc.)
def gen_monday_list():
    mondays = [None, date(2023, 5, 29)]
    for week in range(2,59):
        mondays.append(mondays[week-1] + timedelta(days=7))

    return mondays

#Drops and recreates table in schema data based on passed parameters. INTENDED FOR USE IN DEVELOPMENT ONLY, DOES NOT PROTECT DATA.
#Inputs: string name of table to be recreated, dataframe data dictionary of form/table, string input of primary key to set (really just PID or not PID, in which case PK is set to uuid)
def _DEVONLY_COLSYNC(tname, dic, pkey):
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text(f"drop table if exists data.{tname}"))

        qfrags = []
        for index, row in dic.iterrows():
            qfrags.append(row["db_name"] + " " + row["dtype"])
        conn.execute(text(f"create table data.{tname} ({', '.join(qfrags)});"))
        if pkey == "pid":
            conn.execute(text(f"alter table data.{tname} add constraint {tname}_pk_pid primary key (pid);"))
            conn.execute(text(f"alter table data.{tname} add constraint {tname}_un_uuid unique (uuid);"))
        else:
            conn.execute(text(f"alter table data.{tname} add constraint {tname}_pk_uuid primary key (uuid);"))

        conn.execute(text(f"grant all on table data.{tname} to cmam_testing_admin"))

        conn.commit()
        print("table created")

#Adds non-existing rows (based on uuid) from dataframe to specified table. 
#Inputs: string name of table to be modified, dataframe containing form data
def add_rows(tname, df):
    engine = get_engine()

    #SQL table reflection setup
    metadata_obj = MetaData(schema="data")
    rfl_table = Table(tname, metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        #Get current entries, based on uuid
        result = conn.execute(text(f"select cast(uuid as text) from data.{tname};"))
        table_uuids = result.all()
        table_uuids = [x[0] for x in table_uuids]

        add_dict = df[~df["uuid"].isin(table_uuids)].to_dict('records')

        if(len(add_dict) > 0):
            result = conn.execute(
                insert(rfl_table), add_dict
            )
            conn.commit()
            print("Rows inserted: " + str(result.rowcount))
        else:
            print("No rows detected to insert")

#Adds rows from dataframe to specified table. Does not check whether row already exists, and assumes that is handled prior to calling function
#Inputs: string name of table to be modified, dataframe containing rows to be added
def add_rows_no_check(tname, df):
    engine = get_engine()

    #SQL table reflection setup
    metadata_obj = MetaData(schema="data")
    rfl_table = Table(tname, metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        add_dict = df.to_dict('records')

        if(len(add_dict) > 0):
            result = conn.execute(
                insert(rfl_table), add_dict
            )
            conn.commit()
            print("Rows inserted: " + str(result.rowcount))
        else:
            print("No rows detected to insert")

#Deletes specified rows from specified table in schema data
#Inputs: string table name, list of UUIDs to delete (UUIDs formatted as strings)
def del_rows(tname, dl_list):

    if len(dl_list) == 0:
        print("No rows detected to delete")
        return

    engine = get_engine()

    #SQL table reflection setup
    metadata_obj = MetaData(schema="data")
    rfl_table = Table(tname, metadata_obj, autoload_with=engine)
    
    with engine.connect() as conn:
        #Make deletions
        result = conn.execute(delete(rfl_table).where(rfl_table.c.uuid.in_(dl_list)).returning(rfl_table.c.pid, rfl_table.c.uuid))
        del_res = result.all()
        del_strings = [x[0]+"_"+str(x[1]) for x in del_res]
        if(len(del_strings) == 0):
            print("No rows detected to delete")
        else:
            print(f"Deleted {len(del_strings)} entries for: " + ", ".join(del_strings))
            conn.commit()

#DEVELOPMENT USE ONLY: Drops a specified column if exists. DOES NOT PROTECT DATA.
#Inputs: string table name, string column name 
def _DEVONLY_DROPCOL(tname, colname):
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text(f"ALTER TABLE data.{tname} DROP COLUMN IF EXISTS {colname};"))
        conn.commit()

#Returns a pandas dataframe generated from a specified SQL query of the cmam database. Query can be passed either as a string, or as a separate file.
#Inputs: string name of sql file in ./sql directory containing the query (or None), string query (or None). One or the other of these must be specified. If both specified, file will be used.
#Outputs: pandas dataframe generated from query
def df_from_sql(query_file = None, query_text = None):

    if query_file == None and query_text == None:
        raise ValueError("One of query_file or query_text must be specified!")

    if query_file != None:
        curr_directory = os.path.dirname(os.path.abspath(__file__))
        with open(curr_directory + "/sql/"+query_file, "r") as file:
            stmt = text(file.read().rstrip())
    else:
        stmt = text(query_text)

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    
    return df

#Uploads passed pandas dataframe to cmam database, appending to specified table
#Inputs: pandas dataframe, string table name
def upload_df_append(df, tname):
    engine = get_engine()

    with engine.connect() as conn:
        df.to_sql(tname, conn, if_exists='append', index=False, schema="data")
        conn.commit()

#Returns current program week from th current_pr_week view in the database
#Inputs: none
#Outputs: int - current program wee
def get_program_week():
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT * FROM data.current_pr_week;"))
        return res.first()[0]

#Update database rows based on uuid
#Inputs: pandas dataframe, string table name
def update_rows(df, tname):
    engine = get_engine()
    metadata_obj = MetaData(schema="data")
    rfl_table = Table(tname, metadata_obj, autoload_with=engine)

    with engine.connect() as conn:
        update_df = df.copy()
        update_df.columns = ["b_" + col for col in update_df.columns.to_list()]
        update_dict = update_df.to_dict('records')

        template_dict = {}
        for col in df.columns.to_list():
            if col != "uuid":
                template_dict[col] = bindparam("b_"+col)

        #Create update statement
        stmt = (
            update(rfl_table).where(rfl_table.c.uuid == bindparam('b_uuid')).values(template_dict)
        )
        result = conn.execute(stmt, update_dict)

        conn.commit()

        if result.rowcount != df.shape[0]:
            print("ERROR - rows updated incorrectly")
            return False #problem updating rowcount
        
        return True #success

#Update call status to afternoon
#Inputs: # of rows expected to be updated
#Outputs: boolean success
def update_call_status(numrows):
    engine = get_engine()

    try:
        with engine.connect() as conn:
            stmt = text("UPDATE data.calls SET curr_call = 'afternoon' WHERE date = current_date and (morning_endstatus is null or morning_endstatus != 'Completed') and afternoon_at_queued is null and actual_call = true")
            result = conn.execute(stmt)

            if result.rowcount != numrows:
                return False
            else:
                conn.commit()
                return True
    except Exception as e:
        print(e)
        return False