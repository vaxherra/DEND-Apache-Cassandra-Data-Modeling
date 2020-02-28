# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# Import pre-defined CQL queries
from cql_queries import *  




def combine_events(foldername,output_filename="event_datafile_new.csv"):
    """
    Combines events from the specified folder into a single csv file.
    
    Arguments:
        foldername: a folder containing a list of event data in the *.csv format
        output_filename: output filename for the combined event data

    Returns: 
        None
    
    """
 
    filepath =   './' + foldername +"/"
 
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
    print(len(file_path_list), "files present in ", foldername)
 
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 

    # for every filepath in the file path list 
    for f in file_path_list:

    # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

     # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line) 


    # creating a smaller event data csv file called `output_filename`.csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(output_filename, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))




def populate(filename,session):
    """
    Loops over a provided event file and pushes data into existing Apache Cassandra schemas.
    
    Arguments:
        filename - an input CSV filename
    
    
    """
    # Count number of rows
    with open(filename) as f:
        num_rows = sum(1 for line in f) - 1
    
    current_row=0
    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        for idx,line in enumerate(csvreader):
            current_row+= 1 
            # Extract column names from the first row
            if(idx==0):
                header_names = line
                # A helper dictionary, maps column name -> index
                header_lookup = {  v:i  for i,v in enumerate(header_names) }

            # Read the rest of the lines and upload data to Cassandra
            else:
                # Counter (for feedback to the user), give feedback every 0.05 = 5% 
                if( current_row % int(0.05*num_rows) == 0):
                    print("Processing... %s%% done" % round(current_row*100/num_rows,2))
                # Insert queries

                # Table1/Query1 populate: 'song_session'
                session.execute(ins_query1, ( int(line[header_lookup['sessionId']]), int(line[header_lookup['itemInSession']]), 
                                    line[header_lookup['artist']],line[header_lookup['song']],float(line[header_lookup['length']])) )

                # Table2/Query2 populate: 'artist_session'
                session.execute(ins_query2,  ( 
                line[header_lookup['artist']],
                line[header_lookup['song']],
                line[header_lookup['firstName']],
                line[header_lookup['lastName']],

                int(line[header_lookup['itemInSession']]),
                int(line[header_lookup['userId']]),
                int(line[header_lookup['sessionId']])

            ))

                # Table3/Query3 populate: 'user_session'
                session.execute(ins_query3, (
                line[header_lookup['firstName']],
                line[header_lookup['lastName']],   
                line[header_lookup['song']], 
                int(line[header_lookup['userId']]), 
            ))

            
    
    
def main_program():
    
   
    # Create a joint/combined event_data file
    combine_events("event_data") # default filename
    
    # connect
    from cassandra.cluster import Cluster
    cluster = Cluster()
        # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    try:
        session.set_keyspace('sparkify')
    except Exception as e:
        print(e)

    
    # populate
    populate('event_datafile_new.csv',session)
    
    

    
    
if __name__ == "__main__":
    main_program()