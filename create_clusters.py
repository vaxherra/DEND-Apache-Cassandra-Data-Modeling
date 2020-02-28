# make a connection to a Cassandra instance your local machine 
from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect()

# Create keyspace if not already present
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS sparkify 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)
    
    
    
### TABLE 1. Song Session - for a given session information, can retrive artist name, song name and its length
 
table1_query = "CREATE TABLE IF NOT EXISTS song_session "
table1_query += "(sessionId int, itemInSession int, artist text, song_title text, song_length FLOAT, "
table1_query += "PRIMARY KEY (sessionId, itemInSession)"
table1_query += ")"    

try:
    session.execute(table1_query)
except Exception as e:
    print(e)
    
    
### TABLE 2. Artist Session - for a given user ID, session ID and `intemInSession` field, can retrive artist name, song title, user first and last name

table2_query = """
CREATE TABLE IF NOT EXISTS artist_session 
( 
    artist text, 
    song_title text, 
    user_first_name text, 
    user_last_name text, 
    itemInSession int, 
    userId int, 
    sessionId int
    
, PRIMARY KEY (userId, sessionId, itemInSession)
)

"""
    
try:
    session.execute(table2_query)
except Exception as e:
    print(e)
    
    
    
### TABLE 3. User session - given a song title, can retrive user first and last name. Uses `song` column as partition, and  `userId` field as clustering column.


table3_query = """
CREATE TABLE IF NOT EXISTS user_session 
(
user_first_name text,
user_last_name text,
song text,
userId int,

PRIMARY KEY (song, userId) 
)
"""

# `song` as parition, userId as clustering column
try:
    session.execute(table3_query)
except Exception as e:
    print(e)  
    