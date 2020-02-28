### SELECT QUERIES


# Query1 - retrives artist name, song title and song length for a given sessionID
query1 = """
SELECT artist, song_title, song_length from song_session
WHERE sessionID=%s 
AND itemInSession=%s
"""

# Query2 - retrives song title, user first and last name for a given userId and sessionId
query2 = """
SELECT artist, song_title, user_first_name, user_last_name FROM artist_session
                    WHERE userId = %s
                    AND sessionId = %s 

"""


# Query3 - retrives user first and last name for a given song title
query3 = """
SELECT user_first_name, user_last_name FROM user_session
WHERE song=%s; 
"""

### INSERT QUERIES
ins_query1 = "INSERT INTO song_session(sessionId, itemInSession, artist, song_title, song_length) "
ins_query1 = ins_query1 + "VALUES (%s, %s, %s, %s, %s)"

ins_query2 = "INSERT INTO artist_session(artist, song_title, user_first_name, user_last_name, itemInSession, userId, sessionId )"
ins_query2 += " VALUES ("
ins_query2 += "%s,"*6 + "%s)" # inserting 7 fields

ins_query3= "INSERT INTO user_session (user_first_name, user_last_name, song, userId) "
ins_query3+= "VALUES (%s, %s, %s, %s)"

### Droping tables

drop1 = "DROP TABLE IF EXISTS song_session"
drop2 = "DROP TABLE IF EXISTS artist_session"
drop3 = "DROP TABLE IF EXISTS user_session"


drop_tables=[drop1,drop2,drop3]