# Data Modeling with Apache Cassandra


## Overview

A Sparkify company needs to migrate their existing data relating to song and user acitivities to a non-relational database - Apache Cassandra. Company has a set of predefined queries that their analytics team wants to answer on a regular basis. 

1. Given a user session information, obtain artist name, song title and its length
2. Given user ID and their session ID, obtain name of the artist, song title (sorted by `intemInSession` field), user first and last name
3. Given name of the song, obtain user information (first and last name) of the listener



## Goal 
1. Modeling NoSQL database in Apache Cassandra to reflect the specified needs of queries
2. Create an ETL pipeline for combining and pushing data into predefined schemas
3. Demonstrate how to easily call requested queries


## Project structure

- `Readme.md` - this file, specification of requirements and project structure

- `Project_1B.ipynb` - prototyping of a general workflow in an interactive environment

- `create_clusters.py` - (re)creates a set of EMPTY clusters, does not remove existing clusters for safety

- `cql_queries.py` - set of `CQL` queries in Apache Cassandra that define required schemas and queries according to provided specifications

- `etl.py` - ETL pipeline to upload data into defined Cassandra schemas

- `Demo.ipynb` - shows how to connect to a database and use pre-defined queries in an easy way


## Usage

1. Run `create_cluster.py` to create a keyspace named `sparkify` with empty set of clusters, each for requested query
2. Run `etl.py` to populate clusters with data in `event_data` folder
3. Rin `Demo.ipynb` to easily access requested queries

