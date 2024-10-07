# Twitch ETL tool

The tool can be used for generating insights about twitch streaming platform.

### How to run the solution

Use docker-compose up to start the application (I used Docker Desktop/Colima).
When airflow webserver is up and running, it can be accessed on localhost 8080.
From airflow webserver, the ETL task can be started manually.

There are 3 almost identical DAGs: ETL_per_file to process data per file and ETL.py to combine source files and process as one. They were created to test different options, but are almost the same.
There is a problem that Airflow crashes when processing combined files, so ETL_per_file is a working script.

Separate SQL queries are in SQL_queries.sql file

A short presentation is in "Solution Presentation.pptx"

The data model is in "Database ER diagram.jpeg"

### Steps and justification

I've chosen to build ETL using Docker-compose, PostgreSQL and Airflow. Docker-compose YAML file is used to define services and volumes, and how they depend on each other.
The database solution is open-source PostgreSQL. Airflow is used for ETL.

For container management another solution could be Kubernetes. Kubernetes is a more comprehensive solution and can run multiple container runtimes across multiple machines, while Docker is for a single server.

For database I could have chosen a NoSQL database like MongoDB but the data is structured so I went with a relational db PostgreSQL.
