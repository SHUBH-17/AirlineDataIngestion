# AirlineDataIngestion

This project involves designing and implementing a Redshift solution to manage airline data, including the creation of an airport dimension table and ingestion of port data. Developed an automated process to ingest daily flight data from an S3 bucket with hive-style partitioning, selectively loading flights into a fact table based on departure delays of one hour or more.