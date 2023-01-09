
# Data Pipelines

## Description:

The goal of this project is to use song listen log files and song metadata to perform analytics. 


To do this, we first copy the JSON data from an S3 bucket and process it using Apache Spark with using python. 

The resulting data is organized into a star schema with a central fact table 'songplays'
 and four dimension tables 'users', 'songs', 'artists', and 'time').
 
## Files:

- etl.py: this file contains the python script that extracts, transforms and load the data into the S3 bucket

- dl.cfg: contains the AWS credentials

## How to run it:

1- Edit the AWS credentials in the config file.

2- Create an S3 bucket and replace the output_data variable in the main() function with s3a://<bucket name>/.


3- Run the script file : $ python etl.py