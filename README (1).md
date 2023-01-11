
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

1- Uncomment the AWS credentials and add it in the config file  

 OR

 keep it uncommented and ignore the credentials if you plan to run the script from an EMR cluster

2- Create an S3 bucket and replace the output_data variable in the main() function with s3a://<bucket name>/.


3- To run the script locally run : $ python etl.py

OR 

To run it from an EMR cluster, copy the etl.py to the EMR cluster using the pscp package 
and run : /usr/bin/spark-submit etl.py