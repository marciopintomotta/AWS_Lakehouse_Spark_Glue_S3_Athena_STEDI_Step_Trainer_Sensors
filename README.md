# Creating a data lakehouse solution base on Spark Glue S3 Athena on AWS for the Step Trainer sensors APP  STEDI 


## Project Description 

STEDI, an Step Trainer startup, need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, 
and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Motivation

My goal was building an ELT pipeline based on AWS Glue, AWS S3, Python, and Spark, to create Python scripts to build a lakehouse solution in 
AWS that satisfies these requirements from the STEDI data scientists.

 * AWS S3 for host landing, trusted and cureates data zones
 * AWS Glue Studio jobs (Python, and Spark) to make data transformacion
 * AWS Athena - verify Glue job querying the created/transformed data in each data zone

 The lakehouse solution is bult using 3 data zones (Landing, Trusted, Curated) 


## Project Requirements


### Landing Zone

#### Customer Landing 

##### Query Customer Landing zone using AWS Athena



#### Accelerometer Landing 

##### Query Accelerometer Landing zone using AWS Athena

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/customer_landing.png " Star")



### Trusted Zone

### Curated Zone
