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


## Project Structure

```
AWS_ELT_Data_Warehouse_S3_2_Redshift_Sparkify/
 ├── images                              Images
 └── accelerometer_landing.sql      SQL script to create accelerometer_landing table
 ├── accelerometer_trusted.py       Python script to create an accelerometer landing data
 ├── customer_landing.sql           Python script to verify the connection to the AWS Redshift cluster
 ├── customer_trusted.py            Python script to create the tables in AWS Redshift
 ├── customers_curated.py           Python script to load data from S3 to staging, fact and dimensional tables in AWS Redshift
 ├── machine_learning_curated.py    Python script to view an Example Output from the fact and dimensional tables on AWS Redshift
 ├── README.md                      Documentation of the project
 ├── step_trainer_trusted.py        Python script to delete the Redshift Cluster on AWS
 
## Project Requirements


### Landing Zone

#### Customer Landing 

##### Query Customer Landing zone using AWS Athena

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/customer_landing.png)

#### Accelerometer Landing 

##### Query Accelerometer Landing zone using AWS Athena

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/accelerometer_landing.png)



### Trusted Zone

##### Customer Trusted job using AWS Glue Studio

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/customer_trusted_job.png)


##### Accelerometer Trusted job using AWS Glue Studio

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/accelerometer_trusted_job.png)


##### Step Trainer Trusted job using AWS Glue Studio

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/step_trainer_trusted_job.png)



### Curated Zone

##### Customer Curated job using AWS Glue Studio

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/customers_curated_job.png)