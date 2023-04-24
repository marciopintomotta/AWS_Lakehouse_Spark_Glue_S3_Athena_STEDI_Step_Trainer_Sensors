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

 The lakehouse solution is bult using 3 data zones (Landing, Trusted, Curated) on AWS S3 and a set of Glue jobs, which transform and move the data through the zones according to the requirements.

 
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

A Python script using Spark that sanitizes the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/customer_trusted_job.png)


##### Accelerometer Trusted job using AWS Glue Studio

 A Python script using Spark that sanitizes the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/accelerometer_trusted_job.png)


##### Step Trainer Trusted job using AWS Glue Studio

A Python script using Spark that read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/step_trainer_trusted_job.png)



### Curated Zone

##### Customer Curated job using AWS Glue Studio

A Python script using Spark that sanitizes the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/customers_curated_job.png)


##### Machine Learning Curated job using AWS Glue Studio

A Python script using Spark that creates an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.

![Alt text](https://github.com/marciopintomotta/AWS_Lakehouse_Spark_Glue_S3_Athena_STEDI_Step_Trainer_Sensors/blob/master/images/machine_learning_curated_job.png)


## Project Structure

```
AWS_ELT_Data_Warehouse_S3_2_Redshift_Sparkify/
 ├── images                         Images files
 └── accelerometer_landing.sql      SQL script to create accelerometer_landing table
 ├── accelerometer_trusted.py       Python script using Spark to move from landing to trust accelerometer data from customers who agreed to share their data
 ├── customer_landing.sql           SQL script to create customer_landing table
 ├── customer_trusted.py            Python script using Spark to move from landing to trust customer data from customers who agreed to share their data
 ├── customers_curated.py           Python script using Spark to move from trust to curated customer data who have accelerometer data and have agreed to share their data
 ├── machine_learning_curated.py    Python script using Spark to move from trust to curated Trainer Readings and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data
 ├── README.md                      Documentation of the project
 ├── step_trainer_trusted.py        Python script using Spark to move from landing to trust step trainer data who have accelerometer data and have agreed to share 