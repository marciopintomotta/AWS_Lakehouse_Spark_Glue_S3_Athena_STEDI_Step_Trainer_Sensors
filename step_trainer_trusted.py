import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


"""

A Python script using Spark that read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that 
contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

"""


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Step Trainer landing
S3StepTrainerlanding_node1682333743210 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aws/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3StepTrainerlanding_node1682333743210",
)

# Script generated for node S3 Customer Curated
S3CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aws/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1",
)

# Script generated for node Join
Join_node1682333625703 = Join.apply(
    frame1=S3CustomerCurated_node1,
    frame2=S3StepTrainerlanding_node1682333743210,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1682333625703",
)

# Script generated for node Drop Fields
DropFields_node1682339217694 = DropFields.apply(
    frame=Join_node1682333625703,
    paths=[
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1682339217694",
)

# Script generated for node S3 Step Trainer Trusted
S3StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682339217694,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aws/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3StepTrainerTrusted_node3",
)

job.commit()