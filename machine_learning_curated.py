import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

"""

A Python script using Spark that creates an aggregated table that has each of the Step Trainer Readings, 
and the associated accelerometer reading data for the same timestamp, 
but only for customers who have agreed to share their data, 
and make a glue table called machine_learning_curated.

"""



args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Step Trainer trusted
S3StepTrainertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aws/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3StepTrainertrusted_node1",
)

# Script generated for node S3 Accelerometer Trusted
S3AccelerometerTrusted_node1682350682042 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house-aws/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerTrusted_node1682350682042",
    )
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=S3StepTrainertrusted_node1,
    frame2=S3AccelerometerTrusted_node1682350682042,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 Machine Learning Curated
S3MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aws/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3MachineLearningCurated_node3",
)

job.commit()