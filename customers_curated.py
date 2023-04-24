import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Customer Trusted
S3CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aws/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerTrusted_node1",
)

# Script generated for node S3 Accelerometer Trusted
S3AccelerometerTrusted_node1682280707843 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house-aws/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerTrusted_node1682280707843",
    )
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3CustomerTrusted_node1,
    frame2=S3AccelerometerTrusted_node1682280707843,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1682280894833 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "user",
        "timeStamp",
        "x",
        "y",
        "z",
        "shareWithPublicAsOfDate",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1682280894833",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1682332766545 = DynamicFrame.fromDF(
    DropFields_node1682280894833.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1682332766545",
)

# Script generated for node S3 Customer Curated
S3CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1682332766545,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aws/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3CustomerCurated_node3",
)

job.commit()