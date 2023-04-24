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

# Script generated for node S3 Accelerometer Landing
S3AccelerometerLanding_node1682344783364 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house-aws/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerLanding_node1682344783364",
    )
)

# Script generated for node S3 Customer trusted
S3Customertrusted_node1682344781101 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aws/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3Customertrusted_node1682344781101",
)

# Script generated for node Join
Join_node1682344992885 = Join.apply(
    frame1=S3Customertrusted_node1682344781101,
    frame2=S3AccelerometerLanding_node1682344783364,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1682344992885",
)

# Script generated for node Drop Fields
DropFields_node1682345211535 = DropFields.apply(
    frame=Join_node1682344992885,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1682345211535",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1682345427505 = DynamicFrame.fromDF(
    DropFields_node1682345211535.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1682345427505",
)

# Script generated for node S3 - Customer Curated
S3CustomerCurated_node1682345451122 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1682345427505,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aws/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3CustomerCurated_node1682345451122",
)

job.commit()