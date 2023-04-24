import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

# Script generated for node S3 Accelerometer Landing
S3AccelerometerLanding_node1682280707843 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lake-house-aws/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerLanding_node1682280707843",
    )
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=S3CustomerTrusted_node1,
    frame2=S3AccelerometerLanding_node1682280707843,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node2",
)

# Script generated for node Drop Fields
DropFields_node1682280894833 = DropFields.apply(
    frame=Join_node2,
    paths=[
        "serialNumber",
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
    transformation_ctx="DropFields_node1682280894833",
)

# Script generated for node S3 Accelerometer Trusted
S3AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682280894833,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aws/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3AccelerometerTrusted_node3",
)

job.commit()