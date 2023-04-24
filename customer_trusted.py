import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re


"""

A Python script using Spark that sanitizes the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed 
to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.

"""


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Customer Landing
S3CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-aws/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerLanding_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Filter.apply(
    frame=S3CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 Customer Trusted
S3CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-aws/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3CustomerTrusted_node3",
)

job.commit()