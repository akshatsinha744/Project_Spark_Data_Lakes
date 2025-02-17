import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dend-lake-house/customer/landing"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Apply Privacy Filter
PrivacyFilter_node = Filter.apply(
    frame=S3bucket_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node",
)

# Enable dynamic schema inference and update Data Catalog
TrustedCustomerZone_node = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dend-lake-house/customer/trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="TrustedCustomerZone_node",
)

job.commit()
