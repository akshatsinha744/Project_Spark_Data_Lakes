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

# Load data from customer_trusted table
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node",
)

# Load data from accelerometer_landing table
accelerometer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node",
)

# Apply Customer Privacy Filter
CustomerPrivacyFilter_node = Join.apply(
    frame1=accelerometer_landing_node,
    frame2=customer_trusted_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node",
)

# Drop unnecessary fields
DropFields_node = DropFields.apply(
    frame=CustomerPrivacyFilter_node,
    paths=[
        "customername",
        "email",
        "phone",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "birthday",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node",
)

# Enable dynamic schema inference and update Data Catalog
S3bucket_node = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dend-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="S3bucket_node",
)

job.commit()
