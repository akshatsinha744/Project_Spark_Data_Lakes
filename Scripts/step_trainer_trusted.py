import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load customers curated data
customers_curated_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="customers_curated",
    transformation_ctx="customers_curated_node",
)

# Load step trainer landing data
step_trainer_landing_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dend-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node",
)

# Perform Join Operation
Join_node = Join.apply(
    frame1=step_trainer_landing_node,
    frame2=customers_curated_node,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node",
)

# Drop unnecessary fields
DropFields_node = DropFields.apply(
    frame=Join_node,
    paths=[
        "customername", "email", "phone", "birthday", "serialnumber", "registrationdate",
        "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"
    ],
    transformation_ctx="DropFields_node",
)

# Enable dynamic schema inference and update Data Catalog
StepTrainerTrusted_node = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node,
    database="dend",
    table_name="step_trainer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
        "partitionKeys": []  # Add partition keys if applicable
    },
    transformation_ctx="StepTrainerTrusted_node",
)

job.commit()
