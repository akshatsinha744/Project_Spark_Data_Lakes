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

# Load Accelerometer Trusted data
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

# Load Step Trainer Trusted data
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node",
)

# Perform Join Operation
Join_node = Join.apply(
    frame1=StepTrainerTrusted_node,
    frame2=AccelerometerTrusted_node,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node",
)

# Drop unnecessary fields
DropFields_node = DropFields.apply(
    frame=Join_node,
    paths=["user"],
    transformation_ctx="DropFields_node",
)

# Enable dynamic schema inference and update Data Catalog
MachineLearningCurated_node = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node,
    database="dend",
    table_name="machine_learning_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
        "partitionKeys": []  # Add partition keys if applicable
    },
    transformation_ctx="MachineLearningCurated_node",
)

job.commit()
