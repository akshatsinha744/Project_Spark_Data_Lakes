
# STEDI Lakehouse Solution: Step Trainer Trusted Data Pipeline

## Overview

This project implements a data pipeline using AWS Glue, Amazon S3, and the AWS Glue Data Catalog to curate sensor data for the STEDI project. The pipeline processes customer and step trainer data, joining and cleaning the datasets so that only trusted step trainer data is retained for downstream analytics and machine learning.

The solution is implemented in two ways:
- **Code-Based AWS Glue Job:** A PySpark script that reads, processes, and writes data.
- **Visual ETL Workflow:** A no-code approach using AWS Glue Studio to create the same pipeline.

## Architecture

**Data Sources:**
- **Customers Curated Data:**  
  Stored in the AWS Glue Data Catalog (table: `customers_curated` in database `dend` or `stedi` as per your configuration).
- **Step Trainer Landing Data:**  
  Stored as JSON files in Amazon S3 at:  
  `s3://dend-lake-house/step_trainer/landing/`

**Processing Steps:**
1. **Read Input Data:**  
   - Read customer curated data from the Glue Data Catalog.
   - Read step trainer landing data from S3 (JSON format).
2. **Join Operation:**  
   - Perform an inner join between the two datasets using the join keys:  
     - `serialNumber` from step trainer landing data.
     - `serialnumber` from customers curated data.
3. **Drop Unwanted Fields:**  
   - Remove fields such as `customername`, `email`, `phone`, `birthday`, `serialnumber`, `registrationdate`, `lastupdatedate`, `sharewithresearchasofdate`, `sharewithpublicasofdate`, and `sharewithfriendsasofdate`.
4. **Output the Trusted Data:**  
   - Write the resulting trusted step trainer data back to the AWS Glue Data Catalog as the table `step_trainer_trusted` (or optionally to S3).

## AWS Glue Visual ETL Workflow

Using AWS Glue Studio, the Visual ETL job is constructed with the following nodes:

1. **Source Node – Customers Curated:**  
   - **Type:** Data Catalog  
   - **Database:** `dend` (or `stedi` if updated)  
   - **Table:** `customers_curated`  
   
2. **Source Node – Step Trainer Landing:**  
   - **Type:** Amazon S3  
   - **Format:** JSON (with multiline disabled)  
   - **S3 Path:** `s3://dend-lake-house/step_trainer/landing/`  
   - **Recurse:** Enabled (to include files in subdirectories)

3. **Join Node:**  
   - **Join Type:** Inner Join  
   - **Join Keys:**  
     - Left: `serialNumber` (from Step Trainer Landing)  
     - Right: `serialnumber` (from Customers Curated)

4. **Drop Fields Node:**  
   - **Fields to Drop:** `customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate`

5. **Sink Node (Target):**  
   - **Type:** Data Catalog (or Amazon S3, if preferred)  
   - **Database:** `dend` (or `stedi`)  
   - **Table:** `step_trainer_trusted`

## Code Implementation

Below is an example AWS Glue Python script that performs the same steps as the Visual ETL workflow. It includes joining the datasets, dropping unwanted fields, and writing the output in JSON format. It also includes an extra step to drop duplicate records.

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Retrieve the job name from the arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load Customers Curated Data from the Glue Data Catalog
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="dend",                # Change to "stedi" if required
    table_name="customers_curated",
    transformation_ctx="customer_trusted_node",
)

# Load Step Trainer Landing Data from S3 (JSON Format)
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

# Join Step Trainer Landing with Customers Curated on serial number
join_node = Join.apply(
    frame1=step_trainer_landing_node,
    frame2=customer_trusted_node,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="join_node",
)

# Drop Unwanted Fields from the Joined Data
drop_fields_node = DropFields.apply(
    frame=join_node,
    paths=[
        "customername", "email", "phone", "birthday",
        "serialnumber", "registrationdate", "lastupdatedate",
        "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"
    ],
    transformation_ctx="drop_fields_node",
)

# Drop Duplicate Records
drop_duplicates_node = DynamicFrame.fromDF(
    drop_fields_node.toDF().dropDuplicates(),
    glueContext,
    "drop_duplicates_node"
)

# Write the Resulting Trusted Data to the Glue Data Catalog (or to S3)
sink_node = glueContext.write_dynamic_frame.from_catalog(
    frame=drop_duplicates_node,
    database="dend",                # Change to "stedi" if required
    table_name="step_trainer_trusted",
    transformation_ctx="sink_node",
)

job.commit()
```

## Prerequisites

- **AWS Account:** With permissions to use AWS Glue, S3, and the AWS Glue Data Catalog.
- **IAM Role:** Ensure your AWS Glue job has the necessary permissions.
- **S3 Bucket Setup:**  
  - `s3://dend-lake-house/step_trainer/landing/` should contain the JSON landing files.
- **Glue Data Catalog:**  
  - Tables `customers_curated` and `step_trainer_trusted` should be defined in the database (`dend` or `stedi` as appropriate).

## How to Run

1. **Using Visual ETL:**
   - Open AWS Glue Studio.
   - Create (or edit) the Visual ETL job following the steps outlined above.
   - Save and run the job.
2. **Using the Code-Based Job:**
   - Upload the provided Python script to AWS Glue.
   - Configure the job parameters (e.g., JOB_NAME) and ensure the correct IAM role is attached.
   - Run the job and monitor its progress via the AWS Glue console.

## Output

After successful execution, the curated trusted step trainer data will be available in the Glue Data Catalog table `step_trainer_trusted` and/or written to the designated S3 location (if configured that way).

## Contact

For questions or further assistance, please contact [Your Name or Team] at [your.email@example.com].
