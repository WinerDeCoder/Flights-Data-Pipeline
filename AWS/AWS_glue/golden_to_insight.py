import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1700826034745 = glueContext.create_dynamic_frame.from_catalog(database="gggg", table_name="goldminder", transformation_ctx="AWSGlueDataCatalog_node1700826034745")

# Script generated for node Select Fields
SelectFields_node1700901308370 = SelectFields.apply(frame=AWSGlueDataCatalog_node1700826034745, paths=["date", "flight_iata", "airline_iata", "airline_name", "arrival_estimated", "arrival_scheduled", "arrival_iata", "arrival_timezone", "arrival_airport", "departure_estimated", "departure_scheduled", "departure_iata", "departure_timezone", "departure_airport", "flight_status", "flight_date"], transformation_ctx="SelectFields_node1700901308370")

# Script generated for node Drop Duplicates
DropDuplicates_node1700826045369 =  DynamicFrame.fromDF(SelectFields_node1700901308370.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1700826045369")

# Script generated for node Amazon S3
AmazonS3_node1700826078409 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1700826045369, connection_type="s3", format="csv", connection_options={"path": "s3://testgolden", "partitionKeys": ["flight_date", "departure_airport", "arrival_airport", "airline_name"]}, transformation_ctx="AmazonS3_node1700826078409")

job.commit()