import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1776415181016 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1776415181016")

# Script generated for node Customer trusted
Customertrusted_node1776415113132 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customertrusted_node1776415113132")

# Script generated for node Join
Join_node1776415222917 = Join.apply(frame1=AccelerometerLanding_node1776415181016, frame2=Customertrusted_node1776415113132, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1776415222917")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1776415222917, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776415096462", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1776415455710 = glueContext.getSink(path="s3://s3-glue-athena-udacity/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1776415455710")
AccelerometerTrusted_node1776415455710.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1776415455710.setFormat("json")
AccelerometerTrusted_node1776415455710.writeFrame(Join_node1776415222917)
job.commit()