import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node step trainer trusted
steptrainertrusted_node1776439643015 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1776439643015")

# Script generated for node accelerometer trusted
accelerometertrusted_node1776439606266 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1776439606266")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.sensorreadingtime,
s.serialnumber,
s.distancefromobject,
a.user,
a.x,
a.y,
a.z
from myDataSource s
join myDataSource1 a 
on a.timestamp = s.sensorreadingtime
'''
SQLQuery_node1776439696194 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":steptrainertrusted_node1776439643015, "myDataSource1":accelerometertrusted_node1776439606266}, transformation_ctx = "SQLQuery_node1776439696194")

# Script generated for node machine learning curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776439696194, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776438315087", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearningcurated_node1776440058526 = glueContext.getSink(path="s3://s3-glue-athena-udacity/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1776440058526")
machinelearningcurated_node1776440058526.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machinelearningcurated_node1776440058526.setFormat("json")
machinelearningcurated_node1776440058526.writeFrame(SQLQuery_node1776439696194)
job.commit()