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

# Script generated for node customer trusted
customertrusted_node1776438320662 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1776438320662")

# Script generated for node accelerometer trusted
accelerometertrusted_node1776438388271 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1776438388271")

# Script generated for node SQL Query
SqlQuery0 = '''
select  distinct c.* 
from myDataSource1 c
join myDataSource a
on a.user = c.email
'''
SQLQuery_node1776438426191 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometertrusted_node1776438388271, "myDataSource1":customertrusted_node1776438320662}, transformation_ctx = "SQLQuery_node1776438426191")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776438426191, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776438315087", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1776438569694 = glueContext.getSink(path="s3://s3-glue-athena-udacity/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1776438569694")
customercurated_node1776438569694.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customercurated_node1776438569694.setFormat("json")
customercurated_node1776438569694.writeFrame(SQLQuery_node1776438426191)
job.commit()