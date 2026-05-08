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

# Script generated for node step trainer landing
steptrainerlanding_node1776439079733 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="steptrainerlanding_node1776439079733")

# Script generated for node customer curated
customercurated_node1776439048245 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customercurated_node1776439048245")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.* 
from myDataSource s 
join myDataSource1 c 
on c.serialnumber = s.serialnumber
'''
SQLQuery_node1776439137527 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":steptrainerlanding_node1776439079733, "myDataSource1":customercurated_node1776439048245}, transformation_ctx = "SQLQuery_node1776439137527")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776439137527, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776438315087", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1776439271708 = glueContext.getSink(path="s3://s3-glue-athena-udacity/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1776439271708")
steptrainertrusted_node1776439271708.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1776439271708.setFormat("json")
steptrainertrusted_node1776439271708.writeFrame(SQLQuery_node1776439137527)
job.commit()