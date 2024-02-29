import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import round
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame



args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1702693073862 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://parthsourcebucket"], "recurse": True},
    transformation_ctx="AmazonS3_node1702693073862",
)

# Script generated for node Drop Fields
DropFields_node1702695178207 = DropFields.apply(
    frame=AmazonS3_node1702693073862,
    paths=["Dividends", "Stock Splits"],
    transformation_ctx="DropFields_node1702695178207"
)

DropFields_node1702695178207.toDF().printSchema()
DropFields_node1702695178207.toDF().show(5)



# Script to covert to Open,High,Low,Close to 2 decimal places

df = DropFields_node1702695178207.toDF()
df = df.withColumn("Open", round(df["Open"], 2))
df = df.withColumn("High", round(df["High"], 2))
df = df.withColumn("Low", round(df["Low"], 2))
df = df.withColumn("Close", round(df["Close"], 2))
df.show(5)

# convert back to dynamic frame
DropFields_node1702695178207 = DynamicFrame.fromDF(df, glueContext, "nested")


# Script generated for node Amazon S3
AmazonS3_node1702695317455 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1702695178207,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://parthdestinationbucket", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1702695317455",
)

job.commit()