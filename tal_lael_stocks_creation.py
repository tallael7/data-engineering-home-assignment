import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, avg, desc
from pyspark.sql.types import DoubleType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Path to your CSV file in S3
input_path = "s3://data-engineer-assignment-tal-lael-dev/dw_staging/stocks_data.csv"

# Load your CSV or source into DataFrame
df = spark.read.option("header", "true").csv(input_path)

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("stocks_data")

##----------------------------------------------------------Run the SQL query to create result1
result_df = spark.sql("""
select cast(date as date) as date 
    ,avg(cast(close as decimal(10,5)) )  as avg_closing_rate 
    from stocks_data 
    group by cast(date as date) 
    order by cast(date as date) desc
""")
DesVarible='avg_closing_rate'

##write into result1
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") ##----- set the partitionOverwrite to over write only the single partition
result_df.write.format("parquet").mode("overwrite").save("s3://data-engineer-assignment-tal-lael-dev/dw/"+DesVarible,header = 'true') 
result_df.unpersist()

##----------------------------------------------------------run code for result2
df = df.withColumn("close", col("close").cast(DoubleType()))
df = df.withColumn("volume", col("volume").cast(DoubleType()))

df = df.withColumn("worth", col("close") * col("volume"))

# average worth per ticker
avg_worth_df = df.groupBy("ticker") \
    .agg(avg("worth").alias("avg_worth"))

# highest average worth
top_stock_df = avg_worth_df.orderBy(desc("avg_worth")).limit(1)

DesVarible="top_stock"
##write into result2
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") ##----- set the partitionOverwrite to over write only the single partition
top_stock_df.write.format("parquet").mode("overwrite").save("s3://data-engineer-assignment-tal-lael-dev/dw/"+DesVarible,header = 'true') 
top_stock_df.unpersist()
avg_worth_df.unpersist()

##----------------------------------------------------------run code for result3
# highest stddev ticker
top_stddev_df = spark.sql("""
with  top_stddev 
as (
    select ticker,
  --cast(date as date) as date,
  --cast(close as decimal(10,5)) as close,
  ROUND(((cast(close as decimal(10,5)) -
  LAG(cast(close as decimal(10,5))) OVER (PARTITION BY ticker ORDER BY cast(date as date)))/(LAG(cast(close as decimal(10,5))) OVER (PARTITION BY ticker ORDER BY cast(date as date))))* 100,2) as percent_change
  FROM stocks_data
  )
  select ticker,
  cast(stddev(percent_change) as double) as standard_deviation
  from top_stddev
  group by ticker order by stddev(percent_change) desc
  limit 1
""")

DesVarible="top_standard_deviation"
##write into result2
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") ##----- set the partitionOverwrite to over write only the single partition
top_stddev_df.write.format("parquet").mode("overwrite").save("s3://data-engineer-assignment-tal-lael-dev/dw/"+DesVarible,header = 'true') 
top_stddev_df.unpersist()

##----------------------------------------------------------run code for result4
# 3 highest percent_return_30d
top_lastM_change_df = spark.sql("""
WITH joined AS (
  SELECT
    cur.ticker,
    CAST(cur.date AS date) AS cur_date,
    CAST(cur.close AS double) AS cur_close,
    date_add(CAST(cur.date AS date) , -30) AS date_minus_30,
    CAST(prv.date AS date) AS prv_date,
    CAST(NULLIF(prv.close, '') AS double) AS prv_close
  FROM stocks_data AS cur
  LEFT JOIN stocks_data AS prv
    ON cur.ticker = prv.ticker
    AND date_add(CAST(cur.date AS date), -30) = CAST(prv.date AS date)
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY ticker, cur_date ORDER BY prv_date DESC) AS rn
  FROM joined
  WHERE prv_close IS NOT NULL
)
SELECT
  ticker,
  cur_date as date,
  --cur_close,
  --prv_date,
  --prv_close,
  ((cur_close - prv_close) / prv_close) * 100 AS percent_return_30d
FROM ranked
WHERE rn = 1
ORDER BY percent_return_30d DESC
LIMIT 3;
""")

DesVarible="top_lastM_change"
##write into result2
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") ##----- set the partitionOverwrite to over write only the single partition
top_lastM_change_df.write.format("parquet").mode("overwrite").save("s3://data-engineer-assignment-tal-lael-dev/dw/"+DesVarible,header = 'true') 


job.commit()