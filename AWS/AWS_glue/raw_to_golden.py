from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.window import Window
import datetime
from awsglue.utils import getResolvedOptions
import sys
import json
import pandas as pd
import pytz
import time
os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
time.tzset()

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Thiết lập thông tin AWS S3

current_hour = datetime.datetime.now().hour
current_hour = f"{current_hour:02d}"
current_date = datetime.date.today().strftime("%d")
current_month = datetime.date.today().strftime("%m")
current_year = datetime.date.today().strftime("%Y")
current_minute = datetime.datetime.now().minute
current_time = datetime.datetime.now().strftime("%Y-%m-%d")
print("HHHH",current_hour)

# current_hour = "23"
# current_date = "21"
# current_month = "11"
# current_year = "2023"

bucket_name = 'lastnifi'
target_bucket = 'goldminder'
raw_zone_prefix = f"{current_year}/{current_month}/{current_date}/{current_hour}/"

golden_zone_prefix = f"{current_year}/{current_month}/{current_date}/{current_hour}/{current_minute}"
table = 'customer'

# Tạo kết nối tới S3
s3 = boto3.client('s3')

# Lấy danh sách các thư mục con theo thứ tự từ mới nhất đến cũ nhất

# Lấy thư mục mới nhất trong customer và cdc
file_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=raw_zone_prefix)['Contents']
#newest_file = file_list[-1]
latest_file = s3.get_object(Bucket=bucket_name, Key=file_list[-1]['Key'])['Body'].read().decode("utf-8")

json_data = json.loads(latest_file)

print("here",type(json_data))

json_data = json_data["data"]
transform_json = []
for i in json_data:
    dict_value = {}
    for key,value in i.items():
        #print("key",key)
        if isinstance(value,dict):
            #print("key",key)
            for sub_key,sub_value in value.items():
                sub_key = key+'_'+sub_key
                dict_value[sub_key] = sub_value
        else:
            dict_value[key] = value 
    transform_json.append(dict_value)
    
df = pd.DataFrame(transform_json)

print(df.columns)

df = df[['flight_status','departure.airport', 'departure.iata','departure.scheduled','departure.estimated','departure.delay','arrival.airport','arrival.iata','arrival.scheduled','arrival.estimated', 'airline.name','airline.iata','flight.iata' ]]




spark = SparkSession.builder.getOrCreate()
df_spark = spark.createDataFrame(df)

df_spark = df_spark.withColumn("date_partition", lit(current_time))

# Lưu tệp Parquet kết quả vào golden_zone
s3_output_path = f"s3://{target_bucket}/{golden_zone_prefix}"
        
df_spark.coalesce(1).write.mode("overwrite").option("header", "false").csv(s3_output_path)


# Dừng SparkSession cho xử lý SCD Type 2
spark.stop()

# Dừng phiên bản Spark
sc.stop()