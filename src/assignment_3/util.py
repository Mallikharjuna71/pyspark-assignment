# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
data = [
(1, 101, 'login', '2023-09-05 08:30:00'),
(2, 102, 'click', '2023-09-06 12:45:00'),
(3, 101, 'click', '2023-09-07 14:15:00'),
(4, 103, 'login', '2023-09-08 09:00:00'),
(5, 102, 'logout', '2023-09-09 17:30:00'),
(6, 101, 'click', '2023-09-10 11:20:00'),
(7, 103, 'click', '2023-09-11 10:15:00'),
(8, 102, 'click', '2023-09-12 13:10:00')
]
user_log_df = spark.createDataFrame(data, ['log id', 'user$id', 'action', 'timestamp'])
user_log_df.display()
user_log_df.printSchema()

# COMMAND ----------

#2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
user_log_df = user_log_df.withColumnRenamed('log id', 'log_id').withColumnRenamed('user$id', 'user_id').withColumnRenamed('action', 'user_activity').withColumnRenamed('timestamp', 'time_stamp')
user_log_df.display()

# COMMAND ----------

#4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its
#data type
user_log_df = user_log_df.withColumn('time_stamp',to_date(date_format(col('time_stamp'), 'yyyy-MM-dd')))
user_log_df.printSchema()

# COMMAND ----------

#3. Write a query to calculate the number of actions performed by each user in the last 7 days
from datetime import timedelta
latest_date = user_log_df.select(max('time_stamp')).collect()[0][0]
start_date = latest_date - timedelta(days=6)
user_log_df.filter(col('time_stamp').between(start_date, latest_date)).groupby(col('user_id')).agg(count('*').alias('activity_count')).display()

# COMMAND ----------

#5. Write the data frame as a CSV file with different write options except
user_log_df.write.mode("overwrite").option("header", "true").csv("user_activity_log.csv")

# COMMAND ----------

#6. Write it as a managed table with the Database name as user and table name as login_details with overwrite mode.
user_log_df.write.mode('overwrite').saveAsTable('user_log_table')
