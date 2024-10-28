# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
data = [("1234567891234567",),("5678912345671234",),("9123456712345678",),("1234567812341122",),("1234567812341342",)]
schema = ['card_number',]
credit_card_df = spark.createDataFrame(data, schema)
credit_card_df.display()

# COMMAND ----------

#2. print number of partitions

credit_card_df.rdd.getNumPartitions() #8

# COMMAND ----------

#3. decrease the partition size to 5
rp_df = credit_card_df.rdd.coalesce(5)

# COMMAND ----------

rp_df.getNumPartitions()

# COMMAND ----------

#4. increase the partition size back to its original partition size
od = rp_df.repartition(8)
od.getNumPartitions()

# COMMAND ----------

#5.Create a UDF to print only the last 4 digits marking the remaining digits as *
#Eg: ************4567
#6.output should have 2 columns as card_number, masked_card_number
def mask(k):
    return f"{'*'*(len(k)-4)+k[-4:]}"
mask_udf = udf(lambda x: mask(x))
credit_card_df.withColumn('masked_card_number', mask_udf(credit_card_df.card_number)).display()
