# Databricks notebook source
# 1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data
from pyspark.sql.types import *
from pyspark.sql.functions import *
data1 = [(1, "iphone13"),(1, "dell i5 core"),(2, "iphone13"),(2, "dell i5 core"),(3, "iphone13"),(3, "dell i5 core"),(1, "dell i3 core"),(1, "hp i5 core"),(1, "iphone14"),(3, "iphone14"),(4, "iphone13"), (1, "iphone13")]
schema1 = StructType([StructField('customer', IntegerType()), StructField('product_model', StringType())])
purchase_data_df = spark.createDataFrame(data1, schema1)
purchase_data_df.display()

# COMMAND ----------

data_2 = [("iphone13",),("dell i5 core",),("dell i3 core",),("hp i5 core",),("iphone14",)]
schema_2 = StructType([StructField('product_model', StringType())])
product_data_df = spark.createDataFrame(data_2, schema_2)
product_data_df.display()

# COMMAND ----------

# 2.Find the customers who have bought only iphone13
purchase_data_df.filter(col('product_model')=='iphone13').select(col('customer')).distinct().display()

# COMMAND ----------

#3.Find customers who upgraded from product iphone13 to product iphone14
purchase_data_df.groupby(col('customer')).agg(collect_set(col('product_model')).alias('product_list')).filter((array_contains(col('product_list'), 'iphone13'))&(array_contains(col('product_list'), 'iphone14'))).select(col('customer')).display()

# COMMAND ----------

i13 = purchase_data_df.filter(col('product_model')=='iphone13').select('customer')
i14 = purchase_data_df.filter(col('product_model')=='iphone14').select('customer')
i13.intersect(i14).display()

# COMMAND ----------

# 4.Find customers who have bought all models in the new Product Data
Customer_product_df = purchase_data_df.groupby('customer').agg(countDistinct('product_model').alias('product_distinct_count'))
count = product_data_df.count()
Customer_product_df.filter(Customer_product_df.product_distinct_count==count).select('customer').display()
