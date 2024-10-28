# Databricks notebook source
#1. Read JSON file provided in the attachment using the dynamic function
json_data = spark.read.format("json").options(multiLine=True).load("dbfs:/FileStore/shared_uploads/arjun37ca@gmail.com/json/nested_json_file.json")
json_data.display()

# COMMAND ----------

#2. flatten the data frame which is a custom schema
from pyspark.sql.functions import *
fltten_json_data = json_data.select('id', 'properties', explode('employees').alias('employee')).select( '*','employee.empId', 'employee.empName', "properties.name", "properties.storeSize").drop('properties', 'employee')
fltten_json_data.display()

# COMMAND ----------

#3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)
print(json_data.count())
print(fltten_json_data.count())

# COMMAND ----------

#4. Differentiate the difference using explode, explode outer, posexplode functions
data = [
    (1, ["apple", "banana", None]),
    (2, ["orange", None, "grape"]),
    (3, None),
    (None, ["pear", "peach"])
]

d = spark.createDataFrame(data, ["id", "fruits"])
d.display()
d.select('id', explode(d.fruits).alias('fruits')).display()
d.select('id', explode_outer(d.fruits).alias('fruits')).display()
d.select('id', posexplode(d.fruits).alias('index','fruits')).display()


# COMMAND ----------

#5. Filter the id which is equal to 1001
fltten_json_data.filter(fltten_json_data.empId==1001).display()

# COMMAND ----------

# 6. convert the column names from camel case to snake case
def convertColumnName(d):
    for i in d.columns:
        sc = ''
        for c in i:
            if c.isupper():
                sc += '_'+c.lower()
            else:
                sc += c
        d = d.withColumnRenamed(i, sc)
    return d
fltten_json_data = convertColumnName(fltten_json_data)
fltten_json_data.display()


# COMMAND ----------

#7. Add a new column named load_date with the current date
fltten_json_data = fltten_json_data.withColumn('load_date', current_date())
fltten_json_data.display()

# COMMAND ----------

#8. create 3 new columns as year, month, and day from the load_date column
fltten_json_data = fltten_json_data.withColumn('year', year(col('load_date'))).withColumn('month', month(col('load_date'))).withColumn('day', date_format(col('load_date'), 'dd').alias('day'))
fltten_json_data.display()
