# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]
schema = StructType([
    StructField("employee_id", IntegerType()),
    StructField("employee_name", StringType()),
    StructField("department", StringType()),
    StructField("state", StringType()),
    StructField("salary", IntegerType()),
    StructField("age", IntegerType())
])
employee_df = spark.createDataFrame(data, schema)

# COMMAND ----------

d_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")]
d_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])
department_df = spark.createDataFrame(d_data, d_schema)

# COMMAND ----------

c_data = [
    ("ny", "Newyork"),
    ("ca", "California"),
    ("uk", "Russia")]

c_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)])

country_df = spark.createDataFrame(c_data, c_schema)


# COMMAND ----------

# 1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in
# dynamic way
def createDataFrame(d1, s1, d2, s2, d3, s3):
    employee_df = spark.createDataFrame(d1, s1)
    department_df = spark.createDataFrame(d2, s2)
    country_df = spark.createDataFrame(d3, s3)
    return employee_df, department_df, country_df


createDataFrame(data, schema, d_data, d_schema, c_data, c_schema)
employee_df.display()
department_df.display()
country_df.display()


# COMMAND ----------

# 2. Find avg salary of each department
def avg_salary(d):
    return d.groupby('department').agg(avg('salary').alias('avg_salary'))


avg_salary(employee_df).display()

# COMMAND ----------

# 3. Find the employee’s name and department name whose name starts with ‘m’
joined_data = employee_df.join(department_df, employee_df.department == department_df.dept_id, 'inner')
joined_data.filter(joined_data.employee_name.like('m%')).select('employee_name', 'dept_name').display()

# COMMAND ----------

# 4. Create another new column in  employee_df as a bonus by multiplying employee salary *2
employee_bonus_df = employee_df.withColumn('bonus', col('salary') * 2)
employee_bonus_df.display()

# COMMAND ----------

# 5. Reorder the column names of employee_df columns as
# (employee_id,employee_name,salary,State,Age,department)
employee_df = employee_df.select('employee_id', 'employee_name', 'salary', 'age', 'department')
employee_df.display()

# COMMAND ----------

# 6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a
dynamic
way


def joins(a, b):
    a.join(b, a.department == b.dept_id, 'inner').display()
    a.join(b, a.department == b.dept_id, 'left').display()
    a.join(b, a.department == b.dept_id, 'right').display()


joins(employee_df, department_df)

# COMMAND ----------

# 7. Derive a new data frame with country_name instead of State in employee_df
employee_df = employee_bonus_df.join(country_df, employee_bonus_df.state == country_df.country_code, 'left').drop(
    'country_code', 'state', 'bonus')
employee_df.display()


# COMMAND ----------

# 8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the
# load_date column with the current date
def convert_columns(d):
    for i in d.columns:
        sc = ''
        for c in i:
            if c.isupper():
                sc += c.lower()
            else:
                sc += c
        d = d.withColumnRenamed(i, sc)
    d = d.withColumn('load_date', current_date())
    return d


employee_df = convert_columns(employee_df)
employee_df.display()

# COMMAND ----------

# 9. create 2 external tables with parquet, CSV format with the same name database name, and 2 different table  names as CSV and parquet format.
employee_df.write.mode("overwrite").format("parquet").saveAsTable("employee_parquet_table")

# COMMAND ----------

employee_df.write.mode("overwrite").format("csv").saveAsTable("employee_csv_table")
