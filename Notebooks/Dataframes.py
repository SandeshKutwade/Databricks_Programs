# Databricks notebook source
# we have python list 
        data = [("Raj", 28, "Mumbai", 75000),
                ("Priya", 35, "Delhi", 95000),
                ("Amit", 22, "Pune", 45000),
                ("Sara", 45, "Mumbai", 110000),
                ("Rahul", 31, "Delhi", 82000),
                ("Neha", 27, "Pune", 61000)]

        columns = ["name","age","city","salary"]

        create_df = spark.createDataFrame(data , columns)
        create_df.show()
        print("dataframe created with python list data")


# COMMAND ----------

# way 2 = from CSV file 
csv_df= spark.read.csv("/FileStore/tables/sample.csv", header=True, inferSchema= True)
# header = consider first row as a column name 
# inferschema = let spark to automatically detect datatypes

# COMMAND ----------

json_df = spark.read.json("/FileStore/tables/sample.json")
#

# COMMAND ----------

data = [("Raj", 28, "Mumbai", 75000),
        ("Priya", 35, "Delhi", 95000),
        ("Amit", 22, "Pune", 45000),
        ("Sara", 45, "Mumbai", 110000),
        ("Rahul", 31, "Delhi", 82000),
        ("Neha", 27, "Pune", 61000),
        ("Karan", None, "Mumbai", 55000),
        ("Divya", 29, "Delhi", None)]
columns = ["name","age","city","salary"]

simple_df = spark.createDataFrame(data, columns)
simple_df.show()
simple_df.printSchema() 


# COMMAND ----------

print("total number of rows", simple_df.count())
print ("columns",simple_df.columns)
simple_df.describe()

# COMMAND ----------

simple_df.filter((simple_df.city == "Mumbai") & (simple_df.age > 25)).show()

# COMMAND ----------

from pyspark.sql.functions import when 
new_df_with_cat = simple_df.withColumn("level",when (simple_df.salary >= 90000 , "Senior").when (simple_df.salary >= 60000, "Mid").otherwise("Junior"))
new_df_with_cat.show()

# COMMAND ----------

from pyspark.sql.functions import avg, count
simple_df.groupBy("city").agg(
    avg("salary").alias("avg_salary"),
    count("name").alias("headcount")
).orderBy("avg_salary", ascending=False).show()

# COMMAND ----------

print("Rows with null age:")
simple_df.filter(simple_df.age.isNull()).show()

# COMMAND ----------

print("After filling nulls:")
simple_df.fillna({"age": 0, "salary": 0}).show()

# COMMAND ----------

# lets run realworld operation on above dataframe
result = (simple_df
    .fillna({"age": 0, "salary": 0})
    .filter(simple_df.salary > 0)
    .withColumn("tax", simple_df.salary * 0.3)
    .select("name", "city", "salary", "tax")
    .orderBy(simple_df.salary.desc())
)
result.show()

# COMMAND ----------

