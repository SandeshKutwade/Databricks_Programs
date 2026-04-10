# Databricks notebook source
print("Databricks is working and live")
spark.version

# COMMAND ----------

# day 2 practice and ovserving lazy evaluation 
data = [("Raj", 28, "Mumbai"),
        ("Priya", 35, "Delhi"),
        ("Amit", 22, "Pune"),
        ("Sara", 45, "Mumbai"),
        ("Rahul", 31, "Delhi")]
colums = [ "names" , "age", "city" ]


# create dataframe 
df = spark.createDataFrame(data, colums)
print ("only dataframe created and nothing processed yegt!!")




# COMMAND ----------

# here in this cell we will add some transformations 
df_filtered = df.filter(df.age > 25)
df_selected = df_filtered.select("names","age","city")
print("Transformation added but still no movement happens")

# COMMAND ----------

# now its time to add Action phase 
df_selected.show()

# COMMAND ----------

df_selected.explain()

# COMMAND ----------

# lets run one more action 
count = df_filtered.count()
print(f"people above age 25 are : {count}")

# COMMAND ----------

