# Databricks notebook source
# MAGIC %md
# MAGIC Delete Duplicates in a dataframe using pyspark code
# MAGIC
# MAGIC Key function: dropDuplicates()
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# creating a dataframe

data = [(1,'asif',65.22),(1,'asif',65.22),(2,'asif',65.22),(3,'ashiq',67.5)]
cols=['id','name','percentage']

df=spark.createDataFrame(data,cols)
df.display()

# COMMAND ----------

# drop duplicates without parameters. Consider all the columns and removes duplicates.

drop_duplicates=df.dropDuplicates()
drop_duplicates.display()
# df.show()

#drop duplicates on selective columns. The method keeps the first occurrence of each unique combination of values in the specified columns and removes the subsequent duplicates.

drop_duplicates_selective=df.dropDuplicates(['name'])
drop_duplicates_selective.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Add a new column
# MAGIC
# MAGIC key function: withColumn(), lit()- function adds a default value to the column created

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

 #static value to the new column
df1=df.withColumn('address',lit('nellore'))
df1.show() 


df.withColumn('New percentage',df.percentage+2).show() #random value with example

# COMMAND ----------

# MAGIC %md
# MAGIC Drop and rename a column in a dataframe
# MAGIC
# MAGIC key function: drop(), withColumnRenamed()

# COMMAND ----------

df1.show()
df1.drop('address').show()  #drop column called address
df.withColumnRenamed('name','customer').show()  #rename column

# COMMAND ----------

# MAGIC %md
# MAGIC other functions

# COMMAND ----------

df.printSchema() #gets schema

df.head()  #This function returns the first n rows. Default is 1.

df.columns #gets columns


# COMMAND ----------

# MAGIC %md
# MAGIC select particular column
# MAGIC
# MAGIC key function: select

# COMMAND ----------

df.select('name','id').show()
type(df)            #gets the type of a object
type(df.id)  #gets the type of a object

# COMMAND ----------

# MAGIC %md
# MAGIC Print a string n times

# COMMAND ----------

a='pyspark is future'

# 
print(a*10)
print((a+'\n')*10)   # print in a new line 


# COMMAND ----------

# MAGIC %md
# MAGIC fillna and dropna commands
# MAGIC fillna - Replace nulls with a particular value
# MAGIC dropna - delete records from a dataframe if contains nulls

# COMMAND ----------


# drop duplicate records
data = [
    (1, 'Alice', None),
    (2, 'Bob', 'New York'),
    (3, 'Cathy', None),
    (4, None, 'Los Angeles')
]

# Define column names
cols = ['id', 'name', 'city']

df3=spark.createDataFrame(data,cols)
df3.show()

df3.dropna().show()

# Dictonary values for a particular column that needs to be replaced. 

df3.fillna({'name':'david','city':'australia'}).show()

# If given single paramter value - This will replace all the column values whereever is null

df3.fillna('England').show()

