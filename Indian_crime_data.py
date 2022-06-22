# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics of Indian Crime data set using PySpark RDD and Dataframes

# COMMAND ----------

from pyspark import SparkContext 
sc = SparkContext.getOrCreate() 
rdd = sc.textFile("/FileStore/tables/Indian_crime_data.csv")
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Converting all words in a rdd to lowercase and split the lines of a document using space.

# COMMAND ----------

def Func(lines):
  lines = lines.upper() 
  lines = lines.split() 
  return lines 
rdd1 = rdd.map(Func) 
rdd1.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #####flatMap(): A transformation operation that flattens the RDD/DataFrame after applying the function on every element and returns a new PySpark RDD/DataFrame.

# COMMAND ----------

rdd2 = rdd.flatMap(Func) 
rdd2.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### count(): returns the count of records on DataFrame. 

# COMMAND ----------

rdd.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### map(): An RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.

# COMMAND ----------

rdd_mapped = rdd.map(lambda x: (x,1)) 
rdd_grouped = rdd_mapped.groupByKey()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Taking first five elements from rdd_grouped

# COMMAND ----------

print(list((j[0], list(j[1])) for j in rdd_grouped.take(5)))

# COMMAND ----------

# MAGIC %md
# MAGIC #####reduceByKey: Transformation for counting the frequencies of each word in (key,value) pair RDD

# COMMAND ----------

rdd_mapped.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey(False).take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #####PySpark sampling: Getting random sample records from the dataset

# COMMAND ----------

sample1 = rdd_mapped.sample(False,.2,42) 
sample2 = rdd_mapped.sample(False,.2,42) 
join_on_sample1_sample2 = sample1.join(sample2) 
join_on_sample1_sample2.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### getNumPartition: Get the current length/size of partitions 

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Coalesce method: Used to decrease the number of partition in a Data Frame

# COMMAND ----------

rdd_coalesce = rdd.coalesce(1) 
rdd_coalesce.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### parallelize: Used to create an RDD from a list collection

# COMMAND ----------

num_rdd = sc.parallelize(range(1,1000)) 
num_rdd.reduce(lambda x,y: x+y)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### max,min,sum,variance,stdev: To take the maximum, minimum, sum, variance and standard deviation of RDD respectively

# COMMAND ----------

num_rdd.max(),num_rdd.min(), num_rdd.sum(),num_rdd.variance(),num_rdd.stdev()

# COMMAND ----------


