#!/usr/bin/env python
# coding: utf-8

# #### RDD -Resilient Distributed Dataset
# RDD is a fundamental building block of PySpark which is fault-tolerant, immutable distributed collections of objects.
# Immutable meaning, once you create an RDD, you cannot change it. Each record in RDD is divided into logical partitions, which can be computed on different nodes of the cluster. (Divide and Conquer algorithm.
# #### RDD Benefits
# 1. In-Memory Processing: PySpark loads the data from disk and process into memory and keeps the data in memory.
# 2. Immutability: Once you create an RDD, you cannot modify it. If you make any changes to the RDD, PySpark will create a new RDD and maintains the RDD lineage.
# 3. Fault- Tolerance: This is acheived because spark runs on a cluster
# 4. Partitioning: example: If you have records of anything from 1990-2021, you can partition this data for before 2020 - Partition1 and after 2021 - Partition2

# #### Create RDD using parallelize() function

# In[1]:


from pyspark.sql import SparkSession # This is the entry point for spark to work, with RDD, DataFrame, and Dataset
# # Spark session provides API for
# # Spark Context
# # SQL Context
# # Streaming Context
# # Hive Context



# Spark session & context
spark = SparkSession.builder.master('local').getOrCreate()
sc = spark.sparkContext
# # Spark Context is an entry point for your Spark Package and libraries and used programatically to
# # create Spark RDD, accumulators and broadcast variables on the cluster
# # YOU CAN CREATE ONLY ONE SPARKCONTEXT PER JVM (JAVA VIRTUAL MACHINE)


# In[3]:


data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = sc.parallelize(data)


# In[4]:


# Create an empty RDD using the emptyRDD
rddEmpty = sc.emptyRDD


# In[5]:


# Create an empty RDD with Partition
rddPartition = sc.parallelize([], 10) # This creates 10 Partitions


# In[6]:


rdd.collect() # This is not supposed to be used in production code; this must be used for debugging only


# In[7]:


rdd.first() # first element


# In[11]:


rdd.take(2) # first two elements in RDD


# In[12]:


rdd.getNumPartitions()


# In[13]:


rddPartition_2 = sc.parallelize(data, 10)


# In[14]:


rddPartition_2.getNumPartitions()


# In[16]:


# Example
tempData = [59, 57.2, 53.6, 55.4, 51.8, 53.6, 55.4]
parTempData = sc.parallelize(tempData, 2)
parTempData.collect()


# In[17]:


# Write a function to calculate Fahrenheit to Celsius
def fahrenheitToCentigrade(temperature):
    centigrade = (temperature-32) * 5/9
    return centigrade


# In[18]:


fahrenheitToCentigrade(59)


# In[19]:


parCentigradeData = parTempData.map(fahrenheitToCentigrade)


# In[20]:


parCentigradeData.collect()


# In[21]:


# Filter in your RDD Data - You can use filter() function
# Define a predicate - predicate is a function that tests a condition and returns true or false
# Define a predicate
def tempMoreThanThirteen(temperature):
    return temperature >= 13


# In[22]:


filteredTemperatureRDD = parCentigradeData.filter(tempMoreThanThirteen)
filteredTemperatureRDD.collect()


# In[24]:


filteredTemperatureRDD = parCentigradeData.filter(lambda x: x>= 13)
filteredTemperatureRDD.collect()


# ### Perform Basic Data Manipulation
# #### Using a student database, please perform the following objectives
# 1. Average grades per semester, each year, for each student
# 2. Top three students who have the highest average grades in the school year
# 3. Bottom three students who have the lowest average grades in the second year
# 4. All students who have earned more than an 80% average in the second semester

# In[25]:


studentMarksData = [["si1","year1",62.08,62.4],
	["si1","year2",75.94,76.75],
    ["si2","year1",68.26,72.95],
    ["si2","year2",85.49,75.8],
    ["si3","year1",75.08,79.84],
    ["si3","year2",54.98,87.72],
    ["si4","year1",50.03,66.85],
    ["si4","year2",71.26,69.77],
    ["si5","year1",52.74,76.27],
    ["si5","year2",50.39,68.58],
    ["si6","year1",74.86,60.8],
    ["si6","year2",58.29,62.38],
    ["si7","year1",63.95,74.51],
    ["si7","year2",66.69,56.92]]


# In[26]:


# Parallelize the data
studentMarksDataRDD = sc.parallelize(studentMarksData, 4)


# In[27]:


studentMarksDataRDD.take(2)


# In[29]:


# Objective 1: Calculate Average Semester Grades
# In the map function, an for lambda, the value of x is the first collection from the list
# example: x = ['si1', 'year1', 62.08, 62.4]
studentMarksMean = studentMarksDataRDD.map(lambda x: [x[0], x[1], (x[2] + x[3])/2])
studentMarksMean.take(2)


# In[30]:


# Filtering Student Average Grades in the Second Year
secondYearMarks = studentMarksMean.filter(lambda x: "year2" in x)
secondYearMarks.take(2)


# In[31]:


# Finding the top three students
sortedMarksData = secondYearMarks.sortBy(keyfunc = lambda x : -x[2])


# In[32]:


sortedMarksData.collect()


# In[33]:


# Take Ordered Function
topThreeStudents = secondYearMarks.takeOrdered(num=3, key = lambda x : -x[2] )


# In[34]:


topThreeStudents


# In[35]:


bottomThreeStudents = secondYearMarks.takeOrdered(num=3, key = lambda x : x[2] )


# In[36]:


bottomThreeStudents


# In[38]:


# Get all students with 80% Average
moreThan80Marks = secondYearMarks.filter(lambda x : x[2] > 80)
moreThan80Marks.collect()


# In[ ]:





# In[ ]:




