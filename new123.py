import os

# from pyspark.sql.functions import col, regexp_replace
# from pyspark.sql.types import StringType

os.environ['PYSPARK_PYTHON'] = 'python'
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [1,2,3,4,5,6,7,8,9,10,11,12]
# rdd=spark.sparkContext.parallelize(data)
# print(spark.sparkContext)
# ---------------------------------------------------------------------------------------------------------------------

# show():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# columns = ["seqno", "Quote"]
# data = [("1", "Be the change that you wish to see in the world"),
#     ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
#     ("3", "The purpose of our lives is to be happy."),
#     ("4", "Be cool.")]
#
# df = spark.createDataFrame(data=data, schema=columns)
# df.show()   [displays only few content with all columns]
# df.show(truncate=False)      [displays total content with all columns]
# df.show(2, truncate=False)     [displays total content with top 2 columns]
# df.show(2, truncate=25)          [displays only 25 characters and top 2 columns]
# df.show(3, truncate=False, vertical=True) [displays the content vertically of top 3 columns]
# ----------------------------------------------------------------------------------------------------------------------
# select()
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("sparkByExample.com").getOrCreate()
# data = [("James","Smith","USA","CA"),
#     ("Michael","Rose","USA","NY"),
#     ("Robert","Williams","USA","CA"),
#     ("Maria","Jones","USA","FL")]
#
# columns = ["firstname", "lastname", "country", "state"]
# df = spark.createDataFrame(data, columns)
# df.select(df.firstname, df.lastname).show()
# df.select("firstname", "lastname")
# TO GET ALL COLUMNS:
# df.select(*columns).show()
# ---------------------------------------------------------------------------------------------------------------------

# collect()
# Note that collect() is an action hence it does not return a DataFrame instead,
# it returns data in an Array to the driver. Once the data is in an array,
# you can use python for loop to process it further.

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# dept = [("Finance", 10),
#         ("Marketing", 20),
#         ("Sales", 30),
#         ("IT", 40)]
# deptColumns = ["dept_name", "dept_id"]
# deptDF = spark.createDataFrame(dept, deptColumns)
# data_collect = deptDF.collect()
# for each in data_collect:
#     print(f"{each['dept_name'], str(each['dept_id'])}")

# ---------------------------------------------------------------------------------------------------------------------

# withcolumn()

# pySpark withColumn() is a transformation function of DataFrame which is used to change the value,
# convert the datatype of an existing column, create a new column, and many more. In this post,
# It will walk you through commonly used PySpark DataFrame column operations using withColumn() examples.

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [('James', 'Smith', '1991-04-01', 'M', 3000),
#   ('Michael', 'Rose', '2000-05-19', 'M', 4000),
#   ('Robert', 'Williams', '1978-09-05', 'M', 4000),
#   ('Maria', 'Anne', '1967-12-01', 'F', 4000),
#   ('Jen', 'Mary', '1980-02-17', 'F', -1)
# ]
#
#
# columns = ["firstname", "lastname", "dob", "gender", "salary"]
# df = spark.createDataFrame(data, columns)
# print(df.printSchema())
# df1 = df.withColumn("salary", col("salary").cast("Integer")) [changing data type of salary from long to integer]
# print(df1.printSchema())
# ---------------------------------------------------------------------------------------------------------------------

# filter()
# print("welcome")

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import StringType, ArrayType
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("sparkByExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
# ]
#
# schema = StructType([
#     StructField('fullname', StructType([
#         StructField('firstname', StringType(), True),
#         StructField('middlename', StringType(), True),
#         StructField('lastname', StringType(), True)
#     ])),
#     StructField('languages', ArrayType((StringType())), True),
#     StructField('state', StringType(), True),
#     StructField('gender', StringType(), True)
# ])
#
# df = spark.createDataFrame(data, schema)
#
# df.filter(df.state == "OH").show(truncate=False)
# df.filter(col("state") == "OH").show(truncate=False)
# df.filter("state == 'OH'").show(truncate=False)
# df.filter((df.state == 'OH') & (df.gender == 'M')).show(truncate=False)
# li = ["OH", "CA", "DE"]
# df.filter(df.state.isin(li)).show()

#  ---------------------------------------------------------------------------------------------------------------------

# distinct() and drop duplicates():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# data = [("James", "Sales", 3000),
#     ("Michael", "Sales", 4600),
#     ("Robert", "Sales", 4100),
#     ("Maria", "Finance", 3000),
#     ("James", "Sales", 3000),
#     ("Scott", "Finance", 3300),
#     ("Jen", "Finance", 3900),
#     ("Jeff", "Marketing", 3000),
#     ("Kumar", "Marketing", 2000),
#     ("Saif", "Sales", 4100)]
# columns = ["employee_name", "department", "salary"]
# df = spark.createDataFrame(data, columns)
# distinct = df.distinct()
# print("Distinct Count:", distinct.count())
# distinct.show(truncate=False)
#
# d1 = df.dropDuplicates()
# print("Distinct count:", d1.count())
# d1.show()

# ---------------------------------------------------------------------------------------------------------------------

# sort() and order by():

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# data = simpleData = [("James", "Sales", "NY", 90000, 34, 10000), \
#     ("Michael", "Sales", "NY", 86000, 56, 20000), \
#     ("Robert", "Sales", "CA", 81000, 30, 23000), \
#     ("Maria", "Finance", "CA", 90000, 24, 23000), \
#     ("Raman", "Finance", "CA", 99000, 40, 24000), \
#     ("Scott", "Finance", "NY", 83000, 36, 19000), \
#     ("Jen", "Finance", "NY", 79000, 53, 15000), \
#     ("Jeff", "Marketing", "CA", 80000, 25, 18000), \
#     ("Kumar", "Marketing", "NY", 91000, 50, 21000) \
#   ]
# columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
# df = spark.createDataFrame(data, columns)
# df.sort("department").show()
# df.sort(col("department")).show()
# df.orderBy("department").show()
# df.orderBy(col("department")).show()
# df.sort(df.department.asc()).show()
# df.orderBy(df.department.asc()).show()
# df.sort(df.department.desc()).show()
# df.orderBy(df.department.desc()).show()
# ----------------------------------------------------------------------------------------------------------------------
# Groupby():

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sum, avg, max
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# simple_data = [("James", "Sales", "NY", 90000, 34, 10000),
#     ("Michael", "Sales", "NY", 86000, 56, 20000),
#     ("Robert", "Sales", "CA", 81000, 30, 23000),
#     ("Maria", "Finance", "CA", 90000, 24, 23000),
#     ("Raman", "Finance", "CA", 99000, 40, 24000),
#     ("Scott", "Finance", "NY", 83000, 36, 19000),
#     ("Jen", "Finance", "NY", 79000, 53, 15000),
#     ("Jeff", "Marketing", "CA", 80000, 25, 18000),
#     ("Kumar", "Marketing", "NY", 91000, 50, 21000)
#   ]
# schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
# df = spark.createDataFrame(simple_data, schema)
# df.show()
# df.groupBy("department").sum("bonus").show(truncate=False)

# df2 = df.groupby("department").count()
# df2.show()

# df3 = df.groupby("department").min("salary")
# df3.show()

# df4 = df.groupby("department").max("salary")
# df4.show()

# df5 = df.groupby("department").avg("salary")
# df5.show()

# df6 = df.groupby("department", "state").sum("salary", "bonus")
# df6.show()


# df7 = df.groupBy("department") \
#     .agg(sum("salary").alias("sum_salary"),
#          avg("salary").alias("avg_salary"),
#          sum("bonus").alias("sum_bonus"),
#          max("bonus").alias("max_bonus")
#      )\
#     .show(truncate=False)

# ---------------------------------------------------------------------------------------------------------------------
# Inner join()
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [["1", "sravan", "company 1"],
#         ["2", "jaswi", "company 1"],
#         ["3", "rohit", "company 2"],
#         ["4", "sri devi", "company 1"],
#         ["5", "bobby", "company 1"]]
#
# columns = ["ID", "Name", "Company"]
# df = spark.createDataFrame(data, columns)
# # df.show()
#
# data1 = [["1", "45000", "IT"],
#         ["2", "145000", "Manager"],
#         ["6", "45000", "HR"],
#         ["5", "34000", "Sales"]]
# columns1 = ["ID", "SALARY", "DEPARTMENT"]
#
# df1 = spark.createDataFrame(data1, columns1)
# # df1.show()
#
#
# df.join(df1, df.ID == df1.ID, "inner").show()

# ---------------------------------------------------------------------------------------------------------------------
# Left Join()

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [["1", "sravan", "company 1"],
#         ["2", "ojaswi", "company 1"],
#         ["3", "rohith", "company 2"],
#         ["4", "sridevi", "company 1"],
#         ["5", "bobby", "company 1"]]
#
# columns = ["ID", "Name", "Company"]
# df = spark.createDataFrame(data, columns)
#
#
# data1 = [["1", "45000", "IT"],
#         ["2", "145000", "Manager"],
#         ["6", "45000", "HR"],
#         ["5", "34000", "Sales"]]
# columns1 = ["ID", "SALARY", "DEPARTMENT"]
#
# df1 = spark.createDataFrame(data1, columns1)
#
# df.join(df1, df.ID == df1.ID,"left").show()

# ---------------------------------------------------------------------------------------------------------------------
# Right Join()

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [["1", "sravan", "company 1"],
#         ["2", "bob", "company 1"],
#         ["3", "rohit", "company 2"],
#         ["4", "sri", "company 1"],
#         ["5", "bobby", "company 1"]]
#
# columns = ["ID", "Name", "Company"]
# df = spark.createDataFrame(data, columns)
#
# data1 = [["1", "45000", "IT"],
#         ["2", "145000", "Manager"],
#         ["6", "45000", "HR"],
#         ["5", "34000", "Sales"]]
# columns1 = ["ID", "SALARY", "DEPARTMENT"]
#
# df1 = spark.createDataFrame(data1, columns1)
#
# final_df = df1.join(df, df1.ID == df.ID,"right")
# # final_df.show()
# df2 = final_df.select(df.ID, df.Name, df1.SALARY)
# df2.show()

# ---------------------------------------------------------------------------------------------------------------------
# withcolumn()
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "gangadhar", 25000), (2, "shyam", 52000)]
# columns = ("id", "name", "salary")
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("salary", col("salary").cast('Integer'))
# df2 = df.withColumn("salary", col('salary')*2)
# df3 = df2.withColumn("country", lit("India"))
# df4 = df3.withColumn("copiedsalarycolumn", col("salary"))
# df4.show()


# ---------------------------------------------------------------------------------------------------------------------

# withcolumnranamed()
# Data frames are immutable.This method will not change the original dataframe
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col,lit
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "gangadhar", 25000), (2, "shyam", 52000)]
# columns = ("id", "name", "salary")
# df = spark.createDataFrame(data, columns)
# df.withColumnRenamed("salary", "salaries").show()
# ---------------------------------------------------------------------------------------------------------------------
# struct type and struct field:

# structure type is a collection of  structure field ie list of structure fields
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", 3000), (2, "Mahesh", 4000)]
# schema = StructType([
#                      StructField(name="id", dataType=IntegerType()),
#                      StructField(name="name", dataType=StringType()),
#                      StructField(name="salary", dataType=StringType())
#                    ])
# df = spark.createDataFrame(data, schema=schema)
# df.show()
# df.printSchema()
# ---------------------------------------------------------------------------------------------------------------------

# nested columns:

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# data = [(1, ("Gangadhar", "sharma"), 5000), (2, ("Mahesh", "pilli"), 5000)]
# Structname = StructType([
#                          StructField(name="firstname", dataType=StringType()),
#                          StructField(name="lastname", dataType=StringType())
#              ])
# schema = StructType([
#                     StructField(name="id", dataType=IntegerType()),
#                     StructField(name="name", dataType=Structname),
#                     StructField(name="salary", dataType=IntegerType())
#                   ])
#
# df = spark.createDataFrame(data, schema)
# df.show()
# ----------------------------------------------------------------------------------------------------------------------

# Array Type Columns:


# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
# ]
#
# schema = StructType([
#     StructField('name', StructType([
#         StructField('firstname', dataType=StringType()),
#         StructField('middlename', dataType=StringType()),
#         StructField('lastname', dataType=StringType())
#     ])),
#     StructField("languages", dataType=ArrayType(StringType())),
#     StructField("state", dataType=StringType()),
#     StructField("gender", dataType=StringType())
#     ])
#
# df = spark.createDataFrame(data, schema)
#
# df.printSchema()
# df.show(truncate=False)
# ---------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ["Java", "Scala", 123], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB", "DBMS"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB", "JAVA"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB", "CSHARP"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB", 123], "OH", "M")
# ]
#
# schema = StructType([
#     StructField('name', StructType([
#         StructField('firstname', dataType=StringType()),
#         StructField('middlename', dataType=StringType()),
#         StructField('lastname', dataType=StringType())
#     ])),
#
#     StructField("languages", StructType([
#         StructField("primary", dataType=StringType()),
#         StructField("secondary", dataType=StringType()),
#         StructField("extra", dataType=StringType())
#     ])),
#     StructField("state", dataType=StringType()),
#     StructField("gender", dataType=StringType())
#     ])
#
# df = spark.createDataFrame(data, schema)
#
# df.printSchema()
# df.show(truncate=False)
#----------------------------------------------------------------------------------------------------------------------
# FEW METHODS IN ARRAY:
# 1. Explode(): it will give every element of array in a separate row
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", ["python", "sql", "pyspark"]), (2, "Mahesh", ["java", "javascript", "test cases"])]
# columns = ["ID", "Name", "skills"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("skill", explode(df.skills))
# df1.show()

# ---+---------+--------------------+----------+
# | ID|     Name|              skills|     skill|
# +---+---------+--------------------+----------+
# |  1|Gangadhar|[python, sql, pys...|    python|
# |  1|Gangadhar|[python, sql, pys...|       sql|
# |  1|Gangadhar|[python, sql, pys...|   pyspark|
# |  2|   Mahesh|[java, javascript,...|      java|
# |  2|   Mahesh|[java, javascript,...| javascript|
# |  2|   Mahesh|[java, javascript,...|test cases|
# +---+---------+--------------------+----------+

# 2. Split():

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", "python, sql, pyspark"), (2, "Mahesh", "java, javascript, test cases")]
# columns = ["ID", "Name", "skills"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("skill", split(df.skills, ","))
# df1.show(truncate=False)

# ---+---------+----------------------------+--------------------------------+
# |ID |Name     |skills                      |skill                           |
# +---+---------+----------------------------+--------------------------------+
# |1  |Gangadhar|python, sql, pyspark        |[python,  sql,  pyspark]        |
# |2  |Mahesh   |java, javascript, test cases|[java,  javascript,  test cases]|
# +---+---------+----------------------------+--------------------------------+


# 3. Array

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import array
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", "python", "sql", "pyspark"), (2, "Mahesh", "java", "javascript", "test cases")]
# columns = ["ID", "Name", "primary", "secondary", "extra"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("skills", array(df.primary, df.secondary, df.extra))
# df1.show(truncate=False)

# ---+---------+-------+---------+----------+--------------------+
# | ID|     Name|primary|secondary|     extra|              skills|
# +---+---------+-------+---------+----------+--------------------+
# |  1|Gangadhar| python|      sql|   pyspark|[python, sql, pys...|
# |  2|   Mahesh|   java|javascript|test cases|[java, javascript,...|
# +---+---------+-------+---------+----------+--------------------+

# 4.Array_contains:

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import array_contains
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", ["python", "sql", "pyspark"]), (2, "Mahesh", ["java", "javascript", "test cases"])]
# columns = ["ID", "Name", "skills"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("has_python_skill", array_contains(df.skills, "python"))
# df1.show(truncate=False)
# ---+---------+--------------------+----------------+
# | ID|     Name|              skills|has_python_skill|
# +---+---------+--------------------+----------------+
# |  1|Gangadhar|[python, sql, pys...|            true|
# |  2|   Mahesh|[java, javascript,...|           false|
# +---+---------+--------------------+----------------+

# ----------------------------------------------------------------------------------------------------------------------
# Map type column:

# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.withColumn("hair color", df.properties["hair"])
# df2 = df1.withColumn("eye color", df.properties.getItem("eye"))
# df2.show(truncate=False)
# ---------------------------------------------------------------------------------------------------------------------

# map_keys()
# 1.Explode: By using this function  we can get keys and values in seperate columns
# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql.functions import explode
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.select("name", "properties", explode(df.properties))
# df1.show(truncate=False)

# 2.  map_keys(): This will display all keys in list format
# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql.functions import map_keys
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.withColumn("keys", map_keys(df.properties))
# df1.show(truncate=False)

# 3. map_values

# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql.functions import map_values
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.withColumn("values", map_values(df.properties))
# df1.show(truncate=False)


# ----------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import Row
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetEXAMPLE").getOrCreate()
# row1 = Row(name="DGS", address='medak')
# print(f"Hello my name is {row1[0]} and i am from {row1[1]}")
#
# row2 = Row(name="SKS", address="hyderabad")
# print(f"Hello my name is {row2.name} and i am from {row2.address}")
#
# data = [row1, row2]
# df = spark.createDataFrame(data)
# df.show()
# ---------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, col, lit
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [("dgs", "m", "medak", 502110), ("trs", "f", "hyderabad", 543212), ("bjp", "", "delhi", 654323)]
# schema = ["name", "gender", "address", "pincode"]
# df = spark.createDataFrame(data, schema)
# df2 = df.select(df.name,
#                 df.address,
#                 df.pincode,
#                 when(df.gender == "m", "male").when(df.gender == "f", "female").otherwise("unknown").alias("gender type"),
#                 when(df.name == "dgs", "gangadhar").alias("actual name").alias("fullname"))
#
#
# df3 = df2.withColumn("country", lit("india"))
# df3.show()

# ---------------------------------------------------------------------------------------------------------------------

# filter() and where():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "maher", "male", 2000), (2, "wafa", "male", 3000), (3, "asi", "female", 4000)]
# schema = ["id", "name", "gender", "salary"]
# df = spark.createDataFrame(data, schema)
# df.filter(df.gender == "male").show()
# or
# df.filter("gender == 'male'").show()
# where()
# df.where((df.gender == "male") & (df.id == 1)).show()
#----------------------------------------------------------------------------------------------------------------------

# distinct() and drop duplicates():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "maheer", "male", 2000), (2, "wafa", "male", 3000), (3, "asi", "female", 4000), (3, "asi", "female", 4000)]
# schema = ["id", "name", "gender", "salary"]
# df = spark.createDataFrame(data, schema)
# df1 = df.distinct()
# df1.show()
# # dropduplicate()
# data = [(1, "maheer", "male", 2000), (2, "wafa", "male", 3000), (3, "asi", "female", 4000), (3, "sai", "female", 4500)]
# schema = ["id", "name", "gender", "salary"]
# df2 = spark.createDataFrame(data, schema)
# df2.dropDuplicates(["id"]).show()
#----------------------------------------------------------------------------------------------------------------------

# orderBy() and sort()

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "maheer", "male", 2000, "IT"), (2, "wafa", "male", 3000, "HR"), (3, "asi", "female", 4000, "payroll"), (4, "sarfaraj", " male", 4000, "HR")]
# schema = ["id", "name", "gender", "salary", "DEPT"]
# df = spark.createDataFrame(data, schema)
# df.sort("id").show()
# df.orderBy(df.id.desc()).show()
# df.sort(df.DEPT, df.id).show()
# df.orderBy(df.DEPT.desc(), df.id).show()
# ----------------------------------------------------------------------------------------------------------------------

# union() and union all():
# In pyspark union and union all works as same. They won't delete the duplicate rows from the tables
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [(1, "maher", "male", 2000), (2, "wafa", "male", 3000)]
# schema1 = ["id", "name", "gender", "salary"]
# df1 = spark.createDataFrame(data1, schema1)
#
#
# data2 = [(3, "sai", "female", 5000), (4, "ramya", "female", 6000), (2, "wafa", "male", 3000)]
# schema2 = ["id", "name", "gender", "salary"]
# df2 = spark.createDataFrame(data2, schema2)

# newdf = df1.union(df2)
# newdf.show()
#
# newdf1 = df1.unionAll(df2)
# newdf1.show()

# To remove the duplicates use distinct():

# newdf2 = newdf1.distinct()
# newdf2.show()

# ----------------------------------------------------------------------------------------------------------------------
# Joins:

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StringType, StructType, StructField, MapType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", {"eye": "brown", "hair": "black"}), ("wafa", {"eye": "red", "hair": "white"})]
# col1 = StructType([
#        StructField("name", StringType()),
#        StructField("properties", MapType(StringType(), StringType()))
#        ])
# df = spark.createDataFrame(data1, col1)
# df1 = df.withColumn("hair", df.properties["hair"])
# df1.withColumn("eyes", df.properties["eye"]).show(truncate=False)

# ---------------------------------------------------------------------------------------------------------------------
# from_json(): from_json is used to convert json string (dict format) type into map type
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import MapType, StringType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", '{"eye": "brown", "hair": "black"}'), ("wafa", '{"eye": "red", "hair": "white"}')]
# schema = ["id", "properties"]
# df = spark.createDataFrame(data1, schema)
# map_type_schema = MapType(StringType(), StringType())
#
# df1 = df.withColumn("propsmap", from_json(df.properties, map_type_schema))
# df2 = df1.withColumn("hair", df1.propsmap["hair"])
# df3 = df2.withColumn("eye", df1.propsmap["eye"])
# df3.show(truncate=False)
# df1.printSchema()

# ----------------------------------------------------------------------------------------------------------------------
#  Converting Json string type to Struct type:
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import StructType, StructField, StringType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", '{"eye": "brown", "hair": "black"}'), ("wafa", '{"eye": "red", "hair": "white"}')]
# schema = ["id", "properties"]
# df = spark.createDataFrame(data1, schema)
# struct_type = StructType([
#               StructField("eye", StringType()),
#               StructField("hair", StringType())
#               ])
# df1 = df.withColumn("propstruct", from_json(df.properties, struct_type))
# df2 = df1.withColumn("hair", df1.propstruct.hair)
# df3 = df2.withColumn("eye", df2.propstruct.eye)
# df3.show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
# to_json():
# to_json is used to convert Dataframe column Map type or struct type to JSON string

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import to_json
# from pyspark.sql.types import StructType, StructField, StringType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", {"eye": "brown", "hair": "black"}), ("wafa", {"eye": "red", "hair": "white"})]
# schema = ["id", "properties"]
# df = spark.createDataFrame(data1, schema)
# df1 = df.withColumn("propstring", to_json(df.properties))
# df1.show(truncate=False)
# df1.printSchema()
# ----------------------------------------------------------------------------------------------------------------------

# import pyspark
# json_tuple: this is used to select the elements from json string
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import json_tuple
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", '{"eye": "brown", "hair": "black", "skin": "white"}'),
#          ("wafa", '{"eye": "red", "hair": "white", "skin": "black"}')]
# schema = ["name", "properties"]
# df = spark.createDataFrame(data1, schema)
# df.select("name", json_tuple(df.properties, "eye", "hair").alias("eye type", "skin color")).show()

# ---------------------------------------------------------------------------------------------------------------------

# get_json_object:

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import get_json_object
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [("maher", '{"address": {"city": "hyd", "state": "telangana"}, "gender": "male"}'),
#         ("wafa", '{"address": {"city": "guru gram", "state": "haryana"}, "gender": "female"}')]
#
# columns = ["name", "properties"]
# df = spark.createDataFrame(data, columns)
# df1 = df.select("name", get_json_object(df.properties, "$.gender").alias("gender"))
# df1.show()

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ("Java", "Scala", 123), "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB", "DBMS"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB", "JAVA"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB", "CSHARP"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB", 123], "OH", "M")]
#
# schema = StructType([
#          StructField("fullname", StructType([
#              StructField("firstname", StringType()),
#              StructField("middlename", StringType()),
#              StructField("lastname", StringType())
#          ])),
#
#          StructField("languages", StructType([
#              StructField("primary", StringType()),
#              StructField("secondary", StringType()),
#              StructField("extra", StringType())
#          ])),
#
#          StructField("COUNTRY", StringType()),
#          StructField("GENDER", StringType())
#         ])
#
# df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# df.printSchema()






# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# data = [[(1, "python", "java"), (2, "pyspark", "html"), "css", "javascript"]]
# schema = StructType([
#          StructField("languages", StructType([
#                 StructField("id", IntegerType()),
#                 StructField("PRIMARY", StringType()),
#                 StructField("SECONDARY", StringType())
#                 ])),
#          StructField("fundamentals", ArrayType(StringType())),
#          StructField("extras", StringType()),
#          StructField("OTHERS", StringType())
#          ])
#
# df = spark.createDataFrame(data, schema)
# df.show(truncate=False)


# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ["Java", "Scala", 123], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB", "DBMS"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB", "JAVA"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB", "CSHARP"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB", 123], "OH", "M")]
#
# structname1 = StructType([
#                          StructField("firstname", StringType()),
#                          StructField("middlename", StringType()),
#                          StructField("lastname", StringType())])
#
# structname2 = StructType([
#                     StructField("primary", StringType()),
#                     StructField("secondary", StringType()),
#                     StructField("extra", StringType())
#                     ])
#
#
# schema = StructType([
#              StructField("FULLNAME", structname1),
#              StructField("LANGUAGES", structname2),
#              StructField("COUNTRY", StringType()),
#              StructField("GENDER", StringType())])
#
# df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# df.printSchema()

# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName('SparkByExamples.com') \
#     .getOrCreate()
#
# data = [('James', 'Smith', 'M', 3000),
#         ('Anna', 'Rose', 'F', 4100),
#         ('Robert', 'Williams', 'M', 6200),
#         ]
#
# columns = ["firstname", "lastname", "gender", "salary"]
# df = spark.createDataFrame(data=data, schema=columns)
# /df.show()
#
# if 'salary1' not in df.columns:
#     print("aa")
#
# # Add new constanct column
# from pyspark.sql.functions import lit
#
# df.withColumn("bonus_percent", lit(0.3)) \
#     .show()
#
# # Add column from existing column
# df.withColumn("bonus_amount", df.salary * 0.3) \
#     .show()
#
# # Add column by concatinating existing columns
# from pyspark.sql.functions import concat_ws
#
# df.withColumn("name", concat_ws(",", "firstname", 'lastname')) \
#     .show()

# Add current date
# from pyspark.sql.functions import current_date
#
# df.withColumn("current_date", current_date()) \
#     .show()

# from pyspark.sql.functions import when, lit
#
# df.withColumn("grade",
#               when((df.salary < 4000), lit("A"))
#               .when((df.salary >= 4000) & (df.salary <= 5000), lit("B"))
#               .otherwise(lit("C"))
#               ).show()

# Add column using select
# df.select("firstname", "salary", lit(0.3).alias("bonus")).show()
# df.select("firstname", "salary", lit(df.salary * 0.3).alias("bonus_amount")).show()
# df.select("firstname", "salary", current_date().alias("today_date")).show()
#
# # Add columns using SQL
# df.createOrReplaceTempView("PER")
# spark.sql("select firstname,salary, '0.3' as bonus from PER").show()
# spark.sql("select firstname,salary, salary * 0.3 as bonus_amount from PER").show()
# spark.sql("select firstname,salary, current_date() as today_date from PER").show()
# spark.sql("select firstname,salary, " +
#           "case salary when salary < 4000 then 'A' " +
#           "else 'B' END as grade from PER").show()

# spark.sql("select firstname, salary," +
#           "case salary when salary < 4000 then 'A'" +
#           "case salary when salary > 4000 then  'B'" +
#           "else 'c' END as GRADE from PER").show()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col,sum,avg,max
#
# spark = SparkSession.builder \
#                     .appName('SparkByExamples.com') \
#                     .getOrCreate()
#
# simpleData = [("James","Sales","NY",90000,34,10000),
#     ("Michael","Sales","NV",86000,56,20000),
#     ("Robert","Sales","CA",81000,30,23000),
#     ("Maria","Finance","CA",90000,24,23000),
#     ("Raman","Finance","DE",99000,40,24000),
#     ("Scott","Finance","NY",83000,36,19000),
#     ("Jen","Finance","NY",79000,53,15000),
#     ("Jeff","Marketing","NV",80000,25,18000),
#     ("Kumar","Marketing","NJ",91000,50,21000)
#   ]
#
# schema = ["employee_name","department","state","salary","age","bonus"]
# df = spark.createDataFrame(data=simpleData, schema = schema)
# df.printSchema()
# df.show(truncate=False)
#
# dfSort=df.sort(df.state, df.salary).groupBy(df.state).agg(sum(df.salary))
# dfSort.show()


# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import approx_count_distinct, collect_list
# from pyspark.sql.functions import collect_set, sum, avg, max, countDistinct, count
# from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
# from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
# from pyspark.sql.functions import variance, var_samp, var_pop

# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# simpleData = [("James", "Sales", 3000),
#               ("Michael", "Sales", 4600),
#               ("Robert", "Sales", 4100),
#               ("Maria", "Finance", 3000),
#               ("James", "Sales", 3000),
#               ("Scott", "Finance", 3300),
#               ("Jen", "Finance", 3900),
#               ("Jeff", "Marketing", 3000),
#               ("Kumar", "Marketing", 2000),
#               ("Saif", "Sales", 4100)
#               ]
# schema = ["employee_name", "department", "salary"]
#
# df = spark.createDataFrame(data=simpleData, schema=schema)
# df.show(truncate=False)
# print("approx_count_distinct: " + str(df.select(approx_count_distinct("salary")).collect()[0][0]))
# print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
# print("min:" + str(df.select(min("salary")).collect()[0][0]))
# df.select(collect_list("salary")).show(truncate=False)
# df.select(collect_set("salary")).show(truncate=False)
# df2 = df.select(countDistinct("department"))
# df2.show(truncate=False)
# print("Distinct Count of Department &amp; Salary: "+str(df2.collect()[0][0]))

# print("count: ", str(df.select(count("salary")).collect()[0]))
# df.select(first("salary")).show(truncate=False)
# df.select(last("salary")).show()
# df.select(kurtosis("salary")).show(truncate=False)
# df.select(sumDistinct(df.salary)).show()
# df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show(truncate=False)





# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[1]") \
#                     .appName('SparkByExamples.com') \
#                     .getOrCreate()
#
# columns = ["name", "languagesAtSchool", "currentState"]
# data = [("James,Smith", ["Java", "Scala", "C++"], "CA"), \
#     ("Michael,Rose,", ["Spark", "Java", "C++"],"NJ"), \
#     ("Robert,,Williams", ["CSharp","VB"],"NV")]
#
# df = spark.createDataFrame(data=data,schema=columns)
# df.printSchema()
# df.show(truncate=False)
#
# from pyspark.sql.functions import concat_ws
# df2 = df.withColumn("languagesAtSchool", concat_ws(",", df.languagesAtSchool))
# df2.printSchema()

# df2.show(truncate=False)


# df.createOrReplaceTempView("ARRAY_STRING")
# spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool,currentState from ARRAY_STRING").show(truncate=False)

# df.createOrReplaceTempView("Array")
# spark.sql("select name, concat_ws(',',languagesAtSchool")



# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit, col, when, concat_ws, concat
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# simpleData = [("James", "Sales", 3000),
#               ("Michael", "Sales", 4600),
#               ("Robert", "Sales", 4100),
#               ("Maria", "Finance", 3000),
#               ("James", "Sales", 3000),
#               ("Scott", "Finance", 3300),
#               ("Jen", "Finance", 3900),
#               ("Jeff", "Marketing", 3000),
#               ("Kumar", "Marketing", 2000),
#               ("Saif", "Sales", 4100)
#               ]
#
# schema = ["name", "dept", "salary"]
# df = spark.createDataFrame(simpleData, schema)
# from pyspark.sql.functions import concat, lit
#
# # Select the name and department columns and concatenate them with the desired string
# resultDF =  df.select(df.name, df.dept,\
#             concat(lit("My name is "), df.name, lit(" and my department is "), df.dept).alias("output"),\
#             df.salary, lit(df.salary * 2).alias("bonus"))
#
# # Show the result
# resultDF.show()



# from pyspark.sql import SparkSession
#
# # Create a SparkSession
# spark = SparkSession.builder.appName("ReadCSVExample").getOrCreate()
#
# # Read the CSV file into a DataFrame
# df = spark.read.csv("C:\\Users\\gangadhar.sharma\\Desktop\\asdf\\year-provisional-csv.csv", header=True, inferSchema=True)
#
# # Print the contents of the DataFrame
# df.show(truncate=0)



# 1.when:
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, col
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# data = [("James", "M", 60000), ("Michael", "M", 70000),
#         ("Robert", None, 400000), ("Maria", "F", 500000),
#         ("Jen", "", None)]
#
# columns = ["name", "gender", "salary"]
# df = spark.createDataFrame(data, columns)
# df.withColumn("new_gender", when(df.gender == "M", "Male")\
#                             .when(df.gender == "F", "Female")\
#                             .when(df.gender.isNull(), "no gender")\
#                             .otherwise("empty")).show()

# +-------+------+------+----------+
# |   name|gender|salary|new_gender|
# +-------+------+------+----------+
# |  James|     M| 60000|      Male|
# |Michael|     M| 70000|      Male|
# | Robert|  null|400000| no gender|
# |  Maria|     F|500000|    Female|
# |    Jen|      |  null|     empty|
# +-------+------+------+----------+

# df1 = df.select(col("*"), when(df.gender == "M", "Male")\
#                     .when(df.gender == "F", "Female")\
#                     .when(df.gender.isNull(), "no gender")\
#                     .otherwise("empty").alias("new_gender"))
# df1.show()


# df.createOrReplaceTempView("EMP")
# spark.sql("select name, CASE WHEN gender = 'M' THEN 'Male' " +
#                "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN 'no gender'" +
#                "ELSE gender END as new_gender from EMP").show()

# ---------------------------------------------------------------------------------------------------------------------

# 2.expr():

# expr() function takes SQL expression as a string argument, executes the expression, and returns a PySpark Column type.
# Expressions provided with this function are not a compile-time safety like DataFrame operations.

# expr(str)

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr
# data = [("James", "Bond"), ("Scott", "Varsa")]
# df = spark.createDataFrame(data).toDF("col1", "col2")
# df.withColumn("Name", expr("col1 ||' '|| col2")).show()
# --------------------------------------------------------------------------------------------------------------------
# from pyspark.sql.functions import expr
# data = [("James", "M"), ("Michael", "F"), ("Jen", "")]
# columns = ["name", "gender"]
# df = spark.createDataFrame(data, columns)
# df2 = df.withColumn("gender", expr("CASE WHEN gender= 'M' THEN 'MALE' " +
#                                    "WHEN gender = 'F' THEN 'FEMALE' " +
#                                      "ELSE 'unknown' END"))

# df2.show()
# ---------------------------------------------------------------------------------------------------------------------

# date = [("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3)]
# columns = ["date", "inc"]
# df = spark.createDataFrame(date, columns)
# df.select(df.date, df.inc, expr("add_months(date, inc)").alias("inc_date")).show()
# df.select(df.inc, expr("inc+10")).show()
# ---------------------------------------------------------------------------------------------------------------------

#3.lit()

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, lit
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# data = [("111", 50000), ("222", 60000), ("333", 40000)]
# columns = ["EmpId", "Salary"]
# df = spark.createDataFrame(data, columns)
# df1 = df.select(df.EmpId, df.Salary, lit("1").alias("lit_value"))
# df2 = df1.withColumn("lit_value2", when((df.Salary > 50000) & (df.Salary <= 60000), lit("100")).otherwise(lit("200")))
# df2.show()
# ---------------------------------------------------------------------------------------------------------------------

# 4.Split():
# pyspark.sql.functions.split(str, pattern, limit=-1)

# The split() function takes the first argument as the DataFrame column of type String and the
# second argument string delimiter that you want to split on.
# You can also use the pattern as a delimiter.
# This function returns pyspark.sql.Column of type Array.


# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split, col
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# data = [("James, A, Smith", "2018", "M", 3000),
#             ("Michael, Rose, Jones", "2010", "M", 4000),
#             ("Robert,K,Williams", "2010", "M", 4000),
#             ("Maria,Anne,Jones", "2005", "F", 4000),
#             ("Jen,Mary,Brown", "2010", "", -1)
#             ]
#
# columns = ["name", "dob_year", "gender", "salary"]
# df=spark.createDataFrame(data, columns)
# df1 = df.select(split(col("name"), "%").alias("split"))
# df1.show()
# ------------------------------------------------------------------------------------------------------------------

# 5.concat_ws
# concat_ws(sep, *cols)
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# from pyspark.sql.functions import concat_ws
# columns = ["name", "languagesAtSchool", "currentState"]
# data = [("James,,Smith", ["Java", "Scala", "C++"], "CA"), \
#     ("Michael,Rose,", ["Spark", "Java", "C++"], "NJ"), \
#     ("Robert, Williams", ["CSharp", "VB"], "NV")]
#
# df = spark.createDataFrame(data=data,schema=columns)
# df1 = df.withColumn("languages", concat_ws("&", df.languagesAtSchool))
# df1.show()
#-------------------------------------------------------------------------------------------------------
# 6.substring():

# syntax: substring(str, pos, len)

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import substring
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "20200828"), (2, "20180525")]
# columns = ["id", "date"]
# df=spark.createDataFrame(data, columns)
# df1 = df.withColumn('year', substring('date', 1, 4))\
#     .withColumn('month', substring('date', 5, 2))\
#     .withColumn('day', substring('date', 7, 2))
# df2 = df.select("date", substring("date", 1, 4).alias("year"),
#                         substring("date", 5, 2).alias("Month"),
#                         substring("date", 7, 2).alias("day"))
# df2.show()
#-------------------------------------------------------------------------------------------
# 7.translate():
# syntax:translate("col name", "new value", "old value")
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import translate
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# address = [(1, "14851 Jeffrey Rd", "DE"),
#            (2, "43421 Margarita St", "NY"),
#            (3, "13111 Siemon Ave", "CA")]
#
# df = spark.createDataFrame(address, ["id", "address", "state"])
# df.withColumn("new_address", translate("address", "123", "ABC")).show()
# -----------------------------------------------------------------------------------------------

# collect():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# dept = [("Finance", 10),
#     ("Marketing", 20),
#     ("Sales", 30),
#     ("IT", 40)]
# deptColumns = ["dept_name", "dept_id"]
# df = spark.createDataFrame(dept, deptColumns)
# dfc = df.collect()
# dfc1 = df.select("dept_name").collect()
# for each in dfc:
#     print(each[0], ",", each[1])
#----------------------------------------------------------------------------------------------------------------------

# pyspark-column-functions.py

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
data = [("James", "Bond", "100", None),
      ("Ann", "Varsa", "200", 'F'),
      ("Tom Cruise", "XXX", "400", ''),
      ("Tom Brand", None, "400", 'M')]

columns = ["fname", "lname", "id", "gender"]
df = spark.createDataFrame(data, columns)

#alias
# from pyspark.sql.functions import expr, concat_ws
# df.select(df.fname.alias("first name"),
#           df.lname.alias("lastname"),
#           concat_ws(",", df.fname, df.lname).alias("fullname")).show()


# asc, desc:
# df.sort(df.fname.asc()).show()
# df.sort(df.lname.desc()).show()


#cast
# df.select(df.id).printSchema() |-- id: string (nullable = true)
# df.select(df.id.cast("int")).printSchema()     |-- id: integer (nullable = true)

# between
# df.filter(df.id.between(100, 300)).show()

#contains
# df.filter(df.lname.contains("B")).show()

#startswith, endswith()
# df.filter(df.fname.startswith("T")).show()
# df.filter(df.lname.endswith("nd")).show()

#isNull & isNotNull
# df.filter(df.gender.isNull()).show()

# ----------------------------------------------------------------------------------------------------------------------
# from pyspark import SparkContext

# Create a SparkContext
# sc = SparkContext.getOrCreate()
#
# # Define the list
# l = [1, 2, 3, 4, 5, 6]

# Convert the list to an RDD
# rdd = sc.parallelize(l)

# # Perform addition using reduce
# sum_value = rdd.reduce(lambda x, y: x + y)

# print("Sum:", sum_value)
#-----------------------------------------------------------------------------------------------------------------------
# from pyspark.sql import SparkSession

# Create a SparkSession
# spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the file and split it into words/
# lines = spark.read.text("C:\\Users\\gangadhar.sharma\\Desktop\\asdf\\new 2.txt").rdd.map(lambda r: r[0])
# print(lines.collect())
# If we use flatmap then the output will be in the format of ['"', 'R', 'e', 's', 'i', 'l', 'i', 'e', 'n', 't',
# If we use map then the data of whole file will come in a single list where words are not separated ie ["Resilient Distribute Datasets".....

# words = lines.flatMap(lambda line: line.split(" "))
# print(words.collect())
# If we use flatmap then the output will be in the format of ['"Resilient"', 'Distributed', 'Datasets', '(RDD)', 'is', 'a', 'distributed',
# If we use map then the data will come in list in list format


# Count the occurrences of each word
# word_counts = words.countByValue()
# print(word_counts)

# Print the word counts
# for word, count in word_counts.items():
#     print(f"({word}, {count})")

# Stop the SparkSession
# spark.stop()
# ----------------------------------------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession

# Create a SparkSession
# spark = SparkSession.builder.appName("AlphabetCount").getOrCreate()

# Define the name
# name = "gangadhar"
#
#  Create an RDD from the name string
# rdd = spark.sparkContext.parallelize(name)
# print(rdd.collect())
# ['g', 'a', 'n', 'g', 'a', 'd', 'h', 'a', 'r']

# Map each character to (character, 1) tuple

# character_count = rdd.map(lambda c:  (c, 1))
# print(character_counts.collect())
# [('g', 1), ('a', 1), ('n', 1), ('g', 1), ('a', 1), ('d', 1), ('h', 1), ('a', 1), ('r', 1)]
# If we use flatMap:  ['g', 1, 'a', 1, 'n', 1, 'g', 1, 'a', 1, 'd', 1, 'h', 1, 'a', 1, 'r', 1]

# Reduce by key to count the occurrences of each character
# character_counts = character_count.reduceByKey(lambda x, y: x + y).collect()
# print(character_counts)
# [('n', 1), ('g', 2), ('a', 3), ('d', 1), ('r', 1), ('h', 1)]

# for character, count in character_counts:
#     print(f"({character}: {count})")

# ---------------------------------------------------------------------------------------------------------------------
# from pyspark.sql import SparkSession

# Create a SparkSession
# spark = SparkSession.builder.appName("AlphabetCount").getOrCreate()

# Read the file and split it into alphabets
# lines = spark.read.text("C:\\Users\\gangadhar.sharma\\Desktop\\asdf\\new 2.txt").rdd.flatMap(lambda r: r[0])
# If we use flatmap then the output will be in the format of ['"', 'R', 'e', 's', 'i', 'l', 'i', 'e', 'n', 't',
# If we use map then the data of whole file will come in a single list where words are not separated ie ["Resilient Distribute Datasets".....

# Filter out non-alphabetic characters
# alphabets = lines.filter(lambda c: c.isalpha())

# Map each alphabet to (alphabet, 1) tuple
# alphabet_count = alphabets.map(lambda c: (c, 1))

# Reduce by key to count the occurrences of each alphabet
# alphabet_counts = alphabet_count.reduceByKey(lambda x, y: x + y).collect()

# for alphabet, count in alphabet_counts:
#     print(f"({alphabet}: {count})")
#----------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("wcp").getOrCreate()
# words = spark.read.text("C:\\Users\\gangadhar.sharma\\Desktop\\asdf\\new 3.txt").rdd.flatMap(lambda x: x[0])
# letter = words.filter(lambda a: a.isalpha())
# letters = letter.map(lambda c: (c.lower(), 1))
# letter_count = letters.reduceByKey(lambda x, y: x+y).collect()
# for letter, count in letter_count:
#     print(f"({letter}, {count})")
# ----------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("product").getOrCreate()
# l = [1, 2, 3, 4, 5]
# rdd = spark.sparkContext.parallelize(l)
# sum_val = rdd.reduce(lambda a, b: a*b)
# print(sum_val)
# ---------------------------------------------------------------------------------------------------------------------

# l = [2, 4, 6, 8, 10]
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("square").getOrCreate()
# sc = spark.sparkContext.parallelize(l)
# square = sc.map(lambda c: c**2)
# print(square.collect())
# --------------------------------------------------------------------------------------------------------------------
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("square").getOrCreate()
# rdd = spark.sparkContext.parallelize([(1, 2), (1, 4), (2, 1), (2, 3), (2, 5)])
#
# # Reduce the values for each key using reduceByKey()
# reduced_rdd = rdd.reduceByKey(lambda x, y: y // x)
# print(reduced_rdd.collect())
# ---------------------------------------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession
#
# # Create a SparkSession
# spark = SparkSession.builder.appName("AddLists").getOrCreate()
#
# # Define two lists
# list1 = [1, 2, 3, 4, 5]
# list2 = [10, 20, 30, 40, 50]
#
# # Convert lists to RDDs
# rdd1 = spark.sparkContext.parallelize(list1)
# rdd2 = spark.sparkContext.parallelize(list2)
#
# Combine corresponding elements using zip()
# combined_rdd = rdd1.zip(rdd2)
# print(combined_rdd.collect())
#
# Perform addition using map()
# sum_rdd = combined_rdd.map(lambda x: x[0] + x[1])
# print(sum_rdd.collect())
#
# Collect and print the result
# result = sum_rdd.collect()
# print(result)
# --------------------------------------------------------------------------------------------------------------------

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("product").getOrCreate()
# rdd1 = spark.sparkContext.parallelize(range(1, 5))
# rdd2 = spark.sparkContext.parallelize(range(1, 5))
# rdd3 = rdd1.zip(rdd2)
# print(rdd3.collect())
# prod = rdd3.map(lambda x: x[0]*x[1])
# print(prod.collect())
# ----------------------------------------------------------------------------------------------------------------------
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("product").getOrCreate()
# rdd = spark.sparkContext.parallelize([(1, "apple"), (2, "banana"), (1, "orange")])
# grouped_rdd = rdd.reduceByKey(lambda a, b: a+b)
# print(grouped_rdd.collect())  # Output: [(1, ['apple', 'orange']), (2, ['banana'])]


# from pyspark.sql import SparkSession
#
# # Create a SparkSession
# spark = SparkSession.builder.appName("JoinRDDs").getOrCreate()
#
# # Create two RDDs
# rdd1 = spark.sparkContext.parallelize([(1, "apple"), (2, "banana"), (3, "orange")])
# rdd2 = spark.sparkContext.parallelize([(1, "red"), (2, "yellow"), (3, "orange")])
#
# Join the RDDs based on keys
# joined_rdd = rdd1.join(rdd2)

# Print the joined RDD
# print(joined_rdd.collect())

# o/p: [(1, ('apple', 'red')), (2, ('banana', 'yellow')), (3, ('orange', 'orange'))]
# ---------------------------------------------------------------------------------------------------------------------
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("rddtodf").getOrCreate()
# data = [(1, "dgs"), (2, "sks"), (3, "trs")]
# sc = spark.sparkContext.parallelize(data)
# df = spark.createDataFrame(data, ["id", "name"])
# df.show()
# ---------------------------------------------------------------------------------------------------------------------


# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()
#
# arrayData = [
#         ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
#         ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
#         ('Robert', ['CSharp',''],{'hair': 'red', 'eye': ''}),
#         ('Washington',None,None),
#         ('Jefferson',['1','2'],{})]
# df = spark.createDataFrame(data=arrayData, schema = ['name', 'knownLanguages', 'properties'])
# df.show(truncate=False)

# from pyspark.sql.functions import explode
# df1 = df.select(df.name, explode(df.knownLanguages))

print("hello")
print("welcome")