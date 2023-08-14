# Databricks notebook source
# DBTITLE 1,Recycable & Low Fact Product
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Define the schema for the DataFrame
schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("low_fats", StringType(), nullable=False),
    StructField("recyclable", StringType(), nullable=False)
])


data = [
    (0, 'Y', 'N'),
    (1, 'Y', 'Y'),
    (2, 'N', 'Y'),
    (3, 'Y', 'Y'),
    (4,'N','N')
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)
filtered_df = df.filter((df.low_fats == 'Y') & (df.recyclable == 'Y'))
result_df = filtered_df.select("product_id")
# Show the DataFrame
result_df.show()




# COMMAND ----------

# DBTITLE 1,1378. Replace Employee ID With The Unique Identifier
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import broadcast


schema1 = StructType([StructField("id",IntegerType(), nullable = False),
         StructField("name", StringType(), nullable = False)

         ])

data1 = [
    (1,"Alice"),
    (7,"Bob"),
    (11,"Meir"),
    (90,"Winston"),
    (3,"Jonanthan")
]
employees_df = spark.createDataFrame(data = data1, schema=schema1)
schema2 = StructType([StructField("id",IntegerType(),nullable = False),
         StructField("unique_id",IntegerType(),nullable = False)
])

data2 = [
    (3,1),
    (11,2),
    (90,3)
   
]
employee_uni_df = spark.createDataFrame(data =data2, schema=schema2)

result_df = employees_df.join(broadcast(employee_uni_df), on="id", how="left") \
    .select("unique_id", "name")


result_df.show()

#select 
#eu.unique_id as unique_id, e.name as name
#from Employees e left join EmployeeUNI eu on e.id = eu.id




# COMMAND ----------

# DBTITLE 1,627. Swap Salary
from pyspark.sql import SparkSession
from pyspark.sql.functions import when




data = [
    (1, "A", "m", 2500),
    (2, "B", "f", 1500),
    (3, "C", "m", 5500),
    (4, "D", "f", 500)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["id", "name", "sex", "salary"])

# Swap 'f' and 'm' values in the 'sex' column
result_df = df.withColumn("sex", when(df.sex == "f", "m").otherwise("f"))

# Show the result
result_df.show()




# COMMAND ----------

# DBTITLE 1,1068. Product Sales Analysis I
from pyspark.sql import SparkSession

sales_data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

product_data = [
    (100, "Nokia"),
    (200, "Apple"),
    (300, "Samsung")
]

# Create DataFrames
sales_df = spark.createDataFrame(sales_data, ["sale_id", "product_id", "year", "quantity", "price"])
product_df = spark.createDataFrame(product_data, ["product_id", "product_name"])

# Perform SQL join to get the desired result
result_df = sales_df.join(product_df, on="product_id", how="inner") \
    .select("product_name", "year", "price")

# Show the result
result_df.show()

#SELECT product_name ,year,price from Sales JOIN Product on Sales.product_id   = Product.product_id



# COMMAND ----------

# DBTITLE 1,1179. Reformat Department Table
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


data = [
    (1, 8000, "Jan"),
    (2, 9000, "Jan"),
    (3, 10000, "Feb"),
    (1, 7000, "Feb"),
    (1, 6000, "Mar")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["id", "revenue", "month"])

# Pivot the data
pivoted_df = df.groupBy("id").pivot("month", ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]).sum("revenue")

# Rename the columns
columns = ["id"] + [f"{month}_Revenue" for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]]
result_df = pivoted_df.toDF(*columns)

# Show the result
result_df.show()

#SELECT id,
#      SUM(if(month = 'Jan', revenue, null)) AS Jan_Revenue,
#      SUM(if(month = 'Feb', revenue, null)) AS Feb_Revenue,
#       SUM(if(month = 'Mar', revenue, null)) AS Mar_Revenue,
#      SUM(if(month = 'Apr', revenue, null)) AS Apr_Revenue,
#      SUM(if(month = 'May', revenue, null)) AS May_Revenue,
#      SUM(if(month = 'Jun', revenue, null)) AS Jun_Revenue,
#      SUM(if(month = 'Jul', revenue, null)) AS Jul_Revenue,
#      SUM(if(month = 'Aug', revenue, null)) AS Aug_Revenue,
#      SUM(if(month = 'Sep', revenue, null)) AS Sep_Revenue,
#      SUM(if(month = 'Oct', revenue, null)) AS Oct_Revenue,
#       SUM(if(month = 'Nov', revenue, null)) AS Nov_Revenue,
#      SUM(if(month = 'Dec', revenue, null)) AS Dec_Revenue
#FROM Department
#GROUP BY id


# COMMAND ----------

# DBTITLE 1,1484. Group Sold Products By The Date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, concat_ws , sort_array


data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Bible"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["sell_date", "product"])

# Group and aggregate the data
result_df = df.groupBy("sell_date").agg(count("product").alias("num_sold"),
               concat_ws(",", sort_array(collect_list("product"))).alias("products")).orderBy("sell_date")

# Show the result
result_df.show(truncate=False)

#SELECT 
#    sell_date,
#    COUNT(DISTINCT product) AS num_sold,
#    GROUP_CONCAT(DISTINCT product ORDER BY product) AS products
#FROM
#    Activities
#GROUP BY
#    sell_date
#ORDER BY
#    sell_date;


# COMMAND ----------

# DBTITLE 1,175. Combine Two Tables
from pyspark.sql import SparkSession

person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]

address_data = [
    (1, 2, "New York City", "New York"),
    (2, 3, "Leetcode", "California")
]

# Create DataFrames
person_df = spark.createDataFrame(person_data, ["personId", "lastName", "firstName"])
address_df = spark.createDataFrame(address_data, ["addressId", "personId", "city", "state"])

# Perform left join and select the required columns
result_df = person_df.join(address_df, on="personId", how="left").select("firstName", "lastName", "city", "state")

# Show the result
result_df.show(truncate=False)

#select FirstName, LastName, City, State
#from Person left join Address
#on Person.PersonId = Address.PersonId;


# COMMAND ----------

# DBTITLE 1,577. Employee Bonus
from pyspark.sql import SparkSession

employee_data = [
    (3, "Brad", None, 4000),
    (1, "John", 3, 1000),
    (2, "Dan", 3, 2000),
    (4, "Thomas", 3, 4000)
]

bonus_data = [
    (2, 500),
    (4, 2000)
]

# Create DataFrames
employee_df = spark.createDataFrame(employee_data, ["empId", "name", "supervisor", "salary"])
bonus_df = spark.createDataFrame(bonus_data, ["empId", "bonus"])

# Perform left join and filter rows
result_df = employee_df.join(bonus_df, on="empId", how="left_outer").filter("bonus < 1000 or bonus is Null").select("name", "bonus")

# Show the result
result_df.show()

#SELECT e1.name,b1.bonus from Employee e1 LEFT JOIN Bonus b1 ON e1.empId = b1.empId 
#WHERE b1.bonus < 1000 or b1.empId is Null


# COMMAND ----------

# DBTITLE 1,620. Not Boring Movies
from pyspark.sql import SparkSession

data = [
    (1, "War", "great 3D", 8.9),
    (2, "Science", "fiction", 8.5),
    (3, "irish", "boring", 6.2),
    (4, "Ice song", "Fantacy", 8.6),
    (5, "House card", "Interesting", 9.1)
]

# Create a DataFrame
cinema_df = spark.createDataFrame(data, ["id", "movie", "description", "rating"])

# Filter and order the data
result_df = cinema_df.filter((cinema_df.id % 2 == 1) & (cinema_df.description != "boring")) \
    .orderBy(cinema_df.rating.desc())

# Show the result
result_df.show()

#select * from Cinema where description <> "Boring" and id%2 <> 0 order by rating desc

# COMMAND ----------

from pyspark.sql import SparkSession

data = [
    (1, "a@b.com"),
    (2, "c@d.com"),
    (3, "a@b.com")
]

# Create a DataFrame
person_df = spark.createDataFrame(data, ["id", "email"])

# Group by email and count occurrences
result_df = person_df.groupBy("email").count() .filter("count > 1").select("email")

# Show the result
result_df.show()

# select email from Person group by email  having count(email)>1

