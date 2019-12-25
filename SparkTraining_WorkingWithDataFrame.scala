// Databricks notebook source
// MAGIC %md # Retail Data Analysis using Apache Spark on Databricks Community Edition
// MAGIC In this notebook, I will be demonstrating various concepts of Apache Spark such as transformations and actions. I will be executing the examples using various API's present in Apache Spark such as RDD's.
// MAGIC 
// MAGIC The notebook is written and executed on Databricks Community Edition. Getting started with Databricks community edition can be found <a href="https://docs.databricks.com/user-guide/index.html">here</a>. Introduction to Apache Spark with Databricks can be found <a href="https://docs.databricks.com/spark/latest/training/index.html">here</a>
// MAGIC 
// MAGIC ### Datasets
// MAGIC 
// MAGIC 1. <a href="https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/orders.csv"><b>orders.csv</b></a> - Contains the list of orders. The various attributes present in the dataset are
// MAGIC  1. Order ID : The unique identifier for the each order (INTEGER)
// MAGIC  2. Order Date: The date and time when order was placed (DATETIME)
// MAGIC  3. Customer ID : The customer Id of the customer associated to the order (INTEGER)
// MAGIC  4. Order Status : The status associated with the order (VARCHAR)
// MAGIC 2. <a href="https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/order_items.csv"> <b>order_items.csv</b></a> - Contains the details about the each item ordered in the order list
// MAGIC  1. Order Item ID: The unique identifier of the each item in the order list(INTEGER)
// MAGIC  2. Order Item Order ID : The identifier for the order (INTEGER)
// MAGIC  3. Order Item Product ID : The id associated with the product (INTEGER)
// MAGIC  4. Order Item Quantity : The quantity ordered for a particular item (INTEGER)
// MAGIC  5. Order Item Subtotal: The total price for the ordered items (FLOAT)
// MAGIC  6. Order Item Product Price: The price associated with the each product (FLOAT)
// MAGIC 3. <a href="https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/products.csv"> <b>products.csv</b></a> - Contains the details about the each product
// MAGIC  1. Product ID: The unique identifier for the each product (INTEGER)
// MAGIC  2. Product Category ID : The identifier for the category to which product belongs (INTEGER)
// MAGIC  3. Product Description : The description associated with the product (VARCHAR)
// MAGIC  4. Product Price : The price of the product (FLOAT)
// MAGIC  5. Product Image : The url of the image associated with the product (VARCHAR)
// MAGIC 4. <a href="https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/categories.csv"> <b>categories.csv</b></a> - Contains the details about the each product
// MAGIC  1. Category ID: The unique identifier for the each category (INTEGER)
// MAGIC  2. Category Department ID : The identifier for the department to which category belongs (INTEGER)
// MAGIC  3. Category Name : The name of the category (VARCHAR)

// COMMAND ----------

// MAGIC %md ### Downloading the datasets
// MAGIC 
// MAGIC Download the datasets using the shell command wget and the URL, save them into the tmp directory. The URL's for the datasets are
// MAGIC 1. orders.csv : https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/orders.csv
// MAGIC 2. order_items.csv : https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/order_items.csv
// MAGIC 3. products.csv :https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/products.csv
// MAGIC 4. category.csv : https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/categories.csv

// COMMAND ----------

// MAGIC %sh
// MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/orders.csv"
// MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/order_items.csv"
// MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/products.csv"
// MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/categories.csv"

// COMMAND ----------

var  localOrderFilePath = "file:/tmp/orders.csv"
var localOrderItemFilePath = "file:/tmp/order_items.csv"
var localProductFilePath = "file:/tmp/products.csv"
var localCategoriesFilePath = "file:/tmp/categories.csv"
dbutils.fs.mkdirs("dbfs:/datasets")
dbutils.fs.cp(localOrderFilePath, "dbfs:/datasets/")
dbutils.fs.cp(localOrderItemFilePath, "dbfs:/datasets")
dbutils.fs.cp(localProductFilePath, "dbfs:/datasets/")
dbutils.fs.cp(localCategoriesFilePath, "dbfs:/datasets")
//Displaying the files present in the DBFS datasets folder of your cluser
display(dbutils.fs.ls("dbfs:/datasets"))

// COMMAND ----------

// MAGIC %md ### Loading Required Librarires and Creation of Spark Session

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/*
# SparkSession (main entry point to all spark functionalities since 2.X)
# already initialized as 'spark'. Manipulation of DFs included.
# No need to use sqlContext anymore. 'type(spark)'.
# 
# SparkContext accessible through 'sc'.
###
##### Instantiating SparkContext and SparkSession
##### TO REPLACE with the appropriate function (application blanche)
*/
var spark = (SparkSession
             .builder
             .appName("Retail Data Analysis With SPARK DataFrame")
             .enableHiveSupport()
             .getOrCreate()
            )

// COMMAND ----------

// MAGIC %md ### Creating DataFrame for orders table

// COMMAND ----------

var ordersDF = spark.read.csv("dbfs:/datasets/orders.csv")
ordersDF.take(10)
 

// COMMAND ----------

// MAGIC %md ### Creating DataFrame for the order_items table

// COMMAND ----------

var orderItemsDF =  
  spark
  .read
  .option("inferSchema", "true")
  .option("header", "false")
  .csv("dbfs:/datasets/order_items.csv")
orderItemsDF.show()

// COMMAND ----------

// MAGIC %md ### Creating DataFrame for the products table

// COMMAND ----------

var productsDF =spark.read
                      .option("inferSchema", "true")
                      .option("header", "false")
                      .csv("dbfs:/datasets/products.csv")
for(i <- productsDF.take(10)) print(i)
productsDF.show(10)

// COMMAND ----------

// MAGIC %md ###Creating DataFrame for the categories table

// COMMAND ----------

var categoriesDF =spark.read
                      .option("inferSchema", "true")
                      .option("header", "false")
                      .csv("dbfs:/datasets/categories.csv")
for(i <- categoriesDF.take(10)) print(i)
categoriesDF.show(10)

// COMMAND ----------

// MAGIC %md ### Getting the revenue from order_items on daily basis
// MAGIC 1. "Map" through the orders RDD and stored order ID and order date in the RDD
// MAGIC 2. "Map" through the order items RDD and stored order ID and price
// MAGIC 3. Joined both the RDD based on order ID
// MAGIC 4. Summed the total price for each date and sorted by the revenue
// MAGIC 5. Printed first 10 revenues

// COMMAND ----------

def DailyRevenue(orderItemsFD,ordersFD):
    #Getting the revenue from order_items on daily basis
    #"Map" through the orders DF and stored order ID and order date in the RDD
    #"Map" through the order items RDD and stored order ID and price
    #Joined both the RDD based on order ID
    #Summed the total price for each date and sorted by the revenue 
    ordersMapFD = (ordersFD
                     .map(attributes =>  attributes(0),attributes(1))
                    )
                    
    orderItemsMapFD=(orderItemsFD
                        .map(attributes =>  attributes(1),attributes(4))
                         
                    )
                                    
    ordersJoinDF =(
                ordersMapFD
                    .join(orderItemsMapFD)
    
                )
                
    revenuePerDay=(
                ordersJoin
                    .map(lambda x:(x[1][0],x[1][1]))
                    .reduceByKey(lambda acc, val:acc+val)
                    .map(lambda x:(x[1],x[0]))#permutation of key with revenue
                    .sortByKey(ascending= False)
                    .map(lambda x:(x[0],x[1]))
                )
    
    return revenuePerDay

revenuePerDay=DailyRevenue(orderItemsRDD,ordersRDD)
for i in revenuePerDay.take(10): print (i)

// COMMAND ----------

 
var ordersMapFD = ordersDF.map(attributes =>   Row(col(attributes(0),attributes(1)))
                    
