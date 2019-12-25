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
import org.apache.spark.sql.types.FloatType
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

// MAGIC %md ### Creating RDD for orders table

// COMMAND ----------

var ordersRDD = sc.textFile("dbfs:/datasets/orders.csv")
ordersRDD.take(10)
 

// COMMAND ----------

// MAGIC %md ### Creating RDD for the order_items table

// COMMAND ----------

var orderItemsRDD =  sc.textFile("dbfs:/datasets/order_items.csv")
orderItemsRDD.take(10)

// COMMAND ----------

// MAGIC %md ### Creating DataFrame for the products table

// COMMAND ----------

var productsRDD =sc.textFile("dbfs:/datasets/products.csv")
for(product <- productsRDD.take(10)) println(product)
 

// COMMAND ----------

// MAGIC %md ###Creating RDD for the categories table

// COMMAND ----------

var categoriesDF =sc.textFile("dbfs:/datasets/categories.csv")
for(catecgory <- categoriesDF.take(10)) println(catecgory)
 

// COMMAND ----------

// MAGIC %md ### Getting the revenue from order_items on daily basis
// MAGIC 1. "Map" through the orders RDD and stored order ID and order date in the RDD
// MAGIC 2. "Map" through the order items RDD and stored order ID and price
// MAGIC 3. Joined both the RDD based on order ID
// MAGIC 4. Summed the total price for each date and sorted by the revenue
// MAGIC 5. Printed first 10 revenues

// COMMAND ----------

var ordersMapRDD = ordersRDD
                     .map(x=> x.split(","))
                     .map(x=> (x(0), x(1)))
ordersMapRDD.take(10) 

 var orderItemsMapRDD=orderItemsRDD
                        .map(x=> x.split(","))
                        .map(x=> (x(1), (x(4)).toFloat))
orderItemsMapRDD.take(10) 

    var ordersJoin =
                ordersMapRDD
                    .join(orderItemsMapRDD)

ordersJoin.take(10) 

            
    var revenuePerDay=
                ordersJoin
                    .map(x=>(x._2._1,x._2._2))

revenuePerDay.take(10)

// COMMAND ----------

var revenuePerDay=
                ordersJoin
                     .map(x=>(x._2._1,x._2._2))
                    .reduceByKey(_ + _)
                    //.map(x=>(x._2,x._1))//permutation of key with revenue
                    //.sortByKey(false)
                    .sortBy(_._2,false)
                    .map(x=>(x._2,x._1))

 for (revenu<-  revenuePerDay.take(10)) println (revenu._1,revenu._2)           

// COMMAND ----------

import java.lang.Math
def DailyRevenue(orderItemsRDD: RDD[String],ordersRDD:RDD[String]): RDD[(Float, String)]= {  
    /*
    #Getting the revenue from order_items on daily basis
    #"Map" through the orders RDD and stored order ID and order date in the RDD
    #"Map" through the order items RDD and stored order ID and price
    #Joined both the RDD based on order ID
    #Summed the total price for each date and sorted by the revenue 
   */
    var ordersMapRDD = ordersRDD
                     .map(x=> x.split(","))
                     .map(x=> (x(0), x(1)))
                    
                    
    var orderItemsMapRDD=orderItemsRDD
                        .map(x=> x.split(","))
                        .map(x=> (x(1), (x(4)).toFloat))
                    
                                    
    var ordersJoin =
                ordersMapRDD
                    .join(orderItemsMapRDD)
    
                
                
    var revenuePerDay=
                ordersJoin
                     .map(x=>(x._2._1,x._2._2))
                    .reduceByKey(_ + _)
                    
                    .sortBy(_._2,false)
                    .map(x=>(x._2,x._1))
                    
                
    
    return revenuePerDay
}
var revenuePerDay=DailyRevenue(orderItemsRDD,ordersRDD)


for (revenu<-  revenuePerDay.take(10)) println (revenu._1,revenu._2) 
 



 

// COMMAND ----------

val Revenue= revenuePerDay.collect()

// COMMAND ----------

// MAGIC %md ## Visualization of Daily Revenue
// MAGIC  We convert the RDD to DataFrame in order to plot the result

// COMMAND ----------

val dfWithSchema = spark.createDataFrame(revenuePerDay).toDF("Revenue", "Date")
 
display(dfWithSchema.select(to_date(col("Date"), "yyyy-MM-dd").as("Date"),col("Revenue")))

// COMMAND ----------

// MAGIC %md ### Get the number of orders from order_items on daily basis
// MAGIC 1. "Map" through the orders RDD and stored order date in the RDD
// MAGIC 2. Created tuple with order date and 1
// MAGIC 3. Added the total orders for each date
// MAGIC 4. Printed first 10 orders per day

// COMMAND ----------

   
def DailyOrderNumber(ordersRDD:RDD[String]):RDD[(String, Int)]={
    /*
    #Get the number of orders from order_items on daily basis
    #"Map" through the orders RDD and stored order date in the RDD
    #Created tuple with order date and 1
    #Added the total orders for each date
   */
   val ordersPerDay=ordersRDD
                    .map(x=>x.split(",")(1)) // map of date of orders
                    .map(x=>(x,1))//generate a map (date,1) for each date for example [(date1,1), (date2,1,(date1,1)]
                    .reduceByKey(_+_) //Reduce by key ==date for example [(date1,2),(date2,1)]
                    .sortByKey(false) //sort by key==date
                
    //return a rdd([(date, number of orders)])
    return ordersPerDay
                     }
val ordersPerDay=DailyOrderNumber(ordersRDD)
for (order <- ordersPerDay.take(10) ) println (order._1,order._2)

// COMMAND ----------

val dfordersPerDayWithSchema = spark.createDataFrame(ordersPerDay).toDF("Date", "number of orders")
display(dfordersPerDayWithSchema.select(to_date(col("Date"), "yyyy-MM-dd").as("Date"),col("number of orders")))

// COMMAND ----------

// MAGIC %md ### Get total revenue from order_items
// MAGIC 1. "Map" through the orders items RDD and stored price in the RDD
// MAGIC 2. Added the total price
// MAGIC 3. Printed total revenue

// COMMAND ----------

def TotalRevenue(orderItemsRDD:RDD[String]):Float={
    /*
    #Get total revenue from order_items
    #"Map" through the orders items RDD and stored price in the RDD
    #Added the total price
    */
    val totalRevenue= orderItemsRDD
                     .map(x=>x.split(",")(4)toFloat)
                     .reduce((total, revenue) => total + revenue)
                     //.reduce(_+_)
                 
    return totalRevenue
  }
val totalRevenu=TotalRevenue(orderItemsRDD)
println("Total Revenu :"+totalRevenu)

// COMMAND ----------

// MAGIC %md ### Get max priced product in products table
// MAGIC 1. "Map" through the products RDD and check for the max price of the product
// MAGIC 2. Printed max priced product

// COMMAND ----------

def MaxPrice(productsRDD:RDD[String]):Array[String]={
      /*
      #Get max priced product in products table
        #"Map" through the products RDD and check for the max price of the product
        #return max priced product
        */
    val maxPricedProduct= productsRDD
                            .map(x=>x.split(","))
                            .reduce((x, y) => if (y(3).toFloat  > x(3).toFloat ) y else x)//Comparaison between value (price)
                             
                         
    return maxPricedProduct
  
  }
 val MaxPriceProduct=MaxPrice(productsRDD)
print(MaxPriceProduct)
 

// COMMAND ----------

// MAGIC %md ### Computing average revenue
// MAGIC 1. "Map" through the order items RDD, stored total price and computed revenue
// MAGIC 2. "Map" through the order RDD and counted total number of orders
// MAGIC 3. Computed average revenue
// MAGIC 4. Printed average revenue

// COMMAND ----------

//Since countByKey takes in a PairRDD, you have to specify the None, or else it will be taken as a RDD
val ordersByStatus = ordersRDD.map(x=> x.split(",")).map(x=> (x(3), None)).countByKey()

ordersByStatus.keys.foreach( (order) =>  println((order +", "+ ordersByStatus(order))))

// COMMAND ----------


  
def OrdersByStatus(ordersRDD:RDD[String],action:String):RDD[(String, Float)]={
    /*
    #Number of orders by status - Using user specific action=( countByKey,reduceByKey,groupByKey)
    #"Map" through the orders RDD and stored order status
    #Computed total orders by status using countByKey
    #Printed number of orders by status
    */
    if (action=="countByKey"){
       var ordersByStatus = ordersRDD
                        .map(x=> x.split(","))
                        //#Since countByKey takes in a PairRDD, you have to specify the None, or else it will be taken as a RDD
                        .map(x=>  (x(3), None))
                        .countByKey()
                    
      }
    else if (action=="reduceByKey"){
        var ordersByStatus=ordersRDD
                        .map(x=> (x.split(",")(3),1))
                        .reduceByKey(_+_)
                        
      }
    else if (action=="groupByKey"){
        var ordersByStatus =  ordersRDD
                        .map( x=> (x.split(",")(3),1))
                        .groupByKey()
                        .map(x=> 
                               (x._1,
                                 x._2.map(acc=>acc).reduce(_+_).toFloat
                               )
                            )
    }
    else  return null
    
    
    //Return Number of orders by status
      
    return ordersByStatus
}


// COMMAND ----------

var ordersByStatus =  ordersRDD
                        .map( x=> (x.split(",")(3),1))
                        .groupByKey()
                        .map(x=> 
                               (x._1,
                                 x._2.map(acc=>acc).reduce(_+_).toFloat
                               )
                            )
                         
                              
  ordersByStatus.take(10)                           

// COMMAND ----------

//var RDDordersByStatus=sc.parallelize(ordersByStatus.toArray)
var dfordersByStatus = spark.createDataFrame(ordersByStatus).toDF("Status","Number of Orders")
display(dfordersByStatus)

// COMMAND ----------

// MAGIC %md ### Get customer_id with max revenue for each day
// MAGIC 1. "Map" through the orders RDD and stored order ID as key and date and customer ID as value
// MAGIC 2. "Map" through the order items RDD and store order ID and price
// MAGIC 3. Joined both the RDD on order ID
// MAGIC 4. Compared each record to find the customer ID having maximum revenue for each day
// MAGIC 4. Printed customer with max revenue for each day

// COMMAND ----------

  
    var ordersMapRDD=ordersRDD
                    .map(x=> x.split(","))
                    .map(x=> (x(0),(x(1),(x(2).toInt))))
                   
    //#"Map" through the order items RDD and store order ID and price
    var orderItemsMapRDD= orderItemsRDD
                        .map(x=> x.split(","))
                        .map( x=>(x(1),(x(4).toFloat)))
                         .map(x=> ((x(1),(x(4).toFloat))))
                        
    //#Joined both the RDD on order ID
    
  
   var perDayRDD=(ordersJoin
                .map(x=>(x._1._1,(x._1._1,x._1._2))
                )
   // #Compared each record to find the customer ID having maximum revenue for each day
    custIdMaxRev=(perDayRDD
                    .reduceByKey((x,y)=> (x if x._1 >y._1 else y))
                    )

// COMMAND ----------

def CustomerwithMaxRevenu(ordersRDD:RDD[String],orderItemsRDD:RDD[String]):RDD[String]={
   /* #Get customer_id with max revenue for each day
    
    #"Map" through the orders RDD and stored order ID as key and date and customer ID as value
    */
    var ordersMapRDD=ordersRDD
                    .map(x=> x.split(","))
                    .map(x=> (x(0),(x(1),(x(2).toInt))))
                   
    //#"Map" through the order items RDD and store order ID and price
    var orderItemsMapRDD= orderItemsRDD
                        .map(x=> x.split(","))
                       // .map(x=> (x[1],float(x[4])))
                         .map(x=> ((x(1),(x(4).toFloat))))
                         
    //#Joined both the RDD on order ID
     
    var ordersJoin=ordersMapRDD.join(orderItemsMapRDD)
  
   var perDayRDD= ordersJoin
                .map((key,value):((value._1._1),(value._1,value_1._2)))
                 
   // #Compared each record to find the customer ID having maximum revenue for each day
    custIdMaxRev= perDayRDD
                    //.reduceByKey((x,y): (x if x(1)>y(1) else y))
                      .reduceByKey((x, y) => if (x(1).toFloat  > y(1).toFloat ) y else x)//Comparaison between value (price)
                     
   
    //return customer with max revenue for each day
    return custIdMaxRev
    }


// COMMAND ----------

ordersMapRDD=ordersRDD.map(lambda x: x.split(',')).map(lambda x: (x[0],(x[1],int(x[2]))))
orderItemsMapRDD=orderItemsRDD.map(lambda x: x.split(',')).map(lambda x: (x[1],float(x[4])))
ordersJoin=ordersMapRDD.join(orderItemsMapRDD)
perDayRDD=ordersJoin.map(lambda (key,value):(value[0][0],(value[1],value[0][1])))
custIdMaxRev=perDayRDD.reduceByKey(lambda x,y: (x if x[1]>y[1] else y))
for i in custIdMaxRev.take(10): print (i)
