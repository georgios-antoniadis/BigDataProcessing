import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, count, countDistinct


# Create the Spark session:
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

#The following methods point to specific points in the HDFS installation on the machine that
#was used for development. Please change accordingly to match your needs
products = spark.read.parquet("file:///home/savas/data/coursework/data/products_parquet")
products.createOrReplaceTempView("products")
sales = spark.read.parquet("file:///home/savas/data/coursework/data/sales_parquet")
sales.createOrReplaceTempView("sales")
sellers = spark.read.parquet("file:///home/savas/data/coursework/data/sellers_parquet")
sellers.createOrReplaceTempView("sellers")

print('Registered new tables')
print('')

# How many orders?
print("Number of orders: {}".format(products.count()))

# How many products?
print("Number of products: {}".format(sales.count()))

# How many sellers?
print("Number of sellers: {}".format(sellers.count()))

# How many products have been sold at least once?
print("Number of products sold at least once")
sales.agg(countDistinct(col("product_id"))).show()

# Which is the product contained in more orders?
print("Product present in more orders")
sales \
    .groupBy("product_id") \
    .agg(count("*").alias("cnt")) \
    .orderBy(col("cnt").desc()) \
    .limit(1) \
    .show()

# How many distinct products have been sold in each day?
new_df = spark.sql('select date, count(distinct product_id) from sales_table group by date')

print("The average order revenue per product")
spark.sql("""
    SELECT 
        AVG(price * num_pieces_sold) AS average_order_revenue
        FROM sales 
        INNER JOIN products ON sales.product_id = products.product_id
    """).show()


print("Average contribution of an order to the seller's daily quota")
spark.sql("""
    SELECT /*+ BROADCAST(b) */
        a.seller_id,
        AVG(a.num_pieces_sold/b.daily_target) as avg_contribution
    FROM sales AS a 
    INNER JOIN sellers AS b ON a.seller_id = b.seller_id 
    GROUP BY a.seller_id
""").show(truncate=False)