import os
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import col, count, countDistinct


# Create the Spark session:
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "3gb") \
    .appName("Exercise3") \
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

# Who are the second most selling and the least selling persons (sellers) for each product?
# Who are those for product with `product_id = 0`

# First aggregate sales by product_id and seller_id
agg_sales = spark.sql("""
    SELECT 
        CAST(product_id AS int), 
        CAST(seller_id AS int), 
        CAST(SUM(num_pieces_sold) AS int) as num_pieces_sold
    FROM sales 
    GROUP BY product_id, seller_id
""") 
agg_sales.createOrReplaceTempView("agg_sales")

# Rank sellers for each product_id (ascending and descending)
ranked_sales = spark.sql("""
    SELECT 
        product_id, 
        seller_id,
        num_pieces_sold,  
        dense_rank() OVER (PARTITION BY product_id ORDER BY num_pieces_sold ASC) as rank_asc,
        dense_rank() OVER (PARTITION BY product_id ORDER BY num_pieces_sold DESC) as rank_desc
    FROM agg_sales 
    ORDER BY product_id
""") 
ranked_sales.createOrReplaceTempView("ranked_sales")

# Optional: Add seller type column: If both rank values are equal to 1 that means the seller is the only one for this product
ranked_sales = spark.sql("""
    SELECT * ,
    CASE WHEN rank_asc = 1 AND rank_desc = 1 THEN 'Only seller' ELSE 'One of many sellers' END AS type
    FROM ranked_sales
""")
ranked_sales.createOrReplaceTempView("ranked_sales")

# Who are the second most selling persons (sellers) for each product?
# Second most selling is the one who's rank in descending order is 2
print("Second most selling persons (sellers) for each product")
second_most_selling = ranked_sales.where("rank_desc = 2")
second_most_selling.show()

# Who are the least selling persons (sellers) for each product?
# Least selling is the one who's rank in ascending order is 1
print("Least selling persons (sellers) for each product")
least_selling = ranked_sales.where("rank_asc = 1")
least_selling.show()

# Who are those for product with `product_id = 0`
print("Second most selling person (seller) for product_id = 0")
second_most_selling.where("product_id = 0").show()

print("Least selling person (seller) for product_id = 0")
least_selling.where("product_id = 0").show()

