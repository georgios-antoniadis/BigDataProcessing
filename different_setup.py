from pyspark.sql import Row
from pyspark.sql import SparkSession, SQLContext
import hashlib


# Create a new column called "hashed_bill" defined as follows:
# - if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
# - if the order_id is odd: apply SHA256 hashing to the bill text
# Finally, check if there are any duplicate on the new column

#Create the Spark session using the following code
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.driver.memory", "2gb") \
    .config("spark.executor.memory", "3gb") \
    .appName("Exercise4") \
    .getOrCreate()

#Create a UDF to apply it later on during the transformation stage
def myhash(order_id, bill_text):
    ret = bill_text.encode("utf-8")
    # If order_id is even
    if int(order_id) % 2 == 0:
        # Count number of 'A'
        cnt_A = bill_text.count("A")
        # For each 'A' apply MD5 hashing iteratively
        for _ in range(cnt_A):
            ret = hashlib.md5(ret).hexdigest().encode("utf-8")
        ret = ret.decode('utf-8')
    # If order_id is odd
    else:
        # Apply SHA256 hashing
        ret = hashlib.sha256(ret).hexdigest()
    return ret

# Read the source tables
sales = spark.read.parquet("file:///home/savas/data/coursework/data/sales_parquet/*.parquet")


#Applying a map with the user defined function
sales_rdd1 = sales.map(lambda r: Row(
    r["order_id"], r["bill_raw_text"],
    myhash(r["order_id"], r["bill_raw_text"])
))

df = sales_rdd1.toDF().toDF("order_id", "bill_raw_text", "hashed_bill")
print('Sales with hashed_bill')
df.show()

# Optionally, now that you have a dataframe you can check for duplicates using Spark SQL
# df.createOrReplaceTempView("sales_hashed")
# dups_df = spark.sql("""
#     SELECT 
#         hashed_bill, 
#         COUNT(*) as count
#     FROM sales_hashed
#     GROUP BY hashed_bill
#     HAVING COUNT(*) > 1
# """)
# print('Duplicate hashed_bill')
# dups_df.show()

#Or utilize RDDs to do it. Here we are only fetching the column we are interested in
sales_rdd2 = df.select("hashed_bill").rdd

print('Checking for duplicates ...')

#New map reduce for duplicates check
sales_new_map = (sales_rdd2.map(lambda x: (x,1))
                .reduceByKey(lambda x,y: x+y))

#There are duplicates if the count is greater than 1
sales_check_for_duplicates = sales_new_map.filter(lambda x: x[1] > 1).collect()

if len(sales_check_for_duplicates) > 0:
    print("There are duplicate values!: ")
    print(sales_check_for_duplicates)

else:
    print("There are no duplicate values!")