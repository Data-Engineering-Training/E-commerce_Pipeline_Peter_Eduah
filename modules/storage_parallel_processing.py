from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ECommerceDataPipeline") \
    .getOrCreate()

try:
    # Data Ingestion with Spark
    customers_df = spark.read.csv("customers.csv", header=True)
    orders_df = spark.read.csv("orders.csv", header=True)
    deliveries_df = spark.read.csv("deliveries.csv", header=True)

    # Data Transformation with Spark
    joined_df = orders_df.join(deliveries_df, "Order_ID", "inner")

    # Aggregate Metrics Calculation with Spark
    customer_metrics_df = joined_df.groupBy("Customer_ID").agg(
        sum("Total_Price").alias("Total_Transaction_Amount"),
        avg("Total_Price").alias("Average_Order_Value"),
        count("Order_ID").alias("Total_Number_of_Orders")
    )

    # Scalability Considerations
    # Design the data pipeline to scale horizontally by adding more nodes to the Spark cluster
    # Spark automatically parallelizes data processing tasks across the cluster
    # Additional worker nodes can improve performance and throughput

    # Data Storage
    # Choose a suitable storage solution such as HDFS or a distributed database to store the processed data
    # Implement the storage mechanism ensuring it supports efficient retrieval and querying
    customer_metrics_df.write.mode("overwrite").parquet(
        "hdfs://<HDFS_MASTER>:<PORT>/e_commerce_data")  # specify your path url

finally:
    # Stop SparkSession
    spark.stop()
