from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name", "DataFrameExample")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

order_df = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .csv("/Users/saichandchivukula/Desktop/Datasets/orders-201019-002101.csv")
order_df.printSchema()
# find the total orders placed by customers where order_customer_id > 10000
required_df = order_df.repartition(4)\
    .where("order_customer_id > 10000")\
    .select("order_id", "order_customer_id")\
    .groupBy("order_customer_id")\
    .count()

required_df.show()

spark.stop()