from pyspark import SparkContext

sc = SparkContext("local[*]", "LogLevel")
sc.setLogLevel("INFO")

if __name__ == "__main__":
    my_list = ["WARN: Fri Oct 17 10:37:51 BST 2014",
         "ERROR: Wed Jul 01 10:37:51 BST 2015",
         "WARN: Thu Jul 27 10:37:51 BST 2017",
         "WARN: Thu Oct 19 10:37:51 BST 2017",
         "WARN: Wed Jul 30 10:37:51 BST 2014"]

    local_rdd = sc.parallelize(my_list)
else:
    local_rdd =  sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigLog.txt")
    print("inside the else part")

mapped_rdd = local_rdd.map(lambda x: (x.split(":")[0], 1))
log_count = mapped_rdd.reduceByKey( lambda x,y : x + y)
result = log_count.collect()

for x in result:
    print(x)