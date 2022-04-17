from pyspark import SparkContext, StorageLevel
from sys import stdin

sc = SparkContext("local[*]", "CustomerOrdered")
base_rdd = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/customerorders-201008-180523.csv")
mapped_rdd = base_rdd.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))
ordersum_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)
filtered_customers = ordersum_rdd.filter(lambda x: x[1] > 5000 )
doubled_amount = filtered_customers.map(lambda x : (x[0], x[1] *2)).persist(StorageLevel.MEMORY_ONLY)

result = doubled_amount.collect()

for x in result:
    print(x)

print(filtered_customers.count())

stdin.readline()