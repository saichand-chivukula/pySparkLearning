from pyspark import SparkContext

sc = SparkContext("local[*]", "BigLog")
sc.setLogLevel("ERROR")

input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigLog.txt")
mapped_rdd = input.map(lambda x: (x.split(":")[0], x.split(":")[1]))
grouped_rdd = mapped_rdd.groupByKey()
word_length =  grouped_rdd.map(lambda x: (x[0], len(x[1])))
result = word_length.collect()

for x in result:
    print(x)