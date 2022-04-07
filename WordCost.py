from pyspark import SparkContext

sc = SparkContext("local[*]", "WordCost")
input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigdatacampaigndata-201014-183159.csv")

required_cols = input.map(lambda x : (float(x.split(",")[10]), x.split(",")[0]))
flatten_words = required_cols.flatMapValues(lambda x: x.split(" "))
exchanged_values = flatten_words.map(lambda x: (x[1].lower(), x[0]))
word_count = exchanged_values.reduceByKey(lambda x,y : x + y)
sorted_count = word_count.sortBy(lambda x : x[1], False)
result = sorted_count.take(20)

for x in result:
    print(x)

    
