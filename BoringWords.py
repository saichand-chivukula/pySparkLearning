from pyspark import SparkContext

def loadBoringWords():
    boring_words = set(line.strip() for line in open("/Users/saichandchivukula/Desktop/Datasets/boringwords.txt"))
    return boring_words

sc = SparkContext("local[*]", "BoringWords")
broadcast_data = sc.broadcast(loadBoringWords())

input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigdatacampaigndata-201014-183159.csv")
required_cols = input.map(lambda x: (float(x.split(",")[0]), x.split(",")[10]))
flatten_words = required_cols.flatMapValues(lambda x: x.split(" "))
exchanged_values = flatten_words.map(lambda x : (x[1].lower(), x[0]))
filtered_words = exchanged_values.filter(lambda x : x[0] not in  broadcast_data.value)

word_count = filtered_words.reduceByKey(lambda x,y : x + y)
sorted_count = word_count.sortBy(lambda x : x[1])
result = sorted_count.take(20)

for x in result:
    print(x)
