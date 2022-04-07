from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":

    sc = SparkContext("local[*]", "WordCount")
    sc.setLogLevel("ERROR") # this sets the loglevel for the program like INFO, WARN, ERROR, FATAL
    input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/search_data.txt")

    # one input row need to be divided into multiple words
    words = input.flatMap(lambda x : x.split(" "))

    #one input row will give only one output row
    word_count = words.map(lambda x : (x,1))

    final_count = word_count.reduceByKey(lambda x,y : x+y)

    result = final_count.collect()

    for results in result:
        print(results)

    stdin.readline()
    # the above statement makes us to see the the DAG as it makes the program continously running
    # SCALA DAG and PYTHON DAG are different because SCALA uses the SPARK CORE directly while PYTHON uses the API library
    # if we use the above statement we need to stop the program manually


