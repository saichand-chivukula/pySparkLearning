from pyspark import SparkContext

def blank_line_checker(line):
    if(len(line) == 0):
        accum_init.add(1)

sc = SparkContext("local[*]", "BlankLinesCount")
input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/samplefile.txt")

accum_init = sc.accumulator(0)
blank_line_counter = input.foreach(blank_line_checker)

print(accum_init.value)

# we can use foreach on rdd but not in python i.e. not on the local variable list

