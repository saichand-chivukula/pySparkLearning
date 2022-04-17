#find the top rated movies
#1.Atleast 1000 people have rated
#2.Ratings should be greater than 4.5

from pyspark import SparkContext
# from sys import stdin

sc = SparkContext("local[*]", "RatingsMovies")
ratings_rdd = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/ratings-201019-002101.dat")
mapped_rdd = ratings_rdd.map(lambda x: (x.split("::")[1], x.split("::")[2]))
required_tuple = mapped_rdd.mapValues(lambda x: (float(x),1.0))
reduced_rdd = required_tuple.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
filtered_rdd = reduced_rdd.filter(lambda x:x[1][0] > 1000)
final_rdd = filtered_rdd.mapValues(lambda x: x[0]/x[1]).filter(lambda x: x[1] > 4.5)

movies_rdd = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/movies-201019-002101.dat")
movies_mapped_rdd = movies_rdd.map(lambda x: (x.split("::")[0], x.split("::")[1], x.split("::")[2]))
joined_rdd = movies_mapped_rdd.join(final_rdd)

topmovies_rdd = joined_rdd.map(lambda x: x[1][0])

result = topmovies_rdd.collect()
for x in result:
    print(x)

# stdin.readline()