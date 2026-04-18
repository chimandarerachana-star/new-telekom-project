from pyspark.sql import SparkSession
spark =SparkSession.builder.appName("RDD Operations Example").getOrCreate()
sc =spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])
print(rdd.collect())


rdd_map=rdd.map(lambda x: x*2)
print(rdd_map.collect())


rdd_filter = rdd.filter(lambda x: x%2 == 0)
print(rdd_filter.collect())

rdd_filter = rdd.filter(lambda x: x==3)
print(rdd_filter.collect())


lines = sc.parallelize(["hello Spark","pyspark RDD"])
print(lines.collect())

rdd_flat=lines.flatMap(lambda line: line.split(" "))
print(rdd_flat.collect())

rdd_dup = sc.parallelize([1,2,2,3,3,3])
print(rdd_dup.distinct().collect())

rdd1 = sc.parallelize([1,2,3])
rdd2 = sc.parallelize([3,4,5])
rdd3=rdd1.intersection(rdd2)
print(rdd3.collect())

print(rdd1.union(rdd2).collect())

data =[("a",1),("b",2),("a",3),("a",10)]
rdd_kv = sc.parallelize(data)

