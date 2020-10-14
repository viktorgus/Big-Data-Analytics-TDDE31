
from pyspark import SparkContext

sc = SparkContext(appName = "Exercise 2b")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# filter by year
lines = lines.filter(lambda x: int(x[1][0:4])>=1950 and int(x[1][0:4])<=2014 and float(x[3])>=10 )

# filter by temperature

# (key, value) = ( (month , year, station) , (1) ) add 1 to count all occourences. Add station to be able to use unique() to remove same station counts.
month_above10 = lines.map(lambda x: ( ( int(x[1][5:7]) , int(x[1][0:4]), x[0]) ) , 1)


month_temp = month_above10.distinct()
month_reduced = month_temp.map(lambda x: ( ( x[0] , x[1]) , 1))
month_summed = month_reduced.reduceByKey(lambda a, b: a + b)
month_summed = month_summed.sortBy(ascending = False, keyfunc=lambda k: k[0])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
month_summed.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")

