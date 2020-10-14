
from pyspark import SparkContext

sc = SparkContext(appName = "Exercise 2a")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# filter by year and temperature
lines = lines.filter(lambda x: int(x[1][0:4])>=1950 and int(x[1][0:4])<=2014 and float(x[3])>=10 )



# (key, value) = ( (year, month) , 1 ) 
month_above10 = lines.map(lambda x: ( ( int(x[1][5:7]) , int(x[1][0:4]) ) , 1 )  )


month_countabove10 = month_above10.reduceByKey(lambda a,b: a+b)
month_countabove10 = month_countabove10.sortBy(ascending = False, keyfunc=lambda k: k[0])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
month_countabove10.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")

