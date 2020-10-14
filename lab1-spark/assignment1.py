
# 1) What are the lowest and highest temperatures measured each year for the period 1950-2014.
# Provide the lists sorted in the descending order with respect to the maximum temperature. In
# this exercise you will use the temperature-readings.csv file.


from pyspark import SparkContext

sc = SparkContext(appName = "Exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year, temperature)
year_temperature = lines.map(lambda x: (int(x[1][0:4]), (float(x[3]), float(x[3]))  ) ) 

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
max_temperatures = year_temperature.reduceByKey(lambda a,b: (max(a[0], b[0]), min(a[1],b[1]) ) )
max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperatures.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")

