from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Exercise 2a")
SQLContext = SQLContext(sc)

# read file into RDD, map the RDD, map the rows into format (year, month, temperature)
rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
readings = parts.map(lambda p: Row(year=int(p[1][0:4]),
                                   month = int(p[1][5:7]),
                                   temperature = float(p[3])))

# convert RDD to dataframe
readings_table = SQLContext.createDataFrame(readings)
readings_table.registerTempTable("readings")

## Filter out all records except those in years 1950-2014 
filtered = readings_table.filter((readings_table['year'] >= 1950) & (readings_table['year'] <= 2014))
## Filter out records under 10 temperature, as we wish to count the number of readings above 10
filtered = readings_table.filter(readings_table['temperature'] > 10)

# Aggregate all sepearte temperature readings for each year and month into a count. We have thus counted the amount of readings above ten for each year, month.
count_temps = filtered.groupBy('year', 'month').agg({'temperature' : 'count'})
count_temps = count_temps.withColumnRenamed('count(temperature)', 'value')

count_temps = count_temps.sort("value",ascending=False)

count_temps.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")

