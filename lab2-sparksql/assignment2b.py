from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Exercise 2b")
SQLContext = SQLContext(sc)

# read file into RDD, map the RDD, map the rows into format (year, month, station, temperature)
rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
readings = parts.map(lambda p: Row(year=int(p[1][0:4]),
                                   month = int(p[1][5:7]),
                                   station = int(p[0]),
                                   temperature = float(p[3])))

# convert RDD to dataframe
readings_table = SQLContext.createDataFrame(readings)
readings_table.registerTempTable("readings")

## Filter out all records except those in years 1950-2014 
filtered = readings_table.filter((readings_table['year'] >= 1950) & (readings_table['year'] <= 2014))
## Filter out records under 10 temperature, as we wish to count the number of readings above 10
filtered = readings_table.filter(readings_table['temperature'] > 10)

# Aggregate all sepearte temperature readings for each stations readings per year and month. This is done to remove rows of several readings from the same station 
# from the same month and year above ten, as we wish to count only one of theese readings
count_temps = filtered.groupBy('year', 'month', 'station').agg({'temperature' : 'count'})

# Over each year and month: count the amount of different stations. As we have one row for each above ten reading for each station, this reduces to the count of
# unique station readings above 10 for each year and month
count_stations = count_temps.groupBy('year', 'month').agg({'station' : 'count'})
count_stations = count_stations.withColumnRenamed('count(station)', 'value')

count_stations = count_stations.sort("value",ascending=False)

count_stations.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")

