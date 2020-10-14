from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Exercise 1")
SQLContext = SQLContext(sc)

# read file into RDD, map the RDD into format (station, year, temperature)
rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
readings = parts.map(lambda p: Row(station = int(p[0]),
                                   year=int(p[1][0:4]),
                                   temperature = float(p[3])))

# convert RDD to dataframe
readings_table = SQLContext.createDataFrame(readings)
readings_table.registerTempTable("readings")


## Filter out all records except those in years 1950-2014
filtered = readings_table.filter((readings_table['year'] >= 1950) & (readings_table['year'] <= 2014))

# Find max temperature per year for each station. Like using .reduceByKey(); reducing over the key 'year' and taking the max of temperature values.
max_temps = filtered.groupby('year').agg({'temperature' : 'max'})

#Rename max-column to temperature
max_temps = max_temps.withColumnRenamed('max(temperature)','temperature')

# Find min temperature per year for each station. Like using .reduceByKey(); reducing over the key 'year' and taking the min of temperature values.
min_temps = filtered.groupby('year').agg({'temperature' : 'min'})

#Rename min-column to temperature
min_temps = min_temps.withColumnRenamed('min(temperature)','temperature')


max_temps = max_temps.join(filtered, ['year', 'temperature'])
max_temps = max_temps.withColumnRenamed('temperature', 'maxValue')
min_temps = min_temps.join(filtered, ['year', 'temperature'])
min_temps = min_temps.withColumnRenamed('temperature', 'minValue')

# Sort over temperature
max_temps = max_temps.sort("maxValue",ascending=False)
min_temps = min_temps.sort("minValue",ascending=False)

max_temps = max_temps.select('year','station','maxValue')
min_temps = min_temps.select('year','station','minValue')

max_temps.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")
#min_temps.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")

