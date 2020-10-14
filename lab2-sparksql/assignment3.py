from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Exercise 3")
SQLContext = SQLContext(sc)

# read file into RDD, map the RDD, map the rows into format (year, month, day, station, temperature)
rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))
readings = parts.map(lambda p: Row(year=int(p[1][0:4]), 
                                   month=int(p[1][5:7]), 
                                   day=int(p[1][8:10]),
                                   station=int(p[0]),
                                   temperature = float(p[3])))

# convert RDD to dataframe
readings_table = SQLContext.createDataFrame(readings)
readings_table.registerTempTable("readings")

## Filter out all records except those in years 1950-2014 
filtered = readings_table.filter((readings_table['year'] >= 1950) & (readings_table['year'] <= 2014))

# Find max and min temperature for each year, month, day, station
max_temps = filtered.groupby('year', 'month', 'day', 'station').agg({'temperature' : 'max'})
min_temps = filtered.groupby('year', 'month', 'day', 'station').agg({'temperature' : 'min'})

# Combine max and min readings
maxmin = max_temps.join(min_temps, on=['year', 'month', 'day', 'station'], how='inner')

# Find avarage temperature per day by taking the avarage of the max and min value for this day
avg_temp = maxmin.withColumn('daily_avg', (maxmin['max(temperature)']+maxmin['min(temperature)'])/2)

# Find the monthly temperature avarage by avaraging the daily varages over each month, year and station
month_avg = avg_temp.groupby('year', 'month', 'station').agg({'daily_avg' : 'avg'})

# Rename avarage monthly temperature column and sort dataframe over this column
month_avg = month_avg.withColumnRenamed('avg(daily_avg)', 'avgMonthlyTemperature').sort('avgMonthlyTemperature',ascending=False)

month_avg.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")
