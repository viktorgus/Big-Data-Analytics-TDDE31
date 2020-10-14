from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Exercise 4")
SQLContext = SQLContext(sc)

# read file for temperature readings into RDD, map the RDD, map the rows into format (year, month, day, station, temperature)
rdd1 = sc.textFile("BDA/input/temperature-readings.csv")
parts1 = rdd1.map(lambda l: l.split(";"))
temperature = parts1.map(lambda p: Row(year=int(p[1][0:4]), 
                                   month=int(p[1][5:7]), 
                                   day=int(p[1][8:10]),
                                   station=int(p[0]),
                                   temperature = float(p[3])))

# convert RDD to dataframe
temperature_table = SQLContext.createDataFrame(temperature)
temperature_table.registerTempTable("temperature")

# read file for precipitation readings into RDD, map the RDD, map the rows into format (year, month, day, station, precipitation)
rdd2 = sc.textFile("BDA/input/precipitation-readings.csv")
parts2 = rdd2.map(lambda l: l.split(";"))
precipitation = parts2.map(lambda p: Row(year=int(p[1][0:4]), 
                                   month=int(p[1][5:7]), 
                                   day=int(p[1][8:10]),
                                   station=int(p[0]),
                                   precipitation = float(p[3])))

# convert RDD to dataframe
precipitation_table = SQLContext.createDataFrame(precipitation)
precipitation_table.registerTempTable("precipitation")

# Find the total precipitation for each day by summing all readings for each day over all station, months and years
daily_prec = precipitation_table.groupby('year', 'month', 'day', 'station').agg({'precipitation' : 'sum'})
daily_prec = daily_prec.withColumnRenamed('sum(precipitation)', 'daily_p')

# Find max daily precipitation and max temperature for each station over all readings
max_prec = daily_prec.groupby('station').agg({'daily_p' : 'max'})
max_temp = temperature_table.groupby('station').agg({'temperature' : 'max'})

# combine dataframes for max precipitation and max temperatures for all stations
temp_prec = max_temp.join(max_prec, on=['station'], how='inner')
temp_prec = temp_prec.withColumnRenamed('max(temperature)','maxTemp')
temp_prec = temp_prec.withColumnRenamed('max(daily_p)','maxDailyPrecipitation')

# Filter maxtemps to values between 25 and 30, and filter maxprecipitation to values between 100 and 200
temp_prec_filtered = temp_prec.filter((temp_prec['maxTemp'] >= 25.0) & (temp_prec['maxTemp'] <= 30.0) & (temp_prec['maxDailyPrecipitation'] >= 100.0) & (temp_prec['maxDailyPrecipitation'] <= 200.0))

final = temp_prec_filtered.sort('station',ascending=False)

final.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")
