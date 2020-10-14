from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "Exercise 5")
SQLContext = SQLContext(sc)

# read file for temperature readings into RDD, map the RDD, map the rows into format (year, month, day, station, precipitation)
rdd1 = sc.textFile("BDA/input/precipitation-readings.csv")
parts1 = rdd1.map(lambda l: l.split(";"))
precipitation = parts1.map(lambda p: Row(year=int(p[1][0:4]), 
                                   month=int(p[1][5:7]), 
                                   day=int(p[1][8:10]),
                                   station=int(p[0]),
                                   precipitation = float(p[3])))

# convert precipitation RDD to dataframe
precipitation_table = SQLContext.createDataFrame(precipitation)
precipitation_table.registerTempTable("precipitation")

# read file for stations in Ostergotland, map each row to the station ID  (station)
rdd2 = sc.textFile("BDA/input/stations-Ostergotland.csv")
parts2 = rdd2.map(lambda l: l.split(";"))
stations = parts2.map(lambda p: Row(station=int(p[0])))

# convert stations in ostergotland RDD to dataframe
stations_table = SQLContext.createDataFrame(stations)
stations_table.registerTempTable("stations")

# Filter out precipitations to values in year 1993 to 2016
precipitation_table = precipitation_table.filter((precipitation_table['year'] >= 1993) & (precipitation_table['year'] <= 2016))

# Find the monthly precpitation by summing all daily precpitation
monthly_prec = precipitation_table.groupby('year', 'month', 'station').agg({'precipitation' : 'sum'})
monthly_prec = monthly_prec.withColumnRenamed('sum(precipitation)', 'monthly_p')

# only consider readings from stations in Ostergotland by joining the precipitation dataframe with the station dataframe, 
# and thus removing all stations that do not complete the join
monthly_prec_ostergotland = monthly_prec.join(stations_table, on='station')

# find the monthly avarage for all stations by avaraging precipitation over all stations
avgMonthlyPrec = monthly_prec_ostergotland.groupby('year', 'month').agg({'monthly_p' : 'avg'})
avgMonthlyPrec = avgMonthlyPrec.withColumnRenamed('avg(monthly_p)', 'avgMonthlyPrecipitation')

final = avgMonthlyPrec.orderBy(['year','month'], ascending=[0,0])

final.coalesce(1).write.format('csv').mode('overwrite').option("header", "true").save("BDA/output")
