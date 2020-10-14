
from pyspark import SparkContext

sc = SparkContext(appName = "Exercise 4")

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
p_file = sc.textFile("BDA/input/precipitation-readings.csv")

t_lines = temperature_file.map(lambda line: line.split(";"))
p_lines = p_file.map(lambda line: line.split(";"))

# Sum precipitation over days
rain = p_lines.map(lambda x: ( (x[0],x[1]), float(x[3]) ))
rain_daily = rain.reduceByKey(lambda a,b: a+b)

# Show only station number and temperature/precipitation
temp = t_lines.map(lambda x: ( x[0], float(x[3]) ))
rain = rain_daily.map(lambda x: ( x[0][0], x[1] ))

# Maximum temperature and precipitation for each station
max_temp = temp.reduceByKey(lambda a,b: max(a,b))
max_rain = rain.reduceByKey(lambda a,b: max(a,b))
    
# Filter stations by maximum temperature and precipitation
max_temp = max_temp.filter(lambda x : x[1] >= 25 and x[1] <= 30 )
max_rain = max_rain.filter(lambda x : x[1] >= 100 and x[1] <= 200 )

# Intersection of stations with temperature and precipitation within desired intervalls
union = max_temp.join(max_rain)

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
union.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")

