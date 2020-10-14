from pyspark import SparkContext

sc = SparkContext(appName = "Exercise 5")
# This path is to the file on hdfs
p_file = sc.textFile("BDA/input/precipitation-readings.csv")
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
p_lines = p_file.map(lambda line: line.split(";"))
s_lines = station_file.map(lambda line: line.split(";"))

# Retrieve list of stations in Ostergotland
stations = s_lines.map(lambda x: int(x[0]) ).collect()

# Show date and precipitation
# (key, value) = ( (year, month, station) , p ) 
pstations = p_lines.map(lambda x: ( ( int(x[1][0:4]), int(x[1][5:7]) ,  int(x[0]) ) , float(x[3])  ))

# Filter readings by year and stations in Ostergotland
pstations = pstations.filter(lambda x: x[0][0] >= 1993 and x[0][0] <= 2016 and x[0][2] in stations)

# Sum precipitations over months for each station
p_avg = pstations.reduceByKey(lambda a,b: a+b)

# (key, value) = ( (year, month, ) , (p,1) ) 
p_avg = p_avg.map(lambda x: ( (x[0][0],x[0][1]) , (x[1],1) )  )

# Sum precipitation for each month and number of readings
avg = p_avg.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))

# Calculate averages for each month and year
avg = avg.mapValues(lambda a: (a[0]/a[1]))

# Sort by year
avg = avg.sortBy(ascending = False, keyfunc=lambda k: k[0])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")
