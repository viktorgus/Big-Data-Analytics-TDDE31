
from pyspark import SparkContext

sc = SparkContext(appName = "Exercise 3")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# filter by year
lines = lines.filter(lambda x: int(x[1][0:4])>=1960 and int(x[1][0:4]) <=2014)


# (key, value) = ( (year,month, day, station) , (temperature, temperature) )
daily_temp = lines.map(lambda x: ( ( int(x[1][0:4]), int(x[1][5:7]) , int(x[1][8:10]) , x[0] ) , ( float(x[3]),float(x[3]) ) ))

# reduce over day and station, get max and min for each day
# ((key), (value)) =  ((year,month,day,station) , (max, min)
temp_count = daily_temp.reduceByKey(lambda a,b: ( max(a[0],b[0]), min(a[1],b[1])  )  )

# remove day from key to be able to sum all days. Add a 1 to value to count temperatures summed
# ((key), (value)) =  ((year,month,station) , (max, min, 1) )
sum_and_count = temp_count.map(lambda x: ( (x[0][0] ,x[0][1] ,x[0][3]), (x[1][0], x[1][1], 1)   )   )


# add min and max for each month
# ((key), (value)) =  ((year,month,station) , (max+min, countsums) )

summed = sum_and_count.reduceByKey(lambda a,b: (a[0] + b[0] , a[1] + b[1], a[2] + b[2] )  )
summed = summed.map(lambda x: (  (x[0]), (x[1][0]+x[1][1], x[1][2])  )   )

# ((key), (value)) =  ((year,month,station) , (averages) )

averages = summed.mapValues(lambda x: ( x[0]/(2 * x[1])  ))
averages = averages.sortBy(ascending = False, keyfunc=lambda k: k[0])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
averages.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")

