from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="lab_kernel")

def haversine(lon1, lat1, lon2, lat2):
  """
  Calculate the great circle distance between two points
  on the earth (specified in decimal degrees)
  """
  # convert decimal degrees to radians
  lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
  # haversine formula
  dlon = lon2 - lon1
  dlat = lat2 - lat1
  a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
  c = 2 * asin(sqrt(a))
  km = 6367 * c
  return km


# Compare dates for filtering
def date_comp(x, date):
    x = datetime.strptime(x, "%Y-%m-%d")
    date = datetime.strptime(date, "%Y-%m-%d")
    return x <= date

# Compare times for filtering
def time_comp(xdate, date, xtime, time):
  if (time == "24:00:00"):
    time = "00:00:00"
  xtime = datetime.strptime(xtime, "%H:%M:%S")
  time = datetime.strptime(time, "%H:%M:%S")
  if ((xdate == date) & (time != "00:00:00")):
    boolean = xtime < time
  else:
    boolean = True
  return boolean

# Number of days between two dates
from datetime import datetime
def date_diff(x, date):
    d1 = datetime.strptime(x, "%Y-%m-%d")
    d2 = datetime.strptime(date, "%Y-%m-%d")
    tot_days = abs((d2 - d1).days)
    diff = round(tot_days % 365.25) 
    if(diff>182): 
      diff=365-diff
    return diff

# Number of hours between two times
def time_diff(x, time):
  if (time == "24:00:00"):
    time = "00:00:00"
  t1 = datetime.strptime(x, "%H:%M:%S")
  t2 = datetime.strptime(time, "%H:%M:%S")
  diff = abs((t2-t1).total_seconds()/3600)
  if(diff>12): 
    diff=24-diff
  return diff

# Kernel-value
def k_value(diff, smoothing_koefficient):
  u = diff/smoothing_koefficient
  k = exp(-(u**2))
  return k

# Smoothing coefficient
h_distance = 100
h_date = 25
h_time = 3

# Place and date of interest
a = 58.4274 
b = 14.826
date = "2013-07-04"

# This path is to the file on hdfs
stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")
s_lines = stations.map(lambda line: line.split(";"))
t_lines = temps.map(lambda line: line.split(";"))

#t_lines = t_lines.sample(False,0.1)

# Stat_original (Station, name, measurementHeight, latitude, longitude, readingsFrom, readingsTo, elevation)
stations_data = s_lines.map(lambda x: (str(x[0]), (float(x[3]), float(x[4]))))
bc = sc.broadcast(stations_data.collectAsMap())

# Temp_original (Station, YYYY-MM-DD, HH:MM, temp, quality)
data = t_lines.map(lambda x: ((str(x[0]), str(x[1]), str(x[2]),
                               bc.value[str(x[0])][0], bc.value[str(x[0])][1]) , 
                              float(x[3])))

#  Data = ((Station number, YYYY-MM-DD, HH:MM, lat, lon) , temp)

# Filter dates, remove readings from the same date or ahead in time from the inputed prediction date
filteredByDate = data.filter(lambda x: (date_comp(x[0][1], date))) 

# Calculate kernel disatance from input date and point to all readings' date and points.
loop_data = filteredByDate.map(lambda x: (x[0], 
                                         (k_value(date_diff(x[0][1], date), h_date), 
                                          k_value(haversine(a,b, x[0][3],x[0][4]), h_distance), 
                                          x[1])))
# (Key, Value) = ((Station, Date, Time, lat, lon), (k_date, k_distance, temp))

# Loop data is saved in memory 
loop_data.cache()

for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
             "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:

  loop_data = loop_data.filter(lambda x: (time_comp(x[0][1], date, x[0][2], time)))
  data_temp = loop_data.map(lambda x: (x[0],
                                            (x[1][0], x[1][1],
                                            k_value(time_diff(x[0][2], time), h_time), 
                                            x[1][2])))
  # (Key, Value) = ((Station, Date, Time, lat, lon), (k_date, k_distance, k_time, temp))

  data_sum = data_temp.map(lambda x: (x[0],
                                      (x[1][0] + x[1][1] + x[1][2], 
                                      x[1][0] * x[1][1] * x[1][2], 
                                      x[1][3])))
  # (Key, Value) = ((Station, Date, Time, lat, lon), (sum, product, temp))

              
  data_sum = data_sum.map(lambda x: (time,(x[1][0], x[1][0] * x[1][2], x[1][1], x[1][1] * x[1][2])))
  # (Key, Value) = (Time, (sum_kernel, sum_temp, product_kernel, product_temp))

  summed_data = data_sum.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))
  summed_data = summed_data.map(lambda x: (x[0], x[1][1]/x[1][0], x[1][3]/x[1][2]))

  # Ta bort till loopen
  if (time == "24:00:00"):
    result = summed_data
  else:
    result = result.union(summed_data)

result.coalesce(1, shuffle = False).saveAsTextFile("BDA/output")

             
