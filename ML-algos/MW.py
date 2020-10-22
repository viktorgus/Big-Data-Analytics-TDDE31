from pyspark import SparkContext
import numpy as np

sc = SparkContext(appName = "MW")


data = sc.textFile("data")
data = data.map(lambda line: line.split(";"))
data.cache()

# get euclidian distance
def euclidian(a, b):
  return np.linalg.norm(a-b)

# x[0] is coordinates, x[1] is classification
def MWA(point, h, data):
  # temp = (distance, class) for each obs in data to the point
  temp = data.map(lambda x: (euclidian(point,x[0]),x[1])  )
  filtered = temp.filter(lambda x: x[0] <= h).persist()
  k = filtered.count()

  sum = filtered.reduce(lambda a,b: a[1] + b[1])

  fraction = sum/k
  if (fraction > 0.5):
    result = 1
  else:
    result = 0
  
  return result

# random point
point = (0,0,0)
h = 20

classification = MWA(point, h, data)
