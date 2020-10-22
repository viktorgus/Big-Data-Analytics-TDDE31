from pyspark import SparkContext


def classify(point, k):
    dist = my_data.map(lambda x: (x[0], distance(x[1], point)))
    sort = dist.sortBy(lambda x: x[1])
    top_k = sort.take(k)
    mean = top_k.mean()
    label = 0 if mean < 0.5 else 1
    return label


sc = SparkContext(appName="knn")
lines = sc.textFile("path/subpath/data.csv").map(lambda line: line.split(";"))
# (key, value) = (class, coordinates)
my_data = lines.map(lambda x: (x[0], x[1]))
my_data.cache()

k = 10
point = [1, 2, 3, 4, 5]

class_label = classify(point, k)

