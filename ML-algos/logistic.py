from pyspark import SparkContext
import numpy as np


# Compute logistic regression gradient for a matrix of data points
def gradient(matrix, w):
    y = matrix[:, 0]   # point labels (first column of input file)
    x = matrix[:, 1:]  # point coordinates
    # For each point (x, y), compute gradient function, then sum these up
    return((1.0 / (1.0 + np.exp(-y * x.dot(w))) - 1.0) * y * x.T).sum(1)


def add(x, y):
    x += y
    return x


sc = SparkContext(appName="logistic")
points = sc.textFile("path/subpath/coordinates.csv").map(lambda p: p.split(";"))
points.cache()

d = 2
iterations = 10
# Initialize w to a random value
w = 2 * np.random.ranf(size=d) - 1
print("Initial w: " + str(w))

for i in range(iterations):
    print("On iteration %i" % (i + 1))
    w -= points.map(lambda m: gradient(m, w)).reduce(add)

print("Final w: " + str(w))
