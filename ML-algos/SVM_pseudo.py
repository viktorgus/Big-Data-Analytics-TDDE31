from pyspark import SparkContext
import numpy as np

sc = SparkContext(appName = "SVM")

data = sc.textFile("data")
data = data.map(lambda line: line.split(";"))
data.cache()

D = data.columns.size-1
w = 2 * np.random.ranf(size=D)-1

# weights to be learned for sigma(wT*x) 
w = sc.broadcast(w)

# Learning rate
lr = 0.01

# lambda for lagrange optimization of subconstraint 
C = 1


def y(x):
    return(x.dot(w))

def classify(x):
    return sign(y(x))

# x[0] is t i.e the label

iterations = 1000
for ( i in range(0,iterations)):
    # map each point in the form (t*y(x), (wT*x-t)*x) i.e (label*prediction, gradient for point), to filter out t*y(x)<1
    temp = data.map(lambda x: ( x[0] * y(x[1:5]), (y(x)- x[0] )*x[1:5]  ) ) 
    filtered = temp.filter(lambda x: x[0] >= 1 )
    pointGradients = filtered.map(lambda x: (x[1]))

    gradw = pointGradients.reduce(add)*C*2+w

    w = w - gradw * lr