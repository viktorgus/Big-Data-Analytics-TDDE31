from pyspark import SparkContext


def intermediate_compute(n, data, test):
    nc_init = data.filter(lambda x: x[2] == test[2]).persist()
    nc = nc_init.count()
    nac = nc_init.filter(lambda x: x[0] == test[0]).count()
    nbc = nc_init.filter(lambda x: x[1] == test[1]).count()

    pc = nc / n
    pac = nac / nc
    pbc = nbc / nc
    return pac * pbc * pc


def classify(test, data):
    n = data.count()
    numerator = intermediate_compute(n, data, test)
    data_roof = data.map(lambda x: (x[0], x[1], 1 - x[2]))
    denominator = intermediate_compute(n, data_roof, test)

    pred = numerator / denominator
    label = 0 if pred < 0.5 else 1
    return label


sc = SparkContext(appName="nb")
lines = sc.textFile("data.txt").map(lambda line: line.split())
data = lines.map(lambda x: (x[0], x[1], x[2]))
data.cache()

test = [0, 0, 0]
class_label = classify(test, data)


Projects = {
"UsMis": {
    "budget": 1000000,
    "finalreport": 391
},
"AMee3": {
    "budget": 3700000,
    "finalreport": 391
},
"Bee": {
    "budget": 1300000,
    "finalreport": 121
}}

Reports = {
121: {
    "pages": 70,
    "location": "http://acme.com/beerep"
}, 
391: {
    "pages": 350,
    "location": "http://acme.com/r391699"
},
699: {
    "pages": 350,
    "location": "http://acme.com/r391699"   
} 
}