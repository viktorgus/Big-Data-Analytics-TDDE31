from pyspark import SparkContext
import numpy as np


def closest_point(p, centers):
    best_index = 0
    closest = float("+inf")
    for i in range(len(centers)):
        temp_dist = np.sum((p - centers[i]) ** 2)
        if temp_dist < closest:
            closest = temp_dist
            best_index = i
    return best_index


sc = SparkContext(appName="k-means")
data = sc.textFile("path/subpath/coordinates.csv").map(lambda points: points.split(";"))
data.cache()

k = 2
converge_dist = 0.001
# Initialize k random centroids
k_points = data.takeSample(False, k, 1)
temp_dist = 1.0

while temp_dist > converge_dist:
    # Assign a cluster to each point
    closest = data.map(
        lambda p: (closest_point(p, k_points), (p, 1)))
    # Sum the coordinates for all points assigned to a cluster and count them
    point_stats = closest.reduceByKey(
        lambda p1_c1, p2_c2: (p1_c1[0] + p2_c2[0], p1_c1[1] + p2_c2[1]))
    # Calculate new centroids
    new_points = point_stats.map(
        lambda st: (st[0], st[1][0] / st[1][1])).collect()

    for(ik, p) in new_points:
        # Calculate the distances between old and new centroids
        temp_dist = sum(np.sum((k_points[ik] - p) ** 2))
        # Update the centroids
        k_points[ik] = p

print("Final centers: ", str(k_points))

