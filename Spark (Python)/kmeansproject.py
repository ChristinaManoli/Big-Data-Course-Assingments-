from pyspark.sql import SparkSession
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans, KMeansSummary, KMeansModel

# Load the data and convert csv to DataFrame
spark = SparkSession.builder.appName("Csv file").getOrCreate()
df=spark.read.format("csv").option("inferSchema","true").load("hdfs://master:9000/user/user/csvFile/meddata2022.csv")

#transform input columns to feature vector
from pyspark.ml.feature import VectorAssembler

assemble=VectorAssembler(inputCols=['_c0','_c1','_c2','_c3','_c4'], outputCol='features')
assembled_data=assemble.transform(df)

#Create an evaluator
evaluator = ClusteringEvaluator(predictionCol='prediction',featuresCol='features', metricName='silhouette', distanceMeasure='squaredEuclidean')

"""##Αφου βρέθηκε το κατάλληλο Κ=6"""

# Trains a k-means model.
kmeans = KMeans(featuresCol='features', k=6, initMode='random')
model = kmeans.fit(assembled_data)
output=model.transform(assembled_data)
summary = model.summary

#Evaluate clustering by computing SSE.
cost = summary.trainingCost

#Cluster Centers
centers = model.clusterCenters()

#Evaluate clustering by computing Silhouette score.
score=evaluator.evaluate(output)

#Evaluate clustering by computing size of each cluster.
sizeCluster = summary.clusterSizes

print("For k = 6, Random Initialization: The Cost is : ", cost, " Silhouette Score: ",score ," Cluster size : ",sizeCluster, "\n")
print("Cluster Centers: ")
for center in centers:
	print(center)

# Trains a k-means model.
kmeans = KMeans(featuresCol='features', k=6, initMode='k-means||')
model = kmeans.fit(assembled_data)

output=model.transform(assembled_data)

summary = model.summary

#Evaluate clustering by computing SSE.
cost = summary.trainingCost

#Evaluate clustering by computing Silhouette score.
score=evaluator.evaluate(output)

#Evaluate clustering by computing size of each cluster.
sizeCluster = summary.clusterSizes

#Cluster Centers
centers = model.clusterCenters()

print("For k = 6, k-means|| Initialization: The Cost is : ", cost, " Silhouette Score: ",score ," Cluster size : ",sizeCluster, "\n")
print("Cluster Centers: ")
for center in centers:
	print(center)