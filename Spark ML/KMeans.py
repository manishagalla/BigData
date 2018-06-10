from numpy import array
from pyspark.mllib.clustering import KMeans, KMeansModel
from collections import defaultdict
from pyspark import SparkContext 
sc = SparkContext()

data  = sc.textFile("//Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW3/dataset/itemusermat")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
pdList = parsedData.collect()

model  = KMeans.train(parsedData, 10, maxIterations=10, initializationMode="random")
pd = model.predict(parsedData)

values = pd.collect()
ans = defaultdict(list)

for index,val in enumerate(values):
	ans[val].append(pdList[index][0])
	




mvedata = sc.textFile("/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW3/dataset/movies.dat")
mvedata2 = mvedata.map(lambda line: (line.split("::")[0],[x for x in line.split("::")]))
mve = mvedata2.collect()

mvedict = defaultdict(list)
for i in mve:
	mvedict[i[0]] = i[1]


for i in range(10):
	print "---cluster: " , i + 1 , "---"
	for k in range(5):	
		print mvedict[str(int(ans[i][k]))][0], ", ",mvedict[str(int(ans[i][k]))][1],", ",mvedict[str(int(ans[i][k]))][2]
	print ""



