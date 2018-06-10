from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext

sc = SparkContext()
data = sc.textFile("/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW3/dataset/ratings.dat")
ratings1 = data.map(lambda l: l.split('::'))
ratings  = ratings1.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
rank = 10
numIterations = 10


model = ALS.train(ratings, rank, numIterations)

testdata = ratings.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

#accuracy = 1.0 * ratesAndPreds.filter(lambda )

