from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext 


def avgratings(sc):
	data = sc.textFile("/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW3/dataset/glass.data")
	labData = data.map(lambda row : row.split(",")).map(lambda seq : LabeledPoint(unicode(int(seq[-1]) - 1),seq[:-2]))

	(trainingData, testData) = labData.randomSplit([0.6, 0.4])
	model = NaiveBayes.train(trainingData, 1.0)

	predictionAndLabel = testData.map(lambda p: (model.predict(p.features), p.label))
	accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / testData.count()
	#print (accuracy)
	print('model accuracy {}'.format(accuracy))


if __name__ == "__main__":
	
	sc = SparkContext()
	avgratings(sc)
