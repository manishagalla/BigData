
import numpy as np 
import pandas as pd 
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark import SparkContext 

def cluster(sc):
	data = sc.textFile("/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW3/dataset/glass.data")
	#creating labelled data
	labData = data.map(lambda row : row.split(",")).map(lambda seq : LabeledPoint(unicode(int(seq[-1]) - 1),seq[:-2]))

	(trainingData, testData) = labData.randomSplit([0.6, 0.4])
	model = DecisionTree.trainClassifier(trainingData, numClasses=7, categoricalFeaturesInfo={},impurity='gini', maxDepth=5, maxBins=32)

	predictions = model.predict(testData.map(lambda x: x.features))
	labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
	testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
	print('Test Error = ' + str(testErr))

	#predictionAndLabel = testData.map(lambda p: (model.predict(p.features), p.label))
	accuracy = 1.0 * labelsAndPredictions.filter(lambda (x, v): x == v).count() / testData.count()
	#print (accuracy)
	print('model accuracy {}'.format(accuracy))

if __name__ == "__main__":
	
	sc = SparkContext()
	cluster(sc)
#train = data.zipWithIndex().filter(lambda (key,index) : index%3 == 0)