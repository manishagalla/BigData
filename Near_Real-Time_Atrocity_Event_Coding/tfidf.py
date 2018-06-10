from pyspark.mllib.feature import HashingTF, IDF
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.util import MLUtils
from pyspark.sql import SparkSession
sc = SparkContext()
spark=SparkSession.builder.getOrCreate()
# Load documents (one per line).
documents = sc.textFile("training_data.txt").map(lambda line: line.split("::"))
features = documents.map(lambda line: line[1])
labels = documents.map(lambda line: line[3])

hashingTF = HashingTF(numFeatures=300)
tf = hashingTF.transform(features)
tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
sparse_vectors=tfidf.collect()
_labels=labels.collect()
labelpoint_data=[]
for i,j in zip(_labels,sparse_vectors):
    labelpoint_data.append(LabeledPoint(i,j))
#model = SVMWithSGD.train(sc.parallelize(labelpoint_data), iterations=10)
MLUtils.saveAsLibSVMFile(sc.parallelize(labelpoint_data), 'tempFile')

result=spark.read.format("libsvm") .load("tempFile/part-00003")
for i in range(3):
    inputData = spark.read.format("libsvm") .load("tempFile/part-0000"+str(i))
    result=result.unionAll(inputData)

#(train, test) = result.randomSplit([0.8, 0.2])
(train, test1) = result.randomSplit([0.8, 0.2])
(train1, test) = train.randomSplit([0.8, 0.2])
lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)
ovr = OneVsRest(classifier=lr)
ovrModel = ovr.fit(train)
predictions = ovrModel.transform(train)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))