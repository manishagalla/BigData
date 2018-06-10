graph= sc.textFile("hdfs://localhost:9000/HW2-partB/graph.txt")
targets=graph.map(lambda line:tuple(line.split("\t")[1:]))
addWeights=targets.reduceByKey(lambda x,y:int(x)+int(y))
sorted=addWeights.sortBy(lambda x:x[0]).collect()
for i in sorted:
	print i[0]," ",i[1]