val graph = sc.textFile("hdfs://localhost:9000/test/graph.txt")
val weights = graph.map(line => (line.split("::")(1),line.split("::")(2).toInt)).reduceByKey(_+_)
val result : List[(String,Int)] = weights.collect().toList
val x = collection.mutable.ListMap(result.sortWith(_._1>_._1):_*)
x.foreach(a=>println(a))