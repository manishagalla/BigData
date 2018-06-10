val business_data = sc.textFile("/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW2-Part B/dataset/business.csv")
val review_data = sc.textFile("/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW2-Part B/dataset/review.csv")
val b_data = business_data.map(line => (line.split("::")(0),line.split("::")(1),line.split("::")(2)))
val tempData = review_data.map(line => (line.split("::")(2),line.split("::")(3).toFloat))
val business_group = b_data.map(t=>(t._1,t._2+" -- "+t._3))
val groupData = tempData.groupBy(_._1).mapValues(_.map(_._2))
def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
   num.toDouble( ts.sum ) / ts.size
}
val avgData = groupData.map(t => (t._1,average(t._2)))
val join_data = avgData.join(business_group)
val result : List[(String,(Double,String))] = join_data.collect().toList
val x = collection.mutable.ListMap(result.sortWith(_._2._1<_._2._1):_*).take(10)
println
println
x.foreach(a=>(println(a)))