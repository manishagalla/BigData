bs = sc.textFile('/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW2-Part B/dataset/business.csv')
rw= sc.textFile('/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW2-Part B/dataset/review.csv')
rwSplit = rw.map(lambda x:(x.split("::")[2],x.split("::")[3]))
group=map((lambda (rwSplit,y): (rwSplit, list(y))), sorted(rwSplit.groupByKey().collect()))
avgList=[]
for i in group:
	k =[float(j) for j in i[1]]
	avg = sum(k)/len(k)
	avgList.append((i[0],avg))
avgRDD=sc.parallelize(avgList)
bsMap = bs.map(lambda x:(x.split("::")[0],(x.split("::")[1],x.split("::")[2])))
join=bsMap.join(avgRDD).distinct()
sortedList=join.sortBy(lambda x: x[1][1],ascending=False).collect()[:10]
for i in sortedList:
	bid=i[0]
	avgRating=i[1][1]
	address=i[1][0][0]
	catg = i[1][0][1]
	print bid,address,catg,avgRating

