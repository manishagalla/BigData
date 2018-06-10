bs = sc.textFile('/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW2-Part B/dataset/business.csv')
bsFil = bs.filter(lambda x:"Stanford" in x)
bsMap = bsFil.map(lambda x:(x.split("::")[0],1))
bsMap.collect()
rw= sc.textFile('/Users/manishagalla/Documents/UTD/Spring 17/Big Data/HW2-Part B/dataset/review.csv')
rwSplit = rw.map(lambda x:(x.split("::")[2],[x.split("::")[1],x.split("::")[3]]))
rwSplit.collect()
join=bsMap.join(rwSplit)
for i in join.collect():
	print i[1][1][0], i[1][1][1]