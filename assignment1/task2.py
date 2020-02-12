from pyspark import SparkContext, SparkConf
import json
import sys
reviewfile=sys.argv[1]
businessfile=sys.argv[2]
outputfile=sys.argv[3]
sp=sys.argv[4]
ifspark=4
if(sp=='spark'):
	ifspark=1
else:
	ifspark=0
#ifspark=0
#outputfile=''
n=int(sys.argv[5])
if ifspark==0:
	#businesslist=[]
	businessstar={}
	fo=open(reviewfile,'r')
	for l in fo:
	#print(l)
		d=json.loads(l)
		tempid=d['business_id']
		#print(tempid)
		tempstar=d['stars']
		if tempid not in businessstar:
			#businesslist.append(tempid)
			businessstar[tempid]=[tempstar,1]
		else:
			businessstar[tempid][0]=businessstar[tempid][0]+tempstar
			businessstar[tempid][1]=businessstar[tempid][1]+1

	#print('1ok')
	fo2=open(businessfile,'r')
	#dictlist=[]
	dictbus={}
	for l in fo2:
		d=json.loads(l)
		tempbusiness=d['business_id']
		catestring=d['categories']
		if catestring is not None:
			tempcat=catestring.split(', ')
		else:
			continue
		if tempbusiness in businessstar:
			for c in tempcat:
				if c in dictbus:
					dictbus[c].append(tempbusiness)
				else:
					dictbus[c]=[tempbusiness]
	dictstar={}
	for c in dictbus:
		dictstar[c]=[0,0]
		for b in dictbus[c]:
			dictstar[c][0]=dictstar[c][0]+businessstar[b][0]
			dictstar[c][1]=dictstar[c][1]+businessstar[b][1]
		#print(c)
		#print(dictstar[c])
		dictstar[c]=dictstar[c][0]/dictstar[c][1]
	result=sorted(dictstar.items(), key=lambda kv: (-kv[1], kv[0]))[0:n]
	businessstar={}
	fo=open(reviewfile,'r')
	for l in fo:
	#print(l)
		d=json.loads(l)
		tempid=d['business_id']
		#print(tempid)
		tempstar=d['stars']
		if tempid not in businessstar:
			#businesslist.append(tempid)
			businessstar[tempid]=[tempstar,1]
		else:
			businessstar[tempid][0]=businessstar[tempid][0]+tempstar
			businessstar[tempid][1]=businessstar[tempid][1]+1
	#print('3ok')
	#print(result)
if ifspark==1:
	conf = SparkConf().setAppName("test2").setMaster("local[*]")
	sc = SparkContext(conf = conf)
	reviewrdd=sc.textFile(reviewfile)
	businessrdd=sc.textFile(businessfile)
	def tojson(s):
		return json.loads(s)
	reviewrdd_json = reviewrdd.map(tojson)
	businessrdd_json=businessrdd.map(tojson)
	businessrdd2=businessrdd_json.filter(lambda b: b['categories'] is not None)
	#def getpair(b):
	#	dl=b['categories'].split(', ')
	#	for i in range(len(dl)):
	#		dl[i]=(b['business_id'],dl[i])
	#	return dl
	pairrdd=businessrdd2.flatMap(lambda b: [(b['business_id'],category) for category in b['categories'].split(', ')])

	pairrdd2=reviewrdd_json.map(lambda r: (r['business_id'],[r['stars'],1])).reduceByKey(lambda a, b : [a[0]+b[0],a[1]+b[1]])
	joined = pairrdd.join(pairrdd2)
	finalpair=joined.map(lambda j: j[1]).reduceByKey(lambda a, b : [a[0]+b[0],a[1]+b[1]]).map(lambda p: [p[0],p[1][0]/p[1][1]])
	result=finalpair.takeOrdered(n,lambda p: (-p[1],p[0]))


	#print(result)
	#dbpairrdd=businessrdd_json.map()
	#print(pairrdd.collect())
	#pairrdd2=reviewrdd_json


out={'result':result}
json_str=json.dumps(out)
fo3 = open(outputfile, "w")
fo3.write(json_str)





	#for c in dictstar:
	#	print(c)
	#	print(dictstar[c])
	
			
