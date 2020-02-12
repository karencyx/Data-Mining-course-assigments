from pyspark import SparkContext, SparkConf
import json
import sys
file1=sys.argv[2]
file2=sys.argv[1]



fo = open(file1, "w")
conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf = conf)

reviewrdd=sc.textFile(file2)
#testrdd = sc.parallelize([1, 2, 3, 4, 5])
#A. the total number of reviews.
#def tojson(s):
#	return json.loads(s)
reviewrdd_json = reviewrdd.map(lambda r: json.loads(r))
aresult=reviewrdd_json.count()

#B. The number of reviews in a given year, y 
y=sys.argv[4]
#reviewinyear=reviewrdd_json.filter(lambda r: y in r['date'])
bresult=reviewrdd_json.filter(lambda r: y in r['date']).count()
#lines.filter(lambda line : "Spark" in line).count()
#print(reviewinyear.collect())

#C. The number of distinct users who have written the reviews (0.5pts)
#userrdd=reviewrdd_json.map(lambda r: r['user_id']).distinct()
cresult=reviewrdd_json.map(lambda r: r['user_id']).distinct().count()
#D. Top m users who have the largest number of reviews and its count (0.5pts)
m=int(sys.argv[5])
#pairrdd=reviewrdd_json.map(lambda r : (r['user_id'],1))
pairrdd2=reviewrdd_json.map(lambda r : (r['user_id'],1)).reduceByKey(lambda a,b : a+b)
dresult=pairrdd2.takeOrdered(m,lambda p: -p[1])

#E. Top n frequent words in the review text. The words should be in lower cases. The following punctuations
#import re
n=int(sys.argv[6])
#def transword(w):
#	punclist=['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
	


file3=sys.argv[3]
fo2=open(file3,'r')
stopwords=[]
for l in fo2:
	l=l.strip('\n')
	stopwords.append(l)

#textrdd=reviewrdd_json.map(lambda r: re.sub('[(, [, ,, ., !, ?, :, ;, ), \], -, \n]','',r['text'])).flatMap(lambda line: line.split(" ")).map(lambda w: w.lower())
textrdd=reviewrdd_json.map(lambda r: r['text']).flatMap(lambda line: line.split(" ")).map(lambda w: w.strip().strip('([,.!?:;])-\n')).filter(lambda w: w not in ['','(', '[', ',', '.', '!', '?', ':', ';', ']', ')']).map(lambda w: w.lower())
textrdd2=textrdd.filter(lambda w: w not in stopwords)
wordcount=textrdd2.map(lambda word: (word,1)).reduceByKey(lambda a, b : a + b)
#print(wordcount.collect())
eresult_ini=wordcount.takeOrdered(n,lambda p: -p[1])
#for i in range(len(eresult)):
#	eresult[i]=eresult[i][0]
eresult=[]
for x in eresult_ini:
	eresult.append(x[0])
#print(eresult)

#wordmap=reviewrdd_json.map(lambda r: )







#print(pairrdd.collect())
#print(reviewinyear.collect())
#print(cresult)


#for i in reviewrdd.collect():
#	fo.write(i)
#	fo.write('\n')



#write the file:
result={}
result['A']=aresult
result['B']=bresult
result['C']=cresult
result['D']=dresult
result['E']=eresult
json_str=json.dumps(result)
fo.write(json_str)