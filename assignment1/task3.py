from pyspark import SparkContext, SparkConf
import json
import sys
file1=sys.argv[1]
file2=sys.argv[2]
ptype=sys.argv[3]
conf = SparkConf().setAppName("test").setMaster("local[*]")
sc = SparkContext(conf = conf)
reviewrdd=sc.textFile(file1)

n_partition=int(sys.argv[4])
#if ptype=='customized':
#	reviewrdd=sc.textFile(file1,n_partition)
#	rdd_n=n_partition
#	rdd_d=reviewrdd.glom().map(len).collect()
#else:
#	reviewrdd=sc.textFile(file1)
#	rdd_n=reviewrdd.getNumPartitions()
#	rdd_d=reviewrdd.glom().map(len).collect()
n=int(sys.argv[5])
def tojson(s):
	return json.loads(s)
reviewrdd_json=reviewrdd.map(tojson)
pairrdd=reviewrdd_json.map(lambda r : (r['business_id'],1))
if ptype=='customized':
	pairrdd=pairrdd.partitionBy(n_partition)
	rdd_n=n_partition
else:
	rdd_n=pairrdd.getNumPartitions()
rdd_d=pairrdd.glom().map(len).collect()
resultn=pairrdd.reduceByKey(lambda a,b : a+b).filter(lambda p: p[1]>n).map(lambda p: [p[0],p[1]]).collect()
#print(resultn)






fo = open(file2, "w")
result={}
result['n_partitions']=rdd_n
result['n_items']=rdd_d
result['result']=resultn
json_str=json.dumps(result)
fo.write(json_str)

#print(d)
#print(n)
#print(len(d))