# spark编程 中文分词 统计词频 sparkSQL语句查询
# 使用两种方法过滤
# 1.使用RDD的filter方法过滤
# 2.使用SQL查询语句查询

from __future__ import print_function
import sys
import jieba
from operator import add
from pyspark.sql import SparkSession

#中文分词
def chiwordsplit(x):
    seg=jieba.cut(x)
    #停词文件
    f=open("stopwords.txt",encoding="utf8")
    stopwords=f.readlines()
    for i in range(0,len(stopwords)):
        stopwords[i]=stopwords[i].replace("\n","")
    words=[]
    for word in seg:
        if word not in stopwords and word != " ":
            words.append(word)  
    return words

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: wordcount <input> <output> <k>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()
    k=int(sys.argv[3]) 
    #MapReduce
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: chiwordsplit(x)) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)\
                  .sortBy(lambda x: -x[1])
    #使用RDD的filter方法过滤词频在k次以下的单词             
    Filteredcounts = counts.filter(lambda x: x[1] >= k)              
    output = Filteredcounts.collect()
    
    f=open(sys.argv[2],'w')
    for (word, count) in output:
        print("%s: %i" % (word, count))
        f.write(word+" : "+str(count)+"\n")
    f.close()
    
    
    #SQL 部分
    #RDD创建DataFrame
    df = spark.createDataFrame(counts.collect(), ['word', 'count'])
    df.createOrReplaceTempView("wordlist")    
    #SQL Spark SQL查询统计结果中次数超过k次的词语
    df2 = spark.sql("SELECT * from wordlist WHERE count > "+str(k))
    #把查询结果输出到文件
    SQLoutput=df2.collect()
    f=open("SQL"+sys.argv[2],'w')
    for (word, count) in SQLoutput:
        print("%s: %i" % (word, count))
        f.write(word+" : "+str(count)+"\n")
    f.close()
    
    spark.stop()
