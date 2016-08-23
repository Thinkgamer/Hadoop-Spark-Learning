#-*-coding:utf-8-*-

from pyspark import SparkConf, SparkContext

import jieba

def split(line):
    word_list = jieba.cut(line.strip().split("\t")[1])  #进行中文分词
    ls =[]
    for word in word_list:
        if len(word)>1:      #过滤掉单音节词
            ls.append(word)
    return ls

def combine(line):
    result = ""
    result +=line[0]+"\t"+str(line[1])   #让数字在前，方便统计
    return result

def main(sc):
    text = sc.textFile("/file/douban_movie_data/movie_summary.txt")
    word_list = text.map(split).collect() #保存为列表

    count = sc.parallelize(word_list[0]) #返回列表中的第一个元素
    results = count.map(lambda w:(w,1)).reduceByKey(lambda x,y:x+y).map(combine).sortByKey().saveAsTextFile("/file/douban_movie_data/result")
    print "succeed"

if __name__=="__main__":
    conf = SparkConf().setAppName("wordSplit")
    conf.setMaster("local")
    sc= SparkContext(conf = conf)
    main(sc)