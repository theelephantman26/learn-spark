from pyspark import SparkConf, SparkContext
import re
import collections

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def selectWordsOnly(text):
    return re.compile(r"\W+", re.UNICODE).split(text.lower())

input = sc.textFile("/Users/bhalchandranaik/Desktop/projects/learn-spark/data/Book")
words = input.flatMap(selectWordsOnly)
wordCounts = words.countByValue()
sortedWordCounts = collections.OrderedDict(sorted(wordCounts.items(), key=lambda x: x[1], reverse=True))
for word, count in sortedWordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
