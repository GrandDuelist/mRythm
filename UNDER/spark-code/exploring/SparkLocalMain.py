# from SparkMain import *
#
# if __name__ == '__main__':
#     taxi = TaxiMain()
#     taxi.travelTimeOneDayByHours()
from pyspark import SparkContext

sc = SparkContext('local')
doc = sc.parallelize([['a','b','c'],['b','d','d']])
words = doc.flatMap(lambda d:d).distinct().collect()
word_dict = {w:i for w,i in zip(words,range(len(words)))}
word_dict_b = sc.broadcast(word_dict)

def wordCountPerDoc(d):
    dict={}
    wd = word_dict_b.value
    for w in d:
        if dict.has_key(wd[w]):
            dict[wd[w]] +=1
        else:
            dict[wd[w]] = 1
    return dict
print doc.map(wordCountPerDoc).collect()
print "successful!"

