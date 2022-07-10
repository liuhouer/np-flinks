#coding:utf-8

import findspark
findspark.init()

from numpy import array
from math import sqrt
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans, KMeansModel
import pandas as pd

if __name__ == "__main__":
    sc = SparkContext(appName="KMeansExample",master='local')  # SparkContext


path='C:\\Users\\Bruce\\Downloads\\letter-recognition.data' #数据集路径
Cname = ['字母','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16'] #设置列名称
data=pd.read_csv(path,header=None,names=Cname)
data.index.name='index'     #datadrame结构的行索引与列索引名字
data.columns.name='columns'
print(data)

X=data[data.columns[1:17]]  #提取特征值，不需要第一列的字母值

print(X)

# 读取并处理数据
ScData = sc.parallelize(X)

parsedData = ScData.map(lambda line: array([float(x) for x in line.split(' ')]))


# 训练数据
print(parsedData.collect())

clusters = KMeans.train(parsedData, k=2, maxIterations=10,
                        runs=10, initializationMode="random")



#聚类结果
def sort(point):
    return clusters.predict(point)
clusters_result = parsedData.map(sort)
# Save and load model
# $example off$
print('聚类结果：')
print(clusters_result.collect())

sc.stop()