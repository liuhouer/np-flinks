from sklearn.linear_model import LogisticRegression
import pandas as pd
from sklearn.model_selection import train_test_split

path='C:\\Users\\Bruce\\Downloads\\letter-recognition.data' #数据集路径
Cname = ['字母','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16'] #设置列名称
data=pd.read_csv(path,header=None,names=Cname)
data.index.name='index'     #datadrame结构的行索引与列索引名字
data.columns.name='columns'
print(data)

X=data[data.columns[1:17]]  #提取特征值，不需要第一列的字母值
x_train,x_test,y_train,y_test=train_test_split(X,data["字母"],train_size=0.8,random_state=77)
#设置最大迭代次数为4000，默认为1000.不更改会出现警告提示
lr=LogisticRegression(max_iter=4000)
clm=lr.fit(x_train,y_train)  #输入训练集
print('对测试集的预测结果：')
#输出预测结果、预测结果的结构类型及尺寸
print(clm.predict(x_test),type(clm.predict(x_test)),clm.predict(x_test).shape)
#
print('模型评分：'+ str(clm.score(x_test,y_test))) #用决定系数来打分