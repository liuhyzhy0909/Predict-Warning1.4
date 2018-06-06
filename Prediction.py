from __future__ import division
import pandas as pd
from numpy import *
from numpy import array
from numpy import zeros
import numpy as np
from scipy.optimize import least_squares

def DataNorm(dataset):
    # dataset为输入指标集数据，数据类型为numpy.array
    minvals = dataset.min(0)
    maxvals = dataset.max(0)
    ranges = maxvals - minvals
    normdataset = zeros(shape(dataset))
    m, n = dataset.shape
    normdataset = dataset - tile(minvals, (m, 1))
    normdataset0 = normdataset / tile(ranges, (m, 1))

    return normdataset0

# Holtwinters类
class Holtwinters(object):
    def __init__(self, data):
        self.data = data
        # 误差函数。

    def RMSE(self, params, *args):
        Y = args[0]
        type = args[1]
        # rmse = 0
        mre = 0
        if type == 'linear':
            alpha, beta = params
            a = [Y[0]]
            b = [Y[1] - Y[0]]
            y = [a[0] + b[0]]
            for i in range(len(Y)):
                a.append(alpha * Y[i] + (1 - alpha) * (a[i] + b[i]))
                b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
                y.append(a[i + 1] + b[i + 1])
        else:
            alpha, beta, gamma = params
            m = args[2]
            a = [sum(Y[0:m]) / float(m)]
            b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
            if type == 'additive':
                s = [Y[i] - a[0] for i in range(m)]
                y = [a[0] + b[0] + s[0]]
                for i in range(len(Y)):
                    a.append(alpha * (Y[i] - s[i]) + (1 - alpha) * (a[i] + b[i]))
                    b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
                    s.append(gamma * (Y[i] - a[i] - b[i]) + (1 - gamma) * s[i])
                    y.append(a[i + 1] + b[i + 1] + s[i + 1])
            elif type == 'multiplicative':
                s = [Y[i] / a[0] for i in range(m)]
                y = [(a[0] + b[0]) * s[0]]
                for i in range(len(Y)):
                    a.append(alpha * (Y[i] / s[i]) + (1 - alpha) * (a[i] + b[i]))
                    b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
                    s.append(gamma * (Y[i] / (a[i] + b[i])) + (1 - gamma) * s[i])
                    y.append((a[i + 1] + b[i + 1]) * s[i + 1])
            else:
                print('Type must be either linear, additive or multiplicative')
        # rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y, y[:-1])]) / len(Y))
        mre = sum([abs(m - n) / m for m, n in zip(Y, y[:-1])]) / len(Y)
        return mre

    def additive(self, k, n, m, fc, alpha=None, beta=None, gamma=None):
        Y = self.data[-n:, k]
        Y = Y.tolist()
        if (alpha == None or beta == None or gamma == None):
            initial_values = array([0, 1, 0.9])  # [0, 1, 0.8] [0, 1, 0.9]
            type = 'additive'
            parameters = least_squares(self.RMSE, x0=initial_values, args=(Y, type, m), bounds=(0, 1), method='trf')
            param1 = parameters.x.tolist()
            alpha, beta, gamma = param1
        a = [sum(Y[0:m]) / float(m)]
        b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
        s = [Y[i] - a[0] for i in range(m)]
        y = [a[0] + b[0] + s[0]]
        # rmse = 0
        mre = 0
        for i in range(len(Y) + fc):
            if i == len(Y):
                Y.append(a[-1] + b[-1] + s[-m])
            a.append(alpha * (Y[i] - s[i]) + (1 - alpha) * (a[i] + b[i]))
            b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
            s.append(gamma * (Y[i] - a[i] - b[i]) + (1 - gamma) * s[i])
            y.append(a[i + 1] + b[i + 1] + s[i + 1])
        # rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))
        mre = sum([abs(m - n) / m for m, n in zip(Y, y[:-1])]) / len(Y)
        return Y[-fc:], alpha, beta, gamma, mre

    def multiplicative(self, k, n, m, fc, alpha=None, beta=None, gamma=None):
        Y = self.data[-n:, k]
        Y = Y.tolist()
        if (alpha == None or beta == None or gamma == None):
            initial_values = array([0, 0.9, 0.9])
            type = 'multiplicative'
            parameters = least_squares(self.RMSE, x0=initial_values, args=(Y, type, m), bounds=(0, 1))
            param1 = parameters.x.tolist()
            alpha, beta, gamma = param1
        a = [sum(Y[0:m]) / float(m)]
        b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
        s = [Y[i] / a[0] for i in range(m)]
        y = [(a[0] + b[0]) * s[0]]
        # rmse = 0
        mre = 0
        for i in range(len(Y) + fc):
            if i == len(Y):
                Y.append((a[-1] + b[-1]) * s[-m])
            a.append(alpha * (Y[i] / s[i]) + (1 - alpha) * (a[i] + b[i]))
            b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
            s.append(gamma * (Y[i] / (a[i] + b[i])) + (1 - gamma) * s[i])
            y.append((a[i + 1] + b[i + 1]) * s[i + 1])

        # rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))
        mre = sum([abs(m - n) / m for m, n in zip(Y, y[:-1])]) / len(Y)
        return Y[-fc:], alpha, beta, gamma, mre

    #又加了一个评估函数，用于评估数据的规律性，如果大于一个比较大的阈值，比如0.6，说明周期性比较好，推断预测的效果也比较好。
    def Evaluating(self, inputdata, k=None):
        if k is None:
            k = 12
        m, n = inputdata.shape
        resmat = zeros([n, 1])
        mark = zeros([k, int(m / k)])
        for i in range(n):
            for j in range(int(m / k)):
                mark[:, j] = inputdata[j * k:(j + 1) * k, i]
                mark = DataNorm(mark)
                person = np.corrcoef(mark, rowvar=0)
                avper = np.average(person)
            resmat[i, 0] = avper
        return resmat

def Dataread(filename):
    Dataori = pd.read_csv(filename,encoding='gb2312')
    DataValue = Dataori.values[:,1:] #去掉第一列时间
    DataValue = DataValue.astype(double) #将数据转化为double型
    return DataValue
'''
data = Dataread('quanyuan_zongbiao - 副本.csv')
data2 = data[::-1]
k = 0#k的设置代表不同的指标
n = 48 #n的设置代表训练样本个数
holt=Holtwinters(data2)
pe = 12 #数据周期,月份数据周期为12.
pre_length = 12 #预测序列长度，若为12，代表预测12个月。
data_pre = holt.multiplicative(k,n, pe, pre_length) #乘法模型预测
data_evaluation = holt.Evaluating(data2)
'''