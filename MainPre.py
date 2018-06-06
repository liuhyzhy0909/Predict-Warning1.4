import Prediction as TP
from numpy import *
from pandas import *
import pandas as pd
import arrow
import SelectMySQL
from GetClassKs import GetClassKs
from statsmodels.tsa.stattools import adfuller
from decimal import *
import datetime

#程序说明：首次插入预测值，根据历史数据计算接下来6个月的预测值，并将预测值插入到mysql数据库中
#由于全院和各科室的

def GetData(sql, host, user, passwd, port, db):
    select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
    df = select.get_df(sql)
    df_ks = df[0]
    del df[0]
    df.insert(1,'dep',df_ks)
    df = df[df[2].notnull()]
    df = df[df[2] != 0]
    df[2] =df[2].apply(lambda x:float(x))
    return df

def testStationarity(ts):
    dftest = adfuller(ts)
    # 对上述函数求得的值进行语义描述
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    return dfoutput

def GetPre(ks_dfs_old,warning_time,pre_time,pre_length):
    # 预测
    pre_dic = {}
    no_pre_depart = []
    ks_dfs_old = CLASS.FillIndex(ks_dfs_old, '1/1/2015', 36)
    pre_Index = pd.date_range(pre_time, periods=pre_length, freq='MS', name='date')

    for k in ks_dfs_old:
        df_k = ks_dfs_old[k]
        df_for_pre = df_k[:warning_time]
        history_data_len = len(df_for_pre)
        df_pre = df_for_pre.tail(history_data_len - history_data_len % 12)
        for_pre = np.array(df_pre)
        # p = testStationarity(for_pre.flatten())[1]
        holt = TP.Holtwinters(for_pre)
        data_evaluation = holt.Evaluating(for_pre)[0][0]
        # print(k,data_evaluation)
        if data_evaluation > 0.35:
            data_preall = holt.multiplicative(0, history_data_len - history_data_len % 12, cycle, pre_length)
            data_pre = data_preall[0]  # 乘法模型预测
            data_pre = pd.DataFrame(data_pre,index=pre_Index)
            #data_pre['d'] = data_pre[0].index()
            pre_dic[k] = data_pre[0]
        else:
            no_pre_depart.append(k)
    return pre_dic

def GreDataforSQL(pre_dic,index_id):
    data = []
    for k in pre_dic:
        df_k = pre_dic[k]
        k_dic = dict(df_k)
        for x, v in k_dic.items():
            t = arrow.get(x)
            y = t.year
            m = t.month
            d = t.day
            data.append((index_id, k, datetime.date(y, m, d), Decimal.from_float(v), type_id))
            # print(index_id,k,x,v,type_id)
    return data

#数据库信息
host = "59.110.68.181"
user = "hozedata"
passwd = "Hozedata@123"
port = 3306
db = "FMS_NEW"
#数据库字段信息
index_id_list = [10200,10301,10302,10304,10305,10306,10314]
type_id = 12
#输入变量
cycle = 12
pre_length = 6
data_list = []
for i in index_id_list:
    index_id = i
    sql = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID="+str(index_id)+" and type=0 order by ACCOUNT_DATE"
    df = GetData(sql, host, user, passwd, port, db)  # 获取各科室历史数据，计算预测值的样本数据
    CLASS = GetClassKs(df)
    warning_time = CLASS.TimeFun()[0]  # 事实数据截止时间
    pre_time = arrow.get(warning_time, "YYYY-MM-DD").shift(months=1).format("YYYY-MM-DD")
    ks_dfs_new, ks_dfs_old, ks_dfs_lack = CLASS.get_ks_classity()  # 科室分类
    pre_dic = GetPre(ks_dfs_old, warning_time, pre_time, pre_length)  # 预测结果

    data = GreDataforSQL(pre_dic, index_id)  # 预测结果转换成可放入数据库的格式
    data_list = data_list + data

select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.InsertPreD(data_list)  # 将预测数据插入数据库
'''
#获取数据
index_id = 10314
sql = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID="+str(index_id)+" and type=0 order by ACCOUNT_DATE"
df = GetData(sql, host, user, passwd, port, db)  # 获取各科室历史数据，计算预测值的样本数据
CLASS = GetClassKs(df)
warning_time = CLASS.TimeFun()[0]  # 事实数据截止时间
pre_time = arrow.get(warning_time, "YYYY-MM-DD").shift(months=1).format("YYYY-MM-DD")
ks_dfs_new, ks_dfs_old, ks_dfs_lack = CLASS.get_ks_classity()  # 科室分类
pre_dic = GetPre(ks_dfs_old, warning_time, pre_time, pre_length)  # 预测结果

data = GreDataforSQL(pre_dic, index_id)  # 预测结果转换成可放入数据库的格式
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.InsertPreD(data)  # 将预测数据插入数据库
'''