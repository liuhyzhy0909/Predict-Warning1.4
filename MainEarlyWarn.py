import Prediction as TP
from numpy import *
from pandas import *
import pandas as pd
import arrow
import SelectMySQL
from GetClassKs import GetClassKs
from decimal import *
import datetime

#程序说明：首次插入预测值，根据历史数据计算接下来6个月的预测值，并将预测值插入到mysql数据库中
#由于全院和各科室的


def GetData(sql, host, user, passwd, port, db):
    select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
    df = select.get_df(sql)
    if df.empty:
        df = df
    else:
        df_ks = df[0]
        del df[0]
        df.insert(1, 'dep', df_ks)
        df = df[df[2].notnull()]
        df = df[df[2] != 0]
        df[2] = df[2].apply(lambda x: float(x))

    return df

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

def Dic2Dic(dic):
    pre_list = {}
    for k, v in dic.items():
        pre_list[v.index.format("YYYY-MM-DD")[1]+'_'+k]= v[0]

    return pre_list

def GreDataforSQL(dic,index_id,type):
    data = []
    for k, v in dic.items():
        da, de = k.split('_')
        t = arrow.get(da).format("YYYY-MM-DD")
        data.append((de,index_id,type,t,v))
    return data

def GreDataforSQL_Degree(dic,index_id,type):
    data = []
    for k, v in dic.items():
        da, de = k.split('_')
        t = arrow.get(da)
        y = t.year
        m = t.month
        d = t.day
        data.append((de,index_id,type,datetime.date(y, m, d),Decimal.from_float(v)))
    return data

def f(x):
    return x[1].strftime("%Y-%m-%d") +'_'+x['dep']

def Df2Dic(df,val_np,target_percent):
    df_tar = df
    df_tar['dd'] = df_tar.apply(lambda x: f(x),axis = 1)
    #df_tar = df_tar.reset_index()
    del df_tar['dep']
    del df_tar[1]
    df_tar = df_tar.set_index('dd')
    dic_tar = df_tar[2].to_dict()
    dic_tar_lim = {}
    for k, v in dic_tar.items():
        dic_tar_lim[k] = v * (1 + (-val_np) * target_percent)
    return dic_tar,dic_tar_lim

#将所有科室的门诊量dataframe转成按科室分组的list，list每一个元素对应一个科室门诊量dataframe
def get_ks_dataframes(al_df):
    ks_dfs = {}
    ks_list = set(al_df['dep'])
    for k in ks_list:
        ks_df = al_df[al_df['dep'] == k]
        k = list(set(ks_df['dep']))[0]
        ks_df.pop('dep')
        v = ks_df.sort_index()
        ks_dfs[k] = v
    return ks_dfs

def one_by_one_sumdataframe(s_df):
    s = len(s_df)
    df_l = list(s_df)
    s_l = list()
    for i in range(s):
        s_l.append(sum(df_l[0:i+1]))
    return s_l

#数据库信息
host = "59.110.68.181"
user = "hozedata"
passwd = "Hozedata@123"
port = 3306
db = "FMS_NEW"
#数据库字段信息
index_id = 10200
type_id = 0#月预警
type_id_q = 5#年累计预警
#输入变量
cycle = 12
pre_length = 1
val_np = 1  # 指标正负向标志
target_percent = 0.1  # 目标值上下限比例

#获取数据
sql = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID="+str(index_id)+" and type=0 order by ACCOUNT_DATE"#历史月度真实数据
df = GetData(sql, host, user, passwd, port, db)  # 获取各科室历史数据，计算预测值的样本数据
CLASS = GetClassKs(df)
warning_time = CLASS.TimeFun()[0]  # 事实数据截止时间
pre_atime = arrow.get(warning_time, "YYYY-MM-DD").shift(months=1)
pre_time = pre_atime.format("YYYY-MM-DD")

ks_dfs_new, ks_dfs_old, ks_dfs_lack = CLASS.get_ks_classity()  # 科室分类
pre_dic = GetPre(ks_dfs_old, warning_time, pre_time, pre_length)  # 预测结果
dic_pre = Dic2Dic(pre_dic)

#月度预警
sql_tar = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID= "+str(index_id)+" and type=8 and ACCOUNT_DATE=\'"+pre_time+"\' order by ACCOUNT_DATE"#预警月份的目标值
df_tar = GetData(sql_tar, host, user, passwd, port, db)
dic_tar,dic_tar_lim = Df2Dic(df_tar,val_np,target_percent)
key_inter = set(dic_tar.keys())&set(dic_pre.keys())
dic_warning = {}
dic_tar_degree = {}
for k in key_inter:
    dic_tar_degree[k] = dic_pre[k]/dic_tar[k]
    if dic_pre[k]<dic_tar_lim[k]:
        dic_warning[k] = 1
    else:
        dic_warning[k] = 0
'''
data = GreDataforSQL(dic_warning,index_id,type_id)  # 预警结果转换成可放入数据库的格式
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.UpdateEWD(data)
'''
#插入月完成度数据到历史表
#type为10
'''
degree_data = GreDataforSQL_Degree(dic_tar_degree,index_id,10)
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.InsertTarDegreeD(degree_data)  # 将月完成度数据插入数据库
'''

#月度预警累加值大于目标值，更新预警标识为0,按天更新
sql_month_sum = "SELECT "#按天表里月累计值，type 为0
#需要指标，科室，日期，value，转成日期加科室的字典，查该字典与目标值大小，得出预警标识


#年累计预警
#预警年份历史数据与当前月预测数据之和与本年度截止到当前月的累积目标值比较计算预警标识和累积完成度
#目标与真实值累加后的字典
sum_year = pre_atime.year
#目标累积值
sql_tar_all = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID= "+str(index_id)+" and type=8 and ACCOUNT_DATE like \'%"+str(sum_year)+"%\' order by ACCOUNT_DATE"
df_tar_all = GetData(sql_tar_all, host, user, passwd, port, db)
tar_all_dic = get_ks_dataframes(df_tar_all)
tar_sum_dic = {}
tar_sum_lim_dic = {}
for k in tar_all_dic:
    tar_sum_dic_k={}
    tar_sum_lim_dic_k = {}
    k_df = tar_all_dic[k]
    depart_col = []
    for i in range(len(k_df)):
        depart_col.append(k)
    k_df['department'] = pd.Series(depart_col, index=k_df.index)
    k_df['sum_tar'] = pd.Series(one_by_one_sumdataframe(k_df[2]),index=k_df.index)
    del k_df[2]
    k_df.columns = [1,'dep',2]
    tar_sum_dic_k,tar_sum_lim_dic_k = Df2Dic(k_df,val_np,0.05)
    tar_sum_lim_dic = dict(tar_sum_lim_dic,**tar_sum_lim_dic_k)
    tar_sum_dic = dict(tar_sum_dic,**tar_sum_dic_k)

#预警年份历史数据之和加上当前月预测值
sql_data_all = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID= "+str(index_id)+" and type=0 and ACCOUNT_DATE like \'%"+str(sum_year)+"%\' order by ACCOUNT_DATE"
df_data_all = GetData(sql_data_all, host, user, passwd, port, db)
if df_data_all.empty:
    dic_warning_sum = dic_warning
    dic_tar_degree_sum = dic_tar_degree
else:
    data_all_dic = get_ks_dataframes(df_data_all)
    data_sum_dic = {}
    for k in data_all_dic:
        data_sum_dic_k = {}
        k_df = data_all_dic[k]
        depart_col = []
        for i in range(len(k_df)):
            depart_col.append(k)
        k_df['department'] = pd.Series(depart_col, index=k_df.index)
        k_df['sum_tar'] = pd.Series(one_by_one_sumdataframe(k_df[2]), index=k_df.index)
        del k_df[2]
        k_df.columns = [1, 'dep', 2]
        data_sum_dic_k = Df2Dic(k_df, val_np, 0.05)[0]
        data_sum = max(data_sum_dic_k.items(), key=lambda x: x[1])
        data_sum = {data_sum[0].split('_')[1]: data_sum[1]}
        data_sum_dic = dict(data_sum_dic, **data_sum)

    dic_pre_for_sum = {}
    for k in dic_pre:
        v = dic_pre[k]
        k = k.split('_')[1]
        dic_pre_for_sum[k] = v

    for_sum_key = set(data_sum_dic.keys()) & set(dic_pre_for_sum.keys())
    data_pre_sum = {}
    for k in for_sum_key:
        data_pre_sum[k] = data_sum_dic[k] + dic_pre_for_sum[k]
    data_pre_sum_date = {}
    for k in data_pre_sum:
        v = data_pre_sum[k]
        k = pre_time + '_' + k
        data_pre_sum_date[k] = v

    key_inter_sum = set(tar_sum_lim_dic.keys()) & set(data_pre_sum_date.keys())
    dic_warning_sum = {}
    dic_tar_degree_sum = {}
    for k in key_inter_sum:
        dic_tar_degree_sum[k] = data_pre_sum_date[k]/tar_sum_dic[k]
        if data_pre_sum_date[k] < tar_sum_lim_dic[k]:
            dic_warning_sum[k] = 1
        else:
            dic_warning_sum[k] = 0
#插入累计预警标识
'''
data_s = GreDataforSQL(dic_warning_sum, index_id, type_id_q)  # 预警结果转换成可放入数据库的格式
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.UpdateEWD(data_s)
'''
#插入年累计完成度到历史表
#type 为11
'''
degree_data_sum = GreDataforSQL_Degree(dic_tar_degree_sum,index_id,11)
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.InsertTarDegreeD(degree_data_sum)  # 将预测数据插入数据库
'''
#预警报告
sql_dep = "SELECT DEPT_ID,DEPT_NAME FROM DICT_DEPT_HOSPITAL"
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
df_dep = select.get_df(sql_dep).set_index(0).to_dict()[1]#指标字典
sql_index = "select KEEPING_INDEX_ID,KEEPING_INDEX_NAME from COST_KEEPING_INDEX"
df_index = select.get_df(sql_index).set_index(0).to_dict()[1]#科室字典


pre_time_desc = pre_atime.format("YYYY年MM月")
q_k = pre_time+'_'+'1007'
h_degree = str(round(dic_tar_degree[q_k]*100,2))
h_degree_sum = str(round(dic_tar_degree_sum[q_k]*100,2))
index_id_desc = df_index[index_id]
dep_degree = list({k:v for k,v in dic_warning.items() if v ==1}.keys())
dep_degree = [x.split('_')[1] for x in dep_degree]#删除全院，待加入
dep_degree_han = []
for x in dep_degree:
    dep_degree_han.append(df_dep[int(x)])
dep_degree_s = str(dep_degree_han).replace('[','').replace(']','').replace('\'','')
dep_degree_sum = list({k:v for k,v in dic_warning_sum.items() if v ==1}.keys())
dep_degree_sum = [x.split('_')[1] for x in dep_degree_sum]#删除全院，待加入
dep_degree_sum_han = []
for x in dep_degree_sum:
    dep_degree_sum_han.append(df_dep[int(x)])
dep_degree_sum_s = str(dep_degree_sum_han).replace('[','').replace(']','').replace('\'','')


lsq = "<a href="">"
lsh = "</a>"

depart_sum_str = ""
for i in range(0,len(dep_degree_sum_han)-2):
    depart_sum_str = depart_sum_str +lsq+dep_degree_sum_han[i]+"，"+lsh
depart_sum_str = depart_sum_str+lsq+dep_degree_sum_han[len(dep_degree_sum_han)-1]+lsh

depart_str = ""
for i in range(0,len(dep_degree_han)-2):
    depart_str = depart_str +lsq+dep_degree_han[i]+"，"+lsh
depart_str=depart_str+lsq+dep_degree_han[len(dep_degree_han)-1]+lsh

warning_desc = "<p>1、预计到"+ pre_time_desc +"，"+index_id_desc+"完成目标值的<span class=\\\'red\\\'>"+h_degree+"%</span>，累计完成目标值的<span class=\\\'red\\\'>"+h_degree_sum+"%</span>。</p><p>2、预计到"+pre_time_desc+"，预计完成不足目标值<span class=\\\'red\\\'>90%</span>的科室有"+depart_str+"；预计累计完成不足目标值<span class=\\\'red\\\'>95%</span>的科室有"+depart_sum_str+"。</p>"
warning_data = [(warning_desc,1,'1013',datetime.date(2018,1,1),1,0)]#预警描述、指标名、全院科室、时间、预警标识、值类型
print(warning_desc)
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.UpdateEWDesc(warning_data)