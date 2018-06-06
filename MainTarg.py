from numpy import *
import TimeDecom as TimeDe
from pandas import *
import SelectMySQL
from GetClassKs import GetClassKs
from decimal import *
import datetime

#根据SQL从数据库取数并将取到的数转成df
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

# 将目标分解到月
def GetDepartmentTarget(cycle,df_k, target):
    TD = TimeDe.TimeDecom(df_k, 'val', cycle)
    season_df = TD.get_seasonindex()
    season_df = TD.date_split(season_df, '-', 'seasonindex')
    ms = season_df['seasonindex'].groupby([season_df['year'], season_df['month']]).sum()
    sea_trans = ms.unstack().T
    weight = TD.get_weight(sea_trans)
    k_depart_month_tar = (weight * target).to_frame()
    k_depart_month_tar.columns = ['target']
    return k_depart_month_tar

#计算各个科室各个月份的目标值的字典
def GetTargetDic(ks_dic,ks_tar):
    mon_target_dic = {}
    for k in ks_dic:
        df_k = ks_dic[k]
        target = ks_tar[k]
        k_month_tar = GetDepartmentTarget(cycle, df_k, target)
        mon_target_dic[k] = k_month_tar
    return mon_target_dic

#将全年目标dataframe转成对应科室的目标值的字典
def GetQtarDic(df):
    del df[1]
    df.index = df['dep']
    del df['dep']
    k_tar = dict(df[2])
    return k_tar

def GreDataforSQL(dic,index_id,type_id):
    data = []
    for k in dic:
        df_k = dic[k]
        k_dic = dict(df_k['target'])
        for x, v in k_dic.items():
            y = 2018
            m = int(x)
            d = 1
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
index_id = 10200
type_id = 8
#输入变量
cycle = 12
#获取数据
sql_1 = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID="+str(index_id)+" and type=0 order by ACCOUNT_DATE"#各科室每月业务收入真实值
#sql_2 = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID=10181 and type=0 order by ACCOUNT_DATE"#全院每月业务收入真实值——
sql_3 = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID="+str(index_id)+" and type=9 and ACCOUNT_DATE='2018-01-01' order by ACCOUNT_DATE"#年度业务收入目标值
df_k = GetData(sql_1, host, user, passwd, port, db)#获取各科室历史数据，计算目标值的样本数据
#df_q = GetData(sql_2, host, user, passwd, port, db)#获取全院历史数据，计算目标值的样本数据
#df = df_k.append(df_q,ignore_index=True)
df = df_k

df_t= GetData(sql_3, host, user, passwd, port, db)#获取各科室及全院的年度目标值
ks_t = set(df_t['dep'])
ks_z = set(df['dep'])
ks_i = ks_t&ks_z
#过滤出既有真实数据又有目标数据的科室
df = df[df['dep'].isin(ks_i)]
df_t = df_t[df_t['dep'].isin(ks_i)]
k_tar = GetQtarDic(df_t)

CLASS = GetClassKs(df)
ks_dfs_new,ks_dfs_old,ks_dfs_lack = CLASS.get_ks_classity()#科室分类
mon_target_dic = GetTargetDic(ks_dfs_old,k_tar)#获得各月目标值

data = GreDataforSQL(mon_target_dic,index_id,type_id)#各月目标结果转换成可放入数据库的格式
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.InsertPreD(data)#将各月目标数据插入数据库