from numpy import *
import pandas as pd
import warnings
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
warnings.filterwarnings("ignore")
import warnings
import arrow
warnings.filterwarnings("ignore")

class GetClassKs(object):
    def __init__(self,df,ks_new_time='2016-07-01'):
        self.df =df
        self.ks_new_time = ks_new_time

    def StandDf(self):
        # 数据字段格式化
        df = self.df
        col_list = ['date', 'department', 'val']  # 重新定义列名，方便后续处理
        df.columns = col_list
        return df

    def Df2TimeIndex(self):
        df_setindex = self.StandDf()
        df_setindex['date'] = pd.to_datetime(df_setindex['date'])
        df_setindex = df_setindex.set_index('date')
        return df_setindex

    def TimeFun(self):
        df = self.Df2TimeIndex()
        warning_time = arrow.get(self.df.date.max()).format("YYYY-MM-DD")  # 完整数据截止的月份，下一个月为需要预警的月
        filter_ks_date = arrow.get(warning_time, "YYYY-MM-DD").shift(months=-6).format("YYYY-MM-DD")
        # back1_year = str(int(self.warning_time.split('-')[0])-1)  # 预警年度的减1年，该年份的指标值是预警年度总目标制定的依据
        warning_year = arrow.get(warning_time, "YYYY-MM-DD").format("YYYY")
        return warning_time, filter_ks_date, warning_year

    def get_ks_classity(self):
        # 将全院数据拆分成科室字典数据，并根据数据情况对科室进行分类
        warning_time, filter_ks_date, warning_year = self.TimeFun()
        old_len_lim = arrow.get(warning_time, "YYYY-MM-DD").month
        df_setindex = self.Df2TimeIndex()
        based_ks_filter_set = set(df_setindex[filter_ks_date:]['department'])  # 从预警时间开始 半年内出现的科室才做分析
        ks_dfs_new = {}
        ks_dfs_old = {}
        ks_dfs_lack = {}
        for k in based_ks_filter_set:  # 只有在近半年内出现过的科室才做分析
            ks_df = df_setindex[df_setindex['department'] == k]
            k = list(set(ks_df['department']))[0]
            ks_df.pop('department')
            v = ks_df.sort_index()
            if (len(ks_df) > 35 and len(ks_df[warning_year]) > old_len_lim * 0.8):#历史数据从2015年开始的参数，如果历史数据变化，参数要相应调整
                ks_dfs_old[k] = v
            elif (len(ks_df[:self.ks_new_time]) < 17 and len(ks_df[self.ks_new_time:]) > 10):
                ks_dfs_new[k] = v
            else:
                ks_dfs_lack[k] = v
        return ks_dfs_new, ks_dfs_old, ks_dfs_lack

    def FillIndex(self,ks_dic, start_time, periods):
        for k in ks_dic:
            df_k = ks_dic[k]
            df_Index = pd.date_range(start_time, periods=periods, freq='MS', name='date')
            df_dif = pd.DataFrame(index=df_Index.difference(df_k.index))
            ks_dic[k] = df_k.append(df_dif).sort_index().fillna(0)
        return ks_dic