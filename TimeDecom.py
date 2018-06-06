import pandas as pd
import numpy as np
from numpy import nan as NA

class TimeDecom(object):
    def __init__(self, df_data,index_name,cycle):
        self.df_data = df_data
        self.index_name=index_name
        self.cycle = cycle

    #计算指标的中心化移动平均值
    def get_seasonindex(self):
        df = self.df_data
        df = df.sort_index()
        df[str(self.cycle) + 'move_mean'] = df[self.index_name].rolling(center=False, window=self.cycle).mean()
        # df['中心化移动平均值'] = pd.rolling_mean(df[str(cycle) + '项移动平均值'], 2)
        df['centermean'] = df[str(self.cycle) + 'move_mean'].rolling(center=False, window=2).mean()
        series_centermean = df['centermean']
        list_centermean_notna = list(series_centermean)[(self.cycle//2):]
        for i in range(self.cycle//2):
            list_centermean_notna.append(NA)
        df['centermean'] = pd.DataFrame(list_centermean_notna,index=df.index)
        df['seasonindex'] = df[self.index_name] / df['centermean']
        df.pop(str(self.cycle) + 'move_mean')
        df.pop('centermean')
        return df

    def date_split(self,df,sp,str2):
        w_split_col = list(df.index.map(lambda x:str(x)[:7]))
        split_col = pd.DataFrame([x.split(sp) for x in w_split_col],index=df.index)
        split_col[str2] = df[str2]
        split_col.columns = ['year', 'month', str2]
        return split_col

    #将列为年 月 季节指数的df转成年为索引，月份为列的df
    def col2time(self,df_seasonindex):
        year_l = list(set(df_seasonindex['year']))
        month_l = list(set(df_seasonindex['month']))
        df_m = pd.DataFrame(np.random.randn(len(year_l), len(month_l)), index=year_l, columns=month_l)
        for i in year_l:
            for j in month_l:
                for n in df_seasonindex['year']:
                    for m in df_seasonindex['month']:
                        if i == n and j == m:
                            res = df_seasonindex.loc[(df_seasonindex['year'] == n) & (df_seasonindex['month'] == m)].seasonindex
                            df_m.at[i, j] = res
        df_m = df_m.sort_index().sort_index(axis=1)
        return df_m.T

    # 日期列分成两列，为年一列，月份一列
    #根据季节指数的年月维度计算各个月份权重
    def get_weight(self,df_seasonindex):
        df_seasonindex['seasonindex'] = df_seasonindex.mean(1)
        total_ave = df_seasonindex['seasonindex'].mean()
        df_seasonindex['seasonindex_part'] = df_seasonindex['seasonindex'] / total_ave
        df_seasonindex['weight'] = df_seasonindex['seasonindex_part'] / self.cycle
        return df_seasonindex['weight']