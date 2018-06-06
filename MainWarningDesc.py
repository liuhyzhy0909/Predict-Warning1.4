# -*- coding: utf-8 -*-
from numpy import *
from pandas import *
import SelectMySQL
from GetClassKs import GetClassKs
import datetime

def getTimelist(matrix):
    timelist = np.array(matrix[:,0])
    rix = matrix[:,1:]
    rows = len(matrix)
    cols = len(matrix[0, :])
    tmpmatrix = np.zeros((rows,cols))
    #数据类型转化为浮点型
    for rowi in range(rows):
        for colj in range(cols):
            tmpmatrix[rowi,colj] = float(matrix[rowi,colj])
    matrix = tmpmatrix
    return matrix

# 单指标短期预警-同环比比较,红色表示同环比都下降10%，黄色表示同比或者环比下降5%
def SingleShort(k,positive_negative,data,WarningList):
    data0 = np.array(data[0, :])  # 取出当前数据
    data1 = np.array(data[1, :])  # 上个月数据
    data12 = np.array(data[12, :])  # 去年这个月数据
    #print("当前月、上个月、去年这个月",data0[0][1],data1[0][1],data12[0][1])
    YoY = (float(data0[0][1])-float(data1[0][1]))/ (float(data1[0][1]) + 0.01)#防止除0
    MoM = (float(data0[0][1])-float(data12[0][1])) /(float(data12[0][1]) + 0.01) #防止除0

    if YoY * positive_negative[0] < -0.1 or MoM * positive_negative[0] < -0.1:
        if WarningList[k] > red:
            WarningList[k] = red
    if (YoY * positive_negative[0] > -0.1 and YoY * positive_negative[0] < 0) or (MoM * positive_negative[0]> -0.1 and MoM* positive_negative[0] < 0):
        if WarningList[k] > yellow:
            WarningList[k] = yellow
    #print(u"同比为：",YoY ,yoy,u"环比为：",MoM,mom)
    return YoY, MoM,WarningList

# 单指标长期预测，
def SingleLong(k,positive_negative,data,WarningList):
    #print(k)
    #data0 = np.array(data[0, :])  # 取出当前数据
    tlist = []
    decreasenum = 0
    for j in range(0, 6):  # 近来1年之内
        changerate = (float(data[j + 1, 1]) - float(data[j, 1])) / (float(data[j + 1, 1]) + 0.01) #防止除0
        if((float(data[0, 1]) - float(data[j + 1, 1])) * positive_negative[0] < 0):#哪些月份高于或者低于本年度其他月份数值，放入时间列表中。
            tlist.append(timelist[j + 1])
            '''
            if changerate * self.positive_negative[i] < 0.05 and changerate * self.positive_negative[i] > 0:
                decreasenum = decreasenum + 1
            '''
    if decreasenum > 2:  # and (data[i,1]-data[i,0])*positive_negative[i]<0.05 and (data[i,1]-data[i,0])*positive_negative[i]>0:
        if WarningList[k] > yellow:
            WarningList[k] = yellow
    '''
    print("和以往近6个月相比低于",end="")
    for i in tlist:
        print(str(i).strip(),"月份,",end="")
    print("。")
    '''
    decreasenum = 0
    continuity = 0#记录连续几个月下降的数值
    flag = 0 #记录连续下降的标志位
    for j in range(0, 12):
        if((float(data[j , 1]) < float(data[j + 1 , 1])) and (flag == 0)):
            continuity = continuity + 1
        else:
            flag = 1
        changerate = (float(data[j + 1, 1]) - float(data[j, 1])) / float(data[j + 1, 1])
        if changerate * positive_negative[0] < 0.1 and changerate * positive_negative[0] > 0:
            decreasenum = decreasenum + 1

        if continuity > 2: #连续3个月或者3个月以上下降,红色报警 # and (data[i,1]-data[i,0])*positive_negative[i]<0.05 and (data[i,1]-data[i,0])*positive_negative[i]>0:
            if WarningList[k] > red:
                WarningList[k] = red
    #print("指标连续",continuity,"个月下降。")
    #print("--------------------------------------------------")
    return tlist,continuity,WarningList

def Holt_Winters_2(k,positive_negative,data,WarningList):  # 二次指数平滑
    data0 = np.array(data[0, :])  # 取出当前数据
    newdata = np.transpose(data)
    datarows = len(newdata)
    datacols = len(newdata[0, :])
    info_data_sales = np.zeros((datarows, datacols - 1))  # 去除当前月份数据,以后做比较
    for im in range(0, datarows):
        for jm in range(0, datacols - 1):  # 将实际数据中的当前月份
            info_data_sales[im][jm] = newdata[im][datacols - 1 - jm]

    S2_1 = []
    S2_2 = []
    for m in range(0, datarows):
        S2_1_empty = []
        x = 0
        for n in range(0, 3):
            x = x + float(info_data_sales[m][n])
        x = x / 3
        S2_1_empty.append(x)
        S2_1.append(S2_1_empty)
        S2_2.append(S2_1_empty)
    a = []  ##这是用来存放阿尔法的数组
    for i in range(0, len(info_data_sales)):
        a.append(0.6)  # 初始值设为0.6

        ##下面是计算一次指数平滑的值
    S2_1_new1 = []
    for i in range(0, len(info_data_sales)):
        S2_1_new = [[]] * datarows
        for j in range(0, len(info_data_sales[i])):
            if j == 0:
                S2_1_new[i].append(
                    float(a[i]) * float(info_data_sales[i][j]) + (1 - float(a[i])) * float(S2_1[i][j]))
            else:
                    S2_1_new[i].append(float(a[i]) * float(info_data_sales[i][j]) + (1 - float(a[i])) * float(
                    S2_1_new[i][j - 1]))  ##计算一次指数的值
        S2_1_new1.append(S2_1_new[i])

        ##下面是计算二次指数平滑的值
    S2_2_new1 = []
    info_MSE = []  ##计算均方误差来得到最优的a(阿尔法)
    for i in range(0, len(info_data_sales)):
        S2_2_new = [[]] * datarows
        MSE = 0
        for j in range(0, len(info_data_sales[i])):
            if j == 0:
                S2_2_new[i].append(float(a[i]) * float(S2_1_new1[i][j]) + (1 - float(a[i])) * float(S2_2[i][j]))
            else:
                S2_2_new[i].append(float(a[i]) * float(S2_1_new1[i][j]) + (1 - float(a[i])) * float(
                S2_2_new[i][j - 1]))  ##计算二次指数的值
            MSE = (int(S2_2_new[i][j]) - int(info_data_sales[i][j])) ** 2 + MSE
        MSE = (np.sqrt(MSE)) / int(len(info_data_sales[i]))
        info_MSE.append(MSE)
        S2_2_new1.append(S2_2_new[i])

    u = 1  # i
    Xt = []
    for i in range(0, len(info_data_sales)):
        At = (float(S2_1_new1[i][len(S2_1_new1[i]) - 1]) * 2 - float(S2_2_new1[i][len(S2_2_new1[i]) - 1]))
        Bt = (float(a[i]) / (1 - float(a[i])) * (
            float(S2_1_new1[i][len(S2_1_new1[i]) - 1]) - float(S2_2_new1[i][len(S2_2_new1[i]) - 1])))
        Xt.append(At + Bt * int(u))

    for i in range(0, len(Xt)):
        changerate = float(Xt[i] - data0[i]) * positive_negative[i] / data0[i]

        if changerate > 0.2:
            if WarningList[k] > red:
                WarningList[k] = red
            '''
            if changerate > 0.2:
                if self.WarningList[namelist[i]] > self.orange:
                    self.WarningList[namelist[i]] = self.orange
            if changerate > 0.1: 
                if self.WarningList[namelist[i]] > self.yellow:
                    self.WarningList[namelist[i]] = self.yellow
           '''
        print("预计本月值为：",round(Xt[i],2) ,"本月实际值为：",data0[i])
    return WarningList

def GetDataDf(sql, host, user, passwd, port, db):
    select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
    df = select.get_df(sql)
    df_ks = df[0]
    del df[0]
    df.insert(1,'dep',df_ks)
    df = df[df[2].notnull()]
    df = df[df[2] != 0]
    df[2] =df[2].apply(lambda x:float(x))
    return df

    #获得数据，并且转化为matrix格式
def GetMatrixData(k_df):
    k_df = k_df.reset_index()
    matrix = np.matrix
    timelist = []
    t_df = k_df.loc[:, ['date']]
    IsDuplicated = t_df.duplicated()
    tm = t_df.drop_duplicates()
    tm = tm['date']
    for i in tm:
        timelist.append(str(i))
    timelist.reverse()

    valuelist = []
    value = k_df['val']
    value = np.array(value)
    value = value.tolist()
    value.reverse()
    for i in value:
        valuelist.append(float(i))

    matrixData = np.matrix([timelist, valuelist])
    matrixData = matrixData.transpose()
    return matrixData

def GetDataSignforSQL(warnlist,index_id,warningsign,warningtype):
    data = []
    for k in warnlist:
        dept = k
        datalist = warning_time.split('-')
        datalist = [int(x) for x in datalist]
        data.append((warningsign,index_id, dept, datetime.date(datalist[0], datalist[1], datalist[2]), warningtype))
    return data

red = 1
#orange = 1
yellow = 2
normal = 3

threshold = -0.1
qy_desci_dept_id = '1007'
warning_sign = 2
warning_type = 0

# 数据库信息
host = "59.110.68.181"
user = "hozedata"
passwd = "Hozedata@123"
port = 3306
db = "FMS_NEW"
# 数据库字段信息
index_id_list = [10200,10301,10302,10304,10305,10306,10314]
#index_id_list = [10306]
positive_negative_list = [1,1,1,1,1,1,-1]

sql_dep = "SELECT DEPT_ID,DEPT_NAME FROM DICT_DEPT_HOSPITAL"
select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
df_dep = select.get_df(sql_dep).set_index(0).to_dict()[1]#指标字典
sql_index = "select KEEPING_INDEX_ID,KEEPING_INDEX_NAME from COST_KEEPING_INDEX"
df_index = select.get_df(sql_index).set_index(0).to_dict()[1]#科室字典
datadesc = []
for i in range(len(index_id_list)):
    index_id = index_id_list[i]
    sql = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID="+str(index_id)+" and type=0 order by ACCOUNT_DATE"
    df = GetDataDf(sql, host, user, passwd, port, db)  # 获取各科室历史数据，计算预测值的样本数据
    CLASS = GetClassKs(df)
    warning_time = CLASS.TimeFun()[0]
    ks_dfs_new, ks_dfs_old, ks_dfs_lack = CLASS.get_ks_classity()  # 科室分类
    namelist = [index_id]
    positive_negative = [positive_negative_list[i]]
    timelist = list(df.drop_duplicates(subset=['date'],keep='first',inplace=False)['date'])
    timelist.reverse()


    ##报警测试
    WarningList1={}
    WarningList2={}
    TH_qy = {}
#k_l = ['349','334','436','348','357','347','337','342','1000','1001','331','319','346','447','426']

    for k in ks_dfs_old:
        k_df = ks_dfs_old[k]
        # WarningList[warning_time+'_'+k+'_'+str(index_id)] = normal
        WarningList1[k] = normal
        WarningList2[k] = normal
        dataMtr = GetMatrixData(k_df)
        warn1 = SingleShort(k, positive_negative, dataMtr,WarningList1)[2]
        warn2 = SingleLong(k, positive_negative, dataMtr,WarningList2)[2]


    warn = {}
    for k in warn1:
        #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        w1 = warn1[k]
        w2 = warn2[k]
        warn[k] = min(w1, w2)

    warn_key = {k:v for k,v in warn.items() if v<3}.keys()
    datasign = GetDataSignforSQL(list(warn_key),index_id,warning_sign,warning_type)
    #print(datasign)

    if (qy_desci_dept_id in list(warn_key) and ks_dfs_old != {}):
        warninglist1 = {}
        warninglist1[qy_desci_dept_id] = normal
        warninglist2 = {}
        warninglist2[qy_desci_dept_id] = normal
        dataMtrqy = GetMatrixData(ks_dfs_old[qy_desci_dept_id])
        tb, hb ,wl= SingleShort(qy_desci_dept_id, positive_negative,dataMtrqy, warninglist1)
        tl,contcount,wl2 = SingleLong(qy_desci_dept_id,positive_negative,dataMtrqy,warninglist2)
        tl = [x.month for x in tl]
        #print(tb,hb,tl,contcount)

        s1 = "1、环比"
        hsq = "<span class=\\\'red\\\'>"
        hsh = "</span>"
        lsq = "<a href="">"
        lsh = "</a>"
        lsjcq = "<span class=\\\'blueBold\\\'>"
        xjjthb = ""
        xjjttb = ""
        lxxjjt = "<img class=\\\'reduce\\\' src=\\\'\\\' alt=\\\'下降\\\'>"
        lxssjt = "<img class=\\\'reduce\\\' src=\\\'\\\' alt=\\\'下降\\\'>"
        s2 = "2、和以往12个月比，低于"
        s2h = "往年变化趋势。"
        index_desc = df_index[index_id]
        s3 = "3、近"+str(contcount)+"个月"+index_desc+"连续"
        d2= ""
        d3 = ""
#img src='" + serverCtx + "/i
        if(hb < 0):
            xjjthb = "<img class=\\\'downArrow\\\' src=\\\'\\\' alt=\\\'下降\\\'>"
        if(tb < 0):
            xjjttb = "<img class=\\\'downArrow\\\' src=\\\'\\\' alt=\\\'下降\\\'>"
        if(contcount > 0):
            if(positive_negative == [1]):
                d3 =  "<p class=\\\'report_textP\\\'>"+s3+"减少"+lxxjjt+"。"+"</p>"
            else:
                d3 = s3+"增加"+lxssjt+"。"+"</p>"
        if(len(tl)>0):
            #print(tl)
            monthstr = ""
            for i in tl:
                print(i)
                monthstr = monthstr+lsq+str(i)+"月，"+lsh
            #monthst =str(tl).replace('[','').replace(']','')
            #print(monthstr)
            d2 = "<p class=\\\'report_textP\\\'>"+s2+monthstr+lsjcq+"不符合"+hsh+s2h+"</p>"
        d1 = "<p class=\\\'report_textP\\\'>"+s1+hsq +str(round((hb+1)*100,2))+"%"+hsh+xjjthb+"，同比"+hsq+str(round((tb+1)*100,2))+"%"+hsh+xjjttb+"。"+"</p>"

        d = d1+d2+d3
        datalist = warning_time.split('-')
        datalist = [int(x) for x in datalist]
        datadesc.append((d,index_id,qy_desci_dept_id,datetime.date(datalist[0], datalist[1], datalist[2]),warning_sign,warning_type))
        print(d)


    #将报警标识更新到数据库
    select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
    select.UpdateWarnSign(datasign)  # 将报警标识更新到数据库

select = SelectMySQL.SelectMySQL(host, user, passwd, port, db)
select.UpdateWarnDesc(datadesc)

