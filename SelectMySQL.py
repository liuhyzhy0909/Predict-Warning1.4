import pymysql as MySQLdb # 这里是python3  如果你是python2.x的话，import MySQLdb
import pandas as pd
'''
conn = MySQLdb.connect(host = '59.110.68.181',  # 远程主机的ip地址，
                                            user = 'hozedata',   # MySQL用户名
                                            db = 'FMS_NEW',   # database名
                                            passwd = 'Hozedata@123',   # 数据库密码
                                            port = 3306,  #数据库监听端口，默认3306
                                            charset = "utf8")  #指定utf8编码的连接
'''
class SelectMySQL(object):
    def __init__(self,host,user,passwd,port,db):
        self.host = host
        self.user = user
        self.passwd=passwd
        self.port = port
        self.db = db

    def get_df(self,sql):
        result = []
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        cur.execute(sql)
        alldata = cur.fetchall()
        # print(alldata)
        for rec in alldata:
            result.append(rec)  # 注意，我这里只是把查询出来的第一列数据保存到结果中了,如果是多列的话，稍微修改下就ok了

        cur.close()
        conn.close()

        df = pd.DataFrame(result)
        return df

    def InsertPreD(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        sql = "INSERT INTO COST_ACCOUNT_RESULTS(INDEX_ID,DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE,TYPE) VALUES (%s, %s, %s, %s, %s)"
        #sql = "insert into COST_ACCOUNT_RESULTS(INDEX_ID,DEPT_ID,ACCOUNT_VALUE,TYPE) values(%s, %s, %s, %s)"
        cur.executemany(sql, data)
        conn.commit()
        conn.close()


    def InsertTarDegreeD(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        sql = "INSERT INTO COST_ACCOUNT_RESULTS(DEPT_ID,INDEX_ID,TYPE,ACCOUNT_DATE,ACCOUNT_VALUE) VALUES (%s, %s, %s, %s, %s)"
        #sql = "insert into COST_ACCOUNT_RESULTS(INDEX_ID,DEPT_ID,ACCOUNT_VALUE,TYPE) values(%s, %s, %s, %s)"
        cur.executemany(sql, data)
        conn.commit()
        conn.close()

    def UpdateWarnDesc(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        for x in data:
            sql = "update COST_ACCOUNT_RESULTS set WARNING_DESCRIPTION = '%s' where  INDEX_ID ='%s' and DEPT_ID = '%s' and ACCOUNT_DATE = '%s' and WARNING_SIGN='%s' and TYPE ='%s'" % (
            x[0], x[1], x[2], x[3], x[4], x[5])
            try:
                cur.execute(sql)  # 执行sql语句
                conn.commit()  # 提交到数据库执行
            except:
                print("update warning error")
                conn.rollback()  # 发生错误后回滚
        conn.close()  # 关闭数据库

    def UpdateWarnSign(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        for x in data:
            sql = "update COST_ACCOUNT_RESULTS set WARNING_SIGN = '%s' where  INDEX_ID ='%s' and DEPT_ID = '%s' and ACCOUNT_DATE = '%s' and TYPE ='%s'" % (
            x[0], x[1], x[2], x[3], x[4])
            try:
                cur.execute(sql)  # 执行sql语句
                conn.commit()  # 提交到数据库执行
            except:
                print("update warning error")
                conn.rollback()  # 发生错误后回滚
        conn.close()  # 关闭数据库

    def UpdatePre(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        for x in data:
            sql = "update COST_ACCOUNT_RESULTS set  ACCOUNT_VALUE= '%s' Where INDEX_ID = '%s' and DEPT_ID = '%s' and ACCOUNT_DATE= '%s' and TYPE='%s'" % (x[3],x[0],x[1],x[2],x[4])
            try:
                cur.execute(sql)  # 执行sql语句
                conn.commit()  # 提交到数据库执行
            except:
                print("update error")
                conn.rollback()  # 发生错误后回滚
        conn.close()  # 关闭数据库

    def UpdateEWD(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        for x in data:
            sql = "update QUOTA_REALTIME_DATA set WARNING_SIGN = '%s' where  QUOTA_ID ='%s' and DEPT_ID = '%s' and STATISTIC_START_TIME = '%s'and TYPE ='%s'" % (x[4],x[1],x[0],x[3],x[2])
            try:
                cur.execute(sql)  # 执行sql语句
                conn.commit()  # 提交到数据库执行
            except:
                print("update error")
                conn.rollback()  # 发生错误后回滚
        conn.close()  # 关闭数据库

    def UpdateEWDesc(self,data):
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                               charset='utf8')
        cur = conn.cursor()
        for x in data:
            sql = "update QUOTA_REALTIME_DATA set WARNING_DESCRIPTION = '%s' where  QUOTA_ID ='%s' and DEPT_ID = '%s' and STATISTIC_START_TIME = '%s' and WARNING_SIGN='%s' and TYPE ='%s'" % (x[0],x[1],x[2],x[3],x[4],x[5])
            try:
                cur.execute(sql)  # 执行sql语句
                conn.commit()  # 提交到数据库执行
            except:
                print("update warning error")
                conn.rollback()  # 发生错误后回滚
        conn.close()  # 关闭数据库

if __name__ == '__main__':
    host = "59.110.68.181"
    user = "hozedata"
    passwd = "Hozedata@123"
    port = 3306
    db = "FMS_NEW"
    sql = "SELECT DEPT_ID,ACCOUNT_DATE,ACCOUNT_VALUE FROM COST_ACCOUNT_RESULTS where INDEX_ID=10200 and type=0 order by ACCOUNT_DATE"
    select = SelectMySQL(host,user,passwd,port,db)
    df = select.get_df(sql)
    print(df)