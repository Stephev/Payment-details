# -*- coding:utf-8 -*-
# @Time    : 2020/7/29 
# @Author  : Stephev
# @Site    : 
# @File    : Payment.py
# @Software:


import pymysql
import csv
import datetime

#D:\workpalce\csv_file
now_time_r = datetime.datetime.now()
now_time = datetime.datetime.strftime(now_time_r,'%Y-%m-%d_%H_%M')

class cnMySQL:
    def __init__(self):
        self._dbhost = '101.37.79.230'
        self._dbuser = 'root'
        self._dbpassword = 'mysql_pass'
        self._dbname = 'xitu'
        self._dbcharset = 'utf8'
        self._dbport = int(3306)
        self._conn = self.connectMySQL()

        if (self._conn):
            self._cursor = self._conn.cursor(cursor=pymysql.cursors.DictCursor)
    
    def connectMySQL(self):
        try:
            conn = pymysql.connect(host=self._dbhost,
                                   user=self._dbuser,
                                   passwd=self._dbpassword,
                                   db=self._dbname,
                                   port=self._dbport,
                                   cursorclass=pymysql.cursors.DictCursor,
                                   charset=self._dbcharset)
        except Exception as e:
            raise
            #print("数据库连接出错")
            conn = False
        return conn

    def close(self):
        if (self._conn):
            try:
                if (type(self._cursor) == 'object'):
                    self._conn.close()
                if (type(self._conn) == 'object'):
                    self._conn.close()
            except Exception:
                print("关闭数据库连接异常")

    def ExecQuery(self,sql):
        """
        执行查询语句
        """
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql)
                res = self._cursor.fetchall()
            except Exception:
                res = False
                print("查询异常")
            self.close()
        return res


#def kmtest():
#    conn = cnMySQL()
#    kmsql = 'select * from renren_distributor;'
#    result = conn.ExecQuery(kmsql)
#    kmtest_csv = "D:\\workpalce\\csv_file\\"+now_time+".csv"
#    print(kmtest_csv)
#    headers = ['id','ref_id','nam']
 #   f = open(kmtest_csv,'w',newline=)
 #   writer = csv.writer(f,dialect='excel')
 #   writer.writerow(headers)
 #   for i in result:
 #       idw = i['id']
 #       ref_idw = i['ref_id']
 #       namw = i['nam']
 #       writer.writerow('%s|%s|%s\n' %(idw,ref_idw,namw))
 #   f.close()
#    return

def kmtest():
    conn = cnMySQL()
    kmsql = 'select * from renren_distributor;'
    result = conn.ExecQuery(kmsql)
    print(result)
    kmtest_csv = "D:\\workpalce\\csv_file\\"+now_time+".csv"
    headers = ['id','ref_id','nam']
    with  open(kmtest_csv,'w',newline='') as f:
        f_csv = csv.DictWriter(f,headers)
        f_csv.writeheader()
        f_csv.writerows(result)
    f.close()
    return


def main():
    kmtest()
    return

if __name__ == '__main__':
    main()

