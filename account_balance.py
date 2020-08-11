# -*- coding:utf-8 -*-
# @Time    : 2020/7/29 
# @Author  : Stephev
# @Site    : 
# @File    : Payment.py
# @Remarks :每日进行代理余额查询


import pymysql
import csv
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import pandas as pd


#D:\workpalce\csv_file
now_time_r = datetime.datetime.now()
now_time = datetime.datetime.strftime(now_time_r,'%Y-%m-%d_%H_%M')
#balance_csv = "D:\\workpalce\\csv_file\\"+now_time+"_代理余额.csv"
balance_csv = "E:\\csv_file\\"+now_time+"_代理余额明细.xlsx"

class cnMySQL:
    def __init__(self):
        self._dbhost = 'rm-bp1cim09l95sf67dplo.mysql.rds.aliyuncs.com'
        self._dbuser = 'python_dba'
        self._dbpassword = 'Python_dba1'
        self._dbname = 'prod_user'
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


def checkBalance():
    """
    提取所有代理的账户余额
    """
    conn = cnMySQL()
    checkbalance_sql = "SELECT a.id 代理id,\
                        a.NAME 代理姓名,\
                        a.auth_code 授权码,\
                    CASE a.level_id \
                        WHEN '1' THEN  '分公司' \
                        WHEN '2' THEN  '合伙人' \
                        WHEN '3' THEN  '官方' \
                        WHEN '4' THEN  '省代' \
                        WHEN '5' THEN  '市代' \
                        WHEN '6' THEN  '会员' ELSE '其他' \
                        END AS 代理等级,\
                    CASE a.region_id \
                        WHEN '1' THEN  '笛梦大区' \
                        WHEN '2' THEN  '环球大区' \
                        WHEN '3' THEN  '辉煌大区' \
                        WHEN '4' THEN  '聚米大区' \
                        WHEN '5' THEN  '聚星大区' \
                        WHEN '6' THEN  '口口大区' \
                        WHEN '7' THEN  '米苏大区' \
                        WHEN '8' THEN  '野狼大区' \
                        WHEN '9' THEN  '海纳百川大区' \
                        WHEN '10' THEN '红红大区' \
                        WHEN '11' THEN '熊熊大区' \
                        WHEN '12' THEN '飞越大区' \
                        WHEN '13' THEN '测试大区' ELSE '其他'\
                        END AS 所属大区,\
                        d.balance_amt 余额\
                    FROM\
                        prod_user.renren_distributor a,\
                        prod_finance.distributor_balance_info d \
                    WHERE  a.id = d.distributor_id;"
    balance = conn.ExecQuery(checkbalance_sql)
    headers = ['代理id','代理姓名','授权码','代理等级','所属大区','余额']
    dt = pd.DataFrame(balance,columns=headers)
    dt.to_excel(balance_csv,index=0)
    """
    with  open(balance_csv,'w',newline='') as f:
        f_csv = csv.DictWriter(f,headers)
        f_csv.writeheader()
        f_csv.writerows(balance)
    f.close()
    """
    return


def sendMail():
    mail_host='smtp.qq.com'
    mail_user='1058582934@qq.com'
    mail_pass='wcasswmrgsnobdgh'

    sender='1058582934@qq.com'
    receivers = ['pd@xitu.com','chenxing@xitu.com','cj@xitu.com','sxl@xitu.com','dxy@xitu.com','xjj@xitu.com','gaowei@xitu.com']
    #receivers=['gaowei@xitu.com']
    
    #创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("技术中心-高巍", 'utf-8')
    message['To'] =  Header("财务报表", 'utf-8')
    subject = '每日代理余额明细'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是今日代理余额报表，请查收。 如有问题可与我联系\n\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 balance_csv 文件
    att1 = MIMEText(open(balance_csv, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1.add_header("Content-Disposition", "attachment", filename=balance_csv)
    message.attach(att1)

    try:
        smtpObj = smtplib.SMTP() 
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user,mail_pass)  
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")
    return


def main():
    checkBalance()
    sendMail()
    return

if __name__ == '__main__':
    main()