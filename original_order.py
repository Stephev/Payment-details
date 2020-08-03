# -*- coding:utf-8 -*-
# @Time    : 2020/7/29 
# @Author  : Stephev
# @Site    : 
# @File    : Payment.py
# @Software:


import pymysql
import csv
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


#D:\workpalce\csv_file
now_time_r = datetime.datetime.now()
now_time = datetime.datetime.strftime(now_time_r,'%Y-%m-%d_%H_%M')
#balance_csv = "D:\\workpalce\\csv_file\\"+now_time+"_代理余额.csv"
balance_csv = "E:\\csv_file\\"+now_time+"_原始单号.csv"
file_name = "原始单号"+now_time+".csv"

class cnMySQL:
    def __init__(self):
        self._dbhost = 'rm-bp1cim09l95sf67dplo.mysql.rds.aliyuncs.com'
        self._dbuser = 'python_dba'
        self._dbpassword = 'Python_dba1'
        self._dbname = 'prod_order'
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
    checkbalance_sql = "SELECT\
                        t.shop_trade_no,\
                        p.biz_trade_no \
                    FROM\
                        rupt_shop_trade AS t\
                        JOIN rupt_purchase_order AS p ON t.fx_purchase_order_no = p.purchase_order_no \
                    WHERE\
                        t.shop_id = 1171 \
                        AND t.fx_shop_id = 32 \
                        AND t.trade_type = 'FENXIAO';"
    balance = conn.ExecQuery(checkbalance_sql)
    headers = ['shop_trade_no','biz_trade_no']
    with  open(balance_csv,'w',newline='') as f:
        f_csv = csv.DictWriter(f,headers)
        f_csv.writeheader()
        f_csv.writerows(balance)
    f.close()
    return


def sendMail():
    mail_host='smtp.qq.com'
    mail_user='1058582934@qq.com'
    mail_pass='wcasswmrgsnobdgh'

    sender='1058582934@qq.com'
    #receivers = ['pd@xitu.com','chenxing@xitu.com','cj@xitu.com','sxl@xitu.com','dxy@xitu.com','xjj@xitu.com','gaowei@xitu.com']
    receivers=['gaowei@xitu.com','xjj@xitu.com','gfy@xitu.com']
    
    #创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("技术中心-高巍", 'utf-8')
    message['To'] =  Header("原始订单号", 'utf-8')
    subject = '原始订单号'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是最新的原始订单报表，请查收。 如有问题可与我联系\n\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 balance_csv 文件
    att1 = MIMEText(open(balance_csv, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1.add_header("Content-Disposition", "attachment", filename=file_name)
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