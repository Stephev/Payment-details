# -*- coding:utf-8 -*-
# @Time    : 2020/7/29 
# @Author  : Stephev
# @Site    : 
# @File    : original_order.py
# @Remarks :订单的原始单号数据提取，发送到目标邮箱


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
#balance_csv = "E:\\csv_file\\"+now_time+"_原始单号.csv"
balance_xlsx = "E:\\csv_file\\"+now_time+"_原始单号.xlsx"
orderdetails_xlsx = "E:\\csv_file\\"+now_time+"_原始单号明细.xlsx"
file_name = "原始单号"+now_time+".xlsx"
file_name1 = "原始单号明细"+now_time+".xlsx"


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
    """
    一开始使用写CSV的方式，但是容易出现编码问题
    with  open(balance_csv,'w',newline='') as f:
        f_csv = csv.DictWriter(f,headers)
        f_csv.writeheader()
        f_csv.writerows(balance)
    f.close()
    """
    dt = pd.DataFrame(balance,columns=headers)
    dt.to_excel(balance_xlsx,index=0)
    #再加一个订单明细sql
    orderdetails_sql = "SELECT  t.shop_trade_no,\
                        p.biz_trade_no,\
                        a.name 姓名,\
                        a.auth_code 授权码,\
                        case a.level_id\
                            when '1' then '分公司'\
                            when '2' then '合伙人'\
                            when '3' then '官方'\
                            when '4' then '省代'\
                            when '5' then '市代'\
                            when '6' then '会员'\
                            else '其他' end as 代理等级,\
                        s.receiver_name 收货人姓名,\
                        s.receiver_province_name 收货省份,\
                        s.receiver_city_name 收货城市,\
                        s.receiver_district_name 收货区,\
                        s.receiver_address 收货详细地址,\
                        case s.region_id\
                            when '1' then '笛梦大区'\
                            when '2' then '环球大区'\
                            when '3' then '辉煌大区'\
                            when '4' then '聚米大区'\
                            when '5' then '聚星大区'\
                            when '6' then '口口大区'\
                            when '7' then '米苏大区'\
                            when '8' then '野狼大区'\
                            when '9' then '海纳百川大区'\
                            when '10' then '红红大区'\
                            when '11' then '熊熊大区'\
                            when '12' then '飞越大区'\
                            when '13' then '测试大区'\
                            else '其他' end as 进货人所属大区\
                    FROM\
                        rupt_shop_trade AS t\
                        JOIN rupt_purchase_order AS p ON t.fx_purchase_order_no = p.purchase_order_no\
                        JOIN rupt_purchase_order AS s ON s.purchase_order_no = p.biz_trade_no\
                        JOIN prod_user.renren_distributor a ON s.source_id = a.id\
                    WHERE\
                        t.shop_id = 1171\
                        AND t.fx_shop_id = 32\
                        AND p.shop_id = 32\
                        AND s.shop_id = 32 \
                        AND t.trade_type = 'FENXIAO'"
    orderdetails = conn.ExecQuery(orderdetails_sql)
    detailheaders = ['shop_trade_no','biz_trade_no','姓名','授权码','代理等级','收货人姓名','收货省份','收货城市','收货区','收货详细地址','进货人所属大区']
    dt = pd.DataFrame(orderdetails,columns=detailheaders)
    dt.to_excel(orderdetails_xlsx,index=0)
    return


def sendMail():
    mail_host='smtp.qq.com'
    mail_user='1058582934@qq.com'
    mail_pass='wcasswmrgsnobdgh'

    sender='1058582934@qq.com'
    #receivers = ['gaowei@xitu.com']
    receivers=['gaowei@xitu.com','xjj@xitu.com','gfy@xitu.com','ht@xitu.com','yzw@xitu.com']
    
    #创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("技术中心-高巍", 'utf-8')
    message['To'] =  Header("原始订单号", 'utf-8')
    subject = '原始订单号'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是最新的原始订单报表，请查收。 如有问题可与我联系\n\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 balance_csv 文件
    att1 = MIMEText(open(balance_xlsx, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1.add_header("Content-Disposition", "attachment", filename=file_name)
    message.attach(att1)

    # 构造附件2，传送当前目录下的 orderdetails_xlsx 文件
    att2 = MIMEText(open(orderdetails_xlsx, 'rb').read(), 'base64', 'utf-8')
    att2["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att2.add_header("Content-Disposition", "attachment", filename=file_name1)
    message.attach(att2)

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