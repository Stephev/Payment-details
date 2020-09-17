# -*- coding:utf-8 -*-
# @Time    : 2020/9/12 
# @Author  : Stephev
# @Site    : 
# @File    : Inventory_quantity.py
# @Remarks :每日进行代理库存查询


import pymysql
import csv
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import pandas as pd


now_time_r = datetime.datetime.now()
now_time = datetime.datetime.strftime(now_time_r,'%Y-%m-%d_%H_%M')
inventoryquantity_xlsx = "E:\\csv_file\\"+now_time+"_代理云仓库存明细.xlsx"

class cnMySQL:
    def __init__(self):
        self._dbhost = 'rm-bp1cim09l95sf67dplo.mysql.rds.aliyuncs.com'
        self._dbuser = 'python_dba'
        self._dbpassword = 'Python_dba1'
        self._dbname = 'prod_inventory'
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


def inventoryQuantity():
    """
    提取所有代理的云仓库存数量
    """
    conn = cnMySQL()
    inventoryquantity_sql = "select \
                                a.name 代理姓名,\
                                case a.level_id \
                                    when '7' then '社长'\
                                    when '8' then '部长'\
                                    when '9' then '次长'\
                                    when '10' then '课长'\
                                else '其他' end as 代理等级,\
                                a.id 代理id,\
                                a.auth_code 代理授权码,\
                                c.mobile 代理手机号,\
                                case a.region_id\
                                    when '21' then '员工账号'\
                                    when '22' then '张月战队'\
                                    else '其他' end as 所属大区,\
                                t3.title 商品名称,\
                                t1.id 商品id,\
                                t4.spec_set 'sku描述',\
                                b.cloud_remain_num    云库存数量,\
                                b.cloud_remain_num    可发货数量\
                            from \
                            prod_inventory.merchant_inventory b\
                            left join prod_user.renren_distributor a on a.id = b.merchant_id\
                            left join prod_user.user c on a.account_id  = c.id\
                            LEFT join  prod_goods.rrs_main_goods_spu t1 on b.main_spu_id = t1.id\
                            left join prod_goods.rrs_main_goods_sku t2 on t1.id = t2.spu_id\
                            left join prod_goods.rrs_supplier_goods_spu t3  on t1.supplier_spu_id = t3.id\
                            left join prod_goods.rrs_supplier_goods_sku t4 on t3.id=t4.spu_id\
                            where  b.team_id  = 1195 and  b.main_spu_id = 10833;"
    inventory = conn.ExecQuery(inventoryquantity_sql)
    headers = ['代理姓名','代理等级','代理id','代理授权码','代理手机号','所属大区','商品名称','商品id','sku描述','云库存数量','可发货数量']
    dt = pd.DataFrame(inventory,columns=headers)
    dt.to_excel(inventoryquantity_xlsx,index=0)
    return


def sendMail():
    mail_host='smtp.qq.com'
    mail_user='1058582934@qq.com'
    mail_pass='wcasswmrgsnobdgh'

    sender='1058582934@qq.com'
    receivers = ['pd@xitu.com','chenxing@xitu.com','cj@xitu.com','sxl@xitu.com','dxy@xitu.com','xjj@xitu.com','gaowei@xitu.com','563415231@qq.com']
    #receivers=['gaowei@xitu.com']
    
    #创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("技术中心-高巍", 'utf-8')
    message['To'] =  Header("财务报表", 'utf-8')
    subject = '安丽缇-代理云库存明细'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是今日代理库存明细报表，请查收。 如有问题可与我联系\n\n\n需要注意：1、云库存数量均为可发货数量\n\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 inventoryquantity_xlsx 文件
    att1 = MIMEText(open(inventoryquantity_xlsx, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1.add_header("Content-Disposition", "attachment", filename=inventoryquantity_xlsx)
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
    inventoryQuantity()
    sendMail()
    return

if __name__ == '__main__':
    main()