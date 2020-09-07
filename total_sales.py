# -*- coding:utf-8 -*-
# @Time    : 2020/8/21 
# @Author  : Stephev
# @Site    : 
# @File    : total_sales.py
# @Remarks :每周五10点提取代理的销售进货总额发送到邮箱,写程序循环去查


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
totalsales_xlsx = "E:\\csv_file\\"+now_time+"_代理进货总额.xlsx"


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

    def ExecQuery(self,sql,*args):
        """
        执行查询语句
        """
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql,args)
                res = self._cursor.fetchall()
            except Exception:
                res = False
                print("查询异常")
            self.close()
        return res

def total_cash_final_cash_together():
    """
    循环找出所有下级代理，并计算他们的销售总额
    """
    canshai_id_tuple = (409,419,422,432,434,441,443,447,451,456,467,481,485,487,488,490,492,507,27702,30878,95324)
    Statistics_rows = []
    for i  in  canshai_id_tuple:
        conn = cnMySQL()
        final_rows = []
        cansai_id = (i,)
        upper_id_tuple = (i,)
        while True:
            rows = conn.ExecQuery("select id from renren_distributor where  ref_id in %s and level_id =2",upper_id_tuple)
            if not rows:
                break
            final_rows.extend(rows)
            upper_id_tuple=tuple(row['id'] for row in rows)
        #print(final_rows)
        #获取sql的where条件的 tuple
        sql_condition_1 = tuple(i['id'] for i in final_rows)
        sql_condition = cansai_id + sql_condition_1
        #print(sql_condition)
        total_cash_result = conn.ExecQuery("select\
                                        d.id 代理id,\
                                        d.NAME 代理姓名,\
                                        d.auth_code 授权码,\
                                        case d.level_id\
                                            when '1' then '分公司'\
                                            when '2' then '合伙人'\
                                            when '3' then '官方'\
                                            when '4' then '省代'\
                                            when '5' then '市代'\
                                            when '6' then '会员'\
                                            else '其他' end as 代理等级,\
                                        case d.region_id\
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
                                            else '其他' end as 进货人所属大区,\
                                            aa.total_cash 订单总额单位分\
                                    from  prod_user.renren_distributor d,(select  \
                                        sum(b.total_fee)  total_cash\
                                    from \
                                        prod_user.renren_distributor a,\
                                        prod_order.rupt_purchase_order b\
                                    where  a.id = b.source_id \
                                    and b.status in (6,7,8,9) and a.id in  %s) aa where d.id in %s",sql_condition,cansai_id)
        #print(total_cash_result)
        Statistics_rows.extend(total_cash_result)
    print(Statistics_rows)
    # 将最后结果写进文件
    headers = ['代理id','代理姓名','授权码','代理等级','进货人所属大区','订单总额单位分']
    dt = pd.DataFrame(Statistics_rows,columns=headers)
    dt.to_excel(totalsales_xlsx,index=0)
    return

def sendMail():
    mail_host='smtp.qq.com'
    mail_user='1058582934@qq.com'
    mail_pass='wcasswmrgsnobdgh'

    sender='1058582934@qq.com'
    #receivers = ['gaowei@xitu.com']
    receivers=['gaowei@xitu.com','xulanfen@xitu.com','huyaqinhyq@xitu.com','yuzhangying@xitu.com','lb@xitu.com']
    
    #创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("技术中心-高巍", 'utf-8')
    message['To'] =  Header("财务报表", 'utf-8')
    subject = '每周五代理进货总额明细'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是每周五代理进货总额明细，请查收。如有问题可与我联系\n\n\n   说明：1.进货总额为个人及其下级等级为合伙人代理的 所有进货订单总额之和，不含运费 \n      2.计入统计的订单状态为代发货，调货中，待收货，完成。\n\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 totalsales_xlsx 文件
    att1 = MIMEText(open(totalsales_xlsx, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1.add_header("Content-Disposition", "attachment", filename=totalsales_xlsx)
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
    total_cash_final_cash_together()
    sendMail()
    return

if __name__ == '__main__':
    main()