# -*- coding:utf-8 -*-
# @Time    : 2020/10/27
# @Author  : Stephev
# @Site    : 
# @File    : anliti_member.py
# @Remarks :安丽缇代理链路数据提取


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
anlitimember_xlsx = "D:\\workpalce\\csv_file\\"+now_time+"_安丽缇代理链路信息.xlsx"
file_name = "安丽缇代理链路信息"+now_time+".xlsx"


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


def anlitiMember():
    """
    判断安丽缇每个代理的上家和上级代理信息
    """
    conn = cnMySQL()
    final_xlsx_rows = []
    anliti_id = conn.ExecQuery("select id from renren_distributor where shop_id = 1195")
    for i in anliti_id:
        altid = i['id']  #取出一个代理的ID
        alit_info = conn.ExecQuery("select level_id,ref_id from renren_distributor where id = %s",altid)
        if alit_info[0]['ref_id'] == 0:  #上级直接为空的，即没有上级，其他字段都以null填充
            alt_rows_0 = conn.ExecQuery("SELECT a.NAME 代理姓名,\
                                            a.auth_code 代理授权码,\
                                            e.mobile 代理手机号,\
                                        CASE\
                                            a.level_id \
                                            WHEN '7' THEN\
                                            '社长' \
                                            WHEN '8' THEN\
                                            '部长' \
                                            WHEN '9' THEN\
                                            '次长' \
                                            WHEN '10' THEN\
                                            '课长' \
                                            ELSE '其他' \
                                            END AS 代理等级,\
                                            'null' 上家代理名,\
                                            'null' 上家代理授权码,\
                                            'null' 上家代理等级,\
                                            'null' 上上家代理名,\
                                            'null' 上上家授权码,\
                                            'null' 上上家代理等级,\
                                            'null' 上级代理名,\
                                            'null' 上级代理授权码,\
                                            'null' 上级代理等级\
                                        FROM\
                                            renren_distributor a,\
                                            user e \
                                        WHERE\
                                            a.account_id = e.id\
                                            AND a.shop_id = 1195 and a.id = %s",altid)
            final_xlsx_rows.extend(alt_rows_0)
            print(final_xlsx_rows)
        else:
            alit_info_1 = conn.ExecQuery("SELECT  a.level_id as level_id,d.level_id  level_id1,d.ref_id ref_id FROM renren_distributor a,renren_distributor d  WHERE a.ref_id = d.id AND a.shop_id = 1195 and a.id = %s",altid)
            if alit_info_1[0]['ref_id'] == 0:     #上上家直接为空，有2种情况，一种是平级，一种是比上家等级低
                if alit_info_1[0]['level_id'] > alit_info_1[0]['level_id1']:  # 上家比自己要等级要高
                    alt_rows_1 = conn.ExecQuery("SELECT a.NAME 代理姓名, \
										a.auth_code 代理授权码,\
                                           e.mobile 代理手机号,\
                                        CASE\
                                            a.level_id \
                                            WHEN '7' THEN\
                                            '社长' \
                                            WHEN '8' THEN\
                                            '部长' \
                                            WHEN '9' THEN\
                                            '次长' \
                                            WHEN '10' THEN\
                                            '课长' \
                                            ELSE '其他' \
                                            END AS 代理等级,\
                                            d.name 上家代理名,\
                                            d.auth_code 上家代理授权码,\
                                            CASE\
                                            d.level_id \
                                            WHEN '7' THEN\
                                            '社长' \
                                            WHEN '8' THEN\
                                            '部长' \
                                            WHEN '9' THEN\
                                            '次长' \
                                            WHEN '10' THEN\
                                            '课长' \
                                            ELSE '其他' \
                                            END AS 上家代理等级,\
                                            'null' 上上家代理名,\
                                            'null' 上上家授权码,\
                                            'null' 上上家代理等级,\
                                            d.name 上级代理名,\
                                            d.auth_code 上级代理授权码,\
                                            CASE\
                                            d.level_id \
                                            WHEN '7' THEN\
                                            '社长' \
                                            WHEN '8' THEN\
                                            '部长' \
                                            WHEN '9' THEN\
                                            '次长' \
                                            WHEN '10' THEN\
                                            '课长' \
                                            ELSE '其他' \
                                            END AS  上级代理等级\
                                        FROM\
                                            renren_distributor a,\
											renren_distributor d,\
                                            user e \
                                        WHERE\
											a.ref_id = d.id \
                                            AND a.account_id = e.id\
                                            AND a.shop_id = 1195 and a.id = %s",altid)
                    print(alt_rows_1)
                    final_xlsx_rows.extend(alt_rows_1)
                else:      #上家和自己等级一样，那么就上上家和上级信息为空
                    alt_rows_2 = conn.ExecQuery("SELECT a.NAME 代理姓名, \
										a.auth_code 代理授权码,\
                                           e.mobile 代理手机号,\
                                        CASE\
                                            a.level_id \
                                            WHEN '7' THEN\
                                            '社长' \
                                            WHEN '8' THEN\
                                            '部长' \
                                            WHEN '9' THEN\
                                            '次长' \
                                            WHEN '10' THEN\
                                            '课长' \
                                            ELSE '其他' \
                                            END AS 代理等级,\
                                            d.name 上家代理名,\
                                            d.auth_code 上家代理授权码,\
                                            CASE\
                                            d.level_id \
                                            WHEN '7' THEN\
                                            '社长' \
                                            WHEN '8' THEN\
                                            '部长' \
                                            WHEN '9' THEN\
                                            '次长' \
                                            WHEN '10' THEN\
                                            '课长' \
                                            ELSE '其他' \
                                            END AS 上家代理等级,\
                                            'null' 上上家代理名,\
                                            'null' 上上家授权码,\
                                            'null' 上上家代理等级,\
                                            'null' 上级代理名,\
                                            'null' 上级代理授权码,\
                                            'null' 上级代理等级\
                                        FROM\
                                            renren_distributor a,\
											renren_distributor d,\
                                            prod_user.user e \
                                        WHERE\
											a.ref_id = d.id \
                                            AND a.account_id = e.id\
                                            AND a.shop_id = 1195 and a.id = %s",altid)
                    final_xlsx_rows.extend(alt_rows_2)
            else:    #上上家信息不为空，那么上家信息都得全，上级信息需要进行判断
                if alit_info[0]['level_id'] == 7:   #如果已经是社长了，只显示上家和上上家信息，没有上级信息
                    alt_rows_5 = conn.ExecQuery("SELECT a.NAME 代理姓名, \
                                                a.auth_code 代理授权码,\
                                                e.mobile 代理手机号,\
                                                CASE\
                                                    a.level_id \
                                                    WHEN '7' THEN\
                                                    '社长' \
                                                    WHEN '8' THEN\
                                                    '部长' \
                                                    WHEN '9' THEN\
                                                    '次长' \
                                                    WHEN '10' THEN\
                                                    '课长' \
                                                    ELSE '其他' \
                                                    END AS 代理等级,\
                                                    d.name 上家代理名,\
                                                    d.auth_code 上家代理授权码,\
                                                    CASE\
                                                    d.level_id \
                                                    WHEN '7' THEN\
                                                    '社长' \
                                                    WHEN '8' THEN\
                                                    '部长' \
                                                    WHEN '9' THEN\
                                                    '次长' \
                                                    WHEN '10' THEN\
                                                    '课长' \
                                                    ELSE '其他' \
                                                    END AS 上家代理等级,\
                                                    c.name 上上家代理名,\
                                                    c.auth_code 上上家授权码,\
                                                    case c.level_id \
                                                    WHEN '7' THEN\
                                                    '社长' \
                                                    WHEN '8' THEN\
                                                    '部长' \
                                                    WHEN '9' THEN\
                                                    '次长' \
                                                    WHEN '10' THEN\
                                                    '课长' \
                                                    ELSE '其他' \
                                                    END AS  上上家代理等级,\
                                                    'null' 上级代理名,\
                                                    'null' 上级代理授权码,\
                                                    'null' 上级代理等级\
                                                FROM\
                                                    renren_distributor a,\
                                                    renren_distributor d,\
                                                    renren_distributor c,\
                                                    prod_user.user e \
                                                WHERE\
                                                    a.ref_id = d.id \
                                                    AND d.ref_id = c.id\
                                                    AND a.account_id = e.id\
                                                    AND a.shop_id = 1195 and a.id = %s",altid)
                    print(alt_rows_5)
                    final_xlsx_rows.extend(alt_rows_5)
                else:
                    alt_rows_3 = conn.ExecQuery("SELECT a.NAME 代理姓名, \
                                            a.auth_code 代理授权码,\
                                            e.mobile 代理手机号,\
                                            CASE\
                                                a.level_id \
                                                WHEN '7' THEN\
                                                '社长' \
                                                WHEN '8' THEN\
                                                '部长' \
                                                WHEN '9' THEN\
                                                '次长' \
                                                WHEN '10' THEN\
                                                '课长' \
                                                ELSE '其他' \
                                                END AS 代理等级,\
                                                d.name 上家代理名,\
                                                d.auth_code 上家代理授权码,\
                                                CASE\
                                                d.level_id \
                                                WHEN '7' THEN\
                                                '社长' \
                                                WHEN '8' THEN\
                                                '部长' \
                                                WHEN '9' THEN\
                                                '次长' \
                                                WHEN '10' THEN\
                                                '课长' \
                                                ELSE '其他' \
                                                END AS 上家代理等级,\
                                                c.name 上上家代理名,\
                                                c.auth_code 上上家授权码,\
                                                CASE\
                                                c.level_id \
                                                WHEN '7' THEN\
                                                '社长' \
                                                WHEN '8' THEN\
                                                '部长' \
                                                WHEN '9' THEN\
                                                '次长' \
                                                WHEN '10' THEN\
                                                '课长' \
                                                ELSE '其他' \
                                                END AS  上上家代理等级\
                                            FROM\
                                                renren_distributor a,\
                                                renren_distributor d,\
                                                renren_distributor c,\
                                                prod_user.user e \
                                            WHERE\
                                                a.ref_id = d.id \
                                                AND d.ref_id = c.id\
                                                AND a.account_id = e.id\
                                                AND a.shop_id = 1195 and a.id = %s",altid)
                    # 上家部分正常直接提取，上级部分就需要进行判断，找出上级id，并查出信息，然后合并rows
                    a_id = i['id']
                    while True:
                        alit_info_3 = conn.ExecQuery("SELECT  a.level_id as level_id,d.level_id  level_id1,d.id new_id\
                            FROM renren_distributor a,renren_distributor d  WHERE a.ref_id = d.id AND a.shop_id = 1195 and a.id = %s",a_id)
                        if alit_info_3[0]['level_id'] > alit_info_3[0]['level_id1']:
                            final_id = alit_info_3[0]['new_id']
                            break
                        a_id = alit_info_3[0]['new_id']
                    alt_rows_4 = conn.ExecQuery("select name 上级代理名,\
                                        auth_code 上级代理授权码,\
                                        CASE\
                                        level_id \
                                        WHEN '7' THEN\
                                        '社长' \
                                        WHEN '8' THEN\
                                        '部长' \
                                        WHEN '9' THEN\
                                        '次长' \
                                        WHEN '10' THEN\
                                        '课长' \
                                        ELSE '其他' \
                                        END AS  上级代理等级\
                                    FROM\
                                        renren_distributor \
                                        WHERE  id = %s",final_id)
                    alt_rows_3[0].update(alt_rows_4[0])
                    final_xlsx_rows.extend(alt_rows_3)

    headers = ['代理姓名','代理授权码','代理手机号','代理等级','上家代理名','上家代理授权码','上家代理等级','上上家代理名','上上家授权码','上上家代理等级','上级代理名','上级代理授权码','上级代理等级']
    dt = pd.DataFrame(final_xlsx_rows,columns=headers)
    dt.to_excel(anlitimember_xlsx,index=0)
    return


def sendMail():
    mail_host='smtp.qq.com'
    mail_user='1058582934@qq.com'
    mail_pass='wcasswmrgsnobdgh'

    sender='1058582934@qq.com'
    receivers = ['gaowei@xitu.com','gfy@xitu.com']
    #receivers=['gaowei@xitu.com','huyaqinhyq@xitu.com','yuzhangying@xitu.com','ht@xitu.com']
    
    #创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("技术中心-高巍", 'utf-8')
    message['To'] =  Header("财务报表", 'utf-8')
    subject = '安丽缇代理链路信息提取'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是安丽缇代理链路信息,请查收。如有问题可与我联系\n\n\n   说明：1.上家和上级为空时，均以null 代替\n\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 totalsales_xlsx 文件
    att1 = MIMEText(open(anlitimember_xlsx, 'rb').read(), 'base64', 'utf-8')
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
    anlitiMember()
    sendMail()
    return

if __name__ == '__main__':
    main()