# -*- coding:utf-8 -*-
# @Time    : 2020/9/03
# @Author  : Stephev
# @Site    : 
# @File    : head_payment_order.py
# @Remarks :每日进行团队长货款支付的订单明细


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
head_payment_xlsx = "E:\\csv_file\\"+now_time+"_团队长货款支付订单.xlsx"


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


def headPayment():
    """
    提取所有代理的账户余额
    """
    conn = cnMySQL()
    head_payment_sql = "SELECT\
                        case a.status\
                            when '2' then '待付款'\
                            when '3' then '待审核'\
                            when '4' then '审核失败'\
                            when '6' then '待发货'\
                            when '7' then '调货中'\
                            when '8' then '待收货'\
                            when '9' then '完成'\
                            when '10' then '已取消'\
                            else '其他' end as 订单状态,\
                        a.purchase_order_no 订单编号,\
                        a.create_time 订单创建时间,\
                        a.pay_time 订单支付时间,\
                        a.pay_time 订单确认收款时间,\
                        a.delivery_time 订单发货时间,\
                        a.end_time 订单结束时间,\
                        a.confirm_time 完成时间,\
                    CASE\
                        a.pay_type \
                        WHEN '0' THEN\
                        '无支付' \
                        WHEN '1' THEN\
                        '线下收款' \
                        WHEN '2' THEN\
                        '货款支付' \
                        WHEN '3' THEN\
                        '转单结算' \
                        WHEN '4' THEN\
                        '商户微信支付' ELSE '其他' \
                        END AS 支付方式,\
                        a.total_fee 订单总额,\
                        a.payment 订单实付金额,\
                        a.total_fee 商品总价,\
                        a.post_fee 运费总额,\
                        a.receiver_name 订单收货人,\
                        a.receiver_mobile 订单收货手机,\
                        a.receiver_province_name 订单收货省,\
                        a.receiver_city_name 订单收货市,\
                        a.receiver_district_name 订单收货区,\
                        a.receiver_address 订单详细地址,\
                        b.title 商品名称,\
                        b.goods_id 商品ID,\
                        b.sku_id sku_ID,\
                        b.sku_desc SKU描述,\
                        b.total_fee 单个商品总价,\
                        b.price 商品供货价,\
                        b.num 购买数量,\
                        b.post_fee 单个运费,\
                    CASE\
                        a.receive_type \
                        WHEN '0' THEN\
                        '默认' \
                        WHEN '1' THEN\
                        '快递到消费者' \
                        WHEN '2' THEN\
                        '快递到代理' \
                        WHEN '3' THEN\
                        '云库存收货' ELSE '其他' \
                        END AS 发货类型,\
                        a.source_id 买家代理id,\
                        a.target_id 卖家代理id,\
                        d.NAME 买家转单用户姓名,\
                        d.auth_code 买家授权码,\
                    CASE\
                        d.level_id \
                        WHEN '1' THEN\
                        '分公司' \
                        WHEN '2' THEN\
                        '合伙人' \
                        WHEN '3' THEN\
                        '官方' \
                        WHEN '4' THEN\
                        '省代' \
                        WHEN '5' THEN\
                        '市代' \
                        WHEN '6' THEN\
                        '会员' ELSE '其他' \
                        END AS 代理等级,\
                        a.biz_trade_no 原始单号,\
                        c.payment 原始订单的实付金额,\
                        c.total_fee 原始订单总金额,\
                        e.total_fee 原始订单单个商品总价,\
                        e.price  原始订单的商品单价,\
                        e.num 原始订单的商品数量,\
                    CASE\
                        a.supplier_id \
                        WHEN '0' THEN\
                        '是' ELSE '否' \
                        END AS 是否自营,\
                    CASE\
                        d.region_id \
                        WHEN '1' THEN\
                        '笛梦大区' \
                        WHEN '2' THEN\
                        '环球大区' \
                        WHEN '3' THEN\
                        '辉煌大区' \
                        WHEN '4' THEN\
                        '聚米大区' \
                        WHEN '5' THEN\
                        '聚星大区' \
                        WHEN '6' THEN\
                        '口口大区' \
                        WHEN '7' THEN\
                        '米苏大区' \
                        WHEN '8' THEN\
                        '野狼大区' \
                        WHEN '9' THEN\
                        '海纳百川大区' \
                        WHEN '10' THEN\
                        '红红大区' \
                        WHEN '11' THEN\
                        '熊熊大区' \
                        WHEN '12' THEN\
                        '飞越大区' \
                        WHEN '13' THEN\
                        '测试大区' ELSE '其他' \
                        END AS 所属大区 \
                    FROM\
                        prod_order.rupt_purchase_order a,\
                        prod_order.rupt_purchase_order_item b,\
                        prod_user.renren_distributor d,\
                        prod_order.rupt_purchase_order c,\
                        prod_order.rupt_purchase_order_item e\
                    WHERE\
                        a.purchase_order_no = b.purchase_order_no \
                        AND a.source_id = d.id \
                        AND c.purchase_order_no = e.purchase_order_no\
                        AND a.biz_trade_no = c.purchase_order_no\
                        AND b.sku_id = e.sku_id\
                        AND a.pay_type IN ( 3,2 ) \
                        AND a.target_type = 1\
                        AND a.create_time > '2020-06-01 00:00:00'\
                        AND a.biz_trade_type = 2\
                    union all\
                    SELECT\
                        case a.status\
                            when '2' then '待付款'\
                            when '3' then '待审核'\
                            when '4' then '审核失败'\
                            when '6' then '待发货'\
                            when '7' then '调货中'\
                            when '8' then '待收货'\
                            when '9' then '完成'\
                            when '10' then '已取消'\
                            else '其他' end as 订单状态,\
                        a.purchase_order_no 订单编号,\
                        a.create_time 订单创建时间,\
                        a.pay_time 订单支付时间,\
                        a.pay_time 订单确认收款时间,\
                        a.delivery_time 订单发货时间,\
                        a.end_time 订单结束时间,\
                        a.confirm_time 完成时间,\
                    CASE\
                        a.pay_type \
                        WHEN '0' THEN\
                        '无支付' \
                        WHEN '1' THEN\
                        '线下收款' \
                        WHEN '2' THEN\
                        '货款支付' \
                        WHEN '3' THEN\
                        '转单结算' \
                        WHEN '4' THEN\
                        '商户微信支付' ELSE '其他' \
                        END AS 支付方式,\
                        a.total_fee 订单总额,\
                        a.payment 订单实付金额,\
                        a.total_fee 商品总价,\
                        a.post_fee 运费总额,\
                        a.receiver_name 订单收货人,\
                        a.receiver_mobile 订单收货手机,\
                        a.receiver_province_name 订单收货省,\
                        a.receiver_city_name 订单收货市,\
                        a.receiver_district_name 订单收货区,\
                        a.receiver_address 订单详细地址,\
                        b.title 商品名称,\
                        b.goods_id 商品ID,\
                        b.sku_id sku_ID,\
                        b.sku_desc SKU描述,\
                        b.total_fee 单个商品总价,\
                        b.price 商品供货价,\
                        b.num 购买数量,\
                        b.post_fee 单个运费,\
                    CASE\
                        a.receive_type \
                        WHEN '0' THEN\
                        '默认' \
                        WHEN '1' THEN\
                        '快递到消费者' \
                        WHEN '2' THEN\
                        '快递到代理' \
                        WHEN '3' THEN\
                        '云库存收货' ELSE '其他' \
                        END AS 发货类型,\
                        a.source_id 买家代理id,\
                        a.target_id 卖家代理id,\
                        d.NAME 买家转单用户姓名,\
                        d.auth_code 买家授权码,\
                    CASE\
                        d.level_id \
                        WHEN '1' THEN\
                        '分公司' \
                        WHEN '2' THEN\
                        '合伙人' \
                        WHEN '3' THEN\
                        '官方' \
                        WHEN '4' THEN\
                        '省代' \
                        WHEN '5' THEN\
                        '市代' \
                        WHEN '6' THEN\
                        '会员' ELSE '其他' \
                        END AS 代理等级,\
                        a.biz_trade_no 原始单号,\
                        c.payment 原始订单的实付金额,\
                        c.total_fee 原始订单总价,\
                        e.total_fee 原始订单单个商品总价,\
                        e.price  原始订单的商品单价,\
                        e.num 原始订单的商品数量,\
                        \
                    CASE\
                        a.supplier_id \
                        WHEN '0' THEN\
                        '是' ELSE '否' \
                        END AS 是否自营,\
                    CASE\
                        d.region_id \
                        WHEN '1' THEN\
                        '笛梦大区' \
                        WHEN '2' THEN\
                        '环球大区' \
                        WHEN '3' THEN\
                        '辉煌大区' \
                        WHEN '4' THEN\
                        '聚米大区' \
                        WHEN '5' THEN\
                        '聚星大区' \
                        WHEN '6' THEN\
                        '口口大区' \
                        WHEN '7' THEN\
                        '米苏大区' \
                        WHEN '8' THEN\
                        '野狼大区' \
                        WHEN '9' THEN\
                        '海纳百川大区' \
                        WHEN '10' THEN\
                        '红红大区' \
                        WHEN '11' THEN\
                        '熊熊大区' \
                        WHEN '12' THEN\
                        '飞越大区' \
                        WHEN '13' THEN\
                        '测试大区' ELSE '其他' \
                        END AS 所属大区 \
                    FROM\
                        prod_order.rupt_purchase_order a,\
                        prod_order.rupt_purchase_order_item b,\
                        prod_user.renren_distributor d,\
                        prod_order.rupt_shop_trade  c,\
                        prod_order.rupt_shop_order e\
                    WHERE\
                        a.purchase_order_no = b.purchase_order_no \
                        AND a.source_id = d.id \
                        AND a.biz_trade_no = c.shop_trade_no\
                        AND c.shop_trade_no = e.shop_trade_no\
                        AND b.sku_id = e.sku_id\
                        AND a.pay_type IN ( 3,2 ) \
                        AND a.target_type = 1\
                        AND a.create_time > '2020-06-01 00:00:00'\
                        AND a.biz_trade_type = 1"
    head_payment = conn.ExecQuery(head_payment_sql)
    headers = ['订单状态','订单编号','订单创建时间','订单支付时间','订单确认收款时间','订单发货时间','订单结束时间','完成时间','支付方式','订单总额','订单实付金额','商品总价','运费总额','订单收货人','订单收货手机','订单收货省','订单收货市','订单收货区','订单详细地址','商品名称','商品ID','sku_ID','SKU描述','单个商品总价','商品供货价','购买数量','单个运费','发货类型','买家代理id','卖家代理id','买家转单用户姓名','买家授权码','代理等级','原始单号','原始订单的实付金额','原始订单总价','原始订单单个商品总价','原始订单的商品单价','原始订单的商品数量','是否自营','所属大区']
    dt = pd.DataFrame(head_payment,columns=headers)
    dt.to_excel(head_payment_xlsx,index=0)
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
    message['To'] =  Header("团队长货款支付订单明细", 'utf-8')
    subject = '团队长货款支付的订单明细'
    message['Subject'] = Header(subject, 'utf-8')

    #邮件正文内容
    message.attach(MIMEText('hi:\n    附件是今日团队长货款支付订单明细，请查收.如有问题可与我联系\n\n\n说明：1、订单所有涉及金额的单位均为分\n     2、订单的时间为从20200601至今\n\n技术中心-高巍', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 head_payment_xlsx 文件
    att1 = MIMEText(open(head_payment_xlsx, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1.add_header("Content-Disposition", "attachment", filename=head_payment_xlsx)
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
    headPayment()
    sendMail()
    return

if __name__ == '__main__':
    main()


