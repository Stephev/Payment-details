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
        self._dbhost = 'rm-bp1h051g0br862eswfo.mysql.rds.aliyuncs.com'
        self._dbuser = 'python_dba'
        self._dbpassword = 'Python_dba1'
        self._dbname = 'uat_order'
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

def noForward():
    """
    写入还没有转交订单
    """
    conn = cnMySQL()
    no_forward = "select  b.purchase_order_no 订单编号,\
	                case b.status\
		            when '2' then '待付款'\
                    when '3' then '待审核'\
                    when '4' then '审核失败'\
		            when '6' then '代发货'\
		            when '7' then '调货中'\
		            when '8' then '待收货'\
		            when '9' then '完成'\
		            when '10' then '已取消'\
		            else '其他' end as 订单状态,\
	                b.submit_time  订单提交时间,\
	                case b.target_type\
		                when '1' then '总部'\
		                when '2' then '代理'\
                     else '其他' end  as 出货人类型,\
	                b.transfer_to 总部订单号,\
	                c.title 商品名称,\
                    c.goods_id 商品id,\
                    c.sku_desc 商品描述,\
                    c.sku_id skuid,\
                    c.num  单品购买数量,\
                    c.price 进货单价,\
	                c.total_fee 总金额,\
	                b.payment  实际付款总额,\
	                c.post_fee 运费,\
	                a.NAME 代理姓名,\
                	a.auth_code 授权码,\
	                case a.level_id\
                    when '1' then '分公司'\
                    when '2' then '合伙人'\
                    when '3' then '官方'\
                    when '4' then '省代'\
                    when '5' then '市代'\
                    when '6' then '会员'\
                    else '其他' end as 代理等级,\
                    a.id 买家id,\
                    case a.region_id\
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
                    from uat_user.renren_distributor a,\
                    uat_order.rupt_purchase_order b,\
                    uat_order.rupt_purchase_order_item  c\
                where  a.id = b.source_id and b.purchase_order_no = c.purchase_order_no  and b.pay_type = 2 and b.submit_time >= '2020-07-28 00:00:00' and b.is_transfer = 0;"
    noresult = conn.ExecQuery(no_forward)
    print(noresult)
    noforward_csv = "D:\\workpalce\\csv_file\\"+now_time+".csv"
    headers = ['订单编号','订单状态','订单提交时间','出货人类型','总部订单号','商品名称','商品id','商品描述','skuid','单品购买数量','进货单价','总金额',\
                '实际付款总额','运费','代理姓名','授权码','代理等级','买家id','进货人所属大区']
    with  open(noforward_csv,'w',newline='') as f:
        f_csv = csv.DictWriter(f,headers)
        f_csv.writeheader()
        f_csv.writerows(noresult)
    f.close()
    return


def isForward():
    """
    已转交订单，查找其总部订单号
    """
    rows = []
    conn = cnMySQL()
    no_forward = "select  b.purchase_order_no 订单编号,\
	                case b.status\
		            when '2' then '待付款'\
                    when '3' then '待审核'\
                    when '4' then '审核失败'\
		            when '6' then '代发货'\
		            when '7' then '调货中'\
		            when '8' then '待收货'\
		            when '9' then '完成'\
		            when '10' then '已取消'\
		            else '其他' end as 订单状态,\
	                b.submit_time  订单提交时间,\
	                case b.target_type\
		                when '1' then '总部'\
		                when '2' then '代理'\
                     else '其他' end  as 出货人类型,\
	                b.transfer_to 总部订单号,\
	                c.title 商品名称,\
                    c.goods_id 商品id,\
                    c.sku_desc 商品描述,\
                    c.sku_id skuid,\
                    c.num  单品购买数量,\
                    c.price 进货单价,\
	                c.total_fee 总金额,\
	                b.payment  实际付款总额,\
	                c.post_fee 运费,\
	                a.NAME 代理姓名,\
                	a.auth_code 授权码,\
	                case a.level_id\
                    when '1' then '分公司'\
                    when '2' then '合伙人'\
                    when '3' then '官方'\
                    when '4' then '省代'\
                    when '5' then '市代'\
                    when '6' then '会员'\
                    else '其他' end as 代理等级,\
                    a.id 买家id,\
                    case a.region_id\
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
                    from uat_user.renren_distributor a,\
                    uat_order.rupt_purchase_order b,\
                    uat_order.rupt_purchase_order_item  c\
                where  a.id = b.source_id and b.purchase_order_no = c.purchase_order_no  and b.pay_type = 2 and b.submit_time >= '2020-07-28 00:00:00' and b.is_transfer = 0;"
    noresult = conn.ExecQuery(no_forward)
    for i in noresult:
        rows.append(i)

    is_forward = "select  b.purchase_order_no 订单编号,\
                        case b.status\
                            when '2' then '待付款'\
                            when '3' then '待审核'\
                            when '4' then '审核失败'\
                            when '6' then '代发货'\
                            when '7' then '调货中'\
                            when '8' then '待收货'\
                            when '9' then '完成'\
                            when '10' then '已取消'\
                            else '其他' end as 订单状态,\
                        b.submit_time  订单提交时间,\
                        case b.target_type \
                            when '1' then '总部' \
                            when '2' then '代理'\
                            else '其他' end  as 出货人类型,\
                        b.transfer_to 总部订单号,\
                        c.title 商品名称,\
                        c.goods_id 商品id,\
                        c.sku_desc 商品描述,\
                        c.sku_id skuid,\
                        c.num  单品购买数量,\
                        c.price 进货单价,\
                        c.total_fee 总金额,\
                        b.payment  实际付款总额,\
                        c.post_fee 运费,\
                        a.NAME 代理姓名,\
                        a.auth_code 授权码,\
                        case a.level_id\
                            when '1' then '分公司'\
                            when '2' then '合伙人'\
                            when '3' then '官方'\
                            when '4' then '省代'\
                            when '5' then '市代'\
                            when '6' then '会员'\
                            else '其他' end as 代理等级,\
                        a.id 买家id,\
                        case a.region_id\
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
                        from \
                        uat_user.renren_distributor a,\
                        uat_order.rupt_purchase_order b,\
                        uat_order.rupt_purchase_order_item  c\
                    where  a.id = b.source_id \
                    and b.purchase_order_no = c.purchase_order_no \
                    and b.pay_type = 2 and b.submit_time >= '2020-07-28 00:00:00' and b.target_type = 2 and b.is_transfer = 1"

    isresult = conn.ExecQuery(is_forward)
    for i in isresult:
        up1_level =  i['总部订单号']
        print(up1_level)
        check_sql = "select purchase_order_no,target_type,transfer_to from uat_order.rupt_purchase_order where purchase_order_no = \'"+up1_level+"\'"
        check_result = conn.ExecQuery(check_sql)
        print(check_result)
        up1_targettype = check_result[0]['target_type']
        if up1_targettype == 1:
            rows.append(i)
        else:
            continue
        while up1_targettype == 2:
            check_sql_1 = "select target_type,transfer_to from uat_order.rupt_purchase_order where purchase_order_no = \'"+up1_level+"\'"
            check_result_1 = conn.ExecQuery(check_sql_1)
            up1_level = check_result_1['transfer_to']
            up1_targettype = check_result_1['target_type']
            if up1_targettype == 1:
                i['总部订单号'] = up1_level
                rows.append(i)
                break
    isforward_csv = "D:\\workpalce\\csv_file\\"+now_time+".csv"
    headers = ['订单编号','订单状态','订单提交时间','出货人类型','总部订单号','商品名称','商品id','商品描述','skuid','单品购买数量','进货单价','总金额',\
                '实际付款总额','运费','代理姓名','授权码','代理等级','买家id','进货人所属大区']
    with  open(isforward_csv,'w',newline='') as f:
        f_csv = csv.DictWriter(f,headers)
        f_csv.writeheader()
        f_csv.writerows(rows)
    f.close()



    return

def main():
    #noForward()
    isForward()
    return

if __name__ == '__main__':
    main()