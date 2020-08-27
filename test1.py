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


def total_cash_final():
    """
    循环找出所有下级代理
    """
    conn = cnMySQL()
    final_rows = []
    #upper_id_tuple = (409,419,422,432,434,441,443,447,451,456,467,481,485,487,488,490,492,507,27702,30878,95324)
    upper_id_tuple = (432,)
    while True:
        #check_sql = 'select id from renren_distributor where  ref_id in '' and level_id =3'
        rows = conn.ExecQuery("select id from renren_distributor where  ref_id in %s and level_id =2",upper_id_tuple)
        if not rows:
            break
        final_rows.extend(rows)
        upper_id_tuple=tuple(row['id'] for row in rows)
    print(final_rows)





def total_cash_final_cash(proxy_id):
    """
    循环找出所有下级代理，并计算他们的销售总额
    """
    conn = cnMySQL()
    final_rows = []
    #upper_id_tuple = (409,419,422,432,434,441,443,447,451,456,467,481,485,487,488,490,492,507,27702,30878,95324)
    cansai_id = (proxy_id,)
    upper_id_tuple = (proxy_id,)
    while True:
        rows = conn.ExecQuery("select id from renren_distributor where  ref_id in %s and level_id =2",upper_id_tuple)
        if not rows:
            break
        final_rows.extend(rows)
        upper_id_tuple=tuple(row['id'] for row in rows)
    print(final_rows)
    #获取sql的where条件的 tuple
    sql_condition_1 = tuple(i['id'] for i in final_rows)
    sql_condition = cansai_id + sql_condition_1
    print(sql_condition)
    total_cash_result = conn.ExecQuery("select  a.NAME 代理姓名,\
                                        a.auth_code 授权码,\
                                        case a.level_id\
                                            when '1' then '分公司'\
                                            when '2' then '合伙人'\
                                            when '3' then '官方'\
                                            when '4' then '省代'\
                                            when '5' then '市代'\
                                            when '6' then '会员'\
                                            else '其他' end as 代理等级,\
                                        sum(b.payment)  订单总额单位分,\
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
                                        prod_user.renren_distributor a,\
                                        prod_order.rupt_purchase_order b\
                                    where  a.id = b.source_id \
                                    and b.status in (6,7,8,9) and a.id in  %s",sql_condition)
    print(total_cash_result)
    #查询一个代理的销售总额并返回结果
    return total_cash_result

def totalStatistics():
    """
    分别查询每个参赛代理的销售总额，形成最终的报表并写进xlsx文件
    """
    canshai_id_tuple = (409,419,422,432,434,441,443,447,451,456,467,481,485,487,488,490,492,507,27702,30878,95324)
    Statistics_rows = []
    for i  in  canshai_id_tuple:
        totoal_cash_result = total_cash_final_cash(i)
        Statistics_rows.append(total_cash_result)
    # 将最后结果写进文件
    headers = ['代理姓名','授权码','代理等级','订单总额单位分','进货人所属大区']
    dt = pd.DataFrame(sales,columns=headers)
    dt.to_excel(totalsales_xlsx,index=0)
    return


def total_cash_final_cash_together():
    """
    循环找出所有下级代理，并计算他们的销售总额
    """
    #canshai_id_tuple = (409,419,422,432,434,441,443,447,451,456,467,481,485,487,488,490,492,507,27702,30878,95324)
    canshai_id_tuple = (419,)
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
        print(final_rows)
        #获取sql的where条件的 tuple
        sql_condition_1 = tuple(i['id'] for i in final_rows)
        sql_condition = cansai_id + sql_condition_1
        print(sql_condition)
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
                                            aa.total_cash\
                                    from  prod_user.renren_distributor d,(select  \
                                        sum(b.payment)  total_cash\
                                    from \
                                        prod_user.renren_distributor a,\
                                        prod_order.rupt_purchase_order b\
                                    where  a.id = b.source_id \
                                    and b.status in (6,7,8,9) and a.id in  %s) aa where d.id in %s",sql_condition,cansai_id)
        print(total_cash_result)
        Statistics_rows.extend(total_cash_result)
    print(Statistics_rows)
    return 

def total_cash_final_cash(proxy_id):
    """
    循环找出所有下级代理，并计算他们的销售总额
    """
    conn = cnMySQL()
    final_rows = []
    cansai_id = (proxy_id,)
    upper_id_tuple = (proxy_id,)
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
                                            aa.total_cash\
                                    from  prod_user.renren_distributor d,(select  \
                                        sum(b.payment)  total_cash\
                                    from \
                                        prod_user.renren_distributor a,\
                                        prod_order.rupt_purchase_order b\
                                    where  a.id = b.source_id \
                                    and b.status in (6,7,8,9) and a.id in  %s) aa where d.id in %s",(sql_condition,))
    #print(total_cash_result)
    #查询一个代理的销售总额并返回结果
    return total_cash_result

def totalStatistics():
    """
    分别查询每个参赛代理的销售总额，形成最终的报表并写进xlsx文件
    """
    canshai_id_tuple = (409,419,422,432,434,441,443,447,451,456,467,481,485,487,488,490,492,507,27702,30878,95324)
    Statistics_rows = []
    for i  in  canshai_id_tuple:
        totoal_cash_result_xlsx = total_cash_final_cash(i)
        Statistics_rows.append(total_cash_result_xlsx)
    print(Statistics_rows)
    # 将最后结果写进文件
    headers = ['代理姓名','授权码','代理等级','订单总额单位分','进货人所属大区']
    dt = pd.DataFrame(Statistics_rows,columns=headers)
    dt.to_excel(totalsales_xlsx,index=0)
    return


def main():
    total_cash_final_cash_together()
    return

if __name__ == '__main__':
    main()

