from new_test_framework.utils import tdLog, tdSql
import numpy as np
import random

class TestDistributeAggApercentile:

    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.vnode_disbutes = None
        cls.ts = 1537146000000

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 ss_keeplocal 3000 vgroups 5")
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        for i in range(20):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )

        for i in range(1,21):
            if i ==1 or i == 4:
                continue
            else:
                tbname = f"{dbname}.ct{i}"
                for j in range(9):
                    tdSql.execute(
                f"insert into {tbname} values ( now()-{(i+j)*10}s, {1*(j+i)}, {11111*(j+i)}, {111*(j+i)}, {11*(j)}, {1.11*(j+i)}, {11.11*(j+i)}, {(j+i)%2}, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
            )
        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdLog.info(" prepare data for distributed_aggregate done! ")

    def check_distribute_datas(self, dbname="testdb"):
        # get vgroup_ids of all
        tdSql.query(f"show {dbname}.vgroups ")
        vgroups = tdSql.queryResult

        vnode_tables={}

        for vgroup_id in vgroups:
            vnode_tables[vgroup_id[0]]=[]


        # check sub_table of per vnode ,make sure sub_table has been distributed
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}' and table_name like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            vnode_tables[table_name[6]].append(table_name[0])
        self.vnode_disbutes = vnode_tables

        count = 0
        for k ,v in vnode_tables.items():
            if len(v)>=2:
                count+=1
        if count < 2:
            tdLog.exit(" the datas of all not satisfy sub_table has been distributed ")

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select apercentile(c1 , 20) from {dbname}.stb1 where c1 is null")
        tdSql.checkRows(1)

        tdSql.query(f"select apercentile(c1 , 20) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,2.800000000)

        tdSql.query(f"select apercentile(c1+c2 ,100) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select apercentile(c1 ,10 ) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0,0,2.000000000)

        tdSql.query(f"select apercentile(c1,20) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select apercentile(c1,20) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select apercentile(c1,20) from {dbname}.stb1 union all select apercentile(c1,20) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,7.389181281)

        # join

        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table db.tb1 using db.st tags(1) ")
        tdSql.execute(" create table db.tb2 using db.st tags(2) ")


        for i in range(10):
            ts = i*10 + self.ts
            tdSql.execute(f" insert into db.tb1 values({ts},{i},{i}.0)")
            tdSql.execute(f" insert into db.tb2 values({ts},{i},{i}.0)")

        tdSql.query(f"select apercentile(tb1.c1,100), apercentile(tb2.c2,100) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,9.000000000)
        tdSql.checkData(0,0,9.000000000)

        # group by
        tdSql.execute(f"use {dbname} ")
        tdSql.query(f" select max(c1),c1  from {dbname}.stb1 group by t1 ")
        tdSql.checkRows(20)
        tdSql.query(f" select max(c1),c1  from {dbname}.stb1 group by c1 ")
        tdSql.checkRows(30)
        tdSql.query(f" select max(c1),c2  from {dbname}.stb1 group by c2 ")
        tdSql.checkRows(31)

        # partition by tbname or partition by tag
        tdSql.query(f"select apercentile(c1 ,10)from {dbname}.stb1 partition by tbname")
        query_data = tdSql.queryResult

        # nest query for support max
        #tdSql.query(f"select apercentile(c2+2,10)+1 from (select max(c1) c2  from {dbname}.stb1)")
        #tdSql.checkData(0,0,31.000000000)
        tdSql.query(f"select apercentile(c1+2,10)+1  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,7.560701700)
        tdSql.query(f"select apercentile(a+2,10)+1  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,7.560701700)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3),spread(c1), apercentile(c1,10) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)
        tdSql.checkData(0,4,28.000000000)
        tdSql.checkData(0,5,4.560701700)

    def test_distribute_agg_apercentile(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.distribute_agg_query()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
