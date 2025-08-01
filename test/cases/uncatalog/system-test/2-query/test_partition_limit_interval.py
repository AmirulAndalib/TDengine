from new_test_framework.utils import tdLog, tdSql

class TestPartitionLimitInterval:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.row_nums = 1000
        cls.tb_nums = 10
        cls.ts = 1537146000000
        cls.dbname = "db1"
        cls.stable = "meters"

    def prepare_datas(self, stb_name , tb_nums , row_nums, dbname="db" ):
        tdSql.execute(f'''create database {self.dbname} MAXROWS 4096 MINROWS 100''')
        tdSql.execute(f'''use {self.dbname}''')
        tdSql.execute(f'''CREATE STABLE {self.dbname}.{self.stable} (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT) TAGS (`groupid` TINYINT, `location` VARCHAR(16))''')
        
        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            tdSql.execute(f"create table {tbname} using {self.dbname}.{self.stable} tags({i} ,'nchar_{i}')")
            tdLog.info(f"create table {tbname} using {self.dbname}.{self.stable} tags({i} ,'nchar_{i}')")
            if i < (self.tb_nums - 2):
                for row in range(row_nums):
                    ts = self.ts + row*1000
                    tdSql.execute(f"insert into {tbname} values({ts} , {row/10}, {215 + (row % 100)})")

                for null in range(5):
                    ts =  self.ts + row_nums*1000 + null*1000
                    tdSql.execute(f"insert into {tbname} values({ts} , NULL , NULL)")

    def basic_query(self):
        tdSql.query(f"select groupid, count(*) from {self.dbname}.{self.stable} partition by groupid interval(1d) limit 100")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 1005)
        
        tdSql.query(f"select groupid, count(*) from {self.dbname}.{self.stable} partition by tbname interval(1d) order by groupid limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)   
        tdSql.checkData(0, 1, 1005)   
        tdSql.checkData(7, 0, 7)  
        tdSql.checkData(7, 1, 1005)  
        
        tdSql.query(f"select groupid, count(*) from {self.dbname}.{self.stable} partition by tbname, groupid interval(5d) order by groupid limit 10")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)   
        tdSql.checkData(0, 1, 1005)   
        tdSql.checkData(7, 0, 7)  
        tdSql.checkData(7, 1, 1005)
        
        tdSql.query(f"select groupid, count(*), min(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) order  by groupid limit 10;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)   
        tdSql.checkData(0, 1, 1005)
        tdSql.checkData(0, 2, 0)  
        tdSql.checkData(7, 0, 7)  
        tdSql.checkData(7, 1, 1005)
        tdSql.checkData(7, 2, 0)
        
        tdSql.query(f"select groupid, min(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 0)
        
        tdSql.query(f"select groupid, avg(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) limit 10000;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, tdSql.getData(7, 1))

        tdSql.query(f"select current, avg(current) from {self.dbname}.{self.stable} partition by current interval(5d) limit 100;")
        tdSql.checkData(0, 0, tdSql.getData(0, 1)) 
        
        tdSql.query(f"select groupid, last(voltage), min(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) limit 10")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, tdSql.getData(7, 1))
        tdSql.checkData(0, 2, tdSql.getData(7, 2))
        
        tdSql.query(f"select groupid, min(current), min(voltage) from {self.dbname}.{self.stable} partition by tbname, groupid interval(5d) limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 0)   
        tdSql.checkData(0, 2, 215)   
        tdSql.checkData(7, 1, 0)  
        tdSql.checkData(7, 2, 215)
        
        tdSql.query(f"select groupid, min(voltage), min(current) from {self.dbname}.{self.stable} partition by tbname, groupid interval(5d) limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 2, 0)   
        tdSql.checkData(0, 1, 215)   
        tdSql.checkData(7, 2, 0)  
        tdSql.checkData(7, 1, 215)           
        
    def test_partition_limit_interval(self):
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

        tdSql.prepare()
        self.prepare_datas("stb",self.tb_nums,self.row_nums)
        self.basic_query()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
