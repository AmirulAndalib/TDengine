from wsgiref.headers import tspecials
import numpy as np
from new_test_framework.utils import tdLog, tdSql

class TestMin:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.ts = 1537146000000

    def test_min(self):
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

        dbname = "db"
        tdSql.prepare()

        intData = []
        floatData = []

        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(f'''create table {dbname}.ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.ntb values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            intData.append(i + 1)
            floatData.append(i + 0.1)
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"
                        % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            intData.append(i + 1)
            floatData.append(i + 0.1)

        # max verifacation
        tdSql.error(f"select min(now()) from {dbname}.stb_1")
        tdSql.error(f"select min(ts) from {dbname}.stb_1")
        tdSql.error(f"select min(col7) from {dbname}.stb_1")
        tdSql.error(f"select min(a) from {dbname}.stb_1")
        tdSql.query(f"select min(1) from {dbname}.stb_1")
        tdSql.error(f"select min(count(c1),count(c2)) from {dbname}.stb_1")

        tdSql.query(f"select min(col1) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col2) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col3) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col4) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col11) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col12) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col13) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col14) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col5) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col6) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col1) from {dbname}.stb_1 where col2>=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select min(col8) from {dbname}.stb_1")
        tdSql.checkData(0,0,'taosdata1')
        tdSql.query(f"select min(col9) from {dbname}.stb_1")
        tdSql.checkData(0,0,'涛思数据1')

        tdSql.error(f"select min(now()) from {dbname}.stb_1")
        tdSql.error(f"select min(ts) from {dbname}.stb_1")
        tdSql.error(f"select min(col7) from {dbname}.stb_1")
        tdSql.error(f"select min(a) from {dbname}.stb_1")
        tdSql.query(f"select min(1) from {dbname}.stb_1")
        tdSql.error(f"select min(count(c1),count(c2)) from {dbname}.stb_1")

        tdSql.query(f"select min(col1) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col2) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col3) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col4) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col11) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col12) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col13) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col14) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col5) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col6) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col1) from {dbname}.stb where col2>=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select min(col8) from {dbname}.stb")
        tdSql.checkData(0,0,'taosdata1')
        tdSql.query(f"select min(col9) from {dbname}.stb")
        tdSql.checkData(0,0,'涛思数据1')

        tdSql.error(f"select min(now()) from {dbname}.stb_1")
        tdSql.error(f"select min(ts) from {dbname}.stb_1")
        tdSql.error(f"select min(col7) from {dbname}.ntb")
        tdSql.error(f"select min(a) from {dbname}.ntb")
        tdSql.query(f"select min(1) from {dbname}.ntb")
        tdSql.error(f"select min(count(c1),count(c2)) from {dbname}.ntb")

        tdSql.query(f"select min(col1) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col2) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col3) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col4) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col11) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col12) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col13) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col14) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col5) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col6) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col1) from {dbname}.ntb where col2>=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select min(col8) from {dbname}.ntb")
        tdSql.checkData(0,0,'taosdata1')
        tdSql.query(f"select min(col9) from {dbname}.ntb")
        tdSql.checkData(0,0,'涛思数据1')

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
