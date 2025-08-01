from new_test_framework.utils import tdLog, tdSql

from wsgiref.headers import tspecials
import numpy as np

class TestHint:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.batchNum = 5
        cls.ts = 1537146000000

    def test_hint(self):
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

        tdSql.execute(f'''create table sta(ts timestamp, col1 int, col2 bigint) tags(tg1 int, tg2 binary(20))''')
        tdSql.execute(f"create table sta1 using sta tags(1, 'a')")
        tdSql.execute(f"create table sta2 using sta tags(2, 'b')")
        tdSql.execute(f"create table sta3 using sta tags(3, 'c')")
        tdSql.execute(f"create table sta4 using sta tags(4, 'a')")
        tdSql.execute(f"insert into sta1 values(1537146000001, 11, 110)")
        tdSql.execute(f"insert into sta1 values(1537146000002, 12, 120)")
        tdSql.execute(f"insert into sta1 values(1537146000003, 13, 130)")
        tdSql.execute(f"insert into sta2 values(1537146000001, 21, 210)")
        tdSql.execute(f"insert into sta2 values(1537146000002, 22, 220)")
        tdSql.execute(f"insert into sta2 values(1537146000003, 23, 230)")
        tdSql.execute(f"insert into sta3 values(1537146000001, 31, 310)")
        tdSql.execute(f"insert into sta3 values(1537146000002, 32, 320)")
        tdSql.execute(f"insert into sta3 values(1537146000003, 33, 330)")
        tdSql.execute(f"insert into sta4 values(1537146000001, 41, 410)")
        tdSql.execute(f"insert into sta4 values(1537146000002, 42, 420)")
        tdSql.execute(f"insert into sta4 values(1537146000003, 43, 430)")

        tdSql.execute(f'''create table stb(ts timestamp, col1 int, col2 bigint) tags(tg1 int, tg2 binary(20))''')
        tdSql.execute(f"create table stb1 using stb tags(1, 'a')")
        tdSql.execute(f"create table stb2 using stb tags(2, 'b')")
        tdSql.execute(f"create table stb3 using stb tags(3, 'c')")
        tdSql.execute(f"create table stb4 using stb tags(4, 'a')")
        tdSql.execute(f"insert into stb1 values(1537146000001, 911, 9110)")
        tdSql.execute(f"insert into stb1 values(1537146000002, 912, 9120)")
        tdSql.execute(f"insert into stb1 values(1537146000003, 913, 9130)")
        tdSql.execute(f"insert into stb2 values(1537146000001, 921, 9210)")
        tdSql.execute(f"insert into stb2 values(1537146000002, 922, 9220)")
        tdSql.execute(f"insert into stb2 values(1537146000003, 923, 9230)")
        tdSql.execute(f"insert into stb3 values(1537146000001, 931, 9310)")
        tdSql.execute(f"insert into stb3 values(1537146000002, 932, 9320)")
        tdSql.execute(f"insert into stb3 values(1537146000003, 933, 9330)")
        tdSql.execute(f"insert into stb4 values(1537146000001, 941, 9410)")
        tdSql.execute(f"insert into stb4 values(1537146000002, 942, 9420)")
        tdSql.execute(f"insert into stb4 values(1537146000003, 943, 9430)")

        tdSql.query(f"select /*+ batch_scan() */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+ no_batch_scan() */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+ batch_scan(a) */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+ batch_scan(a,) */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+ a,a */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+*/ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+ batch_scan(),no_batch_scan() */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        tdSql.query(f"select /*+ no_batch_scan() batch_scan() */ count(*) from sta a, stb b where a.tg1=b.tg1 and a.ts=b.ts and b.tg2 > 'a' interval(1a);")
        tdSql.checkRows(3)

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
