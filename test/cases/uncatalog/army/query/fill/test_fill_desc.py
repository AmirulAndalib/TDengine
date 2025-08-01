from new_test_framework.utils import tdLog, tdSql

class TestFillDesc:
    updatecfgDict = {
        'slowLogScope':"all"
    }

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_fill_desc(self):
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
        stbname = "ocloud_point"
        tbname = "ocloud_point_170658_3837620225_1701134595725266945"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create stable if not exists {dbname}.{stbname}
            (wstart timestamp, point_value float) tags (location binary(64), groupId int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname} using {dbname}.{stbname} tags("California.SanFrancisco", 2)'''
        )

        sqls = []
        for i in range(35, 41):
            if i == 38 or i == 40:
                sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:{i}:00.000', null)")
            else:
                sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:{i}:00.000', 5.0)")

        # sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:36:00.000', 5.0)")
        # sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:37:00.000', 5.0)")
        # sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:38:00.000', null)")
        # sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:39:00.000', 5.0)")
        # sqls.append(f"insert into {dbname}.{tbname} values('2023-12-26 10:40:00.000', null)")


        tdSql.executes(sqls)

        tdLog.printNoPrefix("==========step3:fill data")

        sql = f"select first(point_value) as pointValue from {dbname}.{tbname} where wstart between '2023-12-26 10:35:00' and '2023-12-26 10:40:00' interval(1M) fill(prev) order by wstart desc limit 100"
        data = []
        for i in range(6):
           row = [5]
           data.append(row)
        tdSql.checkDataMem(sql, data)

        tdLog.success(f"{__file__} successfully executed")
