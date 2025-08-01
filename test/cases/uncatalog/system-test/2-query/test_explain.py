import datetime
import random
import os

from new_test_framework.utils import tdLog, tdSql, tdDnodes

PRIMARY_COL = "ts"

INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

NUM_COL     = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [ BOOL_COL, ]
TS_TYPE_COL = [ TS_COL, ]

ALL_COL = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BOOL_COL, BINARY_COL, NCHAR_COL, TS_COL ]
DBNAME = "db"
class TestExplain:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcaseFilename = os.path.split(__file__)[-1]

    def __query_condition(self,tbname):
        query_condition = [f"{tbname}.{col}" for col in ALL_COL]
        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"abs( {tbname}.{num_col} )",
                    f"acos( {tbname}.{num_col} )",
                    f"asin( {tbname}.{num_col} )",
                    f"atan( {tbname}.{num_col} )",
                    f"avg( {tbname}.{num_col} )",
                    f"ceil( {tbname}.{num_col} )",
                    f"cos( {tbname}.{num_col} )",
                    f"count( {tbname}.{num_col} )",
                    f"floor( {tbname}.{num_col} )",
                    f"log( {tbname}.{num_col},  {tbname}.{num_col})",
                    f"max( {tbname}.{num_col} )",
                    f"min( {tbname}.{num_col} )",
                    f"pow( {tbname}.{num_col}, 2)",
                    f"round( {tbname}.{num_col} )",
                    f"sum( {tbname}.{num_col} )",
                    f"sin( {tbname}.{num_col} )",
                    f"sqrt( {tbname}.{num_col} )",
                    f"tan( {tbname}.{num_col} )",
                    f"cast( {tbname}.{num_col} as timestamp)",
                )
            )
            query_condition.extend((f"{num_col} + {any_col}" for any_col in ALL_COL))
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"sum(cast({tbname}.{char_col} as bigint ))",
                    f"max(cast({tbname}.{char_col} as bigint ))",
                    f"min(cast({tbname}.{char_col} as bigint ))",
                    f"avg(cast({tbname}.{char_col} as bigint ))",
                )
            )
        query_condition.extend(
            (
                1010.1,
                ''' "test1234!@#$%^&*():'><?/.,][}{" ''',
                "null"
            )
        )

        return query_condition

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False):
        table_reference = tb_list[0]
        join_condition = table_reference
        join = "inner join" if INNER else "join"
        for i in range(len(tb_list[1:])):
            join_condition += f" {join} {tb_list[i+1]} on {table_reference}.{filter}={tb_list[i+1]}.{filter}"

        return join_condition

    def __where_condition(self, col=None, tbname=None, query_conditon=None):
        if query_conditon and isinstance(query_conditon, str):
            if query_conditon.startswith("count"):
                query_conditon = query_conditon[6:-1]
            elif query_conditon.startswith("max"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("sum"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("min"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("avg"):
                query_conditon = query_conditon[4:-1]

        if query_conditon:
            return f" where {query_conditon} is not null"
        if col in NUM_COL:
            return f" where abs( {tbname}.{col} ) >= 0"
        if col in CHAR_COL:
            return f" where lower( {tbname}.{col} ) like 'bina%' or lower( {tbname}.{col} ) like '_cha%' "
        if col in BOOLEAN_COL:
            return f" where {tbname}.{col} in (false, true)  "
        if col in TS_TYPE_COL or col in PRIMARY_COL:
            return f" where cast( {tbname}.{col} as binary(16) ) is not null "

        return ""

    def __group_condition(self, col, having = None):
        if isinstance(col, str):
            if col.startswith("count"):
                col = col[6:-1]
            elif col.startswith("max"):
                col = col[4:-1]
            elif col.startswith("sum"):
                col = col[4:-1]
            elif col.startswith("min"):
                col = col[4:-1]
            elif col.startswith("avg"):
                col = col[4:-1]
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __single_sql(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_ratio(self, select_clause, from_clause, where_condition="", group_condition=""):
        ratio = random.uniform(0.001,1)
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain ratio {ratio} select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_ratio_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
        ratio = random.uniform(0.001,1)
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain ratio {ratio} verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_ratio_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
        ratio = random.uniform(0.001,1)
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain ratio {ratio} verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_ratio(self, select_clause, from_clause, where_condition="", group_condition=""):
        ratio = random.uniform(0.001,1)
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze ratio {ratio} select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_ratio_verbose_true(self, select_clause, from_clause, where_condition="", group_condition=""):
        ratio = random.uniform(0.001,1)
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze ratio {ratio} verbose true select {select_clause} from {from_clause} {where_condition} {group_condition}"

    def __single_sql_analyze_ratio_verbose_false(self, select_clause, from_clause, where_condition="", group_condition=""):
        ratio = random.uniform(0.001,1)
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0].split("(")[-1] != from_clause.split(".")[0]:
            return
        return f"explain analyze ratio {ratio} verbose false select {select_clause} from {from_clause} {where_condition} {group_condition}"
                    
    @property
    def __tb_list(self, dbname=DBNAME):
        return [
            f"{dbname}.ct1",
            f"{dbname}.ct4",
            f"{dbname}.t1",
            f"{dbname}.ct2",
            f"{dbname}.stb1",
        ]

    def sql_list(self):
        sqls = []
        __no_join_tblist = self.__tb_list
        for tb in __no_join_tblist:
            tbname = tb.split(".")[-1]
            select_claus_list = self.__query_condition(tbname)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition(col=select_claus)
                where_claus = self.__where_condition(query_conditon=select_claus)
                having_claus = self.__group_condition(col=select_claus, having=f"{select_claus} is not null")
                sqls.extend(
                    (
                        self.__single_sql(select_claus, tb, where_claus, having_claus),
                        self.__single_sql(select_claus, tb,),
                        self.__single_sql(select_claus, tb, where_condition=where_claus),
                        self.__single_sql(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_verbose_true(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_verbose_true(select_claus, tb,),
                        self.__single_sql_verbose_true(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_verbose_true(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_verbose_false(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_verbose_false(select_claus, tb,),
                        self.__single_sql_verbose_false(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_verbose_false(select_claus, tb, group_condition=group_claus),
                        
                        self.__single_sql_ratio(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_ratio(select_claus, tb,),
                        self.__single_sql_ratio(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_ratio(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_ratio_verbose_true(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_ratio_verbose_true(select_claus, tb,),
                        self.__single_sql_ratio_verbose_true(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_ratio_verbose_true(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_ratio_verbose_false(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_ratio_verbose_false(select_claus, tb,),
                        self.__single_sql_ratio_verbose_false(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_ratio_verbose_false(select_claus, tb, group_condition=group_claus),
                        
                        self.__single_sql_analyze(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze(select_claus, tb,),
                        self.__single_sql_analyze(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_analyze_verbose_true(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_verbose_true(select_claus, tb,),
                        self.__single_sql_analyze_verbose_true(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_verbose_true(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_analyze_verbose_false(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_verbose_false(select_claus, tb,),
                        self.__single_sql_analyze_verbose_false(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_verbose_false(select_claus, tb, group_condition=group_claus),
                        
                        self.__single_sql_analyze_ratio(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_ratio(select_claus, tb,),
                        self.__single_sql_analyze_ratio(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_ratio(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_analyze_ratio_verbose_true(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_ratio_verbose_true(select_claus, tb,),
                        self.__single_sql_analyze_ratio_verbose_true(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_ratio_verbose_true(select_claus, tb, group_condition=group_claus),
                        self.__single_sql_analyze_ratio_verbose_false(select_claus, tb, where_claus, having_claus),
                        self.__single_sql_analyze_ratio_verbose_false(select_claus, tb,),
                        self.__single_sql_analyze_ratio_verbose_false(select_claus, tb, where_condition=where_claus),
                        self.__single_sql_analyze_ratio_verbose_false(select_claus, tb, group_condition=group_claus),
                    )
                )

        # return filter(None, sqls)
        return list(filter(None, sqls))

    def explain_check(self):
        sqls = self.sql_list()
        tdLog.printNoPrefix("===step 1: curent case, must return query OK")
        for i in range(len(sqls)):
            tdLog.info(f"sql: {sqls[i]}")
            tdSql.query(sqls[i])

    def __test_current(self, dbname=DBNAME):
        
        ratio = random.uniform(0.001,1)
        
        tdSql.query(f"explain select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain select 1 from {dbname}.ct2")
        tdSql.query(f"explain select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        
        tdSql.query(f"explain ratio {ratio} select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain ratio {ratio} select 1 from {dbname}.ct2")
        tdSql.query(f"explain ratio {ratio} select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain ratio {ratio} select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain ratio {ratio} select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain ratio {ratio} select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain ratio {ratio} verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain ratio {ratio} verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain ratio {ratio} verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain ratio {ratio} verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain ratio {ratio} verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain ratio {ratio} verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain ratio {ratio} verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain ratio {ratio} verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain ratio {ratio} verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain ratio {ratio} verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain ratio {ratio} verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain ratio {ratio} verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        
        tdSql.query(f"explain analyze ratio {ratio} select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze ratio {ratio} select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze ratio {ratio} select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze ratio {ratio} select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze ratio {ratio} select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze ratio {ratio} select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze ratio {ratio} verbose true select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze ratio {ratio} verbose true select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()
        
        tdSql.query(f"explain analyze ratio {ratio} verbose false select {INT_COL} from {dbname}.ct1")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select 1 from {dbname}.ct2")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select cast(ceil({DOUBLE_COL}) as bigint) from {dbname}.ct4 group by {DOUBLE_COL}")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select count({SINT_COL}) from {dbname}.ct4 group by {BOOL_COL} having count({SINT_COL}) > 0")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select ct2.{SINT_COL} from {dbname}.ct4 ct4 join {dbname}.ct2 ct2 on ct4.ts=ct2.ts")
        tdSql.query(f"explain analyze ratio {ratio} verbose false select {INT_COL} from {dbname}.stb1 where {INT_COL} is not null and {INT_COL} in (0, 1, 2) or {INT_COL} between 2 and 100 ")

        self.explain_check()

        # coverage explain.c add
        tdSql.query(f"explain verbose true select * from {dbname}.stb1 partition by c1 order by c2")

    def __test_error(self, dbname=DBNAME):
        
        ratio = random.uniform(0.001,1)

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain show databases " )
        tdSql.error( f"explain show {dbname}.stables " )
        tdSql.error( f"explain show {dbname}.tables " )
        tdSql.error( f"explain show {dbname}.vgroups " )
        tdSql.error( f"explain show dnodes " )
        tdSql.error( f'''explain select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain verbose true  show databases " )
        tdSql.error( f"explain verbose true  show {dbname}.stables " )
        tdSql.error( f"explain verbose true  show {dbname}.tables " )
        tdSql.error( f"explain verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain verbose true  show dnodes " )
        tdSql.error( f'''explain verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain verbose false  show databases " )
        tdSql.error( f"explain verbose false  show {dbname}.stables " )
        tdSql.error( f"explain verbose false  show {dbname}.tables " )
        tdSql.error( f"explain verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain verbose false  show dnodes " )
        tdSql.error( f'''explain verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
              

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain ratio {ratio} select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain ratio {ratio} show databases " )
        tdSql.error( f"explain ratio {ratio} show {dbname}.stables " )
        tdSql.error( f"explain ratio {ratio} show {dbname}.tables " )
        tdSql.error( f"explain ratio {ratio} show {dbname}.vgroups " )
        tdSql.error( f"explain ratio {ratio} show dnodes " )
        tdSql.error( f'''explain ratio {ratio} select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain ratio {ratio} verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain ratio {ratio} verbose true  show databases " )
        tdSql.error( f"explain ratio {ratio} verbose true  show {dbname}.stables " )
        tdSql.error( f"explain ratio {ratio} verbose true  show {dbname}.tables " )
        tdSql.error( f"explain ratio {ratio} verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain ratio {ratio} verbose true  show dnodes " )
        tdSql.error( f'''explain ratio {ratio} verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain ratio {ratio} verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain ratio {ratio} verbose false  show databases " )
        tdSql.error( f"explain ratio {ratio} verbose false  show {dbname}.stables " )
        tdSql.error( f"explain ratio {ratio} verbose false  show {dbname}.tables " )
        tdSql.error( f"explain ratio {ratio} verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain ratio {ratio} verbose false  show dnodes " )
        tdSql.error( f'''explain ratio {ratio} verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain analyze select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze show databases " )
        tdSql.error( f"explain analyze show {dbname}.stables " )
        tdSql.error( f"explain analyze show {dbname}.tables " )
        tdSql.error( f"explain analyze show {dbname}.vgroups " )
        tdSql.error( f"explain analyze show dnodes " )
        tdSql.error( f'''explain analyze select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze verbose true  show databases " )
        tdSql.error( f"explain analyze verbose true  show {dbname}.stables " )
        tdSql.error( f"explain analyze verbose true  show {dbname}.tables " )
        tdSql.error( f"explain analyze verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze verbose true  show dnodes " )
        tdSql.error( f'''explain analyze verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze verbose false  show databases " )
        tdSql.error( f"explain analyze verbose false  show {dbname}.stables " )
        tdSql.error( f"explain analyze verbose false  show {dbname}.tables " )
        tdSql.error( f"explain analyze verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze verbose false  show dnodes " )
        tdSql.error( f'''explain analyze verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )

        tdLog.printNoPrefix("===step 0: err case, must return err")
        tdSql.error( f"explain analyze ratio {ratio} select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze ratio {ratio} show databases " )
        tdSql.error( f"explain analyze ratio {ratio} show {dbname}.stables " )
        tdSql.error( f"explain analyze ratio {ratio} show {dbname}.tables " )
        tdSql.error( f"explain analyze ratio {ratio} show {dbname}.vgroups " )
        tdSql.error( f"explain analyze ratio {ratio} show dnodes " )
        tdSql.error( f'''explain analyze ratio {ratio} select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze ratio {ratio} verbose true  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show databases " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show {dbname}.stables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show {dbname}.tables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze ratio {ratio} verbose true  show dnodes " )
        tdSql.error( f'''explain analyze ratio {ratio} verbose true  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
        
        tdSql.error( f"explain analyze ratio {ratio} verbose false  select hyperloglog({INT_COL}) from {dbname}.ct8" )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show databases " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show {dbname}.stables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show {dbname}.tables " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show {dbname}.vgroups " )
        tdSql.error( f"explain analyze ratio {ratio} verbose false  show dnodes " )
        tdSql.error( f'''explain analyze ratio {ratio} verbose false  select hyperloglog(['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'])
                    from {dbname}.ct1
                    where ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null
                    group by ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}']
                    having ['{INT_COL} + {INT_COL}', '{INT_COL} + {BINT_COL}', '{INT_COL} + {SINT_COL}', '{INT_COL} + {TINT_COL}', '{INT_COL} + {FLOAT_COL}', '{INT_COL} + {DOUBLE_COL}', '{INT_COL} + {BOOL_COL}', '{INT_COL} + {BINARY_COL}', '{INT_COL} + {NCHAR_COL}', '{INT_COL} + {TS_COL}'] is not null ''' )
                     

    def all_test(self):
        self.__test_error()
        self.__test_current()

    def __create_tb(self, dbname=DBNAME):

        tdLog.printNoPrefix("==========step1:create table")
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            ) tags (t1 int)
            '''
        create_ntb_sql = f'''create table {dbname}.t1(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                 {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                 {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
            )
            '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(4):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( {i+1} )')
            { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}

    def __insert_data(self, rows, dbname=DBNAME):
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(rows):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( { now_time - i * 1000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( { now_time - i * 7776000000 }, {i}, {11111 * i}, {111 * i % 32767 }, {11 * i % 127}, {1.11*i}, {1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct2 values ( { now_time - i * 7776000000 }, {-i},  {-11111 * i}, {-111 * i % 32767 }, {-11 * i % 127}, {-1.11*i}, {-1100.0011*i}, {i%2}, 'binary{i}', 'nchar_测试_{i}', { now_time + 1 * i } )"
            )
        tdSql.execute(
            f'''insert into {dbname}.ct1 values
            ( { now_time - rows * 5 }, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar_测试_0', { now_time + 8 } )
            ( { now_time + 10000 }, { rows }, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar_测试_9', { now_time + 9 } )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct4 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time +  7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000}, {pow(2,31)-pow(2,15)}, {pow(2,63)-pow(2,30)}, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000}
                )
            (
                { now_time + 2592000000 }, {pow(2,31)-pow(2,16)}, {pow(2,63)-pow(2,31)}, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000}
                )
            '''
        )

        tdSql.execute(
            f'''insert into {dbname}.ct2 values
            ( { now_time - rows * 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3888000000 + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7776000000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            (
                { now_time + 5184000000 }, { -1 * pow(2,31) + pow(2,15) }, { -1 * pow(2,63) + pow(2,30) }, -32766, -126,
                { -1 * 3.2 * pow(10,38) }, { -1.2 * pow(10,308) }, { rows % 2 }, "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 2592000000 }, { -1 * pow(2,31) + pow(2,16) }, { -1 * pow(2,63) + pow(2,31) }, -32767, -127,
                { - 3.3 * pow(10,38) }, { -1.3 * pow(10,308) }, { (rows-1) % 2 }, "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

        for i in range(rows):
            insert_data = f'''insert into {dbname}.t1 values
                ( { now_time - i * 3600000 }, {i}, {i * 11111}, { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2},
                "binary_{i}", "nchar_测试_{i}", { now_time - 1000 * i } )
                '''
            tdSql.execute(insert_data)
        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( { now_time + 10800000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - (( rows // 2 ) * 60 + 30) * 60000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time - rows * 3600000 }, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( { now_time + 7200000 }, { pow(2,31) - pow(2,15) }, { pow(2,63) - pow(2,30) }, 32767, 127,
                { 3.3 * pow(10,38) }, { 1.3 * pow(10,308) }, { rows % 2 },
                "binary_limit-1", "nchar_测试_limit-1", { now_time - 86400000 }
                )
            (
                { now_time + 3600000 } , { pow(2,31) - pow(2,16) }, { pow(2,63) - pow(2,31) }, 32766, 126,
                { 3.2 * pow(10,38) }, { 1.2 * pow(10,308) }, { (rows-1) % 2 },
                "binary_limit-2", "nchar_测试_limit-2", { now_time - 172800000 }
                )
            '''
        )

    def test_explain(self):
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

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(self.rows)

        tdLog.printNoPrefix("==========step3:all check")
        self.all_test()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
