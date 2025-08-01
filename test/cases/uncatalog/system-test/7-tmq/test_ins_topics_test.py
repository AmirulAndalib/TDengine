
import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql

class TestCase:
    hostname = socket.gethostname()
    # rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    # updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def check(self):
        tdLog.info("create database, stb, ctb")
        tdSql.execute("create database if not exists db1 vgroups 4 wal_retention_period 3600")
        tdSql.execute("create table if not exists db1.st(ts timestamp, c1 int, c2 bool, c3 tinyint, c4 double, c5 nchar(8)) tags(t1 int, t2 float, t3 binary(4))")
        tdSql.execute("create table if not exists db1.nt(ts timestamp, c1 smallint, c2 float, c3 binary(64), c4 bigint)")
        tdSql.execute("create table if not exists db1.st1 using db1.st tags(1, 9.3, \"st1\")")

        tdLog.info("create topic")
        tdSql.execute("create topic topic_1 as database db1")
        tdSql.execute("create topic topic_2 with meta as stable db1.st")
        tdSql.execute("create topic topic_3 as select * from db1.nt")
        tdSql.execute("create topic topic_4 as select ts,c3,c5,t2 from db1.st")
        for i in range(5, 21):
            tdSql.execute(f"create topic topic_{i} as select ts,c3,c5,t2 from db1.st")

        tdSql.error("create topic topic_21 as select * from db1.nt")
        tdSql.execute("create topic if not exists topic_1 as database db1")
        for i in range(5, 21):
            tdSql.execute(f"drop topic topic_{i}")


        tdSql.query("select * from information_schema.ins_topics order by topic_name")
        tdSql.checkRows(4)
        tdSql.checkData(0, 4, "NULL")
        tdSql.checkData(0, 5, "no")
        tdSql.checkData(0, 6, "db")
        tdSql.checkData(1, 4, "[{\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"length\":8},{\"name\":\"c1\",\"type\":\"INT\",\"length\":4},{\"name\":\"c2\",\"type\":\"BOOL\",\"length\":1},{\"name\":\"c3\",\"type\":\"TINYINT\",\"length\":1},{\"name\":\"c4\",\"type\":\"DOUBLE\",\"length\":8},{\"name\":\"c5\",\"type\":\"NCHAR\",\"length\":8}]")
        tdSql.checkData(1, 5, "yes")
        tdSql.checkData(1, 6, "stable")
        tdSql.checkData(2, 4, "[{\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"length\":8},{\"name\":\"c1\",\"type\":\"SMALLINT\",\"length\":2},{\"name\":\"c2\",\"type\":\"FLOAT\",\"length\":4},{\"name\":\"c3\",\"type\":\"VARCHAR\",\"length\":64},{\"name\":\"c4\",\"type\":\"BIGINT\",\"length\":8}]")
        tdSql.checkData(2, 5, "no")
        tdSql.checkData(2, 6, "column")
        tdSql.checkData(3, 4, "[{\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"length\":8},{\"name\":\"c3\",\"type\":\"TINYINT\",\"length\":1},{\"name\":\"c5\",\"type\":\"NCHAR\",\"length\":8},{\"name\":\"t2\",\"type\":\"FLOAT\",\"length\":4}]")
        tdSql.checkData(3, 5, "no")
        tdSql.checkData(3, 6, "column")

        tdLog.printNoPrefix("======== test case end ...... ")

    def test_ins_topics(self):
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
        self.check()
        
        tdLog.success(f"{__file__} successfully executed")
