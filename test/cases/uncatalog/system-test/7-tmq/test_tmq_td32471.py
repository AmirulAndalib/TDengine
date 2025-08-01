import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_td32471(self):
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
        tdSql.execute(f'create database if not exists db_32471')
        tdSql.execute(f'use db_32471')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32471'%(buildPath)
        # tdLog.info(cmdStr)
        # os.system(cmdStr)
        #
        # tdSql.execute("drop topic db_32471_topic")
        tdSql.execute(f'alter stable meters add column  item_tags nchar(500)')
        tdSql.execute(f'alter stable meters add column  new_col nchar(100)')
        tdSql.execute("create topic db_32471_topic as select * from db_32471.meters")

        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-06 14:38:05.000',10.30000,219,0.31000, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '1')")

        tdLog.info(cmdStr)
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)

        tdLog.success(f"{__file__} successfully executed")
