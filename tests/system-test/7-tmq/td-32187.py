import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        tdSql.execute(f'create database if not exists db_32187')
        tdSql.execute(f'use db_32187')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))')
        tdSql.execute(f'insert into t1 using s5466 tags("__devicid__") values(1669092069068, 0, 1)')
        tdSql.execute(f'insert into t1(ts, c1, c2) values(1669092069067, 0, 1)')

        tdSql.execute("create topic topic_test with meta as database db_32187")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32187'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())