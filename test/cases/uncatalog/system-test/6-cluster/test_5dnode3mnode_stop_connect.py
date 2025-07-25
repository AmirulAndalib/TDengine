from new_test_framework.utils import tdLog, tdSql
from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os

from test import tdDnodes
sys.path.append("./6-cluster")

from clusterCommonCreate import *
from clusterCommonCheck import *
import time
import socket
import subprocess
from multiprocessing import Process


class Test5dnode3mnodeStopConnect:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def fiveDnodeThreeMnode(self,dnodenumbers,mnodeNums,restartNumber):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}
        dnodenumbers=int(dnodenumbers)
        mnodeNums=int(mnodeNums)
        dbNumbers = int(dnodenumbers * restartNumber)

        tdLog.info("first check dnode and mnode")
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        clusterComCheck.checkDnodes(dnodenumbers)
        
        #check mnode status
        tdLog.info("check mnode status")
        clusterComCheck.checkMnodeStatus(mnodeNums)
        
        # add some error operations and
        tdLog.info("Confirm the status of the dnode again")
        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodenumbers)

        # check status of connection



        # restart all taosd
        tdDnodes=cluster.dnodes
        for i in range(mnodeNums):
            tdDnodes[i].stoptaosd()
            for j in range(dnodenumbers):
                if j != i:
                    cluster.checkConnectStatus(j)
            clusterComCheck.check3mnodeoff(i+1,3)
            clusterComCheck.init(cluster.checkConnectStatus(i+1))
            tdDnodes[i].starttaosd()
            clusterComCheck.checkMnodeStatus(mnodeNums)

        tdLog.info("Take turns stopping all dnodes ")
        # seperate vnode and mnode in different dnodes.
        # create database and stable
        stopcount =0
        while stopcount < restartNumber:
            tdLog.info("first restart loop")
            for i in range(dnodenumbers):
                tdDnodes[i].stoptaosd()
                tdDnodes[i].starttaosd()
            stopcount+=1
        clusterComCheck.checkDnodes(dnodenumbers)
        clusterComCheck.checkMnodeStatus(mnodeNums)

    def test_5dnode3mnode_stop_connect(self):
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
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(5,3,1)

        tdLog.success(f"{__file__} successfully executed")

