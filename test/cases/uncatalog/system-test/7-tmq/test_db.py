
import taos
import platform
import time
import socket
import os
import threading
from enum import Enum

from new_test_framework.utils import tdLog, tdSql, tdDnodes

class actionType(Enum):
    CREATE_DATABASE = 0
    CREATE_STABLE   = 1
    CREATE_CTABLE   = 2
    INSERT_DATA     = 3

class TestCase:
    hostname = socket.gethostname()
    #rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("test")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def newcur(self,cfg,host,port):
        user = "root"
        password = "taosdata"
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def initConsumerTable(self,cdbName='cdb'):
        tdLog.info("create consume database, and consume info table, and consume result table")
        tdSql.query("drop database if exists %s "%(cdbName))
        tdSql.query("create database %s vgroups 1 wal_retention_period 3600"%(cdbName))
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("drop table if exists %s.consumeresult "%(cdbName))

        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

    def initConsumeContentTable(self,id=0,cdbName='cdb'):
        tdSql.query("drop table if exists %s.content_%d "%(cdbName, id))
        tdSql.query("create table %s.content_%d (ts timestamp, contentOfRow binary(1024))"%cdbName, id)

    def initConsumerInfoTable(self,cdbName='cdb'):
        tdLog.info("drop consumeinfo table")
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)

    def insertConsumerInfo(self,consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifmanualcommit,cdbName='cdb'):
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata, ifmanualcommit)
        tdLog.info("consume info sql: %s"%sql)
        tdSql.query(sql)

    def selectConsumeResult(self,expectRows,cdbName='cdb'):
        resultList=[]
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == expectRows:
                break
            else:
                time.sleep(5)

        for i in range(expectRows):
            tdLog.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(tdSql.getData(i , 1), tdSql.getData(i , 2), tdSql.getData(i , 3)))
            resultList.append(tdSql.getData(i , 3))

        return resultList

    def startTmqSimProcess(self,buildPath,cfgPath,pollDelay,dbName,showMsg=1,showRow=1,cdbName='cdb',valgrind=0):
        if valgrind == 1:
            logFile = cfgPath + '/../log/valgrind-tmq.log'
            shellCmd = 'nohup valgrind --log-file=' + logFile
            shellCmd += '--tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all --num-callers=20 -v --workaround-gcc296-bugs=yes '

        if (platform.system().lower() == 'windows'):
            shellCmd = 'mintty -h never -w hide ' + buildPath + '\\build\\bin\\tmq_sim.exe -c ' + cfgPath
            shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, dbName, showMsg, showRow, cdbName)
            shellCmd += "> nul 2>&1 &"
        else:
            shellCmd = 'nohup ' + buildPath + '/build/bin/tmq_sim -c ' + cfgPath
            shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, dbName, showMsg, showRow, cdbName)
            shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period -1 wal_retention_size -1"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, dbName,stbName):
        tsql.execute("create table if not exists %s.%s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%(dbName, stbName))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctables(self,tsql, dbName,stbName,ctbPrefix,ctbNum):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%d)"%(ctbPrefix,i,stbName,i+1)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data_interlaceByMultiTbl(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        ctbDict = {}
        for i in range(ctbNum):
            ctbDict[i] = 0

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfCtb = 0
        while rowsOfCtb < rowsPerTbl:
            for i in range(ctbNum):
                sql += " %s.%s_%d values "%(dbName,ctbPrefix,i)
                for k in range(batchNum):
                    sql += "(%d, %d, 'tmqrow_%d') "%(startTs + ctbDict[i], ctbDict[i], ctbDict[i])
                    ctbDict[i] += 1
                    if (0 == ctbDict[i]%batchNum) or (ctbDict[i] == rowsPerTbl):
                        tsql.execute(sql)
                        sql = "insert into "
                        break
            rowsOfCtb = ctbDict[0]

        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s_%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data_with_autoCreateTbl(self,tsql,dbName,stbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data wiht auto create child table ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s.%s_%d using %s.%s tags (%d) values "%(dbName,ctbPrefix,i,dbName,stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'autodata_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s_%d using %s.%s tags (%d) values " %(dbName,ctbPrefix,i,dbName,stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def prepareEnv(self, **parameterDict):
        # create new connector for my thread
        tsql=self.newcur(parameterDict['cfg'], 'localhost', 6030)

        if parameterDict["actionType"] == actionType.CREATE_DATABASE:
            self.create_database(tsql, parameterDict["dbName"])
        elif parameterDict["actionType"] == actionType.CREATE_STABLE:
            self.create_stable(tsql, parameterDict["dbName"], parameterDict["stbName"])
        elif parameterDict["actionType"] == actionType.CREATE_CTABLE:
            self.create_ctables(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        elif parameterDict["actionType"] == actionType.INSERT_DATA:
            self.insert_data(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"],\
                            parameterDict["rowsPerTbl"],parameterDict["batchNum"])
        else:
            tdLog.exit("not support's action: ", parameterDict["actionType"])

        return

    def tmqCase1(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 1: ")
        '''
        subscribe one db, multi normal table which have not same schema, and include rows of all tables in one insert sql
        '''
        self.initConsumerTable()

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db1',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        self.create_database(tdSql, parameterDict["dbName"])
        tdSql.execute("create table %s.ntb0 (ts timestamp, c1 int)"%(parameterDict["dbName"]))
        tdSql.execute("create table %s.ntb1 (ts timestamp, c1 int, c2 float)"%(parameterDict["dbName"]))
        tdSql.execute("create table %s.ntb2 (ts timestamp, c1 int, c2 float, c3 binary(32))"%(parameterDict["dbName"]))
        tdSql.execute("create table %s.ntb3 (ts timestamp, c1 int, c2 float, c3 binary(32), c4 timestamp)"%(parameterDict["dbName"]))

        tdSql.execute("insert into %s.ntb0 values(now, 1) %s.ntb1 values(now, 1, 1) %s.ntb2 values(now, 1, 1, '1') %s.ntb3 values(now, 1, 1, '1', now)"%(parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"]))
        tdSql.execute("insert into %s.ntb0 values(now, 2)(now+1s, 3) \
                                   %s.ntb1 values(now, 2, 2)(now+1s, 3, 3) \
                                   %s.ntb2 values(now, 2, 2, '2')(now+1s, 3, 3, '3') \
                                   %s.ntb3 values(now, 2, 2, '2', now)(now+1s, 3, 3, '3', now)"\
                                   %(parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"]))
        tdSql.execute("insert into %s.ntb0 values(now, 4)(now+1s, 5) \
                                   %s.ntb1 values(now, 4, 4)(now+1s, 5, 5) \
                                   %s.ntb2 values(now, 4, 4, '4')(now+1s, 5, 5, '5') \
                                   %s.ntb3 values(now, 4, 4, '4', now)(now+1s, 5, 5, '5', now) \
                                   %s.ntb0 values(now+2s, 6)(now+3s, 7) \
                                   %s.ntb1 values(now+2s, 6, 6)(now+3s, 7, 7) \
                                   %s.ntb2 values(now+2s, 6, 6, '6')(now+3s, 7, 7, '7') \
                                   %s.ntb3 values(now+2s, 6, 6, '6', now)(now+3s, 7, 7, '7', now)"\
                                   %(parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"],parameterDict["dbName"]))
        numOfNtb     = 4
        rowsOfPerNtb = 7

        tdLog.info("create topics from db")
        topicFromDb = 'topic_db_mulit_tbl'

        tdSql.execute("create topic %s as database %s" %(topicFromDb, parameterDict['dbName']))
        consumerId     = 0
        expectrowcnt   = numOfNtb * rowsOfPerNtb
        topicList      = topicFromDb
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,enable.auto.commit:false,\
                          auto.commit.interval.ms:6000,auto.offset.reset:earliest'
        self.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 10
        showMsg   = 1
        showRow   = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,parameterDict["dbName"],showMsg, showRow)

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = self.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromDb)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2: ")
        '''
        subscribe one stb, multi child talbe and normal table which have not same schema, and include rows of all tables in one insert sql
        '''
        self.initConsumerTable()

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db2',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        dbName = parameterDict["dbName"]

        self.create_database(tdSql, dbName)

        tdSql.execute("create stable %s.stb (ts timestamp, s1 bigint, s2 binary(32), s3 double) tags (t1 int, t2 binary(32))"%(dbName))
        tdSql.execute("create table %s.ctb0 using %s.stb tags(0, 'ctb0')"%(dbName,dbName))
        tdSql.execute("create table %s.ctb1 using %s.stb tags(1, 'ctb1')"%(dbName,dbName))

        tdSql.execute("create table %s.ntb0 (ts timestamp, c1 binary(32))"%(dbName))
        tdSql.execute("create table %s.ntb1 (ts timestamp, c1 binary(32), c2 float)"%(dbName))
        tdSql.execute("create table %s.ntb2 (ts timestamp, c1 int, c2 float, c3 binary(32))"%(dbName))
        tdSql.execute("create table %s.ntb3 (ts timestamp, c1 int, c2 float, c3 binary(32), c4 timestamp)"%(dbName))

        tdSql.execute("insert into %s.ntb0 values(now, 'ntb0-11') \
                                   %s.ntb1 values(now, 'ntb1', 11) \
                                   %s.ntb2 values(now, 11, 11, 'ntb2') \
                                   %s.ctb0 values(now, 11, 'ctb0', 11) \
                                   %s.ntb3 values(now, 11, 11, 'ntb3', now) \
                                   %s.ctb1 values(now, 11, 'ctb1', 11)"\
                                   %(dbName,dbName,dbName,dbName,dbName,dbName))

        tdSql.execute("insert into %s.ntb0 values(now, 'ntb0-12')(now+1s, 'ntb0-13') \
                                   %s.ntb1 values(now, 'ntb1', 12)(now+1s, 'ntb1', 13) \
                                   %s.ntb2 values(now, 12, 12, 'ntb2')(now+1s, 13, 13, 'ntb2') \
                                   %s.ctb0 values(now, 12, 'ctb0', 12)(now+1s, 13, 'ctb0', 13) \
                                   %s.ntb3 values(now, 12, 12, 'ntb3', now)(now+1s, 13, 13, 'ntb3', now) \
                                   %s.ctb1 values(now, 12, 'ctb1', 12)(now+1s, 13, 'ctb1', 13)"\
                                   %(dbName,dbName,dbName,dbName,dbName,dbName))
        tdSql.execute("insert into %s.ntb0 values(now, 'ntb0-14')(now+1s, 'ntb0-15') \
                                   %s.ntb1 values(now, 'ntb1', 14)(now+1s, 'ntb1', 15) \
                                   %s.ntb2 values(now, 14, 14, 'ntb2')(now+1s, 15, 15, 'ntb2') \
                                   %s.ctb0 values(now, 14, 'ctb0', 14)(now+1s, 15, 'ctb0', 15) \
                                   %s.ntb3 values(now, 14, 14, 'ntb3', now)(now+1s, 15, 15, 'ntb3', now) \
                                   %s.ctb1 values(now, 14, 'ctb1', 14)(now+1s, 15, 'ctb1', 15) \
                                   %s.ntb0 values(now+2s, 'ntb0-16')(now+3s, 'ntb0-17') \
                                   %s.ntb1 values(now+2s, 'ntb1', 16)(now+3s, 'ntb1', 17) \
                                   %s.ntb2 values(now+2s, 16, 16, 'ntb2')(now+3s, 17, 17, 'ntb2') \
                                   %s.ctb0 values(now+2s, 16, 'ctb0', 16)(now+3s, 17, 'ctb0', 17) \
                                   %s.ntb3 values(now+2s, 16, 16, 'ntb3', now)(now+3s, 17, 17, 'ntb3', now) \
                                   %s.ctb1 values(now+2s, 16, 'ctb1', 16)(now+3s, 17, 'ctb1', 17)"\
                                   %(dbName,dbName,dbName,dbName,dbName,dbName,dbName,dbName,dbName,dbName,dbName,dbName))
        numOfNtb     = 4
        numOfCtb     = 2
        rowsOfPerNtb = 7

        tdLog.info("create topics from db")
        topicFromStb = 'topic_stb_mulit_tbl'

        tdSql.execute("create topic %s as stable %s.stb" %(topicFromStb, dbName))
        consumerId     = 0
        expectrowcnt   = numOfCtb * rowsOfPerNtb
        topicList      = topicFromStb
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,enable.auto.commit:false,\
                          auto.commit.interval.ms:6000,auto.offset.reset:earliest'
        self.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 10
        showMsg   = 1
        showRow   = 1
        self.startTmqSimProcess(buildPath,cfgPath,pollDelay,dbName,showMsg, showRow)

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = self.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def test_db(self):
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

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdLog.info("cfgPath: %s" % cfgPath)

        self.tmqCase1(cfgPath, buildPath)
        self.tmqCase2(cfgPath, buildPath)
        
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

