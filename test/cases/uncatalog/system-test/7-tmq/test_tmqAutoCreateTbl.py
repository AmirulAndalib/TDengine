
import taos
import sys
import time
import socket
import os
import threading
from enum import Enum

from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tmqCommon import tmqCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.snapshot   = 0
        cls.vgroups    = 4
        cls.ctbNum     = 500
        cls.rowsPerTbl = 1000

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     500,
                    'rowsPerTbl': 1000,
                    'batchNum':   500,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  20,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1, wal_retention_size=-1,wal_retention_period=-1)
        tdSql.execute("alter database %s wal_retention_period 3600" % (paraDict['dbName']))
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        # tdLog.info("create ctb")
        # tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
        #                      ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tmqCom.insert_data_with_autoCreateTbl(tdSql,paraDict["dbName"],paraDict["stbName"],"ctbx",paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"])

        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    # 自动建表完成数据插入，启动消费
    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     500,
                    'rowsPerTbl': 1000,
                    'batchNum':   500,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  20,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        # tmqCom.initConsumerTable()
        # tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        # tdLog.info("create stb")
        # tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        # tdLog.info("insert data by auto create ctb")
        # tmqCom.insert_data_with_autoCreateTbl(tdSql,paraDict["dbName"],paraDict["stbName"],"ctb",paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:1000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsFromQuery = tdSql.getRows()

        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, totalRowsFromQuery))
        if totalConsumeRows != totalRowsFromQuery:
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     500,
                    'rowsPerTbl': 1000,
                    'batchNum':   1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  20,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        # tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        # tdLog.info("create stb")
        # tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        # tdLog.info("create ctb")
        # tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
        #                      ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        # queryString = "select ts, c1, c2 from %s.%s "%(paraDict['dbName'], paraDict['stbName'])
        queryString = "select ts, c1, c2 from %s.%s where t4 == 'shanghai' and t5 == 'shanghai' "%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        consumerId     = 1
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:1000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tdLog.info("create some new child table and insert data ")
        tmqCom.insert_data_with_autoCreateTbl(tdSql,paraDict["dbName"],paraDict["stbName"],"ctby",paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"])

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsFromQuery = tdSql.getRows()

        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, totalRowsFromQuery))
        if totalConsumeRows != totalRowsFromQuery:
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 2 end ...... ")


    def test_tmq_autoCreate_tbl(self):
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
        self.prepareTestEnv()
        tdLog.printNoPrefix("=============================================")
        tdLog.printNoPrefix("======== snapshot is 0: only consume from wal")
        self.tmqCase1()
        self.tmqCase2()

        self.prepareTestEnv()
        tdLog.printNoPrefix("====================================================================")
        tdLog.printNoPrefix("======== snapshot is 1: firstly consume from tsbs, and then from wal")
        self.snapshot = 1
        self.tmqCase1()
        self.tmqCase2()

        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

