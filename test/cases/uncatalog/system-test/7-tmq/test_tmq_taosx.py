
import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils import tdLog, tdSql, tdCom
from taos.tmq import *

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def checkJson(self, cfgPath, name):
        srcFile = '%s/../log/%s.source'%(cfgPath, name)
        dstFile = '%s/../log/%s.result'%(cfgPath, name)
        tdLog.info("compare file: %s, %s"%(srcFile, dstFile))

        consumeFile = open(srcFile, mode='r')
        queryFile = open(dstFile, mode='r')

        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()
            if src:
                if dst != src:
                    tdLog.exit("compare error: %s != %s"%(src, dst))
            else:
                break
        return

    def checkDropData(self, drop):
        tdSql.execute('use db_taosx')
        tdSql.query("show tables")
        if drop:
            tdSql.checkRows(11)
        else:
            tdSql.checkRows(16)
        tdSql.query("select * from jt order by i")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(0, 2, '{"k1":1,"k2":"hello"}')
        tdSql.checkData(1, 2, None)

        tdSql.query("select * from sttb order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 2, 25)
        tdSql.checkData(0, 5, "sttb3")
        tdSql.checkData(1, 5, "sttb4")

        tdSql.query("select * from stt order by ts")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(2, 1, 21)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(2, 2, 21)
        tdSql.checkData(0, 5, "stt3")
        tdSql.checkData(2, 5, "stt4")

        tdSql.execute('use abc1')
        tdSql.query("show tables")
        if drop:
            tdSql.checkRows(11)
        else:
            tdSql.checkRows(16)
        tdSql.query("select * from jt order by i")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(0, 2, '{"k1":1,"k2":"hello"}')
        tdSql.checkData(1, 2, None)

        tdSql.query("select * from sttb order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 2, 25)
        tdSql.checkData(0, 5, "sttb3")
        tdSql.checkData(1, 5, "sttb4")

        tdSql.query("select * from stt order by ts")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(2, 1, 21)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(2, 2, 21)
        tdSql.checkData(0, 5, "stt3")
        tdSql.checkData(2, 5, "stt4")

        return

    def checkData(self):
        tdSql.execute('use db_taosx')
        tdSql.query("select * from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from ct3 order by c1 desc")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 51)
        tdSql.checkData(0, 4, 940)
        tdSql.checkData(1, 1, 23)
        tdSql.checkData(1, 4, None)

        tdSql.query("select * from st1 order by ts")
        tdSql.checkRows(14)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(4, 1, 4)
        tdSql.checkData(6, 1, 23)

        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(4, 2, 3)
        tdSql.checkData(6, 2, 32)

        tdSql.checkData(0, 3, 'a')
        tdSql.checkData(1, 3, 'b')
        tdSql.checkData(4, 3, 'hwj')
        tdSql.checkData(6, 3, 's21ds')

        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(5, 4, 940)
        tdSql.checkData(6, 4, None)

        tdSql.checkData(0, 5, 1000)
        tdSql.checkData(1, 5, 2000)
        tdSql.checkData(4, 5, 1000)
        tdSql.checkData(6, 5, 5000)

        tdSql.checkData(0, 6, 'ttt')
        tdSql.checkData(1, 6, None)
        tdSql.checkData(4, 6, 'ttt')
        tdSql.checkData(6, 6, None)

        tdSql.checkData(0, 7, True)
        tdSql.checkData(1, 7, None)
        tdSql.checkData(4, 7, True)
        tdSql.checkData(6, 7, None)

        tdSql.checkData(0, 8, None)
        tdSql.checkData(1, 8, None)
        tdSql.checkData(4, 8, None)
        tdSql.checkData(6, 8, None)

        tdSql.query("select * from ct1")
        tdSql.checkRows(7)

        tdSql.query("select * from ct2")
        tdSql.checkRows(0)

        tdSql.query("select * from ct0 order by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "a")
        tdSql.checkData(1, 4, None)

        tdSql.query("select * from n1 order by cc3 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "eeee")
        tdSql.checkData(1, 2, 940)

        tdSql.query("select * from jt order by i desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, '{"k1":1,"k2":"hello"}')

        time.sleep(10)
        tdSql.query("select * from information_schema.ins_tables where table_name = 'stt4'")
        uid1 = tdSql.getData(0, 5)
        uid2 = tdSql.getData(1, 5)
        tdSql.checkNotEqual(uid1, uid2)
        return

    def checkWal1Vgroup(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp")
        self.checkData()
        self.checkDropData(False)

        return

    def checkWal1VgroupOnlyMeta(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -d -onlymeta'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp")

        return

    def checkWal1VgroupTable(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -t'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp")
    
        return

    def checkWalMultiVgroups(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 3 -dv 5'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()
        self.checkDropData(False)

        return

    def checkWalMultiVgroupsRawData(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 3 -dv 5 -raw'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()
        self.checkDropData(False)

        return

    def checkWalMultiVgroupsWithDropTable(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 3 -dv 5 -d'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkDropData(True)

        return

    def checkSnapshot1Vgroup(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -s'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp_snapshot")
        self.checkData()
        self.checkDropData(False)

        return
    
    def checkSnapshot1VgroupBtmeta(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -s -bt'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp_snapshot")
        self.checkData()
        self.checkDropData(False)

        return

    def checkSnapshot1VgroupTable(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -s -t'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp_snapshot")

        return
    
    def checkSnapshot1VgroupTableBtmeta(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -s -t -bt'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp_snapshot")

        return

    def checkSnapshotMultiVgroups(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 2 -dv 4 -s'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()
        self.checkDropData(False)

        return
    
    def checkSnapshotMultiVgroupsBtmeta(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 2 -dv 4 -s -bt'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()
        self.checkDropData(False)

        return

    def checkSnapshotMultiVgroupsWithDropTable(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 2 -dv 4 -s -d'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkDropData(True)

        return
    
    def checkSnapshotMultiVgroupsWithDropTableBtmeta(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 2 -dv 4 -s -d -bt'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkDropData(True)

        return

    def consumeTest(self):
        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')

        tdSql.query("select * from st")
        tdSql.checkRows(8)

        tdSql.execute(f'create topic topic_all with meta as database d1')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_all"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    if index != 1:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                cnt = 0;
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 8:
                    tdLog.exit("consume error")

                index += 1
        finally:
            consumer.close()

    def consume_TS_4540_Test(self):
        tdSql.execute(f'create database if not exists test')
        tdSql.execute(f'use test')
        tdSql.execute(f'CREATE STABLE `test`.`b` ( `time` TIMESTAMP , `task_id` NCHAR(1000) ) TAGS( `key` NCHAR(1000))')
        tdSql.execute(f"insert into `test`.b1 using `test`.`b`(`key`) tags('1') (time, task_id) values ('2024-03-04 12:50:01.000', '32') `test`.b2 using `test`.`b`(`key`) tags('2') (time, task_id) values ('2024-03-04 12:50:01.000', '43') `test`.b3 using `test`.`b`(`key`) tags('3') (time, task_id) values ('2024-03-04 12:50:01.000', '123456')")

        tdSql.execute(f'create topic tt as select tbname,task_id,`key` from b')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
          consumer.subscribe(["tt"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    print(data)
                    if data != [('b1', '32', '1')] and data != [('b2', '43', '2')] and data != [('b3', '123456', '3')]:
                        tdLog.exit(f"index = 0 table b1 error")

        finally:
            consumer.close()
        
    def consume_ts_4544(self):
        tdSql.execute(f'create database if not exists d1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table stt(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into tt1 using stt tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into tt2 using stt tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into tt3 using stt tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into tt1 using stt tags(1) values(now+5s, 11) (now+10s, 12)')

        tdSql.execute(f'create topic topic_in as select * from stt where tbname in ("tt2")')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_in"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        consumer.close()

    def consume_ts_4551(self):
        tdSql.execute(f'use d1')

        tdSql.execute(f'create topic topic_stable as stable stt where tbname like "t%"')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_stable"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
        finally:
            consumer.close()
        print("consume_ts_4551 ok")

    def consume_td_31283(self):
        tdSql.execute(f'create database if not exists d31283')
        tdSql.execute(f'use d31283')

        tdSql.execute(f'create topic topic_31283 with meta as database d31283')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "true",
            # "msg.enable.batchmeta": "true"
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_31283"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdSql.execute(f'create table stt(ts timestamp, i int) tags(t int)')

        hasData = False
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
                hasData = True
        finally:
            consumer.close()

        if not hasData:
            tdLog.exit(f"consume_td_31283 error")

        print("consume_td_31283 ok")

    def consume_TS_5067_Test(self):
        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')

        tdSql.query("select * from st")
        tdSql.checkRows(8)

        tdSql.execute(f'create topic t1 as select * from st')
        tdSql.execute(f'create topic t2 as select * from st')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer1 = Consumer(consumer_dict)

        try:
            consumer1.subscribe(["t1"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer1.poll(1)
                if not res:
                    if index != 1:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                cnt = 0;
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 8:
                    tdLog.exit("consume error")

                index += 1
        finally:
            consumer1.close()

        consumer2 = Consumer(consumer_dict)
        try:
            consumer2.subscribe(["t2"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdSql.query(f'show subscriptions')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "t2")
        tdSql.checkData(0, 1, 'g1')
        tdSql.checkData(1, 0, 't1')
        tdSql.checkData(1, 1, 'g1')

        tdSql.query(f'show consumers')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'g1')
        tdSql.checkData(0, 6, 't2')
        tdSql.execute(f'drop consumer group g1 on t1')
        tdSql.query(f'show consumers')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'g1')
        tdSql.checkData(0, 6, 't2')

        tdSql.query(f'show subscriptions')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t2")
        tdSql.checkData(0, 1, 'g1')

        index = 0
        try:
            while True:
                res = consumer2.poll(1)
                if not res:
                    if index != 1:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                cnt = 0;
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 8:
                    tdLog.exit("consume error")

                index += 1
        finally:
            consumer2.close()

        consumer3 = Consumer(consumer_dict)
        try:
            consumer3.subscribe(["t2"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdSql.query(f'show consumers')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'g1')
        tdSql.checkData(0, 6, 't2')

        tdSql.execute(f'insert into t4 using st tags(3) values(now, 1)')
        try:
            res = consumer3.poll(1)
            if not res:
                tdLog.exit("consume1 error")
        finally:
            consumer3.close()

        tdSql.query(f'show consumers')
        tdSql.checkRows(0)

        tdSql.query(f'show subscriptions')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t2")
        tdSql.checkData(0, 1, 'g1')

        tdSql.execute(f'drop topic t1')
        tdSql.execute(f'drop topic t2')
        tdSql.execute(f'drop database d1')

    def consumeTest_6376(self):
        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'create table vst(ts timestamp, i int) tags(t int) virtual 1')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')
        tdSql.execute(f'create vtable vt1(t1.i) using vst tags(1)')

        tdSql.query("select * from st")
        tdSql.checkRows(8)

        tdSql.execute(f'create topic topic_all with meta as database d1')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "true",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_all"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    if index != 1:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                cnt = 0;
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 8:
                    tdLog.exit("consume error")

                index += 1
        finally:
            consumer.close()

        tdSql.execute(f'drop topic topic_all')
        tdSql.execute(f'drop database d1')

    def test_tmq_taosx(self):
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
        self.consumeTest_6376()
        self.consume_TS_5067_Test()
        self.consumeTest()
        self.consume_ts_4544()
        self.consume_ts_4551()
        self.consume_TS_4540_Test()
        self.consume_td_31283()

        tdSql.prepare()
        self.checkWal1VgroupOnlyMeta()

        self.checkWal1Vgroup()
        self.checkSnapshot1Vgroup()

        self.checkWal1VgroupTable()
        self.checkSnapshot1VgroupTable()

        self.checkWalMultiVgroups()
        # self.checkWalMultiVgroupsRawData()
        self.checkSnapshotMultiVgroups()

        self.checkWalMultiVgroupsWithDropTable()

        self.checkSnapshotMultiVgroupsWithDropTable()

        self.checkSnapshot1VgroupBtmeta()
        self.checkSnapshot1VgroupTableBtmeta()
        self.checkSnapshotMultiVgroupsBtmeta()
        self.checkSnapshotMultiVgroupsWithDropTableBtmeta()

        tdLog.success(f"{__file__} successfully executed")
