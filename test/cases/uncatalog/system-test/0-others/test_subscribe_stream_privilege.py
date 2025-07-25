###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, TDSetSql
import time
import taos
from taos.tmq import *


class TestSubscribeStreamPrivilege:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 143, 'clientCfg':clientCfgDict}

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()
        cls.stbname = 'stb'
        cls.user_name = 'test'
        cls.binary_length = 20  # the length of binary for column_dict
        cls.nchar_length = 20  # the length of nchar for column_dict
        cls.dbnames = ['db1']
        cls.column_dict = {
            'ts': 'timestamp',
            'col1': 'float',
            'col2': 'int',
            'col3': 'float',
        }
        
        cls.tag_dict = {
            't1': 'int',
            't2': f'binary({cls.binary_length})'
        }
        
        cls.tag_list = [
            f'1, "Beijing"',
            f'2, "Shanghai"',
            f'3, "Guangzhou"',
            f'4, "Shenzhen"'
        ]
        
        cls.values_list = [
            f'now, 9.1, 200, 0.3'            
        ]
        
        cls.tbnum = 4
        cls.topic_name = 'topic1'


    def prepare_data(self):
        for db in self.dbnames:
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname, self.column_dict, self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({j})')

    def checkUserPrivileges(self, rowCnt):
        tdSql.query("show user privileges")
        tdSql.checkRows(rowCnt)

    def streamTest(self):
        tdSql.execute("create stream s1 trigger at_once fill_history 1 into so1 as select ts,abs(col2) from stb partition by tbname")
        time.sleep(2)
        tdSql.query("select * from so1")
        tdSql.checkRows(4)
        tdSql.execute("insert into stb_0(ts,col2) values(now, 332)")
        time.sleep(2)
        tdSql.query("select * from so1")
        tdSql.checkRows(5)

        time.sleep(2)
        tdSql.query("select * from information_schema.ins_stream_tasks")
        tdSql.checkData(0, 5, 'ready')

        print(time.time())
        while 1:
            t = time.time()
            if t > 1706254434 :
                break
            else:
                print("time:%d" %(t))
                time.sleep(1)


        tdSql.error("create stream s11 trigger at_once fill_history 1 into so1 as select ts,abs(col2) from stb partition by tbname")

        time.sleep(10)
        tdSql.query("select * from information_schema.ins_stream_tasks")
        tdSql.checkData(0, 5, 'paused')
        tdSql.execute("insert into stb_0(ts,col2) values(now, 3232)")
        tdSql.query("select * from so1")
        tdSql.checkRows(5)

        tdSql.error("resume stream s1")

    def consumeTest(self):
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": self.user_name,
            "td.connect.pass": "123456rf@#",
            "auto.offset.reset": "earliest"
        }
        consumer = Consumer(consumer_dict)

        tdLog.debug("test subscribe topic created by other user")
        exceptOccured = False
        try:
            consumer.subscribe([self.topic_name])
        except TmqError:
            exceptOccured = True

        if not exceptOccured:
            tdLog.exit(f"has no privilege, should except")

        self.checkUserPrivileges(1)
        tdLog.debug("test subscribe topic privilege granted by other user")
        tdSql.execute(f'grant subscribe on {self.topic_name} to {self.user_name}')
        self.checkUserPrivileges(2)

        exceptOccured = False
        try:
            consumer.subscribe([self.topic_name])
        except TmqError:
            exceptOccured = True

        if exceptOccured:
            tdLog.exit(f"has privilege, should not except")

        cnt = 0
        try:
            while True:
                res = consumer.poll(1)
                cnt += 1
                if cnt == 1:
                    if not res:
                        tdLog.exit(f"grant privilege, should get res")
                elif cnt == 2:
                    if res:
                        tdLog.exit(f"revoke privilege, should get NULL")
                    else:
                        break

                tdLog.debug("test subscribe topic privilege revoked by other user")
                tdSql.execute(f'revoke subscribe on {self.topic_name} from {self.user_name}')
                self.checkUserPrivileges(1)
                time.sleep(5)

        finally:
            consumer.close()

    def create_user(self):
        tdSql.execute(f'create topic {self.topic_name} as database {self.dbnames[0]}')
        tdSql.execute(f'create user {self.user_name} pass "123456rf@#"')

    def test_subscribe_stream_privilege(self):
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
        self.prepare_data()
        self.create_user()
        self.consumeTest()
        # self.streamTest()

        tdLog.success("%s successfully executed" % __file__)


