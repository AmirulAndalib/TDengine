# author : wenzhouwww
from new_test_framework.utils import tdLog, tdSql
from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os


import datetime
import inspect
import time
import socket
import subprocess
import threading
sys.path.append(os.path.dirname(__file__))

class Test4dnode1mnodeBasicReplica3QuerydatasStopFollower:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.mnode_list = {}
        cls.dnode_list = {}
        cls.ts = 1483200000000
        cls.ts_step =1000
        cls.db_name ='testdb'
        cls.replica = 3
        cls.vgroups = 1
        cls.tb_nums = 10
        cls.row_nums = 100
        cls.stop_dnode_id = None
        cls.loop_restart_times = 1
        cls.thread_list = []
        cls.max_restart_time = 30
        cls.try_check_times = 10
        cls.query_times = 5


    def check_setup_cluster_status(self):
        tdSql.query("select * from information_schema.ins_mnodes")
        for mnode in tdSql.queryResult:
            name = mnode[1]
            info = mnode
            self.mnode_list[name] = info

        tdSql.query("select * from information_schema.ins_dnodes")
        for dnode in tdSql.queryResult:
            name = dnode[1]
            info = dnode
            self.dnode_list[name] = info

        count = 0
        is_leader = False
        mnode_name = ''
        for k,v in self.mnode_list.items():
            count +=1
            # only for 1 mnode
            mnode_name = k

            if v[2] =='leader':
                is_leader=True

        if count==1 and is_leader:
            tdLog.notice("===== depoly cluster success with 1 mnode as leader =====")
        else:
            tdLog.notice("===== depoly cluster fail with 1 mnode as leader =====")

        for k ,v in self.dnode_list.items():
            if k == mnode_name:
                if v[3]==0:
                    tdLog.notice("===== depoly cluster mnode only success at {} , support_vnodes is {} ".format(mnode_name,v[3]))
                else:
                    tdLog.notice("===== depoly cluster mnode only fail at {} , support_vnodes is {} ".format(mnode_name,v[3]))
            else:
                continue

    def create_database(self, dbname, replica_num ,vgroup_nums ):
        drop_db_sql = "drop database if exists {}".format(dbname)
        create_db_sql = "create database {} replica {} vgroups {}".format(dbname,replica_num,vgroup_nums)

        tdLog.notice(" ==== create database {} and insert rows begin =====".format(dbname))
        tdSql.execute(drop_db_sql)
        tdSql.execute(create_db_sql)
        tdSql.execute("use {}".format(dbname))

    def create_stable_insert_datas(self,dbname ,stablename , tb_nums , row_nums):
        tdSql.execute("use {}".format(dbname))
        tdSql.execute(
        '''create table {}
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''.format(stablename)
        )

        for i in range(tb_nums):
            sub_tbname = "sub_{}_{}".format(stablename,i)
            tdSql.execute("create table {} using {} tags({})".format(sub_tbname, stablename ,i))
            # insert datas about new database

            for row_num in range(row_nums):
                ts = self.ts + self.ts_step*row_num
                tdSql.execute(f"insert into {sub_tbname} values ({ts}, {row_num} ,{row_num}, 10 ,1 ,{row_num} ,{row_num},true,'bin_{row_num}','nchar_{row_num}',now) ")

        tdLog.notice(" ==== stable {} insert rows execute end =====".format(stablename))

    def append_rows_of_exists_tables(self,dbname ,stablename , tbname , append_nums ):

        tdSql.execute("use {}".format(dbname))

        for row_num in range(append_nums):
            tdSql.execute(f"insert into {tbname} values (now, {row_num} ,{row_num}, 10 ,1 ,{row_num} ,{row_num},true,'bin_{row_num}','nchar_{row_num}',now) ")
            # print(f"insert into {tbname} values (now, {row_num} ,{row_num}, 10 ,1 ,{row_num} ,{row_num},true,'bin_{row_num}','nchar_{row_num}',now) ")
        tdLog.notice(" ==== append new rows of table {} belongs to  stable {} execute end =====".format(tbname,stablename))
        os.system("taos -s 'select count(*) from {}.{}';".format(dbname,stablename))

    def check_insert_rows(self, dbname, stablename , tb_nums , row_nums, append_rows):

        tdSql.execute("use {}".format(dbname))

        tdSql.query("select count(*) from {}.{}".format(dbname,stablename))

        while not tdSql.queryResult:
            time.sleep(0.1)
            tdSql.query("select count(*) from {}.{}".format(dbname,stablename))

        status_OK = self.mycheckData("select count(*) from {}.{}".format(dbname,stablename) ,0 , 0 , tb_nums*row_nums+append_rows)

        count = 0
        while not status_OK :
            if count > self.try_check_times:
                os.system("taos -s ' show {}.vgroups; '".format(dbname))
                tdLog.notice(" ==== check insert rows failed  after {}  try check {} times  of database {}".format(count , self.try_check_times ,dbname))
                break
            time.sleep(0.1)
            tdSql.query("select count(*) from {}.{}".format(dbname,stablename))
            while not tdSql.queryResult:
                time.sleep(0.1)
                tdSql.query("select count(*) from {}.{}".format(dbname,stablename))
            status_OK = self.mycheckData("select count(*) from {}.{}".format(dbname,stablename) ,0 , 0 , tb_nums*row_nums+append_rows)
            tdLog.notice(" ==== check insert rows first failed , this is {}_th retry check rows of database {}".format(count , dbname))
            count += 1


        tdSql.query("select distinct tbname from {}.{}".format(dbname,stablename))
        while not tdSql.queryResult:
            time.sleep(0.1)
            tdSql.query("select distinct tbname from {}.{}".format(dbname,stablename))
        status_OK = self.mycheckRows("select distinct tbname from {}.{}".format(dbname,stablename) ,tb_nums)
        count = 0
        while not status_OK :
            if count > self.try_check_times:
                os.system("taos -s ' show {}.vgroups;'".format(dbname))
                tdLog.notice(" ==== check insert rows failed  after {}  try check {} times  of database {}".format(count , self.try_check_times ,dbname))
                break
            time.sleep(0.1)
            tdSql.query("select distinct tbname from {}.{}".format(dbname,stablename))
            while not tdSql.queryResult:
                time.sleep(0.1)
                tdSql.query("select distinct tbname from {}.{}".format(dbname,stablename))
            status_OK = self.mycheckRows("select distinct tbname from {}.{}".format(dbname,stablename) ,tb_nums)
            tdLog.notice(" ==== check insert tbnames first failed , this is {}_th retry check tbnames of database {}".format(count , dbname))
            count += 1

    def _get_stop_dnode_id(self,dbname ,dnode_role):
        tdSql.query("show {}.vgroups".format(dbname))
        vgroup_infos = tdSql.queryResult
        status = False
        for vgroup_info in vgroup_infos:
            if "error" not in vgroup_info:
                status = True
            else:
                status = False
        while status!=True :
            time.sleep(0.1)
            tdSql.query("show {}.vgroups".format(dbname))
            vgroup_infos = tdSql.queryResult
            for vgroup_info in vgroup_infos:
                if "error" not in vgroup_info:
                    status = True
                else:
                    status = False
            # print(status)
        for vgroup_info in vgroup_infos:
            leader_infos = vgroup_info[3:-4]
            # print(vgroup_info)
            for ind ,role in enumerate(leader_infos):
                if role == dnode_role:
                    # print(ind,leader_infos)
                    self.stop_dnode_id = leader_infos[ind-1]
                    break

        return self.stop_dnode_id

    def wait_stop_dnode_OK(self ,newTdSql):
    
        def _get_status():
            # newTdSql=tdCom.newTdSql()

            status =  ""
            newTdSql.query("select * from information_schema.ins_dnodes")
            dnode_infos = newTdSql.queryResult
            for dnode_info in dnode_infos:
                id = dnode_info[0]
                dnode_status = dnode_info[4]
                if id == self.stop_dnode_id:
                    status = dnode_status
                    break
            return status

        status = _get_status()
        while status !="offline":
            time.sleep(0.1)
            status = _get_status()
            # tdLog.notice("==== stop dnode has not been stopped , endpoint is {}".format(self.stop_dnode))
        tdLog.notice("==== stop_dnode has stopped , id is {}".format(self.stop_dnode_id))

    def wait_start_dnode_OK(self ,newTdSql):
    
        def _get_status():
            # newTdSql=tdCom.newTdSql()
            status =  ""
            newTdSql.query("select * from information_schema.ins_dnodes")
            dnode_infos = newTdSql.queryResult
            for dnode_info in dnode_infos:
                id = dnode_info[0]
                dnode_status = dnode_info[4]
                if id == self.stop_dnode_id:
                    status = dnode_status
                    break
            return status

        status = _get_status()
        while status !="ready":
            time.sleep(0.1)
            status = _get_status()
            # tdLog.notice("==== stop dnode has not been stopped , endpoint is {}".format(self.stop_dnode))
        tdLog.notice("==== stop_dnode has restart , id is {}".format(self.stop_dnode_id))

    def _parse_datetime(self,timestr):
        try:
            return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass

    def mycheckRowCol(self, sql, row, col):
        caller = inspect.getframeinfo(inspect.stack()[2][0])
        if row < 0:
            args = (caller.filename, caller.lineno, sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is smaller than zero" % args)
        if col < 0:
            args = (caller.filename, caller.lineno, sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is smaller than zero" % args)
        if row > tdSql.queryRows:
            args = (caller.filename, caller.lineno, sql, row, tdSql.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)
        if col > tdSql.queryCols:
            args = (caller.filename, caller.lineno, sql, col, tdSql.queryCols)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args)

    def mycheckData(self, sql ,row, col, data):
        check_status = True
        self.mycheckRowCol(sql ,row, col)
        if tdSql.queryResult[row][col] != data:
            if tdSql.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if (len(data) >= 28):
                    if pd.to_datetime(tdSql.queryResult[row][col]) == pd.to_datetime(data):
                        tdLog.info("sql:%s, row:%d col:%d data:%d == expect:%s" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                else:
                    if tdSql.queryResult[row][col] == self._parse_datetime(data):
                        tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                return

            if str(tdSql.queryResult[row][col]) == str(data):
                tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                return
            elif isinstance(data, float) and abs(tdSql.queryResult[row][col] - data) <= 0.000001:
                tdLog.info("sql:%s, row:%d col:%d data:%f == expect:%f" %
                            (sql, row, col, tdSql.queryResult[row][col], data))
                return
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, sql, row, col, tdSql.queryResult[row][col], data)
                tdLog.info("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)

                check_status = False

        if data is None:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (sql, row, col, tdSql.queryResult[row][col], data))
        elif isinstance(data, str):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (sql, row, col, tdSql.queryResult[row][col], data))
        # elif isinstance(data, datetime.date):
        #     tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
        #                (sql, row, col, tdSql.queryResult[row][col], data))
        elif isinstance(data, float):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (sql, row, col, tdSql.queryResult[row][col], data))
        else:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%d" %
                       (sql, row, col, tdSql.queryResult[row][col], data))

        return check_status

    def mycheckRows(self, sql, expectRows):
        check_status = True
        if len(tdSql.queryResult) == expectRows:
            tdLog.info("sql:%s, queryRows:%d == expect:%d" % (sql, len(tdSql.queryResult), expectRows))
            return True
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, len(tdSql.queryResult), expectRows)
            tdLog.info("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" % args)
            check_status = False
        return check_status


    def force_stop_dnode(self, dnode_id ):

        tdSql.query("select * from information_schema.ins_dnodes")
        port = None
        for dnode_info in tdSql.queryResult:
            if dnode_id == dnode_info[0]:
                port = dnode_info[1].split(":")[-1]
                break
            else:
                continue
        if port:
            tdLog.notice(" ==== dnode {} will be force stop by kill -9 ====".format(dnode_id))
            psCmd = '''netstat -anp|grep -w LISTEN|grep -w %s |grep -o "LISTEN.*"|awk '{print $2}'|cut -d/ -f1|head -n1''' %(port)
            processID = subprocess.check_output(
                psCmd, shell=True).decode("utf-8")
            ps_kill_taosd = ''' kill -9 {} '''.format(processID)
            # print(ps_kill_taosd)
            os.system(ps_kill_taosd)

    def basic_query_task(self,dbname ,stablename):

        sql = "select * from {}.{} ;".format(dbname , stablename)

        count = 0
        while count < self.query_times:
            os.system(''' taos -s '{}' >>/dev/null '''.format(sql))
            count += 1

    def multi_thread_query_task(self, thread_nums ,dbname , stablename ):

        for i in range(thread_nums):
            task = threading.Thread(target = self.basic_query_task, args=(dbname ,stablename))
            self.thread_list.append(task)

        for thread in self.thread_list:

            thread.start()
        return self.thread_list


    def stop_follower_when_query_going(self):

        tdDnodes = cluster.dnodes
        newTdSql=tdCom.newTdSql()
        self.create_database(dbname = self.db_name ,replica_num= self.replica  , vgroup_nums= 1)
        self.create_stable_insert_datas(dbname = self.db_name , stablename = "stb1" , tb_nums= self.tb_nums ,row_nums= self.row_nums)

        # let query task start 
        self.thread_list = self.multi_thread_query_task(5 ,self.db_name ,'stb1' )


        # force stop follower
        for loop in range(self.loop_restart_times):
            tdLog.debug(" ==== this is {}_th restart follower of database {} ==== ".format(loop ,self.db_name))
            self.stop_dnode_id = self._get_stop_dnode_id(self.db_name,"follower" )
            tdDnodes[self.stop_dnode_id-1].stoptaosd()
            self.wait_stop_dnode_OK(newTdSql)

            start = time.time()
            tdDnodes[self.stop_dnode_id-1].starttaosd()
            self.wait_start_dnode_OK(newTdSql)
            end = time.time()
            time_cost = int(end-start)

            if time_cost > self.max_restart_time:
                tdLog.notice(" ==== restart dnode {} cost too much time , please check ====".format(self.stop_dnode_id))

        for thread in self.thread_list:
            thread.join()


    def test_4dnode1mnode_basic_replica3_querydatas_stop_follower(self):
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

        # basic check of cluster
        self.check_setup_cluster_status()
        self.stop_follower_when_query_going()



        tdLog.success(f"{__file__} successfully executed")

