# author : wenzhouwww
from new_test_framework.utils import tdLog, tdSql
from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os 


import time
import socket
import subprocess ,threading
sys.path.append(os.path.dirname(__file__))

class Test4dnode1mnodeBasicReplica3InsertdatasQuerysLoopRestartLeader:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.mnode_list = {}
        cls.dnode_list = {}
        cls.ts = 1483200000000
        cls.db_name ='testdb'
        cls.replica = 3 
        cls.vgroups = 10
        cls.tb_nums = 10 
        cls.row_nums = 10
        cls.max_restart_time = 30
        cls.restart_server_times = 2 
        cls.query_times = 10


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

    def create_db_check_vgroups(self):

        tdSql.execute("drop database if exists test")
        tdSql.execute("create database if not exists test replica 1 duration 100")
        tdSql.execute("use test")
        time.sleep(3)
        tdSql.execute(
        '''create table stb1
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''
        )
        tdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        
        for i in range(5):
            tdSql.execute("create table sub_tb_{} using stb1 tags({})".format(i,i))
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.query("show tables")
        tdSql.checkRows(6)

        tdSql.query("show test.vgroups;")
        vgroups_infos = {}  # key is id: value is info list
        for vgroup_info in tdSql.queryResult:
            vgroup_id = vgroup_info[0]
            tmp_list = []
            for role in vgroup_info[3:-4]:
                if role in ['leader', 'leader*', 'leader**', 'follower']:
                    tmp_list.append(role)
            vgroups_infos[vgroup_id]=tmp_list

        for k , v in vgroups_infos.items():
            if len(v) == 1 and v[0] in ['leader', 'leader*', 'leader**']:
                tdLog.notice(" === create database replica only 1 role leader  check success of vgroup_id {} ======".format(k))
            else:
                tdLog.notice(" === create database replica only 1 role leader  check fail of vgroup_id {} ======".format(k))

    def create_db_replica_3_insertdatas(self, dbname, replica_num ,vgroup_nums ,tb_nums , row_nums ):
        newTdSql=tdCom.newTdSql()
        drop_db_sql = "drop database if exists {}".format(dbname)
        create_db_sql = "create database {} replica {} vgroups {}".format(dbname,replica_num,vgroup_nums)

        tdLog.notice(" ==== create database {} and insert rows begin =====".format(dbname))
        newTdSql.execute(drop_db_sql)
        time.sleep(3)
        newTdSql.execute(create_db_sql)
        time.sleep(5)
        newTdSql.execute("use {}".format(dbname))
        newTdSql.execute(
        '''create table stb1
        (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
        tags (t1 int)
        '''
        )
        newTdSql.execute(
            '''
            create table t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(32),c9 nchar(32), c10 timestamp)
            '''
        )
        
        for i in range(tb_nums):
            sub_tbname = "sub_tb_{}".format(i)
            newTdSql.execute("create table {} using stb1 tags({})".format(sub_tbname,i))
            # insert datas about new database

            for row_num in range(row_nums):
                if row_num % (int(row_nums*0.1)) == 0 :
                    tdLog.notice( " === database {} writing records {} rows".format(dbname , row_num ) )
                ts = self.ts + 1000*row_num
                newTdSql.execute(f"insert into {sub_tbname} values ({ts}, {row_num} ,{row_num}, 10 ,1 ,{row_num} ,{row_num},true,'bin_{row_num}','nchar_{row_num}',now) ")

        tdLog.notice(" ==== create database {} and insert rows execute end =====".format(dbname))

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

    def wait_stop_dnode_OK(self , newTdSql):
    
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
        tdLog.notice("==== stop_dnode has stopped , id is {} ====".format(self.stop_dnode_id))

    def wait_start_dnode_OK(self , newTdSql ):
    
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
        tdLog.notice("==== stop_dnode has restart , id is {} ====".format(self.stop_dnode_id))

    def get_leader_infos(self , newTdSql , dbname):
        
        # newTdSql=tdCom.newTdSql()
        newTdSql.query("show {}.vgroups".format(dbname))
        vgroup_infos = newTdSql.queryResult

        leader_infos = set()
        for vgroup_info in vgroup_infos:
            leader_infos.add(vgroup_info[3:-4])

        return leader_infos

    def check_revote_leader_success(self, dbname, before_leader_infos , after_leader_infos):

        check_status = False
        vote_act = set(set(after_leader_infos)-set(before_leader_infos))
        if not vote_act:
            print("=======before_revote_leader_infos ======\n" , before_leader_infos)
            print("=======after_revote_leader_infos ======\n" , after_leader_infos)
            tdLog.notice(" ===maybe revote not occured , there is no dnode offline ====")
        else:
            for vgroup_info in vote_act:
                for ind , role in enumerate(vgroup_info):  
                    if role==self.stop_dnode_id:

                        if vgroup_info[ind+1] =="offline" and "leader" in vgroup_info:
                            tdLog.notice(" === revote leader ok , leader is {} now   ====".format(vgroup_info[list(vgroup_info).index("leader")-1]))
                            check_status = True
                        elif vgroup_info[ind+1] !="offline":
                            tdLog.notice(" === dnode {} should be offline ".format(self.stop_dnode_id))
                        else:
                            continue
                        break
        return check_status


    def check_insert_status(self, newTdSql , dbname, tb_nums , row_nums):

        newTdSql.execute("use {}".format(dbname))
        os.system(''' taos -s "select count(*) from {}.{};" '''.format(dbname,'stb1'))
        # try:
        #     newTdSql.query("select count(*) from {}.{}".format(dbname,'stb1'))
        #     # tdSql.checkData(0 , 0 , tb_nums*row_nums)
        #     newTdSql.query("select distinct tbname from {}.{}".format(dbname,'stb1'))
        #     # tdSql.checkRows(tb_nums)
        # except taos.error.ProgrammingError as err:
        #     tdLog.info(err.msg)
        #     pass

    def loop_query_constantly(self, times ,  db_name, tb_nums ,row_nums):

        newTdSql=tdCom.newTdSql()
        for loop_time in range(times):
            tdLog.debug(" === query is going ,this is {}_th query === ".format(loop_time))
            
            self.check_insert_status( newTdSql ,db_name, tb_nums , row_nums)

    
    def loop_restart_follower_constantly(self, times , db_name):

        tdDnodes = cluster.dnodes
        newTdSql=tdCom.newTdSql()

        for loop_time in range(times):

            self.stop_dnode_id = self._get_stop_dnode_id(db_name , "leader")
            
            # print(self.stop_dnode_id)
            # begin stop dnode 
            start = time.time()

            before_leader_infos = self.get_leader_infos( newTdSql ,db_name)
            tdDnodes[self.stop_dnode_id-1].stoptaosd()
            self.wait_stop_dnode_OK(newTdSql)

            start = time.time()
            # get leader info after stop 
            after_leader_infos = self.get_leader_infos(newTdSql , db_name)
             
            revote_status = self.check_revote_leader_success(db_name ,before_leader_infos , after_leader_infos)

            # append rows of stablename when dnode stop make sure revote leaders
            
            while not revote_status:
                after_leader_infos = self.get_leader_infos(newTdSql , db_name)
                revote_status = self.check_revote_leader_success(db_name ,before_leader_infos , after_leader_infos)

            end = time.time()
            time_cost = end - start 
            tdLog.notice(" ==== revote leader of database {} cost time {}  ====".format(db_name , time_cost))

            tdLog.notice(" === this is {}_th restart taosd === ".format(loop_time))

            # begin start dnode 
            tdDnodes[self.stop_dnode_id-1].starttaosd()
            self.wait_start_dnode_OK(newTdSql)
            end = time.time()
            time.sleep(3)
            time_cost = int(end -start)
            if time_cost > self.max_restart_time:
                tdLog.notice(" ==== restart dnode {} cost too much time , please check ====".format(self.stop_dnode_id))
            


    def test_4dnode1mnode_basic_replica3_insertdatas_querys_loop_restart_leader(self):
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

        self.check_setup_cluster_status()
        self.create_db_check_vgroups()
        
        # start writing constantly 
        writing = threading.Thread(target = self.create_db_replica_3_insertdatas, args=(self.db_name , self.replica , self.vgroups , self.tb_nums , self.row_nums))
        writing.start()
        tdSql.query(" show {}.stables ".format(self.db_name))
        while not tdSql.queryResult:
            print(tdSql.queryResult)
            time.sleep(0.1)
            tdSql.query(" show {}.stables ".format(self.db_name))

        restart_servers = threading.Thread(target = self.loop_restart_follower_constantly, args = (self.restart_server_times ,self.db_name))
        restart_servers.start()

        reading = threading.Thread(target = self.loop_query_constantly, args=(self.query_times,self.db_name , self.tb_nums , self.row_nums))
        reading.start()
        
        writing.join()
        reading.join()
        restart_servers.join()

        tdLog.success(f"{__file__} successfully executed")

