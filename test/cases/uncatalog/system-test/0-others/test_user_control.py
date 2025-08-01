from new_test_framework.utils import tdLog, tdSql, DataSet, tdDnodes
import taos
import time
import inspect
import traceback
import socket
from dataclasses  import dataclass
from datetime import datetime


PRIVILEGES_ALL      = "ALL"
PRIVILEGES_READ     = "READ"
PRIVILEGES_WRITE    = "WRITE"

WEIGHT_ALL      = 5
WEIGHT_READ     = 2
WEIGHT_WRITE    = 3

PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_utint"
SINT_UN_COL = "c_usint"
BINT_UN_COL = "c_ubint"
INT_UN_COL = "c_uint"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL = [BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [BOOL_COL, ]
TS_TYPE_COL = [TS_COL, ]

INT_TAG = "t_int"

ALL_COL = [PRIMARY_COL, INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BINARY_COL, NCHAR_COL, BOOL_COL, TS_COL]
TAG_COL = [INT_TAG]

# insert data args：
TIME_STEP = 10000
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"

class TDconnect:
    def __init__(self,
                 host       = None,
                 port       = None,
                 user       = None,
                 password   = None,
                 database   = None,
                 config     = None,
        ) -> None:
        self._conn      = None
        self._host      = host
        self._user      = user
        self._password  = password
        self._database  = database
        self._port      = port
        self._config    = config

    def __enter__(self):
        self._conn = taos.connect(
            host    =self._host,
            port    =self._port,
            user    =self._user,
            password=self._password,
            database=self._database,
            config  =self._config
        )

        self.cursor = self._conn.cursor()
        return self

    def error(self, sql, expectErrInfo = None):
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
        except BaseException as e:
            tdLog.info("err:%s" % (e))
            expectErrNotOccured = False
            self.errno = e.errno
            error_info = repr(e)
            self.error_info = ','.join(error_info[error_info.index('(')+1:-1].split(",")[:-1]).replace("'","")

        if expectErrNotOccured:
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, expect error not occured" )
        else:
            if expectErrInfo != None:
                if expectErrInfo == self.error_info:
                    self.queryRows = 0
                    self.queryCols = 0
                    self.queryResult = None
                    tdLog.info("sql:%s, expected ErrInfo '%s' occured" % (sql, expectErrInfo))
                else:
                    tdLog.exit("%s(%d) failed: sql:%s, ErrInfo '%s' occured, but not expected ErrInfo '%s'" % (caller.filename, caller.lineno, sql, self.error_info, expectErrInfo))

    def query(self, sql, row_tag=None):
        # sourcery skip: raise-from-previous-error, raise-specific-error
        self.sql = sql
        try:
            self.cursor.execute(sql)
            self.queryResult = self.cursor.fetchall()
            self.queryRows = len(self.queryResult)
            self.queryCols = len(self.cursor.description)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.notice(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, {repr(e)}")
            traceback.print_exc()
            raise Exception(repr(e))
        if row_tag:
            return self.queryResult
        return self.queryRows

    def __exit__(self, types, values, trace):
        if self._conn:
            self.cursor.close()
            self._conn.close()


def taos_connect(
    host    = socket.gethostname(),
    port    = 6030,
    user    = "root",
    passwd  = "taosdata",
    database= None,
    config  = None
):
    return TDconnect(
        host = host,
        port=port,
        user=user,
        password=passwd,
        database=database,
        config=config
    )


@dataclass
class User:
    name        : str   = None
    passwd      : str   = None
    db_set      : set   = None
    priv        : str   = None
    priv_weight : int   = 0

class TestUserControl:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    @property
    def __user_list(self):
        return  [f"user_test{i}" for i in range(self.users_count) ]

    def __users(self):
        self.users = []
        self.root_user = User()
        self.root_user.name = "root"
        self.root_user.passwd = "taosdata"
        self.root_user.db_set = set("*")
        self.root_user.priv = PRIVILEGES_ALL
        self.root_user.priv_weight = WEIGHT_ALL
        for i in range(self.users_count):
            user = User()
            user.name = f"user_test{i}"
            user.passwd = f"taosdata@1{i}"
            user.db_set = set()
            self.users.append(user)
        return self.users

    @property
    def __passwd_list(self):
        return  [f"taosdata@1{i}" for i in range(self.users_count) ]

    @property
    def __privilege(self):
        return [ PRIVILEGES_ALL, PRIVILEGES_READ, PRIVILEGES_WRITE ]

    def __priv_level(self, dbname=None):
        return f"{dbname}.*" if dbname else "*.*"


    def create_user_current(self):
        users  = self.__user_list
        passwds = self.__passwd_list
        for i in range(self.users_count):
            tdSql.execute(f"create user {users[i]} pass '{passwds[i]}' ")

        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

    def create_user_err(self):
        sqls = [
            "create users u1 pass 'u1Passwd' ",
            "create user '' pass 'u1Passwd' ",
            "create user  pass 'u1Passwd' ",
            "create user u1 pass u1Passwd ",
            "create user u1 password 'u1Passwd' ",
            "create user u1 pass u1Passwd ",
            "create user u1 pass '' ",
            "create user u1 pass '   ' ",
            "create user u1 pass  ",
            "create user u1 u2 pass 'u1Passwd' 'u2passwd' ",
            "create user u1 u2 pass 'u1Passwd', 'u2passwd' ",
            "create user u1, u2 pass 'u1Passwd', 'u2passwd' ",
            "create user u1, u2 pass 'u1Passwd'  'u2passwd' ",
            # length of user_name must <= 23
            "create user u12345678901234567890123 pass 'u1Passwd' " ,
            # length of passwd must <= 128
            "create user u1 pass 'u12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678' " ,
            # password must have not " ' ~ ` \
            "create user u1 pass 'u1Passwd\\' " ,
            "create user u1 pass 'u1Passwd~' " ,
            "create user u1 pass 'u1Passwd\"' " ,
            "create user u1 pass 'u1Passwd\'' " ,
            "create user u1 pass 'u1Passwd`' " ,
            # must after create a user named u1
            "create user u1 pass 'u1Passwd' " ,
        ]

        tdSql.execute("create user u1 pass 'u1Passwd' ")
        for sql in sqls:
            tdSql.error(sql)

        tdSql.execute("DROP USER u1")

    def __alter_pass_sql(self, user, passwd):
        return f'''ALTER USER {user} PASS '{passwd}' '''

    def alter_pass_current(self):
        self.__init_pass = True
        for count, i in enumerate(range(self.users_count)):
            if self.__init_pass:
                tdSql.query(self.__alter_pass_sql(self.__user_list[i], f"new{self.__passwd_list[i]}"))
                self.__init_pass = count != self.users_count - 1
            else:
                tdSql.query(self.__alter_pass_sql(self.__user_list[i], self.__passwd_list[i] ) )
                self.__init_pass = count == self.users_count - 1

    def alter_pass_err(self):  # sourcery skip: remove-redundant-fstring
        sqls = [
            f"alter users {self.__user_list[0]} pass 'newpassT1' " ,
            f"alter user {self.__user_list[0]} pass '' " ,
            f"alter user {self.__user_list[0]} pass '  ' " ,
            f"alter user anyuser pass 'newpassT1' " ,
            f"alter user {self.__user_list[0]} pass  " ,
            f"alter user {self.__user_list[0]} password 'newpassT1'  " ,
        ]
        for sql in sqls:
            tdSql.error(sql)

    def __grant_user_privileges(self, privilege,  dbname=None, user_name="root"):
        return f"GRANT {privilege} ON {self.__priv_level(dbname)} TO {user_name} "

    def __revoke_user_privileges(self, privilege,  dbname=None, user_name="root"):
        return f"REVOKE {privilege} ON {self.__priv_level(dbname)} FROM {user_name} "

    def __user_check(self, user:User=None, check_priv=PRIVILEGES_ALL):
        if user is None:
            user = self.root_user
        with taos_connect(user=user.name, passwd=user.passwd) as use:
            time.sleep(2)
            if check_priv == PRIVILEGES_ALL:
                use.query(f"use {DBNAME}")
                use.query(f"show {DBNAME}.tables")
                use.query(f"select * from {DBNAME}.{CTBNAME}")
                use.query(f"insert into {DBNAME}.{CTBNAME} (ts) values (now())")
            elif check_priv == PRIVILEGES_READ:
                use.query(f"use {DBNAME}")
                use.query(f"show {DBNAME}.tables")
                use.query(f"select * from {DBNAME}.{CTBNAME}")
                use.error(f"insert into {DBNAME}.{CTBNAME} (ts) values (now())")
            elif check_priv == PRIVILEGES_WRITE:
                use.query(f"use {DBNAME}")
                use.query(f"show {DBNAME}.tables")
                use.error(f"select * from {DBNAME}.{CTBNAME}")
                use.query(f"insert into {DBNAME}.{CTBNAME} (ts) values (now())")
            elif check_priv is None:
                use.error(f"use {DBNAME}")
                use.error(f"show {DBNAME}.tables")
                use.error(f"show tables")
                use.error(f"select * from {DBNAME}.{CTBNAME}")
                use.error(f"insert into {DBNAME}.{CTBNAME} (ts) values (now())")

    def __change_user_priv(self, user: User, pre_priv, invoke=False):
        if user.priv == pre_priv and invoke :
            return
        if user.name == "root":
            return

        if pre_priv.upper() == PRIVILEGES_ALL:
            pre_weight = -5 if invoke else 5
        elif pre_priv.upper() == PRIVILEGES_READ:
            pre_weight = -2 if invoke else 2
        elif pre_priv.upper() == PRIVILEGES_WRITE:
            pre_weight = -3 if invoke else 3
        else:
            return
        pre_weight += user.priv_weight

        if pre_weight >= 5:
            user.priv = PRIVILEGES_ALL
            user.priv_weight = 5
        elif pre_weight == 3:
            user.priv = PRIVILEGES_WRITE
            user.priv_weight = pre_weight
        elif pre_weight == 2:
            user.priv_weight = pre_weight
            user.priv = PRIVILEGES_READ
        elif pre_weight in [1, -1]:
            return
        elif pre_weight <= 0:
            user.priv_weight = 0
            user.priv = ""

        return user

    def grant_user(self, user: User = None, priv=PRIVILEGES_ALL, dbname=None):
        if not user:
            user = self.root_user
        sql = self.__grant_user_privileges(privilege=priv, dbname=dbname, user_name=user.name)
        tdLog.info(sql)
        if (user not in self.users and user.name != "root") or priv not in (PRIVILEGES_ALL, PRIVILEGES_READ, PRIVILEGES_WRITE):
            tdSql.error(sql)
        tdSql.query(sql)
        self.__change_user_priv(user=user, pre_priv=priv)
        user.db_set.add(dbname)
        time.sleep(1)

    def revoke_user(self, user: User = None, priv=PRIVILEGES_ALL, dbname=None):
        sql = self.__revoke_user_privileges(privilege=priv, dbname=dbname, user_name=user.name)
        tdLog.info(sql)
        if user is None or priv not in (PRIVILEGES_ALL, PRIVILEGES_READ, PRIVILEGES_WRITE):
            tdSql.error(sql)
        tdSql.query(sql)
        self.__change_user_priv(user=user, pre_priv=priv, invoke=True)
        if user.name != "root":
            user.db_set.discard(dbname) if dbname else user.db_set.clear()
        time.sleep(1)

    def run_priv_change_current(self):
        tdLog.printNoPrefix("==========step 1.0: if do not grant, can not read/write")
        self.__user_check(user=self.root_user)
        self.__user_check(user=self.users[0], check_priv=None)

        tdLog.printNoPrefix("==========step 1.1: grant read, can read, can not write")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.2: grant write, can write")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.3: grant all, can write and read")
        self.grant_user(user=self.users[2])
        self.__user_check(user=self.users[2], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.4:  grant read to write = all ")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.5:  revoke write from all = read ")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.6: grant write to read = all")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.7:  revoke read from all = write ")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.8: grant read to all = all")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.9: grant write to all = all")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.10: grant all to read = all")
        self.grant_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_ALL)

        tdLog.printNoPrefix("==========step 1.11: grant all to write = all")
        self.grant_user(user=self.users[1], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_ALL)

        ### init user
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.12: revoke read from write = no change")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[1], check_priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.13: revoke write from read = no change")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[0], check_priv=PRIVILEGES_READ)

        tdLog.printNoPrefix("==========step 1.14: revoke read from read = nothing")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.__user_check(user=self.users[0], check_priv=None)

        tdLog.printNoPrefix("==========step 1.15: revoke write from write = nothing")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_WRITE)
        self.__user_check(user=self.users[1], check_priv=None)

        ### init user
        self.grant_user(user=self.users[0], priv=PRIVILEGES_READ)
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_WRITE)

        tdLog.printNoPrefix("==========step 1.16: revoke all from write = nothing")
        self.revoke_user(user=self.users[1], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[1], check_priv=None)

        tdLog.printNoPrefix("==========step 1.17: revoke all from read = nothing")
        self.revoke_user(user=self.users[0], priv=PRIVILEGES_ALL)
        self.__user_check(user=self.users[0], check_priv=None)

        tdLog.printNoPrefix("==========step 1.18: revoke all from all = nothing")
        self.revoke_user(user=self.users[2], priv=PRIVILEGES_ALL)
        time.sleep(3)
        self.__user_check(user=self.users[2], check_priv=None)

    def __grant_err(self):
        return [
            self.__grant_user_privileges(privilege=self.__privilege[0], user_name="") ,
            self.__grant_user_privileges(privilege=self.__privilege[0], user_name="*") ,
            self.__grant_user_privileges(privilege=self.__privilege[1], dbname="not_exist_db", user_name=self.__user_list[0]),
            self.__grant_user_privileges(privilege="any_priv", user_name=self.__user_list[0]),
            self.__grant_user_privileges(privilege="", dbname="db", user_name=self.__user_list[0]) ,
            self.__grant_user_privileges(privilege=" ".join(self.__privilege), user_name=self.__user_list[0]) ,
            f"GRANT {self.__privilege[0]} ON * TO {self.__user_list[0]}" ,
            # f"GRANT {self.__privilege[0]} ON {DBNAME}.{NTBNAME} TO {self.__user_list[0]}" ,
        ]

    def __revoke_err(self):
        return [
            self.__revoke_user_privileges(privilege=self.__privilege[0], user_name="") ,
            self.__revoke_user_privileges(privilege=self.__privilege[0], user_name="*") ,
            self.__revoke_user_privileges(privilege=self.__privilege[1], dbname="not_exist_db", user_name=self.__user_list[0]),
            self.__revoke_user_privileges(privilege="any_priv", user_name=self.__user_list[0]),
            self.__revoke_user_privileges(privilege="", dbname="db", user_name=self.__user_list[0]) ,
            self.__revoke_user_privileges(privilege=" ".join(self.__privilege), user_name=self.__user_list[0]) ,
            f"REVOKE {self.__privilege[0]} ON * FROM {self.__user_list[0]}" ,
            # f"REVOKE {self.__privilege[0]} ON {DBNAME}.{NTBNAME} FROM {self.__user_list[0]}" ,
        ]

    def run_grant_err(self):
        for sql in self.__grant_err():
            tdSql.error(sql)

    def run_revoke_err(self):
        for sql in self.__revoke_err():
            tdSql.error(sql)

    def run_change_priv(self):
        self.run_grant_err()
        self.run_revoke_err()
        self.run_priv_change_current()

    def run_user_create(self):
        self.create_user_current()
        self.create_user_err()

    def run_alter_pass(self):
        self.alter_pass_current()
        self.alter_pass_err()

    def user_login(self, user, passwd):
        login_except = False
        try:
            with taos_connect(user=user, passwd=passwd) as conn:
                cursor = conn.cursor
        except BaseException:
            login_except = True
            cursor = None
        return login_except, cursor

    def login_currrent(self, user, passwd):
        login_except, _ = self.user_login(user, passwd)
        if login_except:
            tdLog.exit(f"connect failed, user: {user} and pass: {passwd} do not match!")
        else:
            tdLog.info("connect successfully, user and pass matched!")

    def login_err(self, user, passwd):
        login_except, _ = self.user_login(user, passwd)
        if login_except:
            tdLog.info("connect failed, except error occured!")
        else:
            tdLog.exit("connect successfully, except error not occrued!")

    def run_alter_user(self):
        options = ["enable", "sysinfo", "createdb"]
        optionErrVals = [-10000, -128, -1, 2, 127, 1000, 10000]
        for optionErrVal in optionErrVals:
            tdSql.error("create user user_alter pass 'taosdata' sysinfo %d" % optionErrVal)
        tdSql.execute("create user user_alter pass 'taosdata'")
        for option in options:
            for optionErrVal in optionErrVals:
                tdSql.error("alter user user_alter %s %d" % (option, optionErrVal))
        tdSql.execute("drop user user_alter")

    def __drop_user(self, user):
        return f"DROP USER {user}"

    def drop_user_current(self):
        for user in self.__user_list:
            tdSql.query(self.__drop_user(user))

    def drop_user_error(self):
        sqls = [
            f"DROP {self.__user_list[0]}",
            f"DROP user {self.__user_list[0]}  {self.__user_list[1]}",
            f"DROP user {self.__user_list[0]} , {self.__user_list[1]}",
            f"DROP users {self.__user_list[0]}  {self.__user_list[1]}",
            f"DROP users {self.__user_list[0]} , {self.__user_list[1]}",
            # "DROP user root",
            "DROP user abcde",
            "DROP user ALL",
        ]

        for sql in sqls:
            tdSql.error(sql)

    def run_drop_user(self):
        # must drop err first
        self.drop_user_error()
        self.drop_user_current()

    def __create_tb(self, stb=STBNAME, ctb_num=20, ntbnum=1, dbname=DBNAME):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table {dbname}.{stb}(
                {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        tdSql.execute(create_stb_sql)

        for i in range(ntbnum):
            create_ntb_sql = f'''create table {dbname}.nt{i+1}(
                    {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
            tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __insert_data(self, rows, ctb_num=20, dbname=DBNAME, star_time=NOW):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        # from ...pytest.util.common import DataSet
        data = DataSet()
        data.get_order_set(rows)

        for i in range(rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.utint_data[i]},
                {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
            '''
            tdSql.execute( f"insert into {dbname}.{NTBNAME} values ( {star_time - i * int(TIME_STEP * 1.2)}, {row_data} )" )

            for j in range(ctb_num):
                tdSql.execute( f"insert into {dbname}.ct{j+1} values ( {star_time - j * i * TIME_STEP}, {row_data} )" )

    def test_user_control(self):
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
        self.__create_tb()
        self.rows = 10
        self.users_count = 5
        self.__insert_data(self.rows)
        self.users = self.__users()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        # 默认只有 root 用户
        tdLog.printNoPrefix("==========step0: init, user list only has root account")
        tdSql.query("show users")
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "1")

        # root用户权限
        # 创建用户测试
        tdLog.printNoPrefix("==========step1: create user test")
        self.run_user_create()

        # 查看用户
        tdLog.printNoPrefix("==========step2: show user test")
        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

        # 密码登录认证
        self.login_currrent(self.__user_list[0], self.__passwd_list[0])
        self.login_err(self.__user_list[0], f"new{self.__passwd_list[0]}")

        # 用户权限设置
        self.run_change_priv()

        # 修改密码
        tdLog.printNoPrefix("==========step3: alter user pass test")
        self.run_alter_pass()

        # 密码修改后的登录认证
        tdLog.printNoPrefix("==========step4: check login test")
        self.login_err(self.__user_list[0], self.__passwd_list[0])
        self.login_currrent(self.__user_list[0], f"new{self.__passwd_list[0]}")

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("show users")
        tdSql.checkRows(self.users_count + 1)

        # 普通用户权限
        # 密码登录
        # _, user = self.user_login(self.__user_list[0], f"new{self.__passwd_list[0]}")
        with taos_connect(user=self.__user_list[0], passwd=f"new{self.__passwd_list[0]}") as user:
            # user = conn
            # 不能创建用户
            tdLog.printNoPrefix("==========step4.1: normal user can not create user")
            user.error("create use utest1 pass 'utest1Pass'")
            # 可以查看用户
            tdLog.printNoPrefix("==========step4.2: normal user can show user")
            user.query("show users")
            assert user.queryRows == self.users_count + 1
            # 不可以修改其他用户的密码
            tdLog.printNoPrefix("==========step4.3: normal user can not alter other user pass")
            user.error(self.__alter_pass_sql(self.__user_list[1], self.__passwd_list[1] ))
            user.error(self.__alter_pass_sql("root", "taosdata_root" ))
            # 可以修改自己的密码
            tdLog.printNoPrefix("==========step4.4: normal user can alter owner pass")
            user.query(self.__alter_pass_sql(self.__user_list[0], self.__passwd_list[0]))
            # 不可以删除用户，包括自己
            tdLog.printNoPrefix("==========step4.5: normal user can not drop any user ")
            user.error(f"drop user {self.__user_list[0]}")
            user.error(f"drop user {self.__user_list[1]}")
            user.error("drop user root")
            # 普通用户默认不可创建 db
            user.error("create database ordinary_user_db", expectErrInfo='Insufficient privilege for operation')
            tdSql.execute(f'alter user {self.__user_list[0]} createdb 1')
            tdSql.execute(f'alter user {self.__user_list[0]} createdb 0')
            tdSql.execute(f'alter user {self.__user_list[0]} createdb 1')
            tdSql.execute(f'alter user {self.__user_list[0]} createdb 1')
            user.query("create database ordinary_user_db")
            user.query("drop database ordinary_user_db")
            tdSql.execute(f'alter user {self.__user_list[0]} createdb 0')
            user.error("create database ordinary_user_db", expectErrInfo='Insufficient privilege for operation')

        tdLog.printNoPrefix("==========step5: enable info")
        taos1_conn = taos.connect(user=self.__user_list[1], password=f"new{self.__passwd_list[1]}")
        taos1_conn.query(f"show databases")
        tdSql.execute(f"alter user {self.__user_list[1]} enable 0")
        tdSql.execute(f"alter user {self.__user_list[2]} enable 0")
        taos1_except = True
        try:
            taos1_conn.query("show databases")
        except BaseException:
            taos1_except = False
        if taos1_except:
            tdLog.exit("taos 1 connect except error not occured,  when enable == 0, should not r/w ")
        else:
            tdLog.info("taos 1 connect except error occured,  enable == 0")

        taos2_except = True
        try:
            taos.connect(user=self.__user_list[2], password=f"new{self.__passwd_list[2]}")
        except BaseException:
            taos2_except = False
        if taos2_except:
            tdLog.exit("taos 2 connect except error not occured,  when enable == 0, should not connect")
        else:
            tdLog.info("taos 2 connect except error occured,  enable == 0, can not login")

        tdLog.printNoPrefix("==========step6: sysinfo info")
        taos3_conn = taos.connect(user=self.__user_list[3], password=f"new{self.__passwd_list[3]}")
        taos3_conn.query(f"show dnodes")
        taos3_conn.query(f"show {DBNAME}.vgroups")
        tdSql.execute(f"alter user {self.__user_list[3]} sysinfo 0")
        tdSql.execute(f"alter user {self.__user_list[4]} sysinfo 0")
        taos3_except = True
        try:
            taos3_conn.query(f"show dnodes")
            taos3_conn.query(f"show {DBNAME}.vgroups")
        except BaseException:
            taos3_except = False
        if taos3_except:
            tdLog.exit("taos 3 query except error not occured,  when sysinfo == 0, should not show info:dnode/monde/qnode ")
        else:
            tdLog.info("taos 3 query except error occured,  sysinfo == 0, can not show dnode/vgroups")

        taos4_conn = taos.connect(user=self.__user_list[4], password=f"new{self.__passwd_list[4]}")
        taos4_except = True
        try:
            taos4_conn.query(f"show mnodes")
            taos4_conn.query(f"show {DBNAME}.vgroups")
        except BaseException:
            taos4_except = False
        if taos4_except:
            tdLog.exit("taos 4 query except error not occured,  when sysinfo == 0, when enable == 0, should not show info:dnode/monde/qnode")
        else:
            tdLog.info("taos 4 query except error occured,  sysinfo == 0, can not show dnode/vgroups")

        # alter 用户测试
        tdLog.printNoPrefix("==========step7: alter ordinary user")
        self.run_alter_user()

        # root删除用户测试
        tdLog.printNoPrefix("==========step8: super user drop normal user")
        self.run_drop_user()

        tdSql.query("show users")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "1")

        tdDnodes.stop(1)
        tdDnodes.start(1)

        # 删除后无法登录
        self.login_err(self.__user_list[0], self.__passwd_list[0])
        self.login_err(self.__user_list[0], f"new{self.__passwd_list[0]}")
        self.login_err(self.__user_list[1], self.__passwd_list[1])
        self.login_err(self.__user_list[1], f"new{self.__passwd_list[1]}")

        tdSql.query("show users")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")
        tdSql.checkData(0, 1, "1")


        tdLog.success(f"{__file__} successfully executed")

