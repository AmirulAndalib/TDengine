import datetime
from enum import Enum
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *
from util.dnodes import *
from util.sqlset import *


DBNAME = "db"

class TDDataType(Enum):
    NULL       = 0
    BOOL       = 1   
    TINYINT    = 2   
    SMALLINT   = 3   
    INT        = 4   
    BIGINT     = 5   
    FLOAT      = 6   
    DOUBLE     = 7   
    VARCHAR    = 8   
    TIMESTAMP  = 9   
    NCHAR      = 10  
    UTINYINT   = 11  
    USMALLINT  = 12  
    UINT       = 13  
    UBIGINT    = 14  
    JSON       = 15  
    VARBINARY  = 16  
    DECIMAL    = 17  
    BLOB       = 18  
    MEDIUMBLOB = 19
    BINARY     = 8   
    GEOMETRY   = 20  
    MAX        = 21


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        self.TIMESTAMP_MIN = -1000
        self.TIMESTAMP_BASE = 1706716800
        tdSql.init(conn.cursor())
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database if not exists db vgroups 1')

    def __create_tb(self, dbname="db"):
        CREATE_STB_LIST = [ f"create table {dbname}.stb_vc (ts timestamp, c0 binary(50), c1 varchar(50)) tags(t0 varchar(50), t1 binary(50));",
                            f"create table {dbname}.stb_nc (ts timestamp, c0 nchar(50), c1 nchar(50)) tags(t0 nchar(50), t1 nchar(50));",
                            f"create table {dbname}.stb_ts (ts timestamp, c0 timestamp, c1 timestamp) tags(t0 timestamp, t1 timestamp);",
                            f"create table {dbname}.stb_bo (ts timestamp, c0 bool, c1 bool) tags(t0 bool, t1 bool);",
                            f"create table {dbname}.stb_vb (ts timestamp, c0 varbinary(50), c1 varbinary(50)) tags(t0 varbinary(50), t1 varbinary(50));",
                            f"create table {dbname}.stb_in (ts timestamp, c0 int, c1 smallint) tags(t0 bigint, t1 tinyint);",
                            f"create table {dbname}.stb_ui (ts timestamp, c0 int unsigned, c1 smallint unsigned) tags(t0 bigint unsigned, t1 tinyint unsigned);",
                            f"create table {dbname}.stb_fl (ts timestamp, c0 float, c1 float) tags(t0 float, t1 float);",
                            f"create table {dbname}.stb_db (ts timestamp, c0 float, c1 float) tags(t0 float, t1 float);",
                            f"create table {dbname}.stb_ge (ts timestamp, c0 geometry(512), c1 geometry(512)) tags(t0 geometry(512), t1 geometry(512));",
                            f"create table {dbname}.stb_js (ts timestamp, c0 int) tags(t0 json);" ]
        for _stb in CREATE_STB_LIST:
            tdSql.execute(_stb)
        tdSql.query(f'show {dbname}.stables')
        tdSql.checkRows(len(CREATE_STB_LIST))

    def _query_check_varchar(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                if check_item == okv or check_item == nv:
                    check_result = True
                if check_result == False and (okv[0:1] == '\'' or okv[0:1] == '\"'):
                    if check_item == okv[1:-1]:
                        check_result = True
                if check_result == False and (nv[0:1] == '\'' or nv[0:1] == '\"'):
                    if check_item == nv[1:-1]:
                        check_result = True
                if check_result == False:
                    if check_item == nv.strip().lower():
                        check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_int(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                if check_item == okv or check_item == nv:
                    check_result = True
                if check_item == nv.strip().lower():
                    check_result = True
                if check_result == False and (okv.find('1') != -1 or okv.find('2') != -1):
                    if check_item != 0:
                        check_result = True
                if check_result == False and (nv.find('1') != -1 or nv.find('2') != -1):
                    if check_item != 0:
                        check_result = True
                if check_item == 0:
                        check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_bool(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                elif result[i][j] == True:
                    check_item = "true"
                else:
                    check_item = "false"
                if check_item == okv.strip().lower() or check_item == nv.strip().lower():
                    check_result = True
                if check_result == False and (nv[0:1] == '\'' or nv[0:1] == '\"'):
                    if check_item == nv[1:-1].strip().lower():
                        check_result = True
                if check_result == False and (nv.find('1') != -1 or nv.find('2') != -1): # char 1 or 2 exist for non-zero values
                    if check_item == "true":
                        check_result = True
                else:
                    if check_item == "false":
                        check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_timestamp(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                    if nv.lower().find(check_item) != -1:
                        check_result = True
                else:
                    check_item = int(result[i][j].timestamp())
                if check_result == False and nv.lower().find("now") != -1 or nv.lower().find("today") != -1 or nv.lower().find("now") != -1 or nv.lower().find("today") != -1: 
                    if check_item > self.TIMESTAMP_BASE:
                        check_result = True
                if check_result == False and check_item > self.TIMESTAMP_MIN:
                    check_result = True
                tdSql.checkEqual(check_result, True)


    def _query_check(self, dbname="db", stbname="", ctbname="", nRows = 0, okv = None, nv = None, dtype = TDDataType.NULL):
        result = None
        if dtype != TDDataType.GEOMETRY: # geometry query by py connector need to be supported
            tdSql.query(f'select * from {dbname}.{stbname}')
            tdSql.checkRows(nRows)
            result = tdSql.queryResult

        if dtype == TDDataType.VARCHAR or  dtype == TDDataType.NCHAR:
            self._query_check_varchar(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.TIMESTAMP:
            self._query_check_timestamp(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.BOOL:
            self._query_check_bool(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.VARBINARY:
            pass
        elif dtype == TDDataType.INT:
            self._query_check_int(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.UINT:
            self._query_check_int(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.FLOAT or dtype == TDDataType.DOUBLE:
            self._query_check_int(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.GEOMETRY: 
            pass
        else:
            tdLog.info(f"unknown data type %s" % (dtype))
    
        if ctbname != "":
            tdSql.execute(f'drop table {dbname}.{ctbname}')

    def __insert_query_common(self, dbname="db", stbname="", ctbname="", oklist=[], kolist=[], okv=None, dtype = TDDataType.NULL):
        tdLog.info(f'{dbname}.{stbname} {ctbname}, oklist:%d, kolist:%d'%(len(oklist), len(kolist)))
        tdSql.checkEqual(34, len(oklist) + len(kolist))

        for _l in kolist:
            for _e in _l:
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv})' %(_e))
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s)' %(_e))
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s)' %(_e, _e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s) values(now, {okv}, {okv})' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv}) values(now, {okv}, {okv})' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, %s, {okv})' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, {okv}, %s)' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, %s, %s)' %(_e, _e))
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, {okv}, {okv})')
                self._query_check(dbname,stbname, "", 1, okv, _e, dtype)
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = {okv}')
                tdSql.error(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e))
                tdSql.error(f'alter table {dbname}.{ctbname} set tag t1 = %s' %(_e))
                tdSql.execute(f'drop table {dbname}.{ctbname}')
        for _l in oklist:
            for _e in _l:
                tdLog.info(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv})' %(_e))
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv})' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now + 0s, %s, {okv})' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now + 1s, {okv}, %s)' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now + 2s, %s, %s)' %(_e, _e))
                tdLog.info(f'insert into {dbname}.{ctbname} values(now + 0s, %s, {okv})' %(_e))
                tdLog.info(f'insert into {dbname}.{ctbname} values(now + 1s, {okv}, %s)' %(_e))
                tdLog.info(f'insert into {dbname}.{ctbname} values(now + 2s, %s, %s)' %(_e, _e))
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e))
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t1 = %s' %(_e))
                self._query_check(dbname,stbname, ctbname, 3, okv, _e, dtype)
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s)' %(_e, _e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now, %s, %s)' %(_e, _e))
                self._query_check(dbname,stbname, ctbname, 1, okv, _e, dtype)
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv}) values(now, %s, {okv})' %(_e, _e))
                self._query_check(dbname,stbname, ctbname, 1, okv, _e, dtype)
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s) values(now, {okv}, %s)' %(_e, _e))
                self._query_check(dbname,stbname, ctbname, 1, okv, _e, dtype)
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s) values(now, %s, %s)' %(_e, _e, _e, _e))
                self._query_check(dbname,stbname, ctbname, 1, okv, _e, dtype)

    def __insert_query_json(self, dbname="db", stbname="", ctbname="", oklist=[], kolist=[], okv=None):
        tdLog.info(f'{dbname}.{stbname} {ctbname}, oklist:%d, kolist:%d'%(len(oklist), len(kolist)))
        tdSql.checkEqual(34, len(oklist) + len(kolist))

        for _l in kolist:
            for _e in _l:
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s)' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s) values(now, 1)' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}) values(now, 1)')
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = {okv}')
                tdSql.error(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e))
                tdSql.execute(f'drop table {dbname}.{ctbname}')
        for _l in oklist:
            for _e in _l:
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s)' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now, 1)')
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e))
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}')

    def __insert_query_exec(self):
        STR_EMPTY = ['\'\'', "\"\"", '\' \'', "\"    \""]
        STR_INTEGER_P = ["\"42\"", '\'+42\'', '\'+0\'', '\'-0\'', '\'0x2A\'', '\'-0X0\'', '\'+0x0\'', '\'0B00101010\'', '\'-0b00\'']
        STR_INTEGER_M = ['\'-128\'', '\'-0X1\'', '\"-0x34\"', '\'-0b01\'', '\'-0B00101010\'']
        STR_FLOAT_P = ['\'42.1\'', "\"+0.003\"", "\'-0.0\'"]
        STR_FLOAT_M = ["\"-32.001\""]
        STR_FLOAT_E_P = ['\'1e1\'', "\"3e-2\"", "\"-3e-5\""]
        STR_FLOAT_E_M = ["\"-0.3E+1\""]
        STR_MISC = ["\"123ab\"", '\'123d\'', '\'-12s\'', '\'\x012\'', '\'x12\'',  '\'x\'', '\'NULL \'', '\' NULL\'', '\'True \'', '\' False\'', 
                    '\'0B0101 \'', '\' 0B0101\'', '\' -0x01 \'', '\'-0x02 \'']
        STR_OPTR = ['\'1*10\'', '\'1+2\'', '\'-2-0\'','\'1%2\'', '\'2/0\'', '\'1&31\'']
        STR_TSK = ['\'now\'', '\'today\'']
        STR_TSK_MISC = ['\'now+1s\'', '\' now\'', '\'today \'', '\'today+1m\'', '\'today-1w\'']
        STR_TSKP = ['\'now()\'', '\'today()\'']
        STR_TSKP_MISC = ['\'now()+1s\'', '\' now()\'', '\'now( )\'', '\'today() \'', '\'today())\'', '\'today()+1m\'', '\'today()-1w\'']
        STR_BOOL = ['\'true\'', '\'false\'', '\'TRUE\'', '\'FALSE\'', '\'tRuE\'', '\'falsE\'']
        STR_TS = ["\"2024-02-01 00:00:01.001-08:00\"", "\'2024-02-01T00:00:01.001+09:00\'", "\"2024-02-01\"", "\'2024-02-02 00:00:01\'", "\'2024-02-02 00:00:01.009\'"]
        STR_VARBIN = ['\'\\x12\'', '\'\\x13\'', '\' \\x14 \'', '\'\\x12ab\'']
        STR_JSON_O = ['\'{\"k1\":\"v1\"}\'', '\' {} \'']
        STR_JSON_A = ['\'[]\'']
        STR_GEO = ['\' POINT(1.0 1.0)\'', '\'LINESTRING(1.00 +2.0, 2.1 -3.2, 5.00 5.01) \'', '\'POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))\'' ]
        STR_NULL  = ['\'NuLl\'', '\'null\'', '\'NULL\'']

        RAW_INTEGER_P = [' 42 ', '+042 ', ' +0', '0 ', '-0', '0', ' 0X2A', ' -0x0 ', '+0x0  ', '  0B00101010', ' -0b00']
        RAW_INTEGER_M = [' -42 ', ' -0128',' -0x1', '  -0X2A', '-0b01  ', ' -0B00101010 ']
        RAW_FLOAT_P = [' 123.012', ' 0.0', ' +0.0', ' -0.0  ']
        RAW_FLOAT_M = ['-128.001 ']
        RAW_FLOAT_E_P = [' 1e-100', ' +0.1E+2', ' -0.1E-10']
        RAW_FLOAT_E_M = [" -1E2 "]
        RAW_MISC = ['123abc', "123c", '-123d', '+', '-', ' *', ' /', '% ', '&', "|", "^", "&&", "||", "!", " =", ' None ', 'NONE', 'now+1 s', 'now-1','now-1y','now+2 d',
                    'today+1 s', 'today-1','today-1y','today+2 d', 'now()+1 s', 'now()-1','now()-1y','now()+2 d', 'today()+1 s', 'today()-1','today()-1y','today()+2 d']
        RAW_OPTR = ['1*10', '1+2', '-2-0','1%2', '2/0', '1&31']
        RAW_TSK = [' now ', 'today ']
        RAW_TSK_OPTR = [' now +1s', 'today + 2d']
        RAW_TSKP = ['now( ) ', ' toDay() ']
        RAW_TSKP_OPTR = [' noW ( ) + 1s',  'nOw( ) + 2D', 'NOW () + 000s', ' today()+1M', 'today( ) - 1w ', 'TodaY ( ) - 1U ']
        RAW_BOOL = ['true', 'false', ' TRUE ', 'FALSE  ', '  tRuE', '  falsE    ']
        RAW_NULL = ['NuLl', 'null ', ' NULL', ' NULL ']

        OK_VC = [STR_EMPTY, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, 
                 STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M, 
                 RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_TSK, RAW_BOOL, RAW_NULL]
        KO_VC = [RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR]
        OK_NC = OK_VC
        KO_NC = KO_VC
        OK_TS = [STR_TSK, STR_INTEGER_P, STR_INTEGER_M, STR_TSKP, STR_TS, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, RAW_TSK, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, RAW_NULL]
        KO_TS = [STR_EMPTY, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_MISC, STR_OPTR, STR_TSK_MISC, STR_TSKP_MISC, STR_BOOL, STR_VARBIN,
                 STR_JSON_O, STR_JSON_A, STR_GEO, RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_MISC, RAW_OPTR, RAW_BOOL]
        OK_BO = [STR_BOOL, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M,RAW_BOOL, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, 
                 RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_NULL]
        KO_BO = [STR_EMPTY,  STR_TSK, STR_TSKP, STR_TS,  STR_MISC, STR_OPTR, STR_TSK_MISC, STR_TSKP_MISC, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK, 
                 RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, RAW_MISC, RAW_OPTR]
        OK_VB = [STR_EMPTY, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, 
                 STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, STR_NULL, RAW_NULL]
        KO_VB = [RAW_INTEGER_P, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_TSK, RAW_BOOL, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR]
        OK_IN = [STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M,
                 RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_NULL]
        KO_IN = [STR_EMPTY, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK,
                 RAW_BOOL, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR]
        OK_UI = [STR_INTEGER_P, STR_FLOAT_P, STR_FLOAT_E_P, STR_NULL, RAW_INTEGER_P, RAW_FLOAT_P, RAW_FLOAT_E_P, RAW_NULL]
        KO_UI = [STR_EMPTY, STR_MISC, STR_INTEGER_M, STR_FLOAT_M, STR_FLOAT_E_M, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, 
                 STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK, RAW_BOOL, RAW_INTEGER_M, RAW_FLOAT_M, RAW_FLOAT_E_M, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR]
        OK_FL = [RAW_INTEGER_P, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_NULL, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M,
                  RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_NULL]
        KO_FL = [STR_EMPTY, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK, 
                 RAW_BOOL, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR]
        OK_DB = OK_FL
        KO_DB = KO_FL
        OK_GE = [STR_GEO, STR_NULL, RAW_NULL]
        KO_GE = [STR_EMPTY, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_JSON_O, STR_JSON_A, STR_VARBIN, RAW_TSK, RAW_BOOL, RAW_MISC,
                 RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, RAW_INTEGER_P, RAW_INTEGER_M, 
                 RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M]
        OK_JS = [STR_EMPTY, STR_JSON_O, STR_NULL, RAW_NULL]
        KO_JS = [STR_JSON_A, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_GEO, STR_VARBIN, RAW_TSK, RAW_BOOL, RAW_MISC, RAW_OPTR, 
                 RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, RAW_INTEGER_P, RAW_INTEGER_M, 
                 RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M]
        
        PARAM_LIST = [
                        ["db", "stb_vc", "ctb_vc", OK_VC, KO_VC, "\'vc\'", TDDataType.VARCHAR],
                        ["db", "stb_nc", "ctb_nc", OK_NC, KO_NC, "\'nc\'", TDDataType.NCHAR],
                        ["db", "stb_ts", "ctb_ts", OK_TS, KO_TS, "now", TDDataType.TIMESTAMP],
                        ["db", "stb_bo", "ctb_bo", OK_BO, KO_BO, "true", TDDataType.BOOL],
                        ["db", "stb_vb", "ctb_vb", OK_VB, KO_VB, "\'\\x12\'", TDDataType.VARBINARY],
                        ["db", "stb_in", "ctb_in", OK_IN, KO_IN, "-1", TDDataType.UINT],
                        ["db", "stb_ui", "ctb_ui", OK_UI, KO_UI, "1", TDDataType.UINT],
                        ["db", "stb_fl", "ctb_fl", OK_FL, KO_FL, "1.0", TDDataType.FLOAT],
                        ["db", "stb_db", "ctb_db", OK_DB, KO_DB, "1.0", TDDataType.DOUBLE],
                        ["db", "stb_ge", "ctb_ge", OK_GE, KO_GE, "\'POINT(1.0 1.0)\'", TDDataType.GEOMETRY] 
                      ]

        # check with common function
        for _pl in PARAM_LIST:
            self.__insert_query_common(_pl[0], _pl[1], _pl[2], _pl[3], _pl[4], _pl[5], _pl[6])
        # check json
        self.__insert_query_json("db", "stb_js", "ctb_js", OK_JS, KO_JS, "\'{\"k1\":\"v1\",\"k2\":\"v2\"}\'")


    def run(self):
        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()
        self.__insert_query_exec()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
