import ctypes

TAOS_SYSTEM_ERROR = ctypes.c_int32(0x80ff0000).value
TAOS_DEF_ERROR_CODE = ctypes.c_int32(0x80000000).value


TSDB_CODE_MND_FUNC_NOT_EXIST    = (TAOS_DEF_ERROR_CODE | 0x0374)


TSDB_CODE_TSC_INVALID_OPERATION = (TAOS_DEF_ERROR_CODE | 0x0200)

TSDB_CODE_UDF_FUNC_EXEC_FAILURE = (TAOS_DEF_ERROR_CODE | 0x290A)


TSDB_CODE_TSC_INTERNAL_ERROR = (TAOS_DEF_ERROR_CODE | 0x02FF)

TSDB_CODE_PAR_SYNTAX_ERROR = (TAOS_DEF_ERROR_CODE | 0x2600)

TSDB_CODE_PAR_INVALID_COLS_FUNCTION = (TAOS_DEF_ERROR_CODE | 0x2689)

