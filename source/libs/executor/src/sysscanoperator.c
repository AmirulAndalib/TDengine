/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "executorInt.h"
#include "filter.h"
#include "functionMgt.h"
#include "geosWrapper.h"
#include "nodes.h"
#include "querynodes.h"
#include "systable.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "tcompare.h"
#include "thash.h"
#include "trpc.h"
#include "ttypes.h"

typedef int (*__optSysFilter)(void* a, void* b, int16_t dtype);
typedef int32_t (*__sys_filte)(void* pMeta, SNode* cond, SArray* result);
typedef int32_t (*__sys_check)(SNode* cond);

typedef struct SSTabFltArg {
  void*        pMeta;
  void*        pVnode;
  SStorageAPI* pAPI;
} SSTabFltArg;

typedef struct SSysTableIndex {
  int8_t  init;
  SArray* uids;
  int32_t lastIdx;
} SSysTableIndex;

typedef struct SSysTableScanInfo {
  SRetrieveMetaTableRsp* pRsp;
  SRetrieveTableReq      req;
  SEpSet                 epSet;
  tsem_t                 ready;
  SReadHandle            readHandle;
  int32_t                accountId;
  const char*            pUser;
  bool                   sysInfo;
  bool                   showRewrite;
  bool                   restore;
  bool                   skipFilterTable;
  SNode*                 pCondition;  // db_name filter condition, to discard data that are not in current database
  SMTbCursor*            pCur;        // cursor for iterate the local table meta store.
  SSysTableIndex*        pIdx;        // idx for local table meta
  SHashObj*              pSchema;
  SColMatchInfo          matchInfo;
  SName                  name;
  SSDataBlock*           pRes;
  int64_t                numOfBlocks;  // extract basic running information.
  SLoadRemoteDataInfo    loadInfo;
  SLimitInfo             limitInfo;
  int32_t                tbnameSlotId;
  STableListInfo*        pTableListInfo;
  SReadHandle*           pHandle;
  SStorageAPI*           pAPI;

  // file set iterate
  struct SFileSetReader* pFileSetReader;
} SSysTableScanInfo;

typedef struct {
  const char* name;
  __sys_check chkFunc;
  __sys_filte fltFunc;
} SSTabFltFuncDef;

typedef struct MergeIndex {
  int idx;
  int len;
} MergeIndex;

typedef struct SBlockDistInfo {
  SSDataBlock*    pResBlock;
  STsdbReader*    pHandle;
  SReadHandle     readHandle;
  STableListInfo* pTableListInfo;
  uint64_t        uid;  // table uid
} SBlockDistInfo;

typedef struct {
  int8_t   type;
  tb_uid_t uid;
} STableId;

static int32_t sysChkFilter__Comm(SNode* pNode);
static int32_t sysChkFilter__DBName(SNode* pNode);
static int32_t sysChkFilter__VgroupId(SNode* pNode);
static int32_t sysChkFilter__TableName(SNode* pNode);
static int32_t sysChkFilter__CreateTime(SNode* pNode);
static int32_t sysChkFilter__Ncolumn(SNode* pNode);
static int32_t sysChkFilter__Ttl(SNode* pNode);
static int32_t sysChkFilter__STableName(SNode* pNode);
static int32_t sysChkFilter__Uid(SNode* pNode);
static int32_t sysChkFilter__Type(SNode* pNode);

static int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__VgroupId(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__TableName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__CreateTime(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Ncolumn(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Ttl(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__STableName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Uid(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Type(void* arg, SNode* pNode, SArray* result);

const SSTabFltFuncDef filterDict[] = {
    {.name = "table_name", .chkFunc = sysChkFilter__TableName, .fltFunc = sysFilte__TableName},
    {.name = "db_name", .chkFunc = sysChkFilter__DBName, .fltFunc = sysFilte__DbName},
    {.name = "create_time", .chkFunc = sysChkFilter__CreateTime, .fltFunc = sysFilte__CreateTime},
    {.name = "columns", .chkFunc = sysChkFilter__Ncolumn, .fltFunc = sysFilte__Ncolumn},
    {.name = "ttl", .chkFunc = sysChkFilter__Ttl, .fltFunc = sysFilte__Ttl},
    {.name = "stable_name", .chkFunc = sysChkFilter__STableName, .fltFunc = sysFilte__STableName},
    {.name = "vgroup_id", .chkFunc = sysChkFilter__VgroupId, .fltFunc = sysFilte__VgroupId},
    {.name = "uid", .chkFunc = sysChkFilter__Uid, .fltFunc = sysFilte__Uid},
    {.name = "type", .chkFunc = sysChkFilter__Type, .fltFunc = sysFilte__Type}};

#define SYSTAB_FILTER_DICT_SIZE (sizeof(filterDict) / sizeof(filterDict[0]))

static int32_t buildDbTableInfoBlock(bool sysInfo, const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta,
                                     size_t size, const char* dbName, int64_t* pRows);

static char* SYSTABLE_SPECIAL_COL[] = {"db_name", "vgroup_id"};

static int32_t        buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity);
static SSDataBlock*   buildInfoSchemaTableMetaBlock(char* tableName);
static void           destroySysScanOperator(void* param);
static int32_t        loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code);
static __optSysFilter optSysGetFilterFunc(int32_t ctype, bool* reverse, bool* equal);

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock);

static int32_t sysTableUserColsFillOneTableCols(const SSysTableScanInfo* pInfo, const char* dbname, int32_t* pNumOfRows,
                                                const SSDataBlock* dataBlock, char* tName,
                                                SSchemaWrapper* schemaRow, char* tableType, SColRefWrapper *colRef);

static int32_t sysTableUserColsFillOneVirtualChildTableCols(const SSysTableScanInfo* pInfo, const char* dbname, int32_t* pNumOfRows,
                                                            const SSDataBlock* dataBlock, char* tName, char* stName,
                                                            SSchemaWrapper* schemaRow, char* tableType, SColRefWrapper *colRef, tb_uid_t uid, int32_t vgId);

static void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                               SFilterInfo* pFilterInfo, SExecTaskInfo* pTaskInfo);

static int32_t vnodeEstimateRawDataSize(SOperatorInfo* pOperator, SDbSizeStatisInfo* pStatisInfo);

int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  void*        pVnode = pArg->pVnode;

  const char* db = NULL;
  pArg->pAPI->metaFn.getBasicInfo(pVnode, &db, NULL, NULL, NULL);

  SName   sn = {0};
  char    dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  code = tNameGetDbName(&sn, varDataVal(dbname));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  int ret = func(dbname, pVal->datum.p, TSDB_DATA_TYPE_VARCHAR);
  if (ret == 0) return 0;

  return -2;
}

int32_t sysFilte__VgroupId(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  void*        pVnode = ((SSTabFltArg*)arg)->pVnode;

  int64_t vgId = 0;
  pArg->pAPI->metaFn.getBasicInfo(pVnode, NULL, (int32_t*)&vgId, NULL, NULL);

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  int ret = func(&vgId, &pVal->datum.i, TSDB_DATA_TYPE_BIGINT);
  if (ret == 0) return 0;

  return -1;
}

int32_t sysFilte__TableName(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false, equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  SMetaFltParam param = {.suid = 0,
                         .cid = 0,
                         .type = TSDB_DATA_TYPE_VARCHAR,
                         .val = pVal->datum.p,
                         .reverse = reverse,
                         .equal = equal,
                         .filterFunc = func};
  return -1;
}

int32_t sysFilte__CreateTime(void* arg, SNode* pNode, SArray* result) {
  SSTabFltArg* pArg = arg;
  SStorageAPI* pAPI = pArg->pAPI;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false, equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;

  SMetaFltParam param = {.suid = 0,
                         .cid = 0,
                         .type = TSDB_DATA_TYPE_BIGINT,
                         .val = &pVal->datum.i,
                         .reverse = reverse,
                         .equal = equal,
                         .filterFunc = func};

  int32_t ret = pAPI->metaFilter.metaFilterCreateTime(pArg->pVnode, &param, result);
  return ret;
}

int32_t sysFilte__Ncolumn(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__Ttl(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__STableName(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__Uid(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int32_t sysFilte__Type(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;
  bool           equal = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse, &equal);
  if (func == NULL) return -1;
  return -1;
}

int optSysDoCompare(__compar_fn_t func, int8_t comparType, void* a, void* b) {
  int32_t cmp = func(a, b);
  switch (comparType) {
    case OP_TYPE_LOWER_THAN:
      if (cmp < 0) return 0;
      break;
    case OP_TYPE_LOWER_EQUAL: {
      if (cmp <= 0) return 0;
      break;
    }
    case OP_TYPE_GREATER_THAN: {
      if (cmp > 0) return 0;
      break;
    }
    case OP_TYPE_GREATER_EQUAL: {
      if (cmp >= 0) return 0;
      break;
    }
    case OP_TYPE_EQUAL: {
      if (cmp == 0) return 0;
      break;
    }
    default:
      return -1;
  }
  return cmp;
}

static int optSysFilterFuncImpl__LowerThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_LOWER_THAN, a, b);
}
static int optSysFilterFuncImpl__LowerEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_LOWER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__GreaterThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_GREATER_THAN, a, b);
}
static int optSysFilterFuncImpl__GreaterEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_GREATER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__Equal(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_EQUAL, a, b);
}

static int optSysFilterFuncImpl__NoEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  if (func == NULL) {
    return -1;
  }
  return optSysDoCompare(func, OP_TYPE_NOT_EQUAL, a, b);
}

static int32_t optSysTabFilte(void* arg, SNode* cond, SArray* result);
static int32_t optSysTabFilteImpl(void* arg, SNode* cond, SArray* result);
static int32_t optSysCheckOper(SNode* pOpear);
static int32_t optSysMergeRslt(SArray* mRslt, SArray* rslt);

static SSDataBlock* sysTableScanFromMNode(SOperatorInfo* pOperator, SSysTableScanInfo* pInfo, const char* name,
                                          SExecTaskInfo* pTaskInfo);
void                extractTbnameSlotId(SSysTableScanInfo* pInfo, const SScanPhysiNode* pScanNode);

static void sysTableScanFillTbName(SOperatorInfo* pOperator, const SSysTableScanInfo* pInfo, const char* name,
                                   SSDataBlock* pBlock);

__optSysFilter optSysGetFilterFunc(int32_t ctype, bool* reverse, bool* equal) {
  if (ctype == OP_TYPE_LOWER_EQUAL || ctype == OP_TYPE_LOWER_THAN) {
    *reverse = true;
  }
  if (ctype == OP_TYPE_EQUAL) {
    *equal = true;
  }
  if (ctype == OP_TYPE_LOWER_THAN)
    return optSysFilterFuncImpl__LowerThan;
  else if (ctype == OP_TYPE_LOWER_EQUAL)
    return optSysFilterFuncImpl__LowerEqual;
  else if (ctype == OP_TYPE_GREATER_THAN)
    return optSysFilterFuncImpl__GreaterThan;
  else if (ctype == OP_TYPE_GREATER_EQUAL)
    return optSysFilterFuncImpl__GreaterEqual;
  else if (ctype == OP_TYPE_EQUAL)
    return optSysFilterFuncImpl__Equal;
  else if (ctype == OP_TYPE_NOT_EQUAL)
    return optSysFilterFuncImpl__NoEqual;
  return NULL;
}

static bool sysTableIsOperatorCondOnOneTable(SNode* pCond, char* condTable) {
  SOperatorNode* node = (SOperatorNode*)pCond;
  if (node->opType == OP_TYPE_EQUAL) {
    if (nodeType(node->pLeft) == QUERY_NODE_COLUMN &&
        strcasecmp(nodesGetNameFromColumnNode(node->pLeft), "table_name") == 0 &&
        nodeType(node->pRight) == QUERY_NODE_VALUE) {
      SValueNode* pValue = (SValueNode*)node->pRight;
      if (pValue->node.resType.type == TSDB_DATA_TYPE_NCHAR || pValue->node.resType.type == TSDB_DATA_TYPE_VARCHAR) {
        char* value = nodesGetValueFromNode(pValue);
        tstrncpy(condTable, varDataVal(value), TSDB_TABLE_NAME_LEN);
        return true;
      }
    }
  }
  return false;
}

static bool sysTableIsCondOnOneTable(SNode* pCond, char* condTable) {
  if (pCond == NULL) {
    return false;
  }
  if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode* node = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND == node->condType) {
      SNode* pChild = NULL;
      FOREACH(pChild, node->pParameterList) {
        if (QUERY_NODE_OPERATOR == nodeType(pChild) && sysTableIsOperatorCondOnOneTable(pChild, condTable)) {
          return true;
        }
      }
    }
  }

  if (QUERY_NODE_OPERATOR == nodeType(pCond)) {
    return sysTableIsOperatorCondOnOneTable(pCond, condTable);
  }

  return false;
}

static SSDataBlock* doOptimizeTableNameFilter(SOperatorInfo* pOperator, SSDataBlock* dataBlock, char* dbname) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;

  char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(tableName, pInfo->req.filterTb);

  SMetaReader smrTable = {0};
  pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  int32_t code = pAPI->metaReaderFn.getTableEntryByName(&smrTable, pInfo->req.filterTb);
  if (code != TSDB_CODE_SUCCESS) {
    // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
    pAPI->metaReaderFn.clearReader(&smrTable);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  if (smrTable.me.type == TSDB_SUPER_TABLE) {
    pAPI->metaReaderFn.clearReader(&smrTable);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  if (smrTable.me.type == TSDB_CHILD_TABLE) {
    int64_t suid = smrTable.me.ctbEntry.suid;
    pAPI->metaReaderFn.clearReader(&smrTable);
    pAPI->metaReaderFn.initReader(&smrTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrTable, suid);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
      pAPI->metaReaderFn.clearReader(&smrTable);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }
  }

  char            typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSchemaWrapper* schemaRow = NULL;
  SColRefWrapper* colRef = NULL;
  if (smrTable.me.type == TSDB_SUPER_TABLE) {
    schemaRow = &smrTable.me.stbEntry.schemaRow;
    STR_TO_VARSTR(typeName, "CHILD_TABLE");
  } else if (smrTable.me.type == TSDB_NORMAL_TABLE) {
    schemaRow = &smrTable.me.ntbEntry.schemaRow;
    STR_TO_VARSTR(typeName, "NORMAL_TABLE");
  } else if (smrTable.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
    schemaRow = &smrTable.me.ntbEntry.schemaRow;
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(typeName, "VIRTUAL_NORMAL_TABLE");
  } else if (smrTable.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    colRef = &smrTable.me.colRef;
    STR_TO_VARSTR(typeName, "VIRTUAL_CHILD_TABLE");
  }

  code = sysTableUserColsFillOneTableCols(pInfo, dbname, &numOfRows, dataBlock, tableName, schemaRow, typeName, colRef);
  if (code != TSDB_CODE_SUCCESS) {
    pAPI->metaReaderFn.clearReader(&smrTable);
    pInfo->loadInfo.totalRows = 0;
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  pAPI->metaReaderFn.clearReader(&smrTable);

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  setOperatorCompleted(pOperator);

  qDebug("get cols success, total rows:%" PRIu64 ", current:%" PRId64 " %s", pInfo->loadInfo.totalRows,
         pInfo->pRes->info.rows, GET_TASKID(pTaskInfo));
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

int32_t doExtractDbName(char* dbname, SSysTableScanInfo* pInfo, SStorageAPI* pAPI) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SName       sn = {0};
  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* sysTableScanUserCols(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;
  char               dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSDataBlock*       pDataBlock = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  pDataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_COLS);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  code = doExtractDbName(dbname, pInfo, pAPI);
  QUERY_CHECK_CODE(code, lino, _end);

  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->req.filterTb[0]) {
    SSDataBlock* p = doOptimizeTableNameFilter(pOperator, pDataBlock, dbname);
    blockDataDestroy(pDataBlock);
    return p;
  }

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pInfo->pSchema == NULL) {
    pInfo->pSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    taosHashSetFreeFp(pInfo->pSchema, tDeleteSSchemaWrapperForHash);
  }

  if (!pInfo->pCur || !pInfo->pSchema) {
    qError("sysTableScanUserCols failed since %s", terrstr());
    blockDataDestroy(pDataBlock);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_TABLE_MAX)) == 0)) {
    char typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

    SSchemaWrapper* schemaRow = NULL;
    SColRefWrapper* colRef = NULL;

    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      qDebug("sysTableScanUserCols cursor get super table, %s", GET_TASKID(pTaskInfo));
      void* schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t));
      if (schema == NULL) {
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&pInfo->pCur->mr.me.stbEntry.schemaRow);
        if (pInfo->pCur->mr.me.stbEntry.schemaRow.pSchema) {
          QUERY_CHECK_NULL(schemaWrapper, code, lino, _end, terrno);
        }
        code = taosHashPut(pInfo->pSchema, &pInfo->pCur->mr.me.uid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        QUERY_CHECK_CODE(code, lino, _end);
      }
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      qDebug("sysTableScanUserCols cursor get child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.ctbEntry.suid, sizeof(int64_t));
      if (schema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        if (smrSuperTable.me.stbEntry.schemaRow.pSchema) {
          if (schemaWrapper == NULL) {
            code = terrno;
            lino = __LINE__;
            pAPI->metaReaderFn.clearReader(&smrSuperTable);
            goto _end;
          }
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        schemaRow = schemaWrapper;
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      qDebug("sysTableScanUserCols cursor get normal table, %s", GET_TASKID(pTaskInfo));
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
      STR_TO_VARSTR(typeName, "NORMAL_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      qDebug("sysTableScanUserCols cursor get virtual normal table, %s", GET_TASKID(pTaskInfo));
      schemaRow = &pInfo->pCur->mr.me.ntbEntry.schemaRow;
      STR_TO_VARSTR(typeName, "VIRTUAL_NORMAL_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);
      colRef = &pInfo->pCur->mr.me.colRef;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
      qDebug("sysTableScanUserCols cursor get virtual child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "VIRTUAL_CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

      colRef = &pInfo->pCur->mr.me.colRef;
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.ctbEntry.suid, sizeof(int64_t));
      if (schema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        if (smrSuperTable.me.stbEntry.schemaRow.pSchema) {
          if (schemaWrapper == NULL) {
            code = terrno;
            lino = __LINE__;
            pAPI->metaReaderFn.clearReader(&smrSuperTable);
            goto _end;
          }
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        schemaRow = schemaWrapper;
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      qDebug("sysTableScanUserCols cursor get invalid table, %s", GET_TASKID(pTaskInfo));
      continue;
    }

    if ((numOfRows + schemaRow->nCols) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        break;
      }
    }
    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code =
        sysTableUserColsFillOneTableCols(pInfo, dbname, &numOfRows, pDataBlock, tableName, schemaRow, typeName, colRef);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(pDataBlock);
  pDataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  qDebug("get cols success, rows:%" PRIu64 " %s", pInfo->loadInfo.totalRows, GET_TASKID(pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDataBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserVcCols(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;
  char               dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  SSDataBlock*       pDataBlock = NULL;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);

  pDataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_VC_COLS);
  QUERY_CHECK_NULL(pDataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(pDataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  code = doExtractDbName(dbname, pInfo, pAPI);
  QUERY_CHECK_CODE(code, lino, _end);

  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->req.filterTb[0]) {
    SSDataBlock* p = doOptimizeTableNameFilter(pOperator, pDataBlock, dbname);
    blockDataDestroy(pDataBlock);
    return p;
  }

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pInfo->pSchema == NULL) {
    pInfo->pSchema = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    taosHashSetFreeFp(pInfo->pSchema, tDeleteSSchemaWrapperForHash);
  }

  if (!pInfo->pCur || !pInfo->pSchema) {
    qError("sysTableScanUserCols failed since %s", terrstr());
    blockDataDestroy(pDataBlock);
    pInfo->loadInfo.totalRows = 0;
    return NULL;
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_TABLE_MAX)) == 0)) {
    char typeName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

    SSchemaWrapper* schemaRow = NULL;
    SColRefWrapper* colRef = NULL;

    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_NORMAL_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_VIRTUAL_CHILD_TABLE) {
      qDebug("sysTableScanUserCols cursor get virtual child table, %s", GET_TASKID(pTaskInfo));

      STR_TO_VARSTR(typeName, "VIRTUAL_CHILD_TABLE");
      STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

      colRef = &pInfo->pCur->mr.me.colRef;
      int64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      void*   schema = taosHashGet(pInfo->pSchema, &pInfo->pCur->mr.me.ctbEntry.suid, sizeof(int64_t));
      if (schema != NULL) {
        schemaRow = *(SSchemaWrapper**)schema;
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        STR_TO_VARSTR(stableName, smrSuperTable.me.name);
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
      } else {
        SMetaReader smrSuperTable = {0};
        pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
        code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
        if (code != TSDB_CODE_SUCCESS) {
          // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
          qError("sysTableScanUserCols get meta by suid:%" PRId64 " error, code:%d, %s", suid, code,
                 GET_TASKID(pTaskInfo));

          pAPI->metaReaderFn.clearReader(&smrSuperTable);
          blockDataDestroy(pDataBlock);
          pInfo->loadInfo.totalRows = 0;
          return NULL;
        }
        STR_TO_VARSTR(stableName, smrSuperTable.me.name);
        SSchemaWrapper* schemaWrapper = tCloneSSchemaWrapper(&smrSuperTable.me.stbEntry.schemaRow);
        if (smrSuperTable.me.stbEntry.schemaRow.pSchema) {
          if (schemaWrapper == NULL) {
            code = terrno;
            lino = __LINE__;
            pAPI->metaReaderFn.clearReader(&smrSuperTable);
            goto _end;
          }
        }
        code = taosHashPut(pInfo->pSchema, &suid, sizeof(int64_t), &schemaWrapper, POINTER_BYTES);
        if (code == TSDB_CODE_DUP_KEY) {
          code = TSDB_CODE_SUCCESS;
        }
        schemaRow = schemaWrapper;
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    } else {
      qDebug("sysTableScanUserCols cursor get invalid table, %s", GET_TASKID(pTaskInfo));
      continue;
    }

    if ((numOfRows + schemaRow->nCols) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        break;
      }
    }
    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code = sysTableUserColsFillOneVirtualChildTableCols(pInfo, dbname, &numOfRows, pDataBlock, tableName, stableName, schemaRow, typeName, colRef, pInfo->pCur->mr.me.uid, pOperator->pTaskInfo->id.vgId);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, pDataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(pDataBlock);
  pDataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  qDebug("get cols success, rows:%" PRIu64 " %s", pInfo->loadInfo.totalRows, GET_TASKID(pTaskInfo));

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pDataBlock);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}


static SSDataBlock* sysTableScanUserTags(SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;
  SSDataBlock*   dataBlock = NULL;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  dataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TAGS);
  QUERY_CHECK_NULL(dataBlock, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(dataBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  char condTableName[TSDB_TABLE_NAME_LEN] = {0};
  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->pCondition && sysTableIsCondOnOneTable(pInfo->pCondition, condTableName)) {
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, condTableName);

    SMetaReader smrChildTable = {0};
    pAPI->metaReaderFn.initReader(&smrChildTable, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByName(&smrChildTable, condTableName);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByName, therefore, return directly
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    if (smrChildTable.me.type != TSDB_CHILD_TABLE && smrChildTable.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    SMetaReader smrSuperTable = {0};
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, smrChildTable.me.ctbEntry.suid);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by pAPI->metaReaderFn.getTableEntryByUid
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaReaderFn.clearReader(&smrChildTable);
      blockDataDestroy(dataBlock);
      return NULL;
    }

    code = sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &smrChildTable, dbname, tableName, &numOfRows,
                                            dataBlock);

    pAPI->metaReaderFn.clearReader(&smrSuperTable);
    pAPI->metaReaderFn.clearReader(&smrChildTable);

    QUERY_CHECK_CODE(code, lino, _end);

    if (numOfRows > 0) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;
    }
    blockDataDestroy(dataBlock);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    setOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  }

  int32_t ret = 0;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    QUERY_CHECK_NULL(pInfo->pCur, code, lino, _end, terrno);
  } else {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 0);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  while ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_SUPER_TABLE)) == 0) {
    if (pInfo->pCur->mr.me.type != TSDB_CHILD_TABLE && pInfo->pCur->mr.me.type != TSDB_VIRTUAL_CHILD_TABLE) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

    SMetaReader smrSuperTable = {0};
    pAPI->metaReaderFn.initReader(&smrSuperTable, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);
    uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
    code = pAPI->metaReaderFn.getTableEntryByUid(&smrSuperTable, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno),
             GET_TASKID(pTaskInfo));
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      blockDataDestroy(dataBlock);
      dataBlock = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    if ((smrSuperTable.me.stbEntry.schemaTag.nCols + numOfRows) > pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        pAPI->metaReaderFn.clearReader(&smrSuperTable);
        break;
      }
    }

    // if pInfo->pRes->info.rows == 0, also need to add the meta to pDataBlock
    code = sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &pInfo->pCur->mr, dbname, tableName, &numOfRows,
                                            dataBlock);

    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pAPI->metaReaderFn.clearReader(&smrSuperTable);
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      blockDataDestroy(dataBlock);
      dataBlock = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }
    pAPI->metaReaderFn.clearReader(&smrSuperTable);
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo, pTaskInfo);
    numOfRows = 0;
  }

  blockDataDestroy(dataBlock);
  dataBlock = NULL;
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(dataBlock);
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                        SFilterInfo* pFilterInfo, SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  dataBlock->info.rows = numOfRows;
  pInfo->pRes->info.rows = numOfRows;

  code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, dataBlock->pDataBlock, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = doFilter(pInfo->pRes, pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  blockDataCleanup(dataBlock);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

int32_t convertTagDataToStr(char* str, int32_t strBuffLen, int type, void* buf, int32_t bufSize, int32_t* len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = tsnprintf(str, strBuffLen, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = tsnprintf(str, strBuffLen, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = tsnprintf(str, strBuffLen, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = tsnprintf(str, strBuffLen, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = tsnprintf(str, strBuffLen, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = tsnprintf(str, strBuffLen, "%" PRId64, *(int64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = tsnprintf(str, strBuffLen, "%.5f", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = tsnprintf(str, strBuffLen, "%.9f", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      if (bufSize < 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      memcpy(str, buf, bufSize);
      n = bufSize;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (bufSize < 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      int32_t length = taosUcs4ToMbs((TdUcs4*)buf, bufSize, str, NULL);
      if (length <= 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      n = length;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      n = tsnprintf(str, strBuffLen, "%u", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = tsnprintf(str, strBuffLen, "%u", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = tsnprintf(str, strBuffLen, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = tsnprintf(str, strBuffLen, "%" PRIu64, *(uint64_t*)buf);
      break;

    default:
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (len) *len = n;

  return TSDB_CODE_SUCCESS;
}

static int32_t sysTableGetGeomText(char* iGeom, int32_t nGeom, char** output, int32_t* nOutput) {
#ifdef USE_GEOS
  int32_t code = 0;
  char*   outputWKT = NULL;

  if (nGeom == 0) {
    if (!(*output = taosStrdup(""))) code = terrno;
    *nOutput = 0;
    return code;
  }

  if (TSDB_CODE_SUCCESS != (code = initCtxAsText()) ||
      TSDB_CODE_SUCCESS != (code = doAsText(iGeom, nGeom, &outputWKT))) {
    qError("geo text for systable failed:%s", getGeosErrMsg(code));
    *output = NULL;
    *nOutput = 0;
    return code;
  }

  *output = outputWKT;
  *nOutput = strlen(outputWKT);

  return code;
#else
  TAOS_RETURN(TSDB_CODE_OPS_NOT_SUPPORT);
#endif
}

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char    stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(stableName, (*smrSuperTable).me.name);

  int32_t numOfRows = *pNumOfRows;

  int32_t numOfTags = (*smrSuperTable).me.stbEntry.schemaTag.nCols;
  for (int32_t i = 0; i < numOfTags; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tableName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // super table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, stableName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // tag name
    char tagName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tagName, (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tagName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // tag type
    int8_t tagType = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].type;

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    int32_t tagStrBufflen = 32;
    char    tagTypeStr[VARSTR_HEADER_SIZE + 32];
    int     tagTypeLen = tsnprintf(varDataVal(tagTypeStr), tagStrBufflen, "%s", tDataTypes[tagType].name);
    tagStrBufflen -= tagTypeLen;
    if (tagStrBufflen <= 0) {
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (tagType == TSDB_DATA_TYPE_NCHAR) {
      tagTypeLen += tsnprintf(
          varDataVal(tagTypeStr) + tagTypeLen, tagStrBufflen, "(%d)",
          (int32_t)(((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    } else if (IS_VAR_DATA_TYPE(tagType)) {
      if (IS_STR_DATA_BLOB(tagType)) {
        code = TSDB_CODE_BLOB_NOT_SUPPORT_TAG;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      tagTypeLen += tsnprintf(varDataVal(tagTypeStr) + tagTypeLen, tagStrBufflen, "(%d)",
                              (int32_t)((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE));
    }
    varDataSetLen(tagTypeStr, tagTypeLen);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)tagTypeStr, false);
    QUERY_CHECK_CODE(code, lino, _end);

    STagVal tagVal = {0};
    tagVal.cid = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].colId;
    char*    tagData = NULL;
    uint32_t tagLen = 0;

    if (tagType == TSDB_DATA_TYPE_JSON) {
      tagData = (char*)smrChildTable->me.ctbEntry.pTags;
    } else {
      bool exist = tTagGet((STag*)smrChildTable->me.ctbEntry.pTags, &tagVal);
      if (exist) {
        if (tagType == TSDB_DATA_TYPE_GEOMETRY) {
          code = sysTableGetGeomText(tagVal.pData, tagVal.nData, &tagData, &tagLen);
          QUERY_CHECK_CODE(code, lino, _end);
        } else if (tagType == TSDB_DATA_TYPE_VARBINARY) {
          code = taosAscii2Hex(tagVal.pData, tagVal.nData, (void**)&tagData, &tagLen);
          if (code < 0) {
            qError("varbinary for systable failed since %s", tstrerror(code));
          }
        } else if (IS_VAR_DATA_TYPE(tagType)) {
          tagData = (char*)tagVal.pData;
          tagLen = tagVal.nData;
        } else {
          tagData = (char*)&tagVal.i64;
          tagLen = tDataTypes[tagType].bytes;
        }
      }
    }

    char* tagVarChar = NULL;
    if (tagData != NULL) {
      if (IS_STR_DATA_BLOB(tagType)) {
        code = TSDB_CODE_BLOB_NOT_SUPPORT_TAG;
        goto _end;
      }

      if (tagType == TSDB_DATA_TYPE_JSON) {
        char* tagJson = NULL;
        parseTagDatatoJson(tagData, &tagJson, NULL);
        if (tagJson == NULL) {
          code = terrno;
          goto _end;
        }
        tagVarChar = taosMemoryMalloc(strlen(tagJson) + VARSTR_HEADER_SIZE);
        QUERY_CHECK_NULL(tagVarChar, code, lino, _end, terrno);
        memcpy(varDataVal(tagVarChar), tagJson, strlen(tagJson));
        varDataSetLen(tagVarChar, strlen(tagJson));
        taosMemoryFree(tagJson);
      } else {
        int32_t bufSize = IS_VAR_DATA_TYPE(tagType) ? (tagLen + VARSTR_HEADER_SIZE)
                                                    : (3 + DBL_MANT_DIG - DBL_MIN_EXP + VARSTR_HEADER_SIZE);
        tagVarChar = taosMemoryCalloc(1, bufSize + 1);
        QUERY_CHECK_NULL(tagVarChar, code, lino, _end, terrno);
        int32_t len = -1;
        if (tagLen > 0)
          convertTagDataToStr(varDataVal(tagVarChar), bufSize + 1 - VARSTR_HEADER_SIZE, tagType, tagData, tagLen, &len);
        else
          len = 0;
        varDataSetLen(tagVarChar, len);
      }
    }
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tagVarChar,
                         (tagData == NULL) || (tagType == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(tagData)));
    QUERY_CHECK_CODE(code, lino, _end);

    if (tagType == TSDB_DATA_TYPE_GEOMETRY || tagType == TSDB_DATA_TYPE_VARBINARY) taosMemoryFreeClear(tagData);
    taosMemoryFree(tagVarChar);
    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sysTableUserColsFillOneTableCols(const SSysTableScanInfo* pInfo, const char* dbname, int32_t* pNumOfRows,
                                                const SSDataBlock* dataBlock, char* tName, SSchemaWrapper* schemaRow,
                                                char* tableType, SColRefWrapper* colRef) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (schemaRow == NULL) {
    qError("sysTableUserColsFillOneTableCols schemaRow is NULL");
    return TSDB_CODE_SUCCESS;
  }
  int32_t numOfRows = *pNumOfRows;

  int32_t numOfCols = schemaRow->nCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tableType, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col name
    char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(colName, schemaRow->pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, colName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col type
    int8_t colType = schemaRow->pSchema[i].type;
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    int32_t colStrBufflen = 32;
    char    colTypeStr[VARSTR_HEADER_SIZE + 32];
    int     colTypeLen = tsnprintf(varDataVal(colTypeStr), colStrBufflen, "%s", tDataTypes[colType].name);
    colStrBufflen -= colTypeLen;
    if (colStrBufflen <= 0) {
      code = TSDB_CODE_INVALID_PARA;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (colType == TSDB_DATA_TYPE_VARCHAR) {
      colTypeLen += tsnprintf(varDataVal(colTypeStr) + colTypeLen, colStrBufflen, "(%d)",
                              (int32_t)(schemaRow->pSchema[i].bytes - VARSTR_HEADER_SIZE));
    } else if (colType == TSDB_DATA_TYPE_NCHAR) {
      colTypeLen += tsnprintf(varDataVal(colTypeStr) + colTypeLen, colStrBufflen, "(%d)",
                              (int32_t)((schemaRow->pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }
    varDataSetLen(colTypeStr, colTypeLen);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)colTypeStr, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col length
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (const char*)&schemaRow->pSchema[i].bytes, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col precision, col scale, col nullable
    for (int32_t j = 6; j <= 8; ++j) {
      pColInfoData = taosArrayGet(dataBlock->pDataBlock, j);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);
    }

    // col data source
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (!colRef || !colRef->pColRef[i].hasRef) {
      colDataSetNULL(pColInfoData, numOfRows);
    } else {
      char refColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char tmpColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN] = {0};
      strcat(tmpColName, colRef->pColRef[i].refDbName);
      strcat(tmpColName, ".");
      strcat(tmpColName, colRef->pColRef[i].refTableName);
      strcat(tmpColName, ".");
      strcat(tmpColName, colRef->pColRef[i].refColName);
      STR_TO_VARSTR(refColName, tmpColName);

      code = colDataSetVal(pColInfoData, numOfRows, (char*)refColName, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // col id
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 10);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (const char*)&schemaRow->pSchema[i].colId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t sysTableUserColsFillOneVirtualChildTableCols(const SSysTableScanInfo* pInfo, const char* dbname, int32_t* pNumOfRows,
                                                            const SSDataBlock* dataBlock, char* tName, char* stName,
                                                            SSchemaWrapper* schemaRow, char* tableType, SColRefWrapper *colRef,
                                                            tb_uid_t uid, int32_t vgId) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (schemaRow == NULL) {
    qError("sysTableUserColsFillOneTableCols schemaRow is NULL");
    return TSDB_CODE_SUCCESS;
  }
  int32_t numOfRows = *pNumOfRows;

  int32_t numOfCols = schemaRow->nCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, tName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // stable name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, stName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // col name
    char colName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(colName, schemaRow->pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, colName, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // uid
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&uid, false);
    QUERY_CHECK_CODE(code, lino, _end);


    // col data source
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (!colRef || !colRef->pColRef[i].hasRef) {
      colDataSetNULL(pColInfoData, numOfRows);
    } else {
      code = colDataSetVal(pColInfoData, numOfRows, (char *)&colRef->pColRef[i].id, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 6);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (!colRef || !colRef->pColRef[i].hasRef) {
      colDataSetNULL(pColInfoData, numOfRows);
    } else {
      char refColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      char tmpColName[TSDB_DB_NAME_LEN + TSDB_NAME_DELIMITER_LEN + TSDB_COL_FNAME_LEN] = {0};
      strcat(tmpColName, colRef->pColRef[i].refDbName);
      strcat(tmpColName, ".");
      strcat(tmpColName, colRef->pColRef[i].refTableName);
      strcat(tmpColName, ".");
      strcat(tmpColName, colRef->pColRef[i].refColName);
      STR_TO_VARSTR(refColName, tmpColName);

      code = colDataSetVal(pColInfoData, numOfRows, (char *)refColName, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // vgid
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}


static SSDataBlock* buildInfoSchemaTableMetaBlock(char* tableName) {
  size_t               size = 0;
  const SSysTableMeta* pMeta = NULL;
  getInfosDbMeta(&pMeta, &size);

  int32_t index = 0;
  for (int32_t i = 0; i < size; ++i) {
    if (strcmp(pMeta[i].name, tableName) == 0) {
      index = i;
      break;
    }
  }

  SSDataBlock* pBlock = NULL;
  int32_t      code = createDataBlock(&pBlock);
  if (code) {
    terrno = code;
    return NULL;
  }

  for (int32_t i = 0; i < pMeta[index].colNum; ++i) {
    SColumnInfoData colInfoData =
        createColumnInfoData(pMeta[index].schema[i].type, pMeta[index].schema[i].bytes, i + 1);
    code = blockDataAppendColInfo(pBlock, &colInfoData);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      blockDataDestroy(pBlock);
      pBlock = NULL;
      terrno = code;
      break;
    }
  }

  return pBlock;
}

int32_t buildDbTableInfoBlock(bool sysInfo, const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta, size_t size,
                              const char* dbName, int64_t* pRows) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char    n[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t numOfRows = p->info.rows;

  for (int32_t i = 0; i < size; ++i) {
    const SSysTableMeta* pm = &pSysDbTableMeta[i];
    if (!sysInfo && pm->sysInfo) {
      continue;
    }

    if (strcmp(pm->name, TSDB_INS_TABLE_USERS_FULL) == 0) {
      continue;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    STR_TO_VARSTR(n, pm->name);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    STR_TO_VARSTR(n, dbName);
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, numOfRows);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&pm->colNum, false);
    QUERY_CHECK_CODE(code, lino, _end);

    for (int32_t j = 4; j <= 8; ++j) {
      pColInfoData = taosArrayGet(p->pDataBlock, j);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);
    }

    STR_TO_VARSTR(n, "SYSTEM_TABLE");

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    numOfRows += 1;
  }

  *pRows = numOfRows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  size_t               size = 0;
  const SSysTableMeta* pSysDbTableMeta = NULL;

  getInfosDbMeta(&pSysDbTableMeta, &size);
  code = buildDbTableInfoBlock(pInfo->sysInfo, p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB, &p->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  getPerfDbMeta(&pSysDbTableMeta, &size);
  code = buildDbTableInfoBlock(pInfo->sysInfo, p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB, &p->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  pInfo->pRes->info.rows = p->info.rows;
  code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
  QUERY_CHECK_CODE(code, lino, _end);

  blockDataDestroy(p);
  p = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(p);
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetUserTableMetaInfo(SStoreMetaReader* pMetaReaderFn, SStoreMeta* pMetaFn, void* pVnode,
                                      SMetaReader* pMReader, int64_t uid, const char* dbname, int32_t vgId,
                                      SSDataBlock* p, int32_t rowIndex, const char* idStr) {
  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t lino = 0;
  int32_t code = pMetaReaderFn->getTableEntryByUid(pMReader, uid);
  if (code < 0) {
    qError("failed to get table meta, uid:%" PRId64 ", code:%s, %s", uid, tstrerror(terrno), idStr);
    return code;
  }

  STR_TO_VARSTR(n, pMReader->me.name);

  // table name
  SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

  code = colDataSetVal(pColInfoData, rowIndex, n, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // database name
  pColInfoData = taosArrayGet(p->pDataBlock, 1);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, rowIndex, dbname, false);
  QUERY_CHECK_CODE(code, lino, _end);

  // vgId
  pColInfoData = taosArrayGet(p->pDataBlock, 6);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  code = colDataSetVal(pColInfoData, rowIndex, (char*)&vgId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  int32_t tableType = pMReader->me.type;
  if (tableType == TSDB_CHILD_TABLE) {
    // create time
    int64_t ts = pMReader->me.ctbEntry.btime;
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&ts, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SMetaReader mr1 = {0};
    pMetaReaderFn->initReader(&mr1, pVnode, META_READER_NOLOCK, pMetaFn);

    int64_t suid = pMReader->me.ctbEntry.suid;
    code = pMetaReaderFn->getTableEntryByUid(&mr1, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pMReader->me.name, suid,
             tstrerror(code), idStr);
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&mr1.me.stbEntry.schemaRow.nCols, false);
    if (code != 0) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // super table name
    STR_TO_VARSTR(n, mr1.me.name);
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, n, false);
    pMetaReaderFn->clearReader(&mr1);
    QUERY_CHECK_CODE(code, lino, _end);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (pMReader->me.ctbEntry.commentLen > 0) {
      char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, pMReader->me.ctbEntry.comment);
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pMReader->me.ctbEntry.commentLen == 0) {
      char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, "");
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(pColInfoData, rowIndex);
    }

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ctbEntry.ttlDays, false);
    QUERY_CHECK_CODE(code, lino, _end);

    STR_TO_VARSTR(n, "CHILD_TABLE");

  } else if (tableType == TSDB_NORMAL_TABLE) {
    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.btime, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.schemaRow.nCols, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // super table name
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    if (pMReader->me.ntbEntry.commentLen > 0) {
      char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, pMReader->me.ntbEntry.comment);
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (pMReader->me.ntbEntry.commentLen == 0) {
      char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(comment, "");
      code = colDataSetVal(pColInfoData, rowIndex, comment, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(pColInfoData, rowIndex);
    }

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.ttlDays, false);
    QUERY_CHECK_CODE(code, lino, _end);

    STR_TO_VARSTR(n, "NORMAL_TABLE");
    // impl later
  } else if (tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.btime, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.ntbEntry.schemaRow.nCols, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // super table name
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    STR_TO_VARSTR(n, "VIRTUAL_NORMAL_TABLE");
    // impl later
  } else if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
    // create time
    int64_t ts = pMReader->me.ctbEntry.btime;
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&ts, false);
    QUERY_CHECK_CODE(code, lino, _end);

    SMetaReader mr1 = {0};
    pMetaReaderFn->initReader(&mr1, pVnode, META_READER_NOLOCK, pMetaFn);

    int64_t suid = pMReader->me.ctbEntry.suid;
    code = pMetaReaderFn->getTableEntryByUid(&mr1, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pMReader->me.name, suid,
             tstrerror(code), idStr);
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, (char*)&mr1.me.stbEntry.schemaRow.nCols, false);
    if (code != 0) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // super table name
    STR_TO_VARSTR(n, mr1.me.name);
    pColInfoData = taosArrayGet(p->pDataBlock, 4);
    if (pColInfoData == NULL) {
      pMetaReaderFn->clearReader(&mr1);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    }

    code = colDataSetVal(pColInfoData, rowIndex, n, false);
    pMetaReaderFn->clearReader(&mr1);
    QUERY_CHECK_CODE(code, lino, _end);

    // table comment
    pColInfoData = taosArrayGet(p->pDataBlock, 8);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    // uid
    pColInfoData = taosArrayGet(p->pDataBlock, 5);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, rowIndex, (char*)&pMReader->me.uid, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // ttl
    pColInfoData = taosArrayGet(p->pDataBlock, 7);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    colDataSetNULL(pColInfoData, rowIndex);

    STR_TO_VARSTR(n, "VIRTUAL_CHILD_TABLE");
  }

_end:
  qError("%s failed at line %d since %s, %s", __func__, lino, tstrerror(code), idStr);
  return code;
}

static SSDataBlock* sysTableBuildUserTablesByUids(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SSysTableIndex*    pIdx = pInfo->pIdx;
  SSDataBlock*       p = NULL;
  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  int ret = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t i = pIdx->lastIdx;
  for (; i < taosArrayGetSize(pIdx->uids); i++) {
    tb_uid_t* uid = taosArrayGet(pIdx->uids, i);
    QUERY_CHECK_NULL(uid, code, lino, _end, terrno);

    SMetaReader mr = {0};
    pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);

    code = doSetUserTableMetaInfo(&pAPI->metaReaderFn, &pAPI->metaFn, pInfo->readHandle.vnode, &mr, *uid, dbname, vgId,
                                  p, numOfRows, GET_TASKID(pTaskInfo));

    pAPI->metaReaderFn.clearReader(&mr);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);

    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(p);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  }

  if (numOfRows > 0) {
    p->info.rows = numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  if (i >= taosArrayGetSize(pIdx->uids)) {
    setOperatorCompleted(pOperator);
  } else {
    pIdx->lastIdx = i + 1;
  }

  blockDataDestroy(p);
  p = NULL;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableBuildUserTables(SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;
  int8_t         firstMetaCursor = 0;
  SSDataBlock*   p = NULL;

  SSysTableScanInfo* pInfo = pOperator->info;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    QUERY_CHECK_NULL(pInfo->pCur, code, lino, _end, terrno);
    firstMetaCursor = 1;
  }
  if (!firstMetaCursor) {
    code = pAPI->metaFn.resumeTableMetaCursor(pInfo->pCur, 0, 1);
    if (code != 0) {
      pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  char n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

  int32_t ret = 0;
  while ((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_SUPER_TABLE)) == 0) {
    STR_TO_VARSTR(n, pInfo->pCur->mr.me.name);

    // table name
    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // database name
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    // vgId
    pColInfoData = taosArrayGet(p->pDataBlock, 6);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    int32_t tableType = pInfo->pCur->mr.me.type;
    if (tableType == TSDB_CHILD_TABLE) {
      // create time
      int64_t ts = pInfo->pCur->mr.me.ctbEntry.btime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&ts, false);
      QUERY_CHECK_CODE(code, lino, _end);

      SMetaReader mr = {0};
      pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);

      uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      code = pAPI->metaReaderFn.getTableEntryByUid(&mr, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        pAPI->metaReaderFn.clearReader(&mr);
        pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
        pInfo->pCur = NULL;
        blockDataDestroy(p);
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      if (isTsmaResSTb(mr.me.name)) {
        pAPI->metaReaderFn.clearReader(&mr);
        continue;
      }

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      STR_TO_VARSTR(n, mr.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, n, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pAPI->metaReaderFn.clearReader(&mr);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      if (pInfo->pCur->mr.me.ctbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ctbEntry.comment);
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pInfo->pCur->mr.me.ctbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ctbEntry.ttlDays, false);
      QUERY_CHECK_CODE(code, lino, _end);

      STR_TO_VARSTR(n, "CHILD_TABLE");
    } else if (tableType == TSDB_NORMAL_TABLE) {
      // create time
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.btime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      if (pInfo->pCur->mr.me.ntbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ntbEntry.comment);
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (pInfo->pCur->mr.me.ntbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        code = colDataSetVal(pColInfoData, numOfRows, comment, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        colDataSetNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ttlDays, false);
      QUERY_CHECK_CODE(code, lino, _end);

      STR_TO_VARSTR(n, "NORMAL_TABLE");
    } else if (tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
      // create time
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.btime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      STR_TO_VARSTR(n, "VIRTUAL_NORMAL_TABLE");
    } else if (tableType == TSDB_VIRTUAL_CHILD_TABLE) {
      // create time
      int64_t ts = pInfo->pCur->mr.me.ctbEntry.btime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&ts, false);
      QUERY_CHECK_CODE(code, lino, _end);

      SMetaReader mr = {0};
      pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_NOLOCK, &pAPI->metaFn);

      uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
      code = pAPI->metaReaderFn.getTableEntryByUid(&mr, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        pAPI->metaReaderFn.clearReader(&mr);
        pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
        pInfo->pCur = NULL;
        blockDataDestroy(p);
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      if (isTsmaResSTb(mr.me.name)) {
        pAPI->metaReaderFn.clearReader(&mr);
        continue;
      }

      // number of columns
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schemaRow.nCols, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // super table name
      STR_TO_VARSTR(n, mr.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, n, false);
      QUERY_CHECK_CODE(code, lino, _end);
      pAPI->metaReaderFn.clearReader(&mr);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      colDataSetNULL(pColInfoData, numOfRows);

      STR_TO_VARSTR(n, "VIRTUAL_CHILD_TABLE");
    }

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
    code = colDataSetVal(pColInfoData, numOfRows, n, false);
    QUERY_CHECK_CODE(code, lino, _end);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(p);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
        break;
      }
    }
  }

  if (numOfRows > 0) {
    pAPI->metaFn.pauseTableMetaCursor(pInfo->pCur);
    p->info.rows = numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  blockDataDestroy(p);
  p = NULL;

  // todo temporarily free the cursor here, the true reason why the free is not valid needs to be found
  if (ret != 0) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    setOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static int32_t buildVgDiskUsage(SOperatorInfo* pOperator, SDbSizeStatisInfo* pStaticsInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            vgId = 0;
  const char*        db = NULL;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &pStaticsInfo->dbname, &vgId, NULL, NULL);

  pStaticsInfo->vgId = vgId;

  code = pAPI->metaFn.getDBSize(pInfo->readHandle.vnode, pStaticsInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = vnodeEstimateRawDataSize(pOperator, pStaticsInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  pStaticsInfo->memSize = pStaticsInfo->memSize >> 10;
  pStaticsInfo->l1Size = pStaticsInfo->l1Size >> 10;
  pStaticsInfo->l2Size = pStaticsInfo->l2Size >> 10;
  pStaticsInfo->l3Size = pStaticsInfo->l3Size >> 10;
  pStaticsInfo->cacheSize = pStaticsInfo->cacheSize >> 10;
  pStaticsInfo->walSize = pStaticsInfo->walSize >> 10;
  pStaticsInfo->metaSize = pStaticsInfo->metaSize >> 10;
  pStaticsInfo->rawDataSize = pStaticsInfo->rawDataSize >> 10;
  pStaticsInfo->ssSize = pStaticsInfo->ssSize >> 10;

_end:
  return code;
}
static SSDataBlock* sysTableBuildVgUsage(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SDbSizeStatisInfo  staticsInfo = {0};

  char*        buf = NULL;
  SSDataBlock* p = NULL;

  const char* db = NULL;
  int32_t     numOfCols = 0;
  int32_t     numOfRows = 0;

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    setOperatorCompleted(pOperator);
    return NULL;
  }
  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    if (pInfo->pCur == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  SSDataBlock* pBlock = pInfo->pRes;

  code = buildVgDiskUsage(pOperator, &staticsInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->showRewrite) {
    SSDataBlock*      pBlock = pInfo->pRes;
    SDBBlockUsageInfo usageInfo = {0};
    int32_t           len = tSerializeBlockDbUsage(NULL, 0, &usageInfo);

    usageInfo.dataInDiskSize = staticsInfo.l1Size + staticsInfo.l2Size + staticsInfo.l3Size;
    usageInfo.walInDiskSize = staticsInfo.walSize;
    usageInfo.rawDataSize = staticsInfo.rawDataSize;

    buf = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
    QUERY_CHECK_NULL(buf, code, lino, _end, terrno);

    int32_t tempRes = tSerializeBlockDbUsage(varDataVal(buf), len, &usageInfo);
    if (tempRes != len) {
      QUERY_CHECK_CODE(TSDB_CODE_INVALID_MSG, lino, _end);
    }

    varDataSetLen(buf, len);

    int32_t          slotId = 1;
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 1);
    QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);
    code = colDataSetVal(pColInfo, 0, buf, false);
    QUERY_CHECK_CODE(code, lino, _end);
    taosMemoryFreeClear(buf);
    if (slotId != 0) {
      SColumnInfoData* p1 = taosArrayGet(pBlock->pDataBlock, 0);
      QUERY_CHECK_NULL(p1, code, lino, _end, terrno);
    }

    pBlock->info.rows = 1;
    pOperator->status = OP_EXEC_DONE;
    pInfo->pRes->info.rows = pBlock->info.rows;
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    SName sn = {0};
    char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    code = tNameFromString(&sn, staticsInfo.dbname, T_NAME_ACCT | T_NAME_DB);
    QUERY_CHECK_CODE(code, lino, _end);

    code = tNameGetDbName(&sn, varDataVal(dbname));
    QUERY_CHECK_CODE(code, lino, _end);

    varDataSetLen(dbname, strlen(varDataVal(dbname)));

    p = buildInfoSchemaTableMetaBlock(TSDB_INS_DISK_USAGE);
    QUERY_CHECK_NULL(p, code, lino, _end, terrno);

    code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.vgId, false);
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.walSize, false);  // wal
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.l1Size, false);  // l1_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.l2Size, false);  // l2_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.l3Size, false);  // l3_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.cacheSize, false);  // cache_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.metaSize, false);  // meta_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.ssSize, false);  // ss_size
    QUERY_CHECK_CODE(code, lino, _end);

    pColInfoData = taosArrayGet(p->pDataBlock, numOfCols++);
    code = colDataSetVal(pColInfoData, numOfRows, (char*)&staticsInfo.rawDataSize, false);  // estimate_size
    QUERY_CHECK_CODE(code, lino, _end);
    numOfRows += 1;

    if (numOfRows > 0) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    blockDataDestroy(p);
    p = NULL;

    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    setOperatorCompleted(pOperator);
  }
_end:
  taosMemoryFree(buf);
  if (pInfo->pCur) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserTables(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  SNode* pCondition = pInfo->pCondition;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    code = buildSysDbTableInfo(pInfo, pOperator->resultInfo.capacity);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

    setOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  } else {
    if (pInfo->showRewrite == false) {
      if (pCondition != NULL && pInfo->pIdx == NULL) {
        SSTabFltArg arg = {
            .pMeta = pInfo->readHandle.vnode, .pVnode = pInfo->readHandle.vnode, .pAPI = &pTaskInfo->storageAPI};

        SSysTableIndex* idx = taosMemoryMalloc(sizeof(SSysTableIndex));
        QUERY_CHECK_NULL(idx, code, lino, _end, terrno);
        idx->init = 0;
        idx->uids = taosArrayInit(128, sizeof(int64_t));
        QUERY_CHECK_NULL(idx->uids, code, lino, _end, terrno);
        idx->lastIdx = 0;

        pInfo->pIdx = idx;  // set idx arg

        int flt = optSysTabFilte(&arg, pCondition, idx->uids);
        if (flt == 0) {
          pInfo->pIdx->init = 1;
          SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
          return blk;
        } else if ((flt == -1) || (flt == -2)) {
          qDebug("%s failed to get sys table info by idx, scan sys table one by one", GET_TASKID(pTaskInfo));
        }
      } else if (pCondition != NULL && (pInfo->pIdx != NULL && pInfo->pIdx->init == 1)) {
        SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
        return blk;
      }
    }

    return sysTableBuildUserTables(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}
static SSDataBlock* sysTableScanUsage(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  SNode* pCondition = pInfo->pCondition;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  return sysTableBuildVgUsage(pOperator);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}

static SSDataBlock* sysTableScanUserSTables(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  pInfo->pRes->info.rows = 0;
  pOperator->status = OP_EXEC_DONE;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static int32_t doSetQueryFileSetRow() {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  // TODO

_exit:
  return code;
}

static SSDataBlock* sysTableBuildUserFileSets(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SSDataBlock*       p = NULL;

  // open cursor if not opened
  if (pInfo->pFileSetReader == NULL) {
    code = pAPI->tsdReader.fileSetReaderOpen(pInfo->readHandle.vnode, &pInfo->pFileSetReader);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  const char* db = NULL;
  int32_t     vgId = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, varDataVal(dbname));
  QUERY_CHECK_CODE(code, lino, _end);

  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_FILESETS);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  code = blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _end);

  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t ret = 0;

  // loop to query each entry
  for (;;) {
    int32_t ret = pAPI->tsdReader.fileSetReadNext(pInfo->pFileSetReader);
    if (ret) {
      if (ret == TSDB_CODE_NOT_FOUND) {
        // no more scan entry
        setOperatorCompleted(pOperator);
        pAPI->tsdReader.fileSetReaderClose(&pInfo->pFileSetReader);
        break;
      } else {
        code = ret;
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    // fill the data block
    {
      SColumnInfoData* pColInfoData;
      int32_t          index = 0;

      // db_name
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, dbname, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // vgroup_id
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // fileset_id
      int32_t filesetId = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "fileset_id", &filesetId);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&filesetId, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // start_time
      int64_t startTime = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "start_time", &startTime);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&startTime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // end_time
      int64_t endTime = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "end_time", &endTime);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&endTime, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // total_size
      int64_t totalSize = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "total_size", &totalSize);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&totalSize, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // last_compact
      int64_t lastCompact = 0;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "last_compact_time", &lastCompact);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&lastCompact, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // should_compact
      bool shouldCompact = false;
      code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "should_compact", &shouldCompact);
      QUERY_CHECK_CODE(code, lino, _end);
      pColInfoData = taosArrayGet(p->pDataBlock, index++);
      QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      code = colDataSetVal(pColInfoData, numOfRows, (char*)&shouldCompact, false);
      QUERY_CHECK_CODE(code, lino, _end);

      // // details
      // const char* details = NULL;
      // code = pAPI->tsdReader.fileSetGetEntryField(pInfo->pFileSetReader, "details", &details);
      // QUERY_CHECK_CODE(code, lino, _end);
      // pColInfoData = taosArrayGet(p->pDataBlock, index++);
      // QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
      // code = colDataSetVal(pColInfoData, numOfRows, (char*)&vgId, false);
      // QUERY_CHECK_CODE(code, lino, _end);
    }

    // check capacity
    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(p);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  }

  if (numOfRows > 0) {
    p->info.rows = numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    code = relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  blockDataDestroy(p);
  p = NULL;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(p);
    pTaskInfo->code = code;
    pAPI->tsdReader.fileSetReaderClose(&pInfo->pFileSetReader);
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserFileSets(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSysTableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SNode*             pCondition = pInfo->pCondition;

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pInfo->readHandle.mnd != NULL) {
    // do nothing on mnode
    qTrace("This operator do nothing on mnode, task id:%s", GET_TASKID(pTaskInfo));
    return NULL;
  } else {
#if 0
    if (pInfo->showRewrite == false) {
      if (pCondition != NULL && pInfo->pIdx == NULL) {
        SSTabFltArg arg = {
            .pMeta = pInfo->readHandle.vnode, .pVnode = pInfo->readHandle.vnode, .pAPI = &pTaskInfo->storageAPI};

        SSysTableIndex* idx = taosMemoryMalloc(sizeof(SSysTableIndex));
        QUERY_CHECK_NULL(idx, code, lino, _end, terrno);
        idx->init = 0;
        idx->uids = taosArrayInit(128, sizeof(int64_t));
        QUERY_CHECK_NULL(idx->uids, code, lino, _end, terrno);
        idx->lastIdx = 0;

        pInfo->pIdx = idx;  // set idx arg

        int flt = optSysTabFilte(&arg, pCondition, idx->uids);
        if (flt == 0) {
          pInfo->pIdx->init = 1;
          SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
          return blk;
        } else if ((flt == -1) || (flt == -2)) {
          qDebug("%s failed to get sys table info by idx, scan sys table one by one", GET_TASKID(pTaskInfo));
        }
      } else if (pCondition != NULL && (pInfo->pIdx != NULL && pInfo->pIdx->init == 1)) {
        SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
        return blk;
      }
    }
#endif

    return sysTableBuildUserFileSets(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}

static int32_t getSysTableDbNameColId(const char* pTable) {
  // if (0 == strcmp(TSDB_INS_TABLE_INDEXES, pTable)) {
  //   return 1;
  // }
  return TSDB_INS_USER_STABLES_DBNAME_COLID;
}

static EDealRes getDBNameFromConditionWalker(SNode* pNode, void* pContext) {
  int32_t   code = TSDB_CODE_SUCCESS;
  ENodeType nType = nodeType(pNode);

  switch (nType) {
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* node = (SOperatorNode*)pNode;
      if (OP_TYPE_EQUAL == node->opType) {
        *(int32_t*)pContext = 1;
        return DEAL_RES_CONTINUE;
      }

      *(int32_t*)pContext = 0;
      return DEAL_RES_IGNORE_CHILD;
    }
    case QUERY_NODE_COLUMN: {
      if (1 != *(int32_t*)pContext) {
        return DEAL_RES_CONTINUE;
      }

      SColumnNode* node = (SColumnNode*)pNode;
      if (getSysTableDbNameColId(node->tableName) == node->colId) {
        *(int32_t*)pContext = 2;
        return DEAL_RES_CONTINUE;
      }

      *(int32_t*)pContext = 0;
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_VALUE: {
      if (2 != *(int32_t*)pContext) {
        return DEAL_RES_CONTINUE;
      }

      SValueNode* node = (SValueNode*)pNode;
      char*       dbName = nodesGetValueFromNode(node);
      tstrncpy((char*)pContext, varDataVal(dbName), TSDB_DB_NAME_LEN);
      return DEAL_RES_END;  // stop walk
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static void getDBNameFromCondition(SNode* pCondition, const char* dbName) {
  if (NULL == pCondition) {
    return;
  }
  nodesWalkExpr(pCondition, getDBNameFromConditionWalker, (char*)dbName);
}

static int32_t doSysTableScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  // build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  char               dbName[TSDB_DB_NAME_LEN] = {0};

  while (1) {
    if (isTaskKilled(pOperator->pTaskInfo)) {
      setOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      break;
    }

    blockDataCleanup(pInfo->pRes);

    const char* name = tNameGetTableName(&pInfo->name);
    if (pInfo->showRewrite) {
      getDBNameFromCondition(pInfo->pCondition, dbName);
      if (strncasecmp(name, TSDB_INS_TABLE_COMPACTS, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_COMPACT_DETAILS, TSDB_TABLE_FNAME_LEN) != 0 &&
          strncasecmp(name, TSDB_INS_TABLE_TRANSACTION_DETAILS, TSDB_TABLE_FNAME_LEN) != 0) {
        TAOS_UNUSED(tsnprintf(pInfo->req.db, sizeof(pInfo->req.db), "%d.%s", pInfo->accountId, dbName));
      }
    } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0) {
      getDBNameFromCondition(pInfo->pCondition, dbName);
      if (dbName[0]) TAOS_UNUSED(tsnprintf(pInfo->req.db, sizeof(pInfo->req.db), "%d.%s", pInfo->accountId, dbName));
      (void)sysTableIsCondOnOneTable(pInfo->pCondition, pInfo->req.filterTb);
    }
    bool         filter = true;
    SSDataBlock* pBlock = NULL;
    if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUserTables(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUserTags(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->readHandle.mnd == NULL) {
      pBlock = sysTableScanUserCols(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_VC_COLS, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->readHandle.mnd == NULL) {
      pBlock = sysTableScanUserVcCols(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_STABLES, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->showRewrite &&
               IS_SYS_DBNAME(dbName)) {
      pBlock = sysTableScanUserSTables(pOperator);
    } else if (strncasecmp(name, TSDB_INS_DISK_USAGE, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUsage(pOperator);
    } else if (strncasecmp(name, TSDB_INS_TABLE_FILESETS, TSDB_TABLE_FNAME_LEN) == 0) {
      pBlock = sysTableScanUserFileSets(pOperator);
    } else {  // load the meta from mnode of the given epset
      pBlock = sysTableScanFromMNode(pOperator, pInfo, name, pTaskInfo);
    }

    if (!pInfo->skipFilterTable) sysTableScanFillTbName(pOperator, pInfo, name, pBlock);
    if (pBlock != NULL) {
      bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
      if (limitReached) {
        setOperatorCompleted(pOperator);
      }

      if (pBlock->info.rows == 0) {
        continue;
      }
      (*ppRes) = pBlock;
    } else {
      (*ppRes) = NULL;
    }
    break;
  }

_end:
  if (pTaskInfo->code) {
    qError("%s failed since %s", __func__, tstrerror(pTaskInfo->code));
    T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
  }
  return pTaskInfo->code;
}

static void sysTableScanFillTbName(SOperatorInfo* pOperator, const SSysTableScanInfo* pInfo, const char* name,
                                   SSDataBlock* pBlock) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pBlock == NULL) {
    return;
  }

  if (pInfo->tbnameSlotId != -1) {
    SColumnInfoData* pColumnInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, pInfo->tbnameSlotId);
    QUERY_CHECK_NULL(pColumnInfoData, code, lino, _end, terrno);
    char varTbName[TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(varTbName, name);

    code = colDataSetNItems(pColumnInfoData, 0, varTbName, pBlock->info.rows, true);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static SSDataBlock* sysTableScanFromMNode(SOperatorInfo* pOperator, SSysTableScanInfo* pInfo, const char* name,
                                          SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  while (1) {
    int64_t startTs = taosGetTimestampUs();
    tstrncpy(pInfo->req.tb, tNameGetTableName(&pInfo->name), tListLen(pInfo->req.tb));
    tstrncpy(pInfo->req.user, pInfo->pUser, tListLen(pInfo->req.user));

    int32_t contLen = tSerializeSRetrieveTableReq(NULL, 0, &pInfo->req);
    char*   buf1 = taosMemoryCalloc(1, contLen);
    if (!buf1) {
      return NULL;
    }
    int32_t tempRes = tSerializeSRetrieveTableReq(buf1, contLen, &pInfo->req);
    if (tempRes < 0) {
      code = terrno;
      taosMemoryFree(buf1);
      return NULL;
    }

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = terrno;
      taosMemoryFree(buf1);
      return NULL;
    }

    int32_t msgType = (strcasecmp(name, TSDB_INS_TABLE_DNODE_VARIABLES) == 0) ? TDMT_DND_SYSTABLE_RETRIEVE
                                                                              : TDMT_MND_SYSTABLE_RETRIEVE;

    pMsgSendInfo->param = pOperator;
    pMsgSendInfo->msgInfo.pData = buf1;
    pMsgSendInfo->msgInfo.len = contLen;
    pMsgSendInfo->msgType = msgType;
    pMsgSendInfo->fp = loadSysTableCallback;
    pMsgSendInfo->requestId = pTaskInfo->id.queryId;

    code = asyncSendMsgToServer(pInfo->readHandle.pMsgCb->clientRpc, &pInfo->epSet, NULL, pMsgSendInfo);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    code = tsem_wait(&pInfo->ready);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (pTaskInfo->code) {
      qError("%s load meta data from mnode failed, totalRows:%" PRIu64 ", code:%s", GET_TASKID(pTaskInfo),
             pInfo->loadInfo.totalRows, tstrerror(pTaskInfo->code));
      return NULL;
    }

    SRetrieveMetaTableRsp* pRsp = pInfo->pRsp;
    pInfo->req.showId = pRsp->handle;

    if (pRsp->numOfRows == 0 || pRsp->completed) {
      pOperator->status = OP_EXEC_DONE;
      qDebug("%s load meta data from mnode completed, rowsOfSource:%d, totalRows:%" PRIu64, GET_TASKID(pTaskInfo),
             pRsp->numOfRows, pInfo->loadInfo.totalRows);

      if (pRsp->numOfRows == 0) {
        taosMemoryFree(pRsp);
        return NULL;
      }
    }

    char* pStart = pRsp->data;
    code = extractDataBlockFromFetchRsp(pInfo->pRes, pRsp->data, pInfo->matchInfo.pList, &pStart);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      taosMemoryFreeClear(pRsp);
      T_LONG_JMP(pTaskInfo->env, code);
    }
    updateLoadRemoteInfo(&pInfo->loadInfo, pRsp->numOfRows, pRsp->compLen, startTs, pOperator);

    // todo log the filter info
    code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      taosMemoryFreeClear(pRsp);
      T_LONG_JMP(pTaskInfo->env, code);
    }
    taosMemoryFree(pRsp);
    if (pInfo->pRes->info.rows > 0) {
      return pInfo->pRes;
    } else if (pOperator->status == OP_EXEC_DONE) {
      return NULL;
    }
  }
}

// static int32_t resetSysTableScanOperState(SOperatorInfo* pOper) {
//   SSysTableScanInfo* pInfo = pOper->info;
//   SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
//   SSystemTableScanPhysiNode* pPhynode = (SSystemTableScanPhysiNode*)pOper->pPhyNode;

//   blockDataEmpty(pInfo->pRes);

//   if (pInfo->name.type == TSDB_TABLE_NAME_T) {
//     const char* name = tNameGetTableName(&pInfo->name);
//     if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
//         strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
//         strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 || pInfo->pCur != NULL) {
//       if (pInfo->pAPI != NULL && pInfo->pAPI->metaFn.closeTableMetaCursor != NULL) {
//         pInfo->pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
//       }

//       pInfo->pCur = NULL;
//     }
//   } else {
//     qError("pInfo->name is not initialized");
//   }

//   if (pInfo->pIdx) {
//     taosArrayDestroy(pInfo->pIdx->uids);
//     taosMemoryFree(pInfo->pIdx);
//     pInfo->pIdx = NULL;
//   }

//   if (pInfo->pSchema) {
//     taosHashCleanup(pInfo->pSchema);
//     pInfo->pSchema = NULL;
//   }

//   return 0;
// }

int32_t createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode* pScanPhyNode, const char* pUser,
                                       SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SSysTableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SSysTableScanInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }

  SScanPhysiNode*     pScanNode = &pScanPhyNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t num = 0;
  code = extractColMatchInfo(pScanNode->pScanCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  extractTbnameSlotId(pInfo, pScanNode);

  pInfo->pAPI = &pTaskInfo->storageAPI;

  pInfo->accountId = pScanPhyNode->accountId;
  pInfo->pUser = taosStrdup((void*)pUser);
  QUERY_CHECK_NULL(pInfo->pUser, code, lino, _error, terrno);
  pInfo->sysInfo = pScanPhyNode->sysInfo;
  pInfo->showRewrite = pScanPhyNode->showRewrite;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  pInfo->pCondition = pScanNode->node.pConditions;

  tNameAssign(&pInfo->name, &pScanNode->tableName);
  const char* name = tNameGetTableName(&pInfo->name);
  if (pInfo->showRewrite == false) {
    code = filterInitFromNode(pScanNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                              pTaskInfo->pStreamRuntimeInfo);
  } else {
    if (strncasecmp(name, TSDB_INS_DISK_USAGE, TSDB_TABLE_FNAME_LEN) == 0) {
      pInfo->skipFilterTable = true;
      code = filterInitFromNode(NULL, &pOperator->exprSupp.pFilterInfo, 0, pTaskInfo->pStreamRuntimeInfo);
    } else {
      code = filterInitFromNode(pScanNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                                pTaskInfo->pStreamRuntimeInfo);
    }
  }
  QUERY_CHECK_CODE(code, lino, _error);

  initLimitInfo(pScanPhyNode->scan.node.pLimit, pScanPhyNode->scan.node.pSlimit, &pInfo->limitInfo);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_FILESETS, TSDB_TABLE_FNAME_LEN) == 0) {
    pInfo->readHandle = *(SReadHandle*)readHandle;
  } else {
    if (tsem_init(&pInfo->ready, 0, 0) != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_FAILED;
      goto _error;
    }
    pInfo->epSet = pScanPhyNode->mgmtEpSet;
    pInfo->readHandle = *(SReadHandle*)readHandle;
  }

  setOperatorInfo(pOperator, "SysTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doSysTableScanNext, NULL, destroySysScanOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  // setOperatorResetStateFn(pOperator, resetSysTableScanOperState);
  
  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroySysScanOperator(pInfo);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}

void extractTbnameSlotId(SSysTableScanInfo* pInfo, const SScanPhysiNode* pScanNode) {
  pInfo->tbnameSlotId = -1;
  if (pScanNode->pScanPseudoCols != NULL) {
    SNode* pNode = NULL;
    FOREACH(pNode, pScanNode->pScanPseudoCols) {
      STargetNode* pTargetNode = NULL;
      if (nodeType(pNode) == QUERY_NODE_TARGET) {
        pTargetNode = (STargetNode*)pNode;
        SNode* expr = pTargetNode->pExpr;
        if (nodeType(expr) == QUERY_NODE_FUNCTION) {
          SFunctionNode* pFuncNode = (SFunctionNode*)expr;
          if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
            pInfo->tbnameSlotId = pTargetNode->slotId;
          }
        }
      }
    }
  }
}

void destroySysScanOperator(void* param) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  int32_t            code = tsem_destroy(&pInfo->ready);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
  }
  blockDataDestroy(pInfo->pRes);

  if (pInfo->name.type == TSDB_TABLE_NAME_T) {
    const char* name = tNameGetTableName(&pInfo->name);
    if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_COLS, TSDB_TABLE_FNAME_LEN) == 0 ||
        strncasecmp(name, TSDB_INS_TABLE_VC_COLS, TSDB_TABLE_FNAME_LEN) == 0 ||
        pInfo->pCur != NULL) {
      if (pInfo->pAPI != NULL && pInfo->pAPI->metaFn.closeTableMetaCursor != NULL) {
        pInfo->pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
      }

      pInfo->pCur = NULL;
    }
  } else {
    qError("pInfo->name is not initialized");
  }

  if (pInfo->pIdx) {
    taosArrayDestroy(pInfo->pIdx->uids);
    taosMemoryFree(pInfo->pIdx);
    pInfo->pIdx = NULL;
  }

  if (pInfo->pSchema) {
    taosHashCleanup(pInfo->pSchema);
    pInfo->pSchema = NULL;
  }

  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(pInfo->pUser);

  taosMemoryFreeClear(param);
}

int32_t loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SOperatorInfo*     operator=(SOperatorInfo*) param;
  SSysTableScanInfo* pScanResInfo = (SSysTableScanInfo*)operator->info;
  if (TSDB_CODE_SUCCESS == code) {
    pScanResInfo->pRsp = pMsg->pData;

    SRetrieveMetaTableRsp* pRsp = pScanResInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->handle = htobe64(pRsp->handle);
    pRsp->compLen = htonl(pRsp->compLen);
  } else {
    operator->pTaskInfo->code = rpcCvtErrCode(code);
    if (operator->pTaskInfo->code != code) {
      qError("load systable rsp received, error:%s, cvted error:%s", tstrerror(code),
             tstrerror(operator->pTaskInfo->code));
    } else {
      qError("load systable rsp received, error:%s", tstrerror(code));
    }
  }

  int32_t res = tsem_post(&pScanResInfo->ready);
  if (res != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(res));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t sysChkFilter__Comm(SNode* pNode) {
  // impl
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  EOperatorType  opType = pOper->opType;
  if (opType != OP_TYPE_EQUAL && opType != OP_TYPE_LOWER_EQUAL && opType != OP_TYPE_LOWER_THAN &&
      opType != OP_TYPE_GREATER_EQUAL && opType != OP_TYPE_GREATER_THAN) {
    return -1;
  }
  return 0;
}

static int32_t sysChkFilter__DBName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;

  if (pOper->opType != OP_TYPE_EQUAL && pOper->opType != OP_TYPE_NOT_EQUAL) {
    return -1;
  }

  SValueNode* pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }

  return 0;
}
static int32_t sysChkFilter__VgroupId(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__TableName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__CreateTime(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_TIMESTAMP_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}

static int32_t sysChkFilter__Ncolumn(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Ttl(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__STableName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Uid(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Type(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t optSysTabFilteImpl(void* arg, SNode* cond, SArray* result) {
  if (optSysCheckOper(cond) != 0) return -1;

  SOperatorNode* pNode = (SOperatorNode*)cond;

  int8_t i = 0;
  for (; i < SYSTAB_FILTER_DICT_SIZE; i++) {
    if (strcmp(filterDict[i].name, ((SColumnNode*)(pNode->pLeft))->colName) == 0) {
      break;
    }
  }
  if (i >= SYSTAB_FILTER_DICT_SIZE) return -1;

  if (filterDict[i].chkFunc(cond) != 0) return -1;

  return filterDict[i].fltFunc(arg, cond, result);
}

static int32_t optSysCheckOper(SNode* pOpear) {
  if (nodeType(pOpear) != QUERY_NODE_OPERATOR) return -1;

  SOperatorNode* pOper = (SOperatorNode*)pOpear;
  if (pOper->opType < OP_TYPE_GREATER_THAN || pOper->opType > OP_TYPE_NOT_EQUAL) {
    return -1;
  }

  if (nodeType(pOper->pLeft) != QUERY_NODE_COLUMN || nodeType(pOper->pRight) != QUERY_NODE_VALUE) {
    return -1;
  }
  return 0;
}

static FORCE_INLINE int optSysBinarySearch(SArray* arr, int s, int e, uint64_t k) {
  uint64_t v;
  int32_t  m;
  while (s <= e) {
    m = s + (e - s) / 2;
    v = *(uint64_t*)taosArrayGet(arr, m);
    if (v >= k) {
      e = m - 1;
    } else {
      s = m + 1;
    }
  }
  return s;
}

int32_t optSysIntersection(SArray* in, SArray* out) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  MergeIndex* mi = NULL;
  int32_t     sz = (int32_t)taosArrayGetSize(in);
  if (sz <= 0) {
    goto _end;
  }
  mi = taosMemoryCalloc(sz, sizeof(MergeIndex));
  QUERY_CHECK_NULL(mi, code, lino, _end, terrno);
  for (int i = 0; i < sz; i++) {
    SArray* t = taosArrayGetP(in, i);
    mi[i].len = (int32_t)taosArrayGetSize(t);
    mi[i].idx = 0;
  }

  SArray* base = taosArrayGetP(in, 0);
  for (int i = 0; i < taosArrayGetSize(base); i++) {
    uint64_t tgt = *(uint64_t*)taosArrayGet(base, i);
    bool     has = true;
    for (int j = 1; j < taosArrayGetSize(in); j++) {
      SArray* oth = taosArrayGetP(in, j);
      int     mid = optSysBinarySearch(oth, mi[j].idx, mi[j].len - 1, tgt);
      if (mid >= 0 && mid < mi[j].len) {
        uint64_t val = *(uint64_t*)taosArrayGet(oth, mid);
        has = (val == tgt ? true : false);
        mi[j].idx = mid;
      } else {
        has = false;
      }
    }
    if (has == true) {
      void* tmp = taosArrayPush(out, &tgt);
      if (!tmp) {
        code = terrno;
        goto _end;
      }
    }
  }

_end:
  taosMemoryFreeClear(mi);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int tableUidCompare(const void* a, const void* b) {
  int64_t u1 = *(int64_t*)a;
  int64_t u2 = *(int64_t*)b;
  if (u1 == u2) {
    return 0;
  }
  return u1 < u2 ? -1 : 1;
}

static int32_t optSysMergeRslt(SArray* mRslt, SArray* rslt) {
  // TODO, find comm mem from mRslt
  for (int i = 0; i < taosArrayGetSize(mRslt); i++) {
    SArray* arslt = taosArrayGetP(mRslt, i);
    taosArraySort(arslt, tableUidCompare);
  }
  return optSysIntersection(mRslt, rslt);
}

static int32_t optSysSpecialColumn(SNode* cond) {
  SOperatorNode* pOper = (SOperatorNode*)cond;
  SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
  for (int i = 0; i < sizeof(SYSTABLE_SPECIAL_COL) / sizeof(SYSTABLE_SPECIAL_COL[0]); i++) {
    if (0 == strcmp(pCol->colName, SYSTABLE_SPECIAL_COL[i])) {
      return 1;
    }
  }
  return 0;
}

static int32_t optSysTabFilte(void* arg, SNode* cond, SArray* result) {
  int ret = TSDB_CODE_FAILED;
  if (nodeType(cond) == QUERY_NODE_OPERATOR) {
    ret = optSysTabFilteImpl(arg, cond, result);
    if (ret == 0) {
      SOperatorNode* pOper = (SOperatorNode*)cond;
      SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
      if (0 == strcmp(pCol->colName, "create_time")) {
        return 0;
      }
      return -1;
    }
    return ret;
  }

  if (nodeType(cond) != QUERY_NODE_LOGIC_CONDITION || ((SLogicConditionNode*)cond)->condType != LOGIC_COND_TYPE_AND) {
    return ret;
  }

  SLogicConditionNode* pNode = (SLogicConditionNode*)cond;
  SNodeList*           pList = (SNodeList*)pNode->pParameterList;

  int32_t len = LIST_LENGTH(pList);

  bool    hasIdx = false;
  bool    hasRslt = true;
  SArray* mRslt = taosArrayInit(len, POINTER_BYTES);
  if (!mRslt) {
    return terrno;
  }

  SListCell* cell = pList->pHead;
  for (int i = 0; i < len; i++) {
    if (cell == NULL) break;

    SArray* aRslt = taosArrayInit(16, sizeof(int64_t));
    if (!aRslt) {
      return terrno;
    }

    ret = optSysTabFilteImpl(arg, cell->pNode, aRslt);
    if (ret == 0) {
      // has index
      hasIdx = true;
      if (optSysSpecialColumn(cell->pNode) == 0) {
        void* tmp = taosArrayPush(mRslt, &aRslt);
        if (!tmp) {
          return TSDB_CODE_FAILED;
        }
      } else {
        // db_name/vgroup not result
        taosArrayDestroy(aRslt);
      }
    } else if (ret == -2) {
      // current vg
      hasIdx = true;
      hasRslt = false;
      taosArrayDestroy(aRslt);
      break;
    } else {
      taosArrayDestroy(aRslt);
    }
    cell = cell->pNext;
  }
  if (hasRslt && hasIdx) {
    int32_t code = optSysMergeRslt(mRslt, result);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  for (int i = 0; i < taosArrayGetSize(mRslt); i++) {
    SArray* aRslt = taosArrayGetP(mRslt, i);
    taosArrayDestroy(aRslt);
  }
  taosArrayDestroy(mRslt);
  if (hasRslt == false) {
    return -2;
  }
  if (hasRslt && hasIdx) {
    cell = pList->pHead;
    for (int i = 0; i < len; i++) {
      if (cell == NULL) break;
      SOperatorNode* pOper = (SOperatorNode*)cell->pNode;
      SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
      if (nodeType(pOper->pLeft) == QUERY_NODE_COLUMN && 0 == strcmp(pCol->colName, "create_time")) {
        return 0;
      }
      cell = cell->pNext;
    }
    return -1;
  }
  return -1;
}

static int32_t doGetTableRowSize(SReadHandle* pHandle, uint64_t uid, int32_t* rowLen, const char* idstr) {
  *rowLen = 0;

  SMetaReader mr = {0};
  pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
  int32_t code = pHandle->api.metaReaderFn.getTableEntryByUid(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", uid, tstrerror(terrno), idstr);
    pHandle->api.metaReaderFn.clearReader(&mr);
    return terrno;
  }

  if (mr.me.type == TSDB_SUPER_TABLE) {
    int32_t numOfCols = mr.me.stbEntry.schemaRow.nCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.stbEntry.schemaRow.pSchema[i].bytes;
    }
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    uint64_t suid = mr.me.ctbEntry.suid;
    tDecoderClear(&mr.coder);
    code = pHandle->api.metaReaderFn.getTableEntryByUid(&mr, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno), idstr);
      pHandle->api.metaReaderFn.clearReader(&mr);
      return terrno;
    }

    int32_t numOfCols = mr.me.stbEntry.schemaRow.nCols;

    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.stbEntry.schemaRow.pSchema[i].bytes;
    }
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {
    int32_t numOfCols = mr.me.ntbEntry.schemaRow.nCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.ntbEntry.schemaRow.pSchema[i].bytes;
    }
  }

  pHandle->api.metaReaderFn.clearReader(&mr);
  return TSDB_CODE_SUCCESS;
}

static int32_t doBlockInfoScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SBlockDistInfo* pBlockScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;

  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  code = doGetTableRowSize(&pBlockScanInfo->readHandle, pBlockScanInfo->uid, (int32_t*)&blockDistInfo.rowSize,
                           GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _end);

  code = pAPI->tsdReader.tsdReaderGetDataBlockDistInfo(pBlockScanInfo->pHandle, &blockDistInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = (int32_t)pAPI->tsdReader.tsdReaderGetNumOfInMemRows(pBlockScanInfo->pHandle, &blockDistInfo.numOfInmemRows);
  QUERY_CHECK_CODE(code, lino, _end);

  SSDataBlock* pBlock = pBlockScanInfo->pResBlock;

  int32_t          slotId = pOperator->exprSupp.pExprInfo->base.resSchema.slotId;
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, slotId);
  QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  int32_t len = tSerializeBlockDistInfo(NULL, 0, &blockDistInfo);
  char*   p = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);

  int32_t tempRes = tSerializeBlockDistInfo(varDataVal(p), len, &blockDistInfo);
  if (tempRes < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }
  varDataSetLen(p, len);

  code = colDataSetVal(pColInfo, 0, p, false);
  QUERY_CHECK_CODE(code, lino, _end);

  taosMemoryFree(p);

  // make the valgrind happy that all memory buffer has been initialized already.
  if (slotId != 0) {
    SColumnInfoData* p1 = taosArrayGet(pBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(p1, code, lino, _end, terrno);
    int64_t v = 0;
    colDataSetInt64(p1, 0, &v);
  }

  pBlock->info.rows = 1;
  pOperator->status = OP_EXEC_DONE;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = pBlock;
  return code;
}

static void destroyBlockDistScanOperatorInfo(void* param) {
  SBlockDistInfo* pDistInfo = (SBlockDistInfo*)param;
  blockDataDestroy(pDistInfo->pResBlock);
  if (pDistInfo->readHandle.api.tsdReader.tsdReaderClose != NULL) {
    pDistInfo->readHandle.api.tsdReader.tsdReaderClose(pDistInfo->pHandle);
  }
  tableListDestroy(pDistInfo->pTableListInfo);
  taosMemoryFreeClear(param);
}

static int32_t initTableblockDistQueryCond(uint64_t uid, SQueryTableDataCond* pCond) {
  memset(pCond, 0, sizeof(SQueryTableDataCond));

  pCond->order = TSDB_ORDER_ASC;
  pCond->numOfCols = 1;
  pCond->colList = taosMemoryCalloc(1, sizeof(SColumnInfo));
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t));
  if (pCond->colList == NULL || pCond->pSlotList == NULL) {
    taosMemoryFree(pCond->colList);
    taosMemoryFree(pCond->pSlotList);
    return terrno;
  }

  pCond->colList->colId = 1;
  pCond->colList->type = TSDB_DATA_TYPE_TIMESTAMP;
  pCond->colList->bytes = sizeof(TSKEY);
  pCond->colList->pk = 0;

  pCond->pSlotList[0] = 0;

  pCond->twindows = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pCond->suid = uid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;

  return TSDB_CODE_SUCCESS;
}

int32_t createDataBlockInfoScanOperator(SReadHandle* readHandle, SBlockDistScanPhysiNode* pBlockScanNode,
                                        STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                        SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t         code = 0;
  int32_t         lino = 0;
  SBlockDistInfo* pInfo = taosMemoryCalloc(1, sizeof(SBlockDistInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = code = terrno;
    goto _error;
  }

  pInfo->pResBlock = createDataBlockFromDescNode(pBlockScanNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pResBlock, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pInfo->pResBlock, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  {
    SQueryTableDataCond cond = {0};
    code = initTableblockDistQueryCond(pBlockScanNode->suid, &cond);
    QUERY_CHECK_CODE(code, lino, _error);

    pInfo->pTableListInfo = pTableListInfo;
    int32_t num = 0;
    code = tableListGetSize(pTableListInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    void* pList = tableListGetInfo(pTableListInfo, 0);

    code = readHandle->api.tsdReader.tsdReaderOpen(readHandle->vnode, &cond, pList, num, pInfo->pResBlock,
                                                   (void**)&pInfo->pHandle, pTaskInfo->id.str, NULL);
    cleanupQueryTableDataCond(&cond);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  pInfo->readHandle = *readHandle;
  pInfo->uid = (pBlockScanNode->suid != 0) ? pBlockScanNode->suid : pBlockScanNode->uid;

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pBlockScanNode->pScanPseudoCols, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfCols, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "DataBlockDistScanOperator", QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doBlockInfoScanNext, NULL, destroyBlockDistScanOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo) {
    pInfo->pTableListInfo = NULL;
    destroyBlockDistScanOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

static int32_t buildTableListInfo(SOperatorInfo* pOperator, STableId* id, STableListInfo** ppTableListInfo) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            line = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  SReadHandle*       pReadHandle = &pInfo->readHandle;
  SArray*            pList = NULL;

  STableListInfo* pTableListInfo = tableListCreate();
  QUERY_CHECK_NULL(ppTableListInfo, code, line, _end, terrno);

  if (id->type == TSDB_SUPER_TABLE) {
    pList = taosArrayInit(4, sizeof(uint64_t));
    QUERY_CHECK_NULL(pList, code, line, _end, terrno);

    code = pReadHandle->api.metaFn.getChildTableList(pReadHandle->vnode, id->uid, pList);
    QUERY_CHECK_CODE(code, line, _end);

    size_t num = taosArrayGetSize(pList);
    for (int32_t i = 0; i < num; ++i) {
      uint64_t* id = taosArrayGet(pList, i);
      if (id == NULL) {
        continue;
      }
      code = tableListAddTableInfo(pTableListInfo, *id, 0);
      QUERY_CHECK_CODE(code, line, _end);
    }
    taosArrayDestroy(pList);
    pList = NULL;

  } else if (id->type == TSDB_NORMAL_TABLE) {
    code = tableListAddTableInfo(pTableListInfo, id->uid, 0);
    QUERY_CHECK_CODE(code, line, _end);
  }
  *ppTableListInfo = pTableListInfo;
  return code;
_end:
  taosArrayDestroy(pList);
  tableListDestroy(pTableListInfo);
  return code;
}
static int32_t vnodeEstimateDataSizeByUid(SOperatorInfo* pOperator, STableId* id, SDbSizeStatisInfo* pStaticInfo) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             line = 0;
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*        pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo*  pInfo = pOperator->info;
  SQueryTableDataCond cond = {0};

  SReadHandle* pReadHandle = &pInfo->readHandle;

  STableListInfo* pTableListInfo = NULL;
  code = buildTableListInfo(pOperator, id, &pTableListInfo);
  QUERY_CHECK_CODE(code, line, _end);

  tb_uid_t tbId = id->type == TSDB_SUPER_TABLE ? id->uid : 0;

  code = initTableblockDistQueryCond(tbId, &cond);
  QUERY_CHECK_CODE(code, line, _end);

  pInfo->pTableListInfo = pTableListInfo;

  int32_t num = 0;
  code = tableListGetSize(pTableListInfo, &num);
  QUERY_CHECK_CODE(code, line, _end);

  void* pList = tableListGetInfo(pTableListInfo, 0);

  code = pReadHandle->api.tsdReader.tsdReaderOpen(pReadHandle->vnode, &cond, pList, num, NULL, (void**)&pInfo->pHandle,
                                                  pTaskInfo->id.str, NULL);
  cleanupQueryTableDataCond(&cond);
  QUERY_CHECK_CODE(code, line, _end);

  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  code = doGetTableRowSize(pReadHandle, id->uid, (int32_t*)&blockDistInfo.rowSize, GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, line, _end);

  code = pAPI->tsdReader.tsdReaderGetDataBlockDistInfo(pInfo->pHandle, &blockDistInfo);
  QUERY_CHECK_CODE(code, line, _end);

  code = pAPI->tsdReader.tsdReaderGetNumOfInMemRows(pInfo->pHandle, &blockDistInfo.numOfInmemRows);
  QUERY_CHECK_CODE(code, line, _end);

  int64_t rawDiskSize = 0, rawCacheSize = 0;
  rawDiskSize = (blockDistInfo.totalRows + blockDistInfo.numOfSttRows) * blockDistInfo.rowSize;
  rawCacheSize = blockDistInfo.numOfInmemRows * blockDistInfo.rowSize;
  pStaticInfo->rawDataSize += rawDiskSize;
  pStaticInfo->cacheSize += rawCacheSize;

  if (pInfo->pHandle != NULL) {
    pReadHandle->api.tsdReader.tsdReaderClose(pInfo->pHandle);
    pInfo->pHandle = NULL;
  }

  tableListDestroy(pInfo->pTableListInfo);
  pInfo->pTableListInfo = NULL;
  return code;
_end:

  if (pInfo->pHandle != NULL) {
    pReadHandle->api.tsdReader.tsdReaderClose(pInfo->pHandle);
    pInfo->pHandle = NULL;
  }

  tableListDestroy(pInfo->pTableListInfo);
  pInfo->pTableListInfo = NULL;
  if (code) {
    pTaskInfo->code = code;
    return code;
  }
  cleanupQueryTableDataCond(&cond);
  return code;
}

static int32_t vnodeEstimateRawDataSizeImpl(SOperatorInfo* pOperator, SArray* pTableList,
                                            SDbSizeStatisInfo* pStaticInfo) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;

  int32_t rowLen = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < taosArrayGetSize(pTableList); i++) {
    STableId* id = (STableId*)taosArrayGet(pTableList, i);
    code = vnodeEstimateDataSizeByUid(pOperator, id, pStaticInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return code;
}

static int32_t vnodeEstimateRawDataSize(SOperatorInfo* pOperator, SDbSizeStatisInfo* pStaticInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t line = 0;

  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*       pAPI = &pTaskInfo->storageAPI;
  SSysTableScanInfo* pInfo = pOperator->info;
  int32_t            numOfRows = 0;
  int32_t            ret = 0;

  if (pInfo->pCur == NULL) {
    pInfo->pCur = pAPI->metaFn.openTableMetaCursor(pInfo->readHandle.vnode);
    if (pInfo->pCur == NULL) {
      TAOS_CHECK_GOTO(terrno, &line, _exit);
    }
  }

  SArray* pIdList = taosArrayInit(16, sizeof(STableId));
  if (pIdList == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _exit);
  }

  while (((ret = pAPI->metaFn.cursorNext(pInfo->pCur, TSDB_CHILD_TABLE)) == 0)) {
    if (pInfo->pCur->mr.me.type == TSDB_SUPER_TABLE) {
      STableId id = {.type = TSDB_SUPER_TABLE, .uid = pInfo->pCur->mr.me.uid};
      if (taosArrayPush(pIdList, &id) == NULL) {
        TAOS_CHECK_GOTO(terrno, &line, _exit);
      }
    } else if (pInfo->pCur->mr.me.type == TSDB_CHILD_TABLE) {
      continue;
    } else if (pInfo->pCur->mr.me.type == TSDB_NORMAL_TABLE) {
      STableId id = {.type = TSDB_NORMAL_TABLE, .uid = pInfo->pCur->mr.me.uid};
      if (taosArrayPush(pIdList, &id) == NULL) {
        TAOS_CHECK_GOTO(terrno, &line, _exit);
      }
    }
  }
  if (pInfo->pCur) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }

  code = vnodeEstimateRawDataSizeImpl(pOperator, pIdList, pStaticInfo);

_exit:
  if (pInfo->pCur) {
    pAPI->metaFn.closeTableMetaCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
    pTaskInfo->code = code;
  }

  taosArrayDestroy(pIdList);
  return code;
}
