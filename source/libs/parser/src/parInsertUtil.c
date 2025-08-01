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

#include "parInsertUtil.h"

#include "catalog.h"
#include "parInt.h"
#include "parUtil.h"
#include "querynodes.h"
#include "tRealloc.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tdataformat.h"
#include "tmisce.h"
#include "ttypes.h"

void qDestroyBoundColInfo(void* pInfo) {
  if (NULL == pInfo) {
    return;
  }

  SBoundColInfo* pBoundInfo = (SBoundColInfo*)pInfo;

  taosMemoryFreeClear(pBoundInfo->pColIndex);
}

static char* tableNameGetPosition(SToken* pToken, char target) {
  bool inEscape = false;
  bool inQuote = false;
  char quotaStr = 0;

  for (uint32_t i = 0; i < pToken->n; ++i) {
    if (*(pToken->z + i) == target && (!inEscape) && (!inQuote)) {
      return pToken->z + i;
    }

    if (*(pToken->z + i) == TS_ESCAPE_CHAR) {
      if (!inQuote) {
        inEscape = !inEscape;
      }
    }

    if (*(pToken->z + i) == '\'' || *(pToken->z + i) == '"') {
      if (!inEscape) {
        if (!inQuote) {
          quotaStr = *(pToken->z + i);
          inQuote = !inQuote;
        } else if (quotaStr == *(pToken->z + i)) {
          inQuote = !inQuote;
        }
      }
    }
  }

  return NULL;
}

int32_t insCreateSName(SName* pName, SToken* pTableName, int32_t acctId, const char* dbName, SMsgBuf* pMsgBuf) {
  const char* msg1 = "name too long";
  const char* msg2 = "invalid database name";
  const char* msg3 = "db is not specified";
  const char* msg4 = "invalid table name";

  int32_t code = TSDB_CODE_SUCCESS;
  char*   p = tableNameGetPosition(pTableName, TS_PATH_DELIMITER[0]);

  if (p != NULL) {  // db has been specified in sql string so we ignore current db path
    int32_t dbLen = p - pTableName->z;
    if (dbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }
    char name[TSDB_DB_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, dbLen);
    int32_t actualDbLen = strdequote(name);

    code = tNameSetDbName(pName, acctId, name, actualDbLen);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    int32_t tbLen = pTableName->n - dbLen - 1;
    if (tbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg4);
    }

    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, p + 1, tbLen);
    /*tbLen = */ (void)strdequote(tbname);

    code = tNameFromString(pName, tbname, T_NAME_TABLE);
    if (code != 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  } else {  // get current DB name first, and then set it into path
    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, pTableName->z, pTableName->n);
    int32_t tbLen = strdequote(tbname);
    if (tbLen >= TSDB_TABLE_NAME_LEN) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
    if (tbLen == 0) {
      return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, "invalid table name");
    }

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);
    (void)strdequote(name);

    if (dbName == NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }
    if (name[0] == '\0') return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, msg4);

    code = tNameSetDbName(pName, acctId, dbName, strlen(dbName));
    if (code != TSDB_CODE_SUCCESS) {
      code = buildInvalidOperationMsg(pMsgBuf, msg2);
      return code;
    }

    code = tNameFromString(pName, name, T_NAME_TABLE);
    if (code != 0) {
      code = buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  if (NULL != strchr(pName->tname, '.')) {
    code = generateSyntaxErrMsgExt(pMsgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, "The table name cannot contain '.'");
  }

  return code;
}

int16_t insFindCol(SToken* pColname, int16_t start, int16_t end, SSchema* pSchema) {
  while (start < end) {
    if (strlen(pSchema[start].name) == pColname->n && strncmp(pColname->z, pSchema[start].name, pColname->n) == 0) {
      return start;
    }
    ++start;
  }
  return -1;
}

int32_t insBuildCreateTbReq(SVCreateTbReq* pTbReq, const char* tname, STag* pTag, int64_t suid, const char* sname,
                            SArray* tagName, uint8_t tagNum, int32_t ttl) {
  pTbReq->type = TD_CHILD_TABLE;
  pTbReq->ctb.pTag = (uint8_t*)pTag;
  pTbReq->name = taosStrdup(tname);
  if (!pTbReq->name) return terrno;
  pTbReq->ctb.suid = suid;
  pTbReq->ctb.tagNum = tagNum;
  if (sname) {
    pTbReq->ctb.stbName = taosStrdup(sname);
    if (!pTbReq->ctb.stbName) return terrno;
  }
  pTbReq->ctb.tagName = taosArrayDup(tagName, NULL);
  if (!pTbReq->ctb.tagName) return terrno;
  pTbReq->ttl = ttl;
  pTbReq->commentLen = -1;

  return TSDB_CODE_SUCCESS;
}

static void initBoundCols(int32_t ncols, int16_t* pBoundCols) {
  for (int32_t i = 0; i < ncols; ++i) {
    pBoundCols[i] = i;
  }
}

static int32_t initColValues(STableMeta* pTableMeta, SArray* pValues) {
  SSchema* pSchemas = getTableColumnSchema(pTableMeta);
  int32_t  code = 0;
  for (int32_t i = 0; i < pTableMeta->tableInfo.numOfColumns; ++i) {
    SColVal val = COL_VAL_NONE(pSchemas[i].colId, pSchemas[i].type);
    if (NULL == taosArrayPush(pValues, &val)) {
      code = terrno;
      break;
    }
  }
  return code;
}

int32_t insInitColValues(STableMeta* pTableMeta, SArray* aColValues) { return initColValues(pTableMeta, aColValues); }

int32_t insInitBoundColsInfo(int32_t numOfBound, SBoundColInfo* pInfo) {
  pInfo->numOfCols = numOfBound;
  pInfo->numOfBound = numOfBound;
  pInfo->hasBoundCols = false;
  pInfo->mixTagsCols = false;
  pInfo->pColIndex = taosMemoryCalloc(numOfBound, sizeof(int16_t));
  if (NULL == pInfo->pColIndex) {
    return terrno;
  }
  for (int32_t i = 0; i < numOfBound; ++i) {
    pInfo->pColIndex[i] = i;
  }
  return TSDB_CODE_SUCCESS;
}

void insResetBoundColsInfo(SBoundColInfo* pInfo) {
  pInfo->numOfBound = pInfo->numOfCols;
  pInfo->hasBoundCols = false;
  pInfo->mixTagsCols = false;
  for (int32_t i = 0; i < pInfo->numOfCols; ++i) {
    pInfo->pColIndex[i] = i;
  }
}

void insCheckTableDataOrder(STableDataCxt* pTableCxt, SRowKey* rowKey) {
  // once the data block is disordered, we do NOT keep last timestamp any more
  if (!pTableCxt->ordered) {
    return;
  }

  if (tRowKeyCompare(rowKey, &pTableCxt->lastKey) < 0) {
    pTableCxt->ordered = false;
  }

  if (tRowKeyCompare(rowKey, &pTableCxt->lastKey) == 0) {
    pTableCxt->duplicateTs = true;
  }

  // TODO: for variable length data type, we need to copy it out
  pTableCxt->lastKey = *rowKey;
  return;
}

void insDestroyBoundColInfo(SBoundColInfo* pInfo) { taosMemoryFreeClear(pInfo->pColIndex); }

static int32_t createTableDataCxt(STableMeta* pTableMeta, SVCreateTbReq** pCreateTbReq, STableDataCxt** pOutput,
                                  bool colMode, bool ignoreColVals) {
  STableDataCxt* pTableCxt = taosMemoryCalloc(1, sizeof(STableDataCxt));
  if (NULL == pTableCxt) {
    *pOutput = NULL;
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pTableCxt->lastKey = (SRowKey){0};
  pTableCxt->ordered = true;
  pTableCxt->duplicateTs = false;

  pTableCxt->pMeta = tableMetaDup(pTableMeta);
  if (NULL == pTableCxt->pMeta) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS == code) {
    pTableCxt->pSchema =
        tBuildTSchema(getTableColumnSchema(pTableMeta), pTableMeta->tableInfo.numOfColumns, pTableMeta->sversion);
    if (NULL == pTableCxt->pSchema) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  pTableCxt->hasBlob = schemaHasBlob(pTableCxt->pSchema);

  if (TSDB_CODE_SUCCESS == code) {
    code = insInitBoundColsInfo(pTableMeta->tableInfo.numOfColumns, &pTableCxt->boundColsInfo);
  }
  if (TSDB_CODE_SUCCESS == code && !ignoreColVals) {
    pTableCxt->pValues = taosArrayInit(pTableMeta->tableInfo.numOfColumns, sizeof(SColVal));
    if (NULL == pTableCxt->pValues) {
      code = terrno;
    } else {
      code = initColValues(pTableMeta, pTableCxt->pValues);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pTableCxt->pData = taosMemoryCalloc(1, sizeof(SSubmitTbData));
    if (NULL == pTableCxt->pData) {
      code = terrno;
    } else {
      pTableCxt->pData->flags = (pCreateTbReq != NULL && NULL != *pCreateTbReq) ? SUBMIT_REQ_AUTO_CREATE_TABLE : 0;
      pTableCxt->pData->flags |= colMode ? SUBMIT_REQ_COLUMN_DATA_FORMAT : 0;
      pTableCxt->pData->suid = pTableMeta->suid;
      pTableCxt->pData->uid = pTableMeta->uid;
      pTableCxt->pData->sver = pTableMeta->sversion;
      pTableCxt->pData->pCreateTbReq = pCreateTbReq != NULL ? *pCreateTbReq : NULL;
      int8_t flag = pTableCxt->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT;
      if (pCreateTbReq != NULL) *pCreateTbReq = NULL;
      if (pTableCxt->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
        pTableCxt->pData->aCol = taosArrayInit(128, sizeof(SColData));
        if (NULL == pTableCxt->pData->aCol) {
          code = terrno;
        }
      } else {
        pTableCxt->pData->aRowP = taosArrayInit(128, POINTER_BYTES);
        if (NULL == pTableCxt->pData->aRowP) {
          code = terrno;
        }
      }
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pTableCxt;
    parserDebug("uid:%" PRId64 ", create table data context, code:%d, vgId:%d", pTableMeta->uid, code,
                pTableMeta->vgId);
  } else {
    insDestroyTableDataCxt(pTableCxt);
  }

  return code;
}

static int32_t rebuildTableData(SSubmitTbData* pSrc, SSubmitTbData** pDst, int8_t hasBlob) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SSubmitTbData* pTmp = taosMemoryCalloc(1, sizeof(SSubmitTbData));
  if (NULL == pTmp) {
    code = terrno;
  } else {
    pTmp->flags = pSrc->flags;
    pTmp->suid = pSrc->suid;
    pTmp->uid = pSrc->uid;
    pTmp->sver = pSrc->sver;
    pTmp->pCreateTbReq = NULL;
    if (pTmp->flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
      if (pSrc->pCreateTbReq) {
        code = cloneSVreateTbReq(pSrc->pCreateTbReq, &pTmp->pCreateTbReq);
      } else {
        pTmp->flags &= ~SUBMIT_REQ_AUTO_CREATE_TABLE;
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      if (pTmp->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
        pTmp->aCol = taosArrayInit(128, sizeof(SColData));
        if (NULL == pTmp->aCol) {
          code = terrno;
          taosMemoryFree(pTmp);
        }
      } else {
        pTmp->aRowP = taosArrayInit(128, POINTER_BYTES);
        if (NULL == pTmp->aRowP) {
          code = terrno;
          taosMemoryFree(pTmp);
        }

        if (code != 0) {
          taosArrayDestroy(pTmp->aRowP);
          taosMemoryFree(pTmp);
        }
      }

    } else {
      taosMemoryFree(pTmp);
    }
  }

  taosMemoryFree(pSrc);
  if (TSDB_CODE_SUCCESS == code) {
    *pDst = pTmp;
  }

  return code;
}

static void resetColValues(SArray* pValues) {
  int32_t num = taosArrayGetSize(pValues);
  for (int32_t i = 0; i < num; ++i) {
    SColVal* pVal = taosArrayGet(pValues, i);
    pVal->flag = CV_FLAG_NONE;
  }
}

int32_t insGetTableDataCxt(SHashObj* pHash, void* id, int32_t idLen, STableMeta* pTableMeta,
                           SVCreateTbReq** pCreateTbReq, STableDataCxt** pTableCxt, bool colMode, bool ignoreColVals) {
  STableDataCxt** tmp = (STableDataCxt**)taosHashGet(pHash, id, idLen);
  if (NULL != tmp) {
    *pTableCxt = *tmp;
    if (!ignoreColVals) {
      resetColValues((*pTableCxt)->pValues);
    }
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = createTableDataCxt(pTableMeta, pCreateTbReq, pTableCxt, colMode, ignoreColVals);
  if (TSDB_CODE_SUCCESS == code) {
    void* pData = *pTableCxt;  // deal scan coverity
    code = taosHashPut(pHash, id, idLen, &pData, POINTER_BYTES);
  }

  if (TSDB_CODE_SUCCESS != code) {
    insDestroyTableDataCxt(*pTableCxt);
  }
  return code;
}

static void destroyColVal(void* p) {
  SColVal* pVal = p;
  if (TSDB_DATA_TYPE_NCHAR == pVal->value.type || TSDB_DATA_TYPE_GEOMETRY == pVal->value.type ||
      TSDB_DATA_TYPE_VARBINARY == pVal->value.type || TSDB_DATA_TYPE_DECIMAL == pVal->value.type) {
    taosMemoryFreeClear(pVal->value.pData);
  }
}

void insDestroyTableDataCxt(STableDataCxt* pTableCxt) {
  if (NULL == pTableCxt) {
    return;
  }

  taosMemoryFreeClear(pTableCxt->pMeta);
  tDestroyTSchema(pTableCxt->pSchema);
  insDestroyBoundColInfo(&pTableCxt->boundColsInfo);
  taosArrayDestroyEx(pTableCxt->pValues, destroyColVal);
  if (pTableCxt->pData) {
    tDestroySubmitTbData(pTableCxt->pData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pTableCxt->pData);
  }
  taosMemoryFree(pTableCxt);
}

void insDestroyVgroupDataCxt(SVgroupDataCxt* pVgCxt) {
  if (NULL == pVgCxt) {
    return;
  }

  tDestroySubmitReq(pVgCxt->pData, TSDB_MSG_FLG_ENCODE);
  taosMemoryFree(pVgCxt->pData);

  taosMemoryFree(pVgCxt);
}

void insDestroyVgroupDataCxtList(SArray* pVgCxtList) {
  if (NULL == pVgCxtList) {
    return;
  }

  size_t size = taosArrayGetSize(pVgCxtList);
  for (int32_t i = 0; i < size; i++) {
    void* p = taosArrayGetP(pVgCxtList, i);
    insDestroyVgroupDataCxt(p);
  }

  taosArrayDestroy(pVgCxtList);
}

void insDestroyVgroupDataCxtHashMap(SHashObj* pVgCxtHash) {
  if (NULL == pVgCxtHash) {
    return;
  }

  void** p = taosHashIterate(pVgCxtHash, NULL);
  while (p) {
    insDestroyVgroupDataCxt(*(SVgroupDataCxt**)p);

    p = taosHashIterate(pVgCxtHash, p);
  }

  taosHashCleanup(pVgCxtHash);
}

void insDestroyTableDataCxtHashMap(SHashObj* pTableCxtHash) {
  if (NULL == pTableCxtHash) {
    return;
  }

  void** p = taosHashIterate(pTableCxtHash, NULL);
  while (p) {
    insDestroyTableDataCxt(*(STableDataCxt**)p);

    p = taosHashIterate(pTableCxtHash, p);
  }

  taosHashCleanup(pTableCxtHash);
}

static int32_t fillVgroupDataCxt(STableDataCxt* pTableCxt, SVgroupDataCxt* pVgCxt, bool isRebuild, bool clear) {
  int32_t code = 0;
  if (NULL == pVgCxt->pData->aSubmitTbData) {
    pVgCxt->pData->aSubmitTbData = taosArrayInit(128, sizeof(SSubmitTbData));
    if (pVgCxt->pData->aSubmitTbData == NULL) {
      return terrno;
    }
    if (pTableCxt->hasBlob) {
      pVgCxt->pData->aSubmitBlobData = taosArrayInit(128, sizeof(SBlobSet*));
      if (NULL == pVgCxt->pData->aSubmitBlobData) {
        return terrno;
      }
    }
  }

  // push data to submit, rebuild empty data for next submit
  if (!pTableCxt->hasBlob) pTableCxt->pData->pBlobSet = NULL;

  if (NULL == taosArrayPush(pVgCxt->pData->aSubmitTbData, pTableCxt->pData)) {
    return terrno;
  }

  if (pTableCxt->hasBlob) {
    parserDebug("blob row transfer %p, pData %p, %s", pTableCxt->pData->pBlobSet, pTableCxt->pData, __func__);
    if (NULL == taosArrayPush(pVgCxt->pData->aSubmitBlobData, &pTableCxt->pData->pBlobSet)) {
      return terrno;
    }
    pTableCxt->pData->pBlobSet = NULL;  // reset blob row to NULL, so that it will not be freed in destroy
  }

  if (isRebuild) {
    code = rebuildTableData(pTableCxt->pData, &pTableCxt->pData, pTableCxt->hasBlob);
  } else if (clear) {
    taosMemoryFreeClear(pTableCxt->pData);
  }
  parserDebug("uid:%" PRId64 ", add table data context to vgId:%d", pTableCxt->pMeta->uid, pVgCxt->vgId);

  return code;
}

static int32_t createVgroupDataCxt(int32_t vgId, SHashObj* pVgroupHash, SArray* pVgroupList, SVgroupDataCxt** pOutput) {
  SVgroupDataCxt* pVgCxt = taosMemoryCalloc(1, sizeof(SVgroupDataCxt));
  if (NULL == pVgCxt) {
    return terrno;
  }
  pVgCxt->pData = taosMemoryCalloc(1, sizeof(SSubmitReq2));
  if (NULL == pVgCxt->pData) {
    insDestroyVgroupDataCxt(pVgCxt);
    return terrno;
  }

  pVgCxt->vgId = vgId;
  int32_t code = taosHashPut(pVgroupHash, &pVgCxt->vgId, sizeof(pVgCxt->vgId), &pVgCxt, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS == code) {
    if (NULL == taosArrayPush(pVgroupList, &pVgCxt)) {
      code = terrno;
      insDestroyVgroupDataCxt(pVgCxt);
      return code;
    }
    //    uDebug("td23101 2vgId:%d, uid:%" PRIu64, pVgCxt->vgId, pTableCxt->pMeta->uid);
    *pOutput = pVgCxt;
  } else {
    insDestroyVgroupDataCxt(pVgCxt);
  }
  return code;
}

int insColDataComp(const void* lp, const void* rp) {
  SColData* pLeft = (SColData*)lp;
  SColData* pRight = (SColData*)rp;
  if (pLeft->cid < pRight->cid) {
    return -1;
  } else if (pLeft->cid > pRight->cid) {
    return 1;
  }

  return 0;
}

int32_t insTryAddTableVgroupInfo(SHashObj* pAllVgHash, SStbInterlaceInfo* pBuildInfo, int32_t* vgId,
                                 STableColsData* pTbData, SName* sname) {
  if (*vgId >= 0 && taosHashGet(pAllVgHash, (const char*)vgId, sizeof(*vgId))) {
    return TSDB_CODE_SUCCESS;
  }

  SVgroupInfo      vgInfo = {0};
  SRequestConnInfo conn = {.pTrans = pBuildInfo->transport,
                           .requestId = pBuildInfo->requestId,
                           .requestObjRefId = pBuildInfo->requestSelf,
                           .mgmtEps = pBuildInfo->mgmtEpSet};

  int32_t code = catalogGetTableHashVgroup((SCatalog*)pBuildInfo->pCatalog, &conn, sname, &vgInfo);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = taosHashPut(pAllVgHash, (const char*)&vgInfo.vgId, sizeof(vgInfo.vgId), (char*)&vgInfo, sizeof(vgInfo));
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t insGetStmtTableVgUid(SHashObj* pAllVgHash, SStbInterlaceInfo* pBuildInfo, STableColsData* pTbData,
                             uint64_t* uid, int32_t* vgId) {
  STableVgUid* pTbInfo = NULL;
  int32_t      code = 0;

  if (pTbData->getFromHash) {
    pTbInfo = (STableVgUid*)tSimpleHashGet(pBuildInfo->pTableHash, pTbData->tbName, strlen(pTbData->tbName));
  }

  if (NULL == pTbInfo) {
    SName sname;
    code = qCreateSName(&sname, pTbData->tbName, pBuildInfo->acctId, pBuildInfo->dbname, NULL, 0);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }

    STableMeta*      pTableMeta = NULL;
    SRequestConnInfo conn = {.pTrans = pBuildInfo->transport,
                             .requestId = pBuildInfo->requestId,
                             .requestObjRefId = pBuildInfo->requestSelf,
                             .mgmtEps = pBuildInfo->mgmtEpSet};
    code = catalogGetTableMeta((SCatalog*)pBuildInfo->pCatalog, &conn, &sname, &pTableMeta);

    if (TSDB_CODE_PAR_TABLE_NOT_EXIST == code) {
      parserWarn("stmt2 async bind don't find table:%s.%s, try auto create table", sname.dbname, sname.tname);
      return code;
    }

    if (TSDB_CODE_SUCCESS != code) {
      parserError("stmt2 async get table meta:%s.%s failed, code:%d", sname.dbname, sname.tname, code);
      return code;
    }

    *uid = pTableMeta->uid;
    *vgId = pTableMeta->vgId;

    STableVgUid tbInfo = {.uid = *uid, .vgid = *vgId};
    code = tSimpleHashPut(pBuildInfo->pTableHash, pTbData->tbName, strlen(pTbData->tbName), &tbInfo, sizeof(tbInfo));
    if (TSDB_CODE_SUCCESS == code) {
      code = insTryAddTableVgroupInfo(pAllVgHash, pBuildInfo, vgId, pTbData, &sname);
    }

    taosMemoryFree(pTableMeta);
  } else {
    *uid = pTbInfo->uid;
    *vgId = pTbInfo->vgid;
  }

  return code;
}

int32_t qBuildStmtFinOutput1(SQuery* pQuery, SHashObj* pAllVgHash, SArray* pVgDataBlocks) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pQuery->pRoot;

  if (TSDB_CODE_SUCCESS == code) {
    code = insBuildVgDataBlocks(pAllVgHash, pVgDataBlocks, &pStmt->pDataBlocks, true);
  }

  return code;
}

int32_t checkAndMergeSVgroupDataCxtByTbname(STableDataCxt* pTbCtx, SVgroupDataCxt* pVgCxt, SSHashObj* pTableNameHash,
                                            char* tbname) {
  if (NULL == pVgCxt->pData->aSubmitTbData) {
    pVgCxt->pData->aSubmitTbData = taosArrayInit(128, sizeof(SSubmitTbData));
    if (NULL == pVgCxt->pData->aSubmitTbData) {
      return terrno;
    }
    if (pTbCtx->hasBlob) {
      pVgCxt->pData->aSubmitBlobData = taosArrayInit(128, sizeof(SBlobSet*));
      if (pVgCxt->pData->aSubmitBlobData == NULL) {
        return terrno;
      }
    }
  }

  int32_t        code = TSDB_CODE_SUCCESS;
  SArray**       rowP = NULL;

  rowP = (SArray**)tSimpleHashGet(pTableNameHash, tbname, strlen(tbname));

  if (rowP != NULL && *rowP != NULL) {
    for (int32_t j = 0; j < taosArrayGetSize(*rowP); ++j) {
      SRow* pRow = (SRow*)taosArrayGetP(pTbCtx->pData->aRowP, j);
      if (pRow) {
        if (NULL == taosArrayPush(*rowP, &pRow)) {
          return terrno;
        }
      }

      if (pTbCtx->hasBlob == 0) {
        code = tRowSort(*rowP);
        TAOS_CHECK_RETURN(code);

        code = tRowMerge(*rowP, pTbCtx->pSchema, 0);
        TAOS_CHECK_RETURN(code);
      } else {
        code = tRowSortWithBlob(pTbCtx->pData->aRowP, pTbCtx->pSchema, pTbCtx->pData->pBlobSet);
        TAOS_CHECK_RETURN(code);

        code = tRowMergeWithBlob(pTbCtx->pData->aRowP, pTbCtx->pSchema, pTbCtx->pData->pBlobSet, 0);
        TAOS_CHECK_RETURN(code);
      }
    }

    parserDebug("merge same uid data: %" PRId64 ", vgId:%d", pTbCtx->pData->uid, pVgCxt->vgId);

    if (pTbCtx->pData->pCreateTbReq != NULL) {
      tdDestroySVCreateTbReq(pTbCtx->pData->pCreateTbReq);
      taosMemoryFree(pTbCtx->pData->pCreateTbReq);
      pTbCtx->pData->pCreateTbReq = NULL;
    }
    return TSDB_CODE_SUCCESS;
  }

  if (pTbCtx->hasBlob == 0) {
    pTbCtx->pData->pBlobSet = NULL;  // if no blob, set it to NULL
  }

  if (NULL == taosArrayPush(pVgCxt->pData->aSubmitTbData, pTbCtx->pData)) {
    return terrno;
  }

  if (pTbCtx->hasBlob) {
    parserDebug("blob row transfer %p, pData %p, %s", pTbCtx->pData->pBlobSet, pTbCtx->pData, __func__);
    if (NULL == taosArrayPush(pVgCxt->pData->aSubmitBlobData, &pTbCtx->pData->pBlobSet)) {
      return terrno;
    }
    pTbCtx->pData->pBlobSet = NULL;  // reset blob row to NULL, so that it will not be freed in destroy
  }

  code = tSimpleHashPut(pTableNameHash, tbname, strlen(tbname), &pTbCtx->pData->aRowP, sizeof(SArray*));

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  parserDebug("uid:%" PRId64 ", add table data context to vgId:%d", pTbCtx->pMeta->uid, pVgCxt->vgId);

  return TSDB_CODE_SUCCESS;
}

int32_t insAppendStmtTableDataCxt(SHashObj* pAllVgHash, STableColsData* pTbData, STableDataCxt* pTbCtx,
                                  SStbInterlaceInfo* pBuildInfo) {
  int32_t  code = TSDB_CODE_SUCCESS;
  uint64_t uid;
  int32_t  vgId;

  pTbCtx->pData->aRowP = pTbData->aCol;

  code = insGetStmtTableVgUid(pAllVgHash, pBuildInfo, pTbData, &uid, &vgId);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  pTbCtx->pMeta->vgId = vgId;
  pTbCtx->pMeta->uid = uid;
  pTbCtx->pData->uid = uid;

  if (!pTbCtx->ordered) {
    code = tRowSort(pTbCtx->pData->aRowP);
  }
  if (code == TSDB_CODE_SUCCESS && (!pTbCtx->ordered || pTbCtx->duplicateTs)) {
    code = tRowMerge(pTbCtx->pData->aRowP, pTbCtx->pSchema, 0);
  }

  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SVgroupDataCxt* pVgCxt = NULL;
  void**          pp = taosHashGet(pBuildInfo->pVgroupHash, &vgId, sizeof(vgId));
  if (NULL == pp) {
    pp = taosHashGet(pBuildInfo->pVgroupHash, &vgId, sizeof(vgId));
    if (NULL == pp) {
      code = createVgroupDataCxt(vgId, pBuildInfo->pVgroupHash, pBuildInfo->pVgroupList, &pVgCxt);
    } else {
      pVgCxt = *(SVgroupDataCxt**)pp;
    }
  } else {
    pVgCxt = *(SVgroupDataCxt**)pp;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = fillVgroupDataCxt(pTbCtx, pVgCxt, false, false);
  }

  if (taosArrayGetSize(pVgCxt->pData->aSubmitTbData) >= 20000) {
    code = qBuildStmtFinOutput1((SQuery*)pBuildInfo->pQuery, pAllVgHash, pBuildInfo->pVgroupList);
    // taosArrayClear(pVgCxt->pData->aSubmitTbData);
    tDestroySubmitReq(pVgCxt->pData, TSDB_MSG_FLG_ENCODE);
    // insDestroyVgroupDataCxt(pVgCxt);
  }

  return code;
}

int32_t insAppendStmt2TableDataCxt(SHashObj* pAllVgHash, STableColsData* pTbData, STableDataCxt* pTbCtx,
                                   SStbInterlaceInfo* pBuildInfo, SVCreateTbReq* ctbReq) {
  int32_t  code = TSDB_CODE_SUCCESS;
  uint64_t uid;
  int32_t  vgId;

  pTbCtx->pData->aRowP = pTbData->aCol;
  pTbCtx->pData->pBlobSet = pTbData->pBlobSet;

  code = insGetStmtTableVgUid(pAllVgHash, pBuildInfo, pTbData, &uid, &vgId);
  if (ctbReq != NULL && code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
    pTbCtx->pData->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
    vgId = (int32_t)ctbReq->uid;
    uid = 0;
    pTbCtx->pMeta->vgId = (int32_t)ctbReq->uid;
    ctbReq->uid = 0;
    pTbCtx->pMeta->uid = 0;
    pTbCtx->pData->uid = 0;
    pTbCtx->pData->pCreateTbReq = ctbReq;
    code = TSDB_CODE_SUCCESS;
  } else {
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    pTbCtx->pMeta->vgId = vgId;
    pTbCtx->pMeta->uid = uid;
    pTbCtx->pData->uid = uid;
    pTbCtx->pData->pCreateTbReq = NULL;

    if (ctbReq != NULL) {
      tdDestroySVCreateTbReq(ctbReq);
      taosMemoryFree(ctbReq);
      ctbReq = NULL;
    }
  }

  if (pTbCtx->hasBlob == 0) {
    if (!pTbData->isOrdered) {
      code = tRowSort(pTbCtx->pData->aRowP);
    }
    if (code == TSDB_CODE_SUCCESS && (!pTbData->isOrdered || pTbData->isDuplicateTs)) {
      code = tRowMerge(pTbCtx->pData->aRowP, pTbCtx->pSchema, PREFER_NON_NULL);
    }
  } else {
    if (!pTbData->isOrdered) {
      code = tRowSortWithBlob(pTbCtx->pData->aRowP, pTbCtx->pSchema, pTbCtx->pData->pBlobSet);
    }
    if (code == TSDB_CODE_SUCCESS && (!pTbData->isOrdered || pTbData->isDuplicateTs)) {
      code = tRowMergeWithBlob(pTbCtx->pData->aRowP, pTbCtx->pSchema, pTbCtx->pData->pBlobSet, 0);
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  SVgroupDataCxt* pVgCxt = NULL;
  void**          pp = taosHashGet(pBuildInfo->pVgroupHash, &vgId, sizeof(vgId));
  if (NULL == pp) {
    pp = taosHashGet(pBuildInfo->pVgroupHash, &vgId, sizeof(vgId));
    if (NULL == pp) {
      code = createVgroupDataCxt(vgId, pBuildInfo->pVgroupHash, pBuildInfo->pVgroupList, &pVgCxt);
    } else {
      pVgCxt = *(SVgroupDataCxt**)pp;
    }
  } else {
    pVgCxt = *(SVgroupDataCxt**)pp;
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = checkAndMergeSVgroupDataCxtByTbname(pTbCtx, pVgCxt, pBuildInfo->pTableRowDataHash, pTbData->tbName);
  }

  if (taosArrayGetSize(pVgCxt->pData->aSubmitTbData) >= 20000) {
    code = qBuildStmtFinOutput1((SQuery*)pBuildInfo->pQuery, pAllVgHash, pBuildInfo->pVgroupList);
    // taosArrayClear(pVgCxt->pData->aSubmitTbData);
    tDestroySubmitReq(pVgCxt->pData, TSDB_MSG_FLG_ENCODE);
    // insDestroyVgroupDataCxt(pVgCxt);
  }

  return code;
}

/*
int32_t insMergeStmtTableDataCxt(STableDataCxt* pTableCxt, SArray* pTableList, SArray** pVgDataBlocks, bool isRebuild,
int32_t tbNum) { SHashObj* pVgroupHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);
  SArray*   pVgroupList = taosArrayInit(8, POINTER_BYTES);
  if (NULL == pVgroupHash || NULL == pVgroupList) {
    taosHashCleanup(pVgroupHash);
    taosArrayDestroy(pVgroupList);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  for (int32_t i = 0; i < tbNum; ++i) {
    STableColsData *pTableCols = (STableColsData*)taosArrayGet(pTableList, i);
    pTableCxt->pMeta->vgId = pTableCols->vgId;
    pTableCxt->pMeta->uid = pTableCols->uid;
    pTableCxt->pData->uid = pTableCols->uid;
    pTableCxt->pData->aCol = pTableCols->aCol;

    SColData* pCol = taosArrayGet(pTableCxt->pData->aCol, 0);
    if (pCol->nVal <= 0) {
      continue;
    }

    if (pTableCxt->pData->pCreateTbReq) {
      pTableCxt->pData->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
    }

    taosArraySort(pTableCxt->pData->aCol, insColDataComp);

    tColDataSortMerge(pTableCxt->pData->aCol);

    if (TSDB_CODE_SUCCESS == code) {
      SVgroupDataCxt* pVgCxt = NULL;
      int32_t         vgId = pTableCxt->pMeta->vgId;
      void**          pp = taosHashGet(pVgroupHash, &vgId, sizeof(vgId));
      if (NULL == pp) {
        code = createVgroupDataCxt(pTableCxt, pVgroupHash, pVgroupList, &pVgCxt);
      } else {
        pVgCxt = *(SVgroupDataCxt**)pp;
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = fillVgroupDataCxt(pTableCxt, pVgCxt, false, false);
      }
    }
  }

  taosHashCleanup(pVgroupHash);
  if (TSDB_CODE_SUCCESS == code) {
    *pVgDataBlocks = pVgroupList;
  } else {
    insDestroyVgroupDataCxtList(pVgroupList);
  }

  return code;
}
*/

static int8_t colDataHasBlob(SColData* pCol) {
  if (IS_STR_DATA_BLOB(pCol->type)) {
    return 1;
  }
  return 0;
}
int32_t insMergeTableDataCxt(SHashObj* pTableHash, SArray** pVgDataBlocks, bool isRebuild) {
  SHashObj* pVgroupHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  SArray*   pVgroupList = taosArrayInit(8, POINTER_BYTES);
  if (NULL == pVgroupHash || NULL == pVgroupList) {
    taosHashCleanup(pVgroupHash);
    taosArrayDestroy(pVgroupList);
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  bool    colFormat = false;

  void* p = taosHashIterate(pTableHash, NULL);
  if (p) {
    STableDataCxt* pTableCxt = *(STableDataCxt**)p;
    colFormat = (0 != (pTableCxt->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT));
  }

  while (TSDB_CODE_SUCCESS == code && NULL != p) {
    STableDataCxt* pTableCxt = *(STableDataCxt**)p;
    if (colFormat) {
      SColData* pCol = taosArrayGet(pTableCxt->pData->aCol, 0);
      if (pCol && pCol->nVal <= 0) {
        p = taosHashIterate(pTableHash, p);
        continue;
      }

      if (pTableCxt->pData->pCreateTbReq) {
        pTableCxt->pData->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
      }
      int8_t isBlob = IS_STR_DATA_BLOB(pCol->type) ? 1 : 0;
      if (isBlob == 0) {
        taosArraySort(pTableCxt->pData->aCol, insColDataComp);
        code = tColDataSortMerge(&pTableCxt->pData->aCol);
      } else {
        taosArraySort(pTableCxt->pData->aCol, insColDataComp);
        code = tColDataSortMergeWithBlob(&pTableCxt->pData->aCol, pTableCxt->pData->pBlobSet);
      }
    } else {
      // skip the table has no data to insert
      // eg: import a csv without valid data
      // if (0 == taosArrayGetSize(pTableCxt->pData->aRowP)) {
      //   parserWarn("no row in tableDataCxt uid:%" PRId64 " ", pTableCxt->pMeta->uid);
      //   p = taosHashIterate(pTableHash, p);
      //   continue;
      // }
      if (pTableCxt->hasBlob == 0) {
        if (!pTableCxt->ordered) {
          code = tRowSort(pTableCxt->pData->aRowP);
        }
        if (code == TSDB_CODE_SUCCESS && (!pTableCxt->ordered || pTableCxt->duplicateTs)) {
        code = tRowMerge(pTableCxt->pData->aRowP, pTableCxt->pSchema, PREFER_NON_NULL);
        }
      } else {
        if (!pTableCxt->ordered) {
        code = tRowSortWithBlob(pTableCxt->pData->aRowP, pTableCxt->pSchema, pTableCxt->pData->pBlobSet);
        }
        if (code == TSDB_CODE_SUCCESS && (!pTableCxt->ordered || pTableCxt->duplicateTs)) {
          code = tRowMergeWithBlob(pTableCxt->pData->aRowP, pTableCxt->pSchema, pTableCxt->pData->pBlobSet, 0);
        }
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      SVgroupDataCxt* pVgCxt = NULL;
      int32_t         vgId = pTableCxt->pMeta->vgId;
      void**          pp = taosHashGet(pVgroupHash, &vgId, sizeof(vgId));
      if (NULL == pp) {
        code = createVgroupDataCxt(vgId, pVgroupHash, pVgroupList, &pVgCxt);
      } else {
        pVgCxt = *(SVgroupDataCxt**)pp;
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = fillVgroupDataCxt(pTableCxt, pVgCxt, isRebuild, true);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      p = taosHashIterate(pTableHash, p);
    }
  }

  taosHashCleanup(pVgroupHash);
  if (TSDB_CODE_SUCCESS == code) {
    *pVgDataBlocks = pVgroupList;
  } else {
    insDestroyVgroupDataCxtList(pVgroupList);
  }

  return code;
}

static int32_t buildSubmitReq(int32_t vgId, SSubmitReq2* pReq, void** pData, uint32_t* pLen) {
  int32_t  code = TSDB_CODE_SUCCESS;
  uint32_t len = 0;
  void*    pBuf = NULL;
  tEncodeSize(tEncodeSubmitReq, pReq, len, code);
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    len += sizeof(SSubmitReq2Msg);
    pBuf = taosMemoryMalloc(len);
    if (NULL == pBuf) {
      return terrno;
    }
    ((SSubmitReq2Msg*)pBuf)->header.vgId = htonl(vgId);
    ((SSubmitReq2Msg*)pBuf)->header.contLen = htonl(len);
    ((SSubmitReq2Msg*)pBuf)->version = htobe64(1);
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
    code = tEncodeSubmitReq(&encoder, pReq);
    tEncoderClear(&encoder);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pData = pBuf;
    *pLen = len;
  } else {
    taosMemoryFree(pBuf);
  }
  return code;
}

static void destroyVgDataBlocks(void* p) {
  if (p == NULL) return;
  SVgDataBlocks* pVg = p;
  taosMemoryFree(pVg->pData);
  taosMemoryFree(pVg);
}


int32_t insResetBlob(SSubmitReq2* p) {
  int32_t code = 0;
  if (p->raw) {
    return TSDB_CODE_SUCCESS;  // no blob data in raw mode
  }

  if (p->aSubmitBlobData != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(p->aSubmitTbData); i++) {
      SSubmitTbData* pSubmitTbData = taosArrayGet(p->aSubmitTbData, i);
      SBlobSet**     ppBlob = taosArrayGet(p->aSubmitBlobData, i);
      SBlobSet*      pBlob = *ppBlob;
      int32_t        nrow = taosArrayGetSize(pSubmitTbData->aRowP);
      int32_t        nblob = 0;
      if (nrow > 0) {
        nblob = taosArrayGetSize(pBlob->pSeqTable);
      }
      uTrace("blob %p row size %d, pData size %d", pBlob, nblob, nrow);
      pSubmitTbData->pBlobSet = pBlob;
      *ppBlob = NULL;  // reset blob row to NULL, so that it will not be freed in destroy
    }
  } else {
    for (int32_t i = 0; i < taosArrayGetSize(p->aSubmitTbData); i++) {
      SSubmitTbData* pSubmitTbData = taosArrayGet(p->aSubmitTbData, i);
      pSubmitTbData->pBlobSet = NULL;  // reset blob row to NULL, so that it will not be freed in destroy
    }
  }

  return code;
}
int32_t insBuildVgDataBlocks(SHashObj* pVgroupsHashObj, SArray* pVgDataCxtList, SArray** pVgDataBlocks, bool append) {
  size_t  numOfVg = taosArrayGetSize(pVgDataCxtList);
  SArray* pDataBlocks = (append && *pVgDataBlocks) ? *pVgDataBlocks : taosArrayInit(numOfVg, POINTER_BYTES);
  if (NULL == pDataBlocks) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < numOfVg; ++i) {
    SVgroupDataCxt* src = taosArrayGetP(pVgDataCxtList, i);
    if (taosArrayGetSize(src->pData->aSubmitTbData) <= 0) {
      continue;
    }
    SVgDataBlocks* dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      code = terrno;
    }

    if (TSDB_CODE_SUCCESS == code) {
      dst->numOfTables = taosArrayGetSize(src->pData->aSubmitTbData);
      code = taosHashGetDup(pVgroupsHashObj, (const char*)&src->vgId, sizeof(src->vgId), &dst->vg);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = insResetBlob(src->pData);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = buildSubmitReq(src->vgId, src->pData, &dst->pData, &dst->size);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = (NULL == taosArrayPush(pDataBlocks, &dst) ? terrno : TSDB_CODE_SUCCESS);
    }
    if (TSDB_CODE_SUCCESS != code) {
      destroyVgDataBlocks(dst);
    }
  }

  if (append) {
    if (NULL == *pVgDataBlocks) {
      *pVgDataBlocks = pDataBlocks;
    }
    return code;
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pVgDataBlocks = pDataBlocks;
  } else {
    taosArrayDestroyP(pDataBlocks, destroyVgDataBlocks);
  }

  return code;
}

static bool findFileds(SSchema* pSchema, TAOS_FIELD* fields, int numFields) {
  for (int i = 0; i < numFields; i++) {
    if (strcmp(pSchema->name, fields[i].name) == 0) {
      return true;
    }
  }

  return false;
}

int32_t checkSchema(SSchema* pColSchema, SSchemaExt* pColExtSchema, int8_t* fields, char* errstr, int32_t errstrLen) {
  if (*fields != pColSchema->type) {
    if (errstr != NULL)
      snprintf(errstr, errstrLen, "column type not equal, name:%s, schema type:%s, data type:%s", pColSchema->name,
               tDataTypes[pColSchema->type].name, tDataTypes[*fields].name);
    return TSDB_CODE_INVALID_PARA;
  }

  if (IS_DECIMAL_TYPE(pColSchema->type)) {
    uint8_t precision = 0, scale = 0;
    decimalFromTypeMod(pColExtSchema->typeMod, &precision, &scale);
    uint8_t precisionData = 0, scaleData = 0;
    int32_t bytes = *(int32_t*)(fields + sizeof(int8_t));
    extractDecimalTypeInfoFromBytes(&bytes, &precisionData, &scaleData);
    if (precision != precisionData || scale != scaleData) {
      if (errstr != NULL)
        snprintf(errstr, errstrLen,
                 "column decimal type not equal, name:%s, schema type:%s, precision:%d, scale:%d, data type:%s, "
                 "precision:%d, scale:%d",
                 pColSchema->name, tDataTypes[pColSchema->type].name, precision, scale, tDataTypes[*fields].name,
                 precisionData, scaleData);
      return TSDB_CODE_INVALID_PARA;
    }
    return 0;
  }

  if (IS_VAR_DATA_TYPE(pColSchema->type) && *(int32_t*)(fields + sizeof(int8_t)) > pColSchema->bytes) {
    if (errstr != NULL)
      snprintf(errstr, errstrLen,
               "column var data bytes error, name:%s, schema type:%s, bytes:%d, data type:%s, bytes:%d",
               pColSchema->name, tDataTypes[pColSchema->type].name, pColSchema->bytes, tDataTypes[*fields].name,
               *(int32_t*)(fields + sizeof(int8_t)));
    return TSDB_CODE_INVALID_PARA;
  }

  if (!IS_VAR_DATA_TYPE(pColSchema->type) && *(int32_t*)(fields + sizeof(int8_t)) != pColSchema->bytes) {
    if (errstr != NULL)
      snprintf(errstr, errstrLen,
               "column normal data bytes not equal, name:%s, schema type:%s, bytes:%d, data type:%s, bytes:%d",
               pColSchema->name, tDataTypes[pColSchema->type].name, pColSchema->bytes, tDataTypes[*fields].name,
               *(int32_t*)(fields + sizeof(int8_t)));
    return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}

#define PRCESS_DATA(i, j)                                                                                 \
  ret = checkSchema(pColSchema, pColExtSchema, fields, errstr, errstrLen);                                \
  if (ret != 0) {                                                                                         \
    goto end;                                                                                             \
  }                                                                                                       \
                                                                                                          \
  if (pColSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {                                                 \
    hasTs = true;                                                                                         \
  }                                                                                                       \
                                                                                                          \
  int8_t* offset = pStart;                                                                                \
  if (IS_VAR_DATA_TYPE(pColSchema->type)) {                                                               \
    pStart += numOfRows * sizeof(int32_t);                                                                \
  } else {                                                                                                \
    pStart += BitmapLen(numOfRows);                                                                       \
  }                                                                                                       \
  char* pData = pStart;                                                                                   \
                                                                                                          \
  SColData* pCol = taosArrayGet(pTableCxt->pData->aCol, j);                                               \
  ret = tColDataAddValueByDataBlock(pCol, pColSchema->type, pColSchema->bytes, numOfRows, offset, pData); \
  if (ret != 0) {                                                                                         \
    goto end;                                                                                             \
  }                                                                                                       \
  fields += sizeof(int8_t) + sizeof(int32_t);                                                             \
  if (needChangeLength && version == BLOCK_VERSION_1) {                                                   \
    pStart += htonl(colLength[i]);                                                                        \
  } else {                                                                                                \
    pStart += colLength[i];                                                                               \
  }                                                                                                       \
  boundInfo->pColIndex[j] = -1;

int rawBlockBindData(SQuery* query, STableMeta* pTableMeta, void* data, SVCreateTbReq* pCreateTb, void* tFields,
                     int numFields, bool needChangeLength, char* errstr, int32_t errstrLen, bool raw) {
  int ret = 0;
  if (data == NULL) {
    uError("rawBlockBindData, data is NULL");
    return TSDB_CODE_APP_ERROR;
  }
  void* tmp =
      taosHashGet(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj, &pTableMeta->uid, sizeof(pTableMeta->uid));
  SVCreateTbReq* pCreateReqTmp = NULL;
  if (tmp == NULL && pCreateTb != NULL) {
    ret = cloneSVreateTbReq(pCreateTb, &pCreateReqTmp);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("cloneSVreateTbReq error");
      goto end;
    }
  }

  STableDataCxt* pTableCxt = NULL;
  ret = insGetTableDataCxt(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj, &pTableMeta->uid,
                           sizeof(pTableMeta->uid), pTableMeta, &pCreateReqTmp, &pTableCxt, true, false);
  if (pCreateReqTmp != NULL) {
    tdDestroySVCreateTbReq(pCreateReqTmp);
    taosMemoryFree(pCreateReqTmp);
  }

  if (ret != TSDB_CODE_SUCCESS) {
    uError("insGetTableDataCxt error");
    goto end;
  }

  pTableCxt->pData->flags |= TD_REQ_FROM_TAOX;
  if (tmp == NULL) {
    ret = initTableColSubmitData(pTableCxt);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("initTableColSubmitData error");
      goto end;
    }
  }

  char* p = (char*)data;
  // | version | total length | total rows | blankFill | total columns | flag seg| block group id | column schema | each
  // column length |
  int32_t version = *(int32_t*)data;
  p += sizeof(int32_t);
  p += sizeof(int32_t);

  int32_t numOfRows = *(int32_t*)p;
  p += sizeof(int32_t);

  int32_t numOfCols = *(int32_t*)p;
  p += sizeof(int32_t);

  p += sizeof(int32_t);
  p += sizeof(uint64_t);

  int8_t* fields = p;
  if (*fields >= TSDB_DATA_TYPE_MAX || *fields < 0) {
    uError("fields type error:%d", *fields);
    ret = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  p += numOfCols * (sizeof(int8_t) + sizeof(int32_t));

  int32_t* colLength = (int32_t*)p;
  p += sizeof(int32_t) * numOfCols;

  char* pStart = p;

  SSchema*       pSchema = getTableColumnSchema(pTableMeta);
  SSchemaExt*    pExtSchemas = getTableColumnExtSchema(pTableMeta);
  SBoundColInfo* boundInfo = &pTableCxt->boundColsInfo;

  if (tFields != NULL && numFields != numOfCols) {
    if (errstr != NULL) snprintf(errstr, errstrLen, "numFields:%d not equal to data cols:%d", numFields, numOfCols);
    ret = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  bool hasTs = false;
  if (tFields == NULL) {
    int32_t len = TMIN(numOfCols, boundInfo->numOfBound);
    for (int j = 0; j < len; j++) {
      SSchema*    pColSchema = &pSchema[j];
      SSchemaExt* pColExtSchema = &pExtSchemas[j];
      PRCESS_DATA(j, j)
    }
  } else {
    for (int i = 0; i < numFields; i++) {
      for (int j = 0; j < boundInfo->numOfBound; j++) {
        SSchema*    pColSchema = &pSchema[j];
        SSchemaExt* pColExtSchema = &pExtSchemas[j];
        char*       fieldName = NULL;
        if (raw) {
          fieldName = ((SSchemaWrapper*)tFields)->pSchema[i].name;
        } else {
          fieldName = ((TAOS_FIELD*)tFields)[i].name;
        }
        if (strcmp(pColSchema->name, fieldName) == 0) {
          PRCESS_DATA(i, j)
          break;
        }
      }
    }
  }

  if (!hasTs) {
    if (errstr != NULL) snprintf(errstr, errstrLen, "timestamp column(primary key) not found in raw data");
    ret = TSDB_CODE_INVALID_PARA;
    goto end;
  }

  // process NULL data
  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    if (boundInfo->pColIndex[c] != -1) {
      SColData* pCol = taosArrayGet(pTableCxt->pData->aCol, c);
      ret = tColDataAddValueByDataBlock(pCol, 0, 0, numOfRows, NULL, NULL);
      if (ret != 0) {
        goto end;
      }
    } else {
      boundInfo->pColIndex[c] = c;  // restore for next block
    }
  }

end:
  return ret;
}

int rawBlockBindRawData(SHashObj* pVgroupHash, SArray* pVgroupList, STableMeta* pTableMeta, void* data) {
  int code = transformRawSSubmitTbData(data, pTableMeta->suid, pTableMeta->uid, pTableMeta->sversion);
  if (code != 0) {
    return code;
  }
  SVgroupDataCxt* pVgCxt = NULL;
  void**          pp = taosHashGet(pVgroupHash, &pTableMeta->vgId, sizeof(pTableMeta->vgId));
  if (NULL == pp) {
    code = createVgroupDataCxt(pTableMeta->vgId, pVgroupHash, pVgroupList, &pVgCxt);
    if (code != 0) {
      return code;
    }
  } else {
    pVgCxt = *(SVgroupDataCxt**)pp;
  }
  if (NULL == pVgCxt->pData->aSubmitTbData) {
    pVgCxt->pData->aSubmitTbData = taosArrayInit(0, POINTER_BYTES);
    pVgCxt->pData->raw = true;
    if (NULL == pVgCxt->pData->aSubmitTbData) {
      return terrno;
    }
  }

  // push data to submit, rebuild empty data for next submit
  if (NULL == taosArrayPush(pVgCxt->pData->aSubmitTbData, &data)) {
    return terrno;
  }

  uTrace("add raw data to vgId:%d, len:%d", pTableMeta->vgId, *(int32_t*)data);

  return 0;
}

