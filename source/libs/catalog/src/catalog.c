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

#include "catalogInt.h"
#include "query.h"
#include "systable.h"
#include "tname.h"
#include "tref.h"
#include "trpc.h"

SCatalogMgmt gCtgMgmt = {0};

int32_t ctgGetDBVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SCtgDBCache** dbCache,
                       SDBVgInfo** pInfo, bool* exists) {
  int32_t code = 0;

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, dbCache));

  if (*dbCache) {
    if (exists) {
      *exists = true;
    }
    
    return TSDB_CODE_SUCCESS;
  }

  if (exists) {
    *exists = false;
    return TSDB_CODE_SUCCESS;
  }

  SUseDbOutput     DbOut = {0};
  SBuildUseDBInput input = {0};

  tstrncpy(input.db, dbFName, tListLen(input.db));
  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;

  CTG_ERR_RET(ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, &DbOut, NULL));

  CTG_ERR_JRET(ctgCloneVgInfo(DbOut.dbVgroup, pInfo));

  CTG_ERR_RET(ctgUpdateVgroupEnqueue(pCtg, dbFName, DbOut.dbId, DbOut.dbVgroup, false));

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(*pInfo);
  *pInfo = DbOut.dbVgroup;

  CTG_RET(code);
}

int32_t ctgRefreshDBVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName) {
  int32_t      code = 0;
  SCtgDBCache* dbCache = NULL;

  CTG_ERR_RET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));

  SUseDbOutput     DbOut = {0};
  SBuildUseDBInput input = {0};
  tstrncpy(input.db, dbFName, tListLen(input.db));

  if (NULL != dbCache) {
    input.dbId = dbCache->dbId;

    ctgReleaseVgInfoToCache(pCtg, dbCache);
  }

  input.vgVersion = CTG_DEFAULT_INVALID_VERSION;
  input.numOfTable = 0;

  code = ctgGetDBVgInfoFromMnode(pCtg, pConn, &input, &DbOut, NULL);
  if (code) {
    if (CTG_DB_NOT_EXIST(code) && (NULL != dbCache)) {
      ctgDebug("db:%s, db no longer exist, dbId:0x%" PRIx64, input.db, input.dbId);
      CTG_ERR_RET(ctgDropDbCacheEnqueue(pCtg, input.db, input.dbId));
    }

    CTG_ERR_RET(code);
  }

  CTG_ERR_RET(ctgUpdateVgroupEnqueue(pCtg, dbFName, DbOut.dbId, DbOut.dbVgroup, true));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgRefreshTbMeta(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgTbMetaCtx* ctx, STableMetaOutput** pOutput,
                         bool syncReq) {
  SVgroupInfo vgroupInfo = {0};
  int32_t     code = 0;

  if (!CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    CTG_ERR_RET(ctgGetTbHashVgroup(pCtg, pConn, ctx->pName, &vgroupInfo, NULL));
  }

  STableMetaOutput  moutput = {0};
  STableMetaOutput* output = taosMemoryCalloc(1, sizeof(STableMetaOutput));
  if (NULL == output) {
    ctgError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(terrno);
  }

  if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
    ctgDebug("tb:%s, will refresh tbmeta, supposed in information_schema", tNameGetTableName(ctx->pName));

    CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, (char*)ctx->pName->dbname, (char*)ctx->pName->tname, output, NULL));
  } else if (CTG_FLAG_IS_STB(ctx->flag)) {
    ctgDebug("tb:%s, will refresh tbmeta, supposed to be stb", tNameGetTableName(ctx->pName));

    // if get from mnode failed, will not try vnode
    CTG_ERR_JRET(ctgGetTbMetaFromMnode(pCtg, pConn, ctx->pName, output, NULL));

    if (CTG_IS_META_NULL(output->metaType)) {
      CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgroupInfo, output, NULL));
    }
  } else {
    ctgDebug("tb:%s, will refresh tbmeta, not supposed to be stb, flag:%d", tNameGetTableName(ctx->pName), ctx->flag);

    // if get from vnode failed or no table meta, will not try mnode
    CTG_ERR_JRET(ctgGetTbMetaFromVnode(pCtg, pConn, ctx->pName, &vgroupInfo, output, NULL));

    if (CTG_IS_META_TABLE(output->metaType) && TSDB_SUPER_TABLE == output->tbMeta->tableType) {
      ctgDebug("tb:%s, will continue to refresh tbmeta since got stb", tNameGetTableName(ctx->pName));

      taosMemoryFreeClear(output->tbMeta);

      CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, output->dbFName, output->tbName, output, NULL));
    } else if (CTG_IS_META_BOTH(output->metaType) || CTG_IS_META_VBOTH(output->metaType)) {
      int32_t exist = 0;
      if (!CTG_FLAG_IS_FORCE_UPDATE(ctx->flag)) {
        CTG_ERR_JRET(ctgTbMetaExistInCache(pCtg, output->dbFName, output->tbName, &exist));
      }

      if (0 == exist) {
        CTG_ERR_JRET(ctgGetTbMetaFromMnodeImpl(pCtg, pConn, output->dbFName, output->tbName, &moutput, NULL));

        if (CTG_IS_META_NULL(moutput.metaType)) {
          SET_META_TYPE_NULL(output->metaType);
        }

        taosMemoryFreeClear(output->tbMeta);
        output->tbMeta = moutput.tbMeta;
        moutput.tbMeta = NULL;
      } else {
        taosMemoryFreeClear(output->tbMeta);

        if (CTG_IS_META_BOTH(output->metaType)) {
          SET_META_TYPE_CTABLE(output->metaType);
        } else {
          SET_META_TYPE_VCTABLE(output->metaType);
        }
      }
    }
  }

  if (CTG_IS_META_NULL(output->metaType)) {
    ctgError("tb:%s, no tbmeta got", tNameGetTableName(ctx->pName));
    CTG_ERR_JRET(ctgRemoveTbMetaFromCache(pCtg, ctx->pName, false, false));
    CTG_ERR_JRET(CTG_ERR_CODE_TABLE_NOT_EXIST);
  }

  if (CTG_IS_META_TABLE(output->metaType)) {
    ctgDebug("tb:%s, tbmeta got, db:%s, tbType:%d", output->tbName, output->dbFName, output->tbMeta->tableType);
  } else {
    ctgDebug("tb:%s, tbmeta got, db:%s, tbType:%d, stbMetaGot:%d", output->ctbName, output->dbFName,
             output->ctbMeta.tableType, CTG_IS_META_BOTH(output->metaType));
  }

  if (pOutput) {
    CTG_ERR_JRET(ctgCloneMetaOutput(output, pOutput));
  }

  code = ctgUpdateTbMetaEnqueue(pCtg, output, syncReq);
  output = NULL;
  CTG_ERR_JRET(code);

  return TSDB_CODE_SUCCESS;

_return:

  if (output) {
    taosMemoryFreeClear(output->tbMeta);
    taosMemoryFreeClear(output->vctbMeta);
    taosMemoryFreeClear(output);
  }
  
  CTG_RET(code);
}

int32_t ctgGetTbMeta(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta) {
  int32_t           code = 0;
  int32_t           line = 0;
  STableMetaOutput* output = NULL;

  CTG_ERR_RET(ctgGetTbMetaFromCache(pCtg, ctx, pTableMeta));
  if (*pTableMeta || (ctx->flag & CTG_FLAG_ONLY_CACHE)) {
    goto _return;
  }

  while (true) {
    CTG_ERR_JRET(ctgRefreshTbMeta(pCtg, pConn, ctx, &output, ctx->flag & CTG_FLAG_SYNC_OP));

    if (CTG_IS_META_TABLE(output->metaType)) {
      *pTableMeta = output->tbMeta;
      goto _return;
    }

    if (CTG_IS_META_BOTH(output->metaType)) {
      TAOS_MEMCPY(output->tbMeta, &output->ctbMeta, sizeof(output->ctbMeta));

      *pTableMeta = output->tbMeta;
      goto _return;
    }

    if (CTG_IS_META_VBOTH(output->metaType)) {
      int32_t colRefSize = output->vctbMeta->numOfColRefs * sizeof(SColRef);
      if (output->tbMeta) {
        int32_t metaSize = CTG_META_SIZE(output->tbMeta);
        int32_t schemaExtSize = 0;
        if (withExtSchema(output->tbMeta->tableType) && output->tbMeta->schemaExt) {
          schemaExtSize = output->tbMeta->tableInfo.numOfColumns * sizeof(SSchemaExt);
        }
        TAOS_MEMCPY(output->tbMeta, output->vctbMeta, sizeof(SVCTableMeta));
        output->tbMeta->colRef = (SColRef *)((char *)output->tbMeta + metaSize + schemaExtSize);
        TAOS_MEMCPY(output->tbMeta->colRef, output->vctbMeta->colRef, colRefSize);
      } else {
        ctgError("tb:%s, tbmeta got, but tbMeta is NULL", output->tbName);
        taosMemoryFreeClear(output->vctbMeta);
        CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }
      output->tbMeta->rversion = output->vctbMeta->rversion;
      output->tbMeta->numOfColRefs = output->vctbMeta->numOfColRefs;
      taosMemoryFreeClear(output->vctbMeta);
      *pTableMeta = output->tbMeta;
      goto _return;
    }

    if ((!CTG_IS_META_CTABLE(output->metaType) && !CTG_IS_META_VCTABLE(output->metaType)) || output->tbMeta) {
      ctgError("invalid metaType:%d", output->metaType);
      taosMemoryFreeClear(output->vctbMeta);
      taosMemoryFreeClear(output->tbMeta);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    // HANDLE ONLY (VIRTUAL) CHILD TABLE META

    SName stbName = *ctx->pName;
    TAOS_STRCPY(stbName.tname, output->tbName);
    SCtgTbMetaCtx stbCtx = {0};
    stbCtx.flag = ctx->flag;
    stbCtx.pName = &stbName;

    CTG_ERR_JRET(ctgReadTbMetaFromCache(pCtg, &stbCtx, pTableMeta));
    if (NULL == *pTableMeta) {
      ctgDebug("tb:%s, stb no longer exist, db:%s", ctx->pName->tname, output->dbFName);
      continue;
    }
    if (CTG_IS_META_CTABLE(output->metaType)) {
      TAOS_MEMCPY(*pTableMeta, &output->ctbMeta, sizeof(output->ctbMeta));
    } else if (CTG_IS_META_VCTABLE(output->metaType)) {
      int32_t colRefSize = output->vctbMeta->numOfColRefs * sizeof(SColRef);
      int32_t metaSize = CTG_META_SIZE(*pTableMeta);
      (*pTableMeta) = taosMemoryRealloc(*pTableMeta, metaSize + colRefSize);
      QUERY_CHECK_NULL(*pTableMeta, code , line, _return, terrno);
      TAOS_MEMCPY(*pTableMeta, output->vctbMeta, sizeof(SVCTableMeta));
      (*pTableMeta)->colRef = (SColRef *)((char *)(*pTableMeta) + metaSize);
      TAOS_MEMCPY((*pTableMeta)->colRef, output->vctbMeta->colRef, colRefSize);
      (*pTableMeta)->numOfColRefs = output->vctbMeta->numOfColRefs;
      (*pTableMeta)->rversion = output->vctbMeta->rversion;
    }

    taosMemoryFreeClear(output->tbMeta);
    taosMemoryFreeClear(output->vctbMeta);

    break;
  }

_return:

  if (CTG_TABLE_NOT_EXIST(code) && ctx->tbInfo.inCache) {
    char dbFName[TSDB_DB_FNAME_LEN] = {0};
    if (CTG_FLAG_IS_SYS_DB(ctx->flag)) {
      TAOS_STRCPY(dbFName, ctx->pName->dbname);
    } else {
      (void)tNameGetFullDbName(ctx->pName, dbFName);
    }

    if (TSDB_SUPER_TABLE == ctx->tbInfo.tbType) {
      (void)ctgDropStbMetaEnqueue(pCtg, dbFName, ctx->tbInfo.dbId, ctx->pName->tname, ctx->tbInfo.suid, false); // already in error
    } else {
      (void)ctgDropTbMetaEnqueue(pCtg, dbFName, ctx->tbInfo.dbId, ctx->pName->tname, false); // already in error
    }
  }

  taosMemoryFreeClear(output);

  if (*pTableMeta) {
    ctgDebug("tb:%s, tbmeta returned, tbType:%d", ctx->pName->tname, (*pTableMeta)->tableType);
    ctgdShowTableMeta(pCtg, ctx->pName->tname, *pTableMeta);
  }

  CTG_RET(code);
}

int32_t ctgUpdateTbMeta(SCatalog* pCtg, STableMetaRsp* rspMsg, bool syncOp) {
  STableMetaOutput* output = taosMemoryCalloc(1, sizeof(STableMetaOutput));
  if (NULL == output) {
    ctgError("malloc %d failed", (int32_t)sizeof(STableMetaOutput));
    CTG_ERR_RET(terrno);
  }

  int32_t code = 0;

  TAOS_STRCPY(output->dbFName, rspMsg->dbFName);

  output->dbId = rspMsg->dbId;

  if (TSDB_CHILD_TABLE == rspMsg->tableType && NULL == rspMsg->pSchemas) {
    TAOS_STRCPY(output->ctbName, rspMsg->tbName);

    SET_META_TYPE_CTABLE(output->metaType);

    CTG_ERR_JRET(queryCreateCTableMetaFromMsg(rspMsg, &output->ctbMeta));
  } else if (TSDB_VIRTUAL_CHILD_TABLE == rspMsg->tableType && NULL == rspMsg->pSchemas) {
    TAOS_STRCPY(output->ctbName, rspMsg->tbName);

    SET_META_TYPE_VCTABLE(output->metaType);

    CTG_ERR_JRET(queryCreateVCTableMetaFromMsg(rspMsg, &output->vctbMeta));
  } else {
    TAOS_STRCPY(output->tbName, rspMsg->tbName);

    SET_META_TYPE_TABLE(output->metaType);

    CTG_ERR_JRET(queryCreateTableMetaFromMsg(rspMsg, rspMsg->tableType == TSDB_SUPER_TABLE, &output->tbMeta));
  }

  code = ctgUpdateTbMetaEnqueue(pCtg, output, syncOp);
  output = NULL;
  CTG_ERR_JRET(code);

  return TSDB_CODE_SUCCESS;

_return:

  if (output) {
    taosMemoryFreeClear(output->tbMeta);
    taosMemoryFreeClear(output->vctbMeta);
    taosMemoryFreeClear(output);
  }
  
  CTG_RET(code);
}

int32_t ctgChkAuth(SCatalog* pCtg, SRequestConnInfo* pConn, SUserAuthInfo *pReq, SUserAuthRes* pRes, bool* exists) {
  bool    inCache = false;
  int32_t code = 0;
  SCtgAuthRsp rsp = {0};
  rsp.pRawRes = pRes;

  CTG_ERR_RET(ctgChkAuthFromCache(pCtg, pReq, false, &inCache, &rsp));

  if (inCache) {
    if (exists) {
      *exists = true;
    }
    
    return TSDB_CODE_SUCCESS;
  } else if (exists) {
    *exists = false;
    return TSDB_CODE_SUCCESS;
  }

  SCtgAuthReq req = {0};
  req.pRawReq = pReq;
  req.pConn = pConn;
  req.onlyCache = false;
  CTG_ERR_RET(ctgGetUserDbAuthFromMnode(pCtg, pConn, pReq->user, &req.authInfo, NULL));

  CTG_ERR_JRET(ctgChkSetAuthRes(pCtg, &req, &rsp));

_return:

  (void)ctgUpdateUserEnqueue(pCtg, &req.authInfo, false); // cache update not fatal error

  CTG_RET(code);
}

int32_t ctgGetTbType(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pTableName, int32_t* tbType) {
  char dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pTableName, dbFName);
  CTG_ERR_RET(ctgReadTbTypeFromCache(pCtg, dbFName, pTableName->tname, tbType));
  if (*tbType > 0) {
    return TSDB_CODE_SUCCESS;
  }

  STableMeta*   pMeta = NULL;
  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;
  CTG_ERR_RET(ctgGetTbMeta(pCtg, pConn, &ctx, &pMeta));

  *tbType = pMeta->tableType;
  taosMemoryFree(pMeta);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbIndex(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pTableName, SArray** pRes) {
  CTG_ERR_RET(ctgReadTbIndexFromCache(pCtg, pTableName, pRes));
  if (*pRes) {
    return TSDB_CODE_SUCCESS;
  }

  STableIndex* pIndex = taosMemoryCalloc(1, sizeof(STableIndex));
  if (NULL == pIndex) {
    CTG_ERR_RET(terrno);
  }

  int32_t code = ctgGetTbIndexFromMnode(pCtg, pConn, (SName*)pTableName, pIndex, NULL);
  if (TSDB_CODE_MND_DB_INDEX_NOT_EXIST == code) {
    code = 0;
    goto _return;
  }
  CTG_ERR_JRET(code);

  SArray* pInfo = NULL;
  CTG_ERR_JRET(ctgCloneTableIndex(pIndex->pIndex, &pInfo));

  *pRes = pInfo;

  CTG_ERR_JRET(ctgUpdateTbIndexEnqueue(pCtg, &pIndex, false));

  return TSDB_CODE_SUCCESS;

_return:

  tFreeSTableIndexRsp(pIndex);
  taosMemoryFree(pIndex);

  taosArrayDestroyEx(*pRes, tFreeSTableIndexInfo);
  *pRes = NULL;

  CTG_RET(code);
}

int32_t ctgGetTbCfg(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pTableName, STableCfg** pCfg) {
  int32_t tbType = 0;
  CTG_ERR_RET(ctgGetTbType(pCtg, pConn, pTableName, &tbType));

  if (TSDB_SUPER_TABLE == tbType) {
    CTG_ERR_RET(ctgGetTableCfgFromMnode(pCtg, pConn, pTableName, pCfg, NULL));
  } else {
    SVgroupInfo vgroupInfo = {0};
    CTG_ERR_RET(ctgGetTbHashVgroup(pCtg, pConn, pTableName, &vgroupInfo, NULL));
    CTG_ERR_RET(ctgGetTableCfgFromVnode(pCtg, pConn, pTableName, &vgroupInfo, pCfg, NULL));
  }

  CTG_RET(TSDB_CODE_SUCCESS);
}

#if 0
int32_t ctgGetTbTag(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pTableName, SArray** pRes) {
  SVgroupInfo vgroupInfo = {0};
  STableCfg* pCfg = NULL;
  int32_t code = 0;

  CTG_ERR_RET(ctgGetTbHashVgroup(pCtg, pConn, pTableName, &vgroupInfo, NULL));
  CTG_ERR_RET(ctgGetTableCfgFromVnode(pCtg, pConn, pTableName, &vgroupInfo, &pCfg, NULL));

  if (NULL == pCfg->pTags || pCfg->tagsLen <= 0) {
    ctgError("invalid tag in tbCfg rsp, pTags:%p, len:%d", pCfg->pTags, pCfg->tagsLen);
    CTG_ERR_JRET(TSDB_CODE_INVALID_MSG);
  }

  SArray* pTagVals = NULL;
  STag*   pTag = (STag*)pCfg->pTags;

  if (tTagIsJson(pTag)) {
    pTagVals = taosArrayInit(1, sizeof(STagVal));
    if (NULL == pTagVals) {
      CTG_ERR_JRET(terrno);
    }

    char* pJson = NULL;
    parseTagDatatoJson(pTag, &pJson, NULL);
    if(NULL == pJson) {
      taosArrayDestroy(pTagVals);
      CTG_ERR_JRET(terrno);
    }
    STagVal tagVal;
    tagVal.cid = 0;
    tagVal.type = TSDB_DATA_TYPE_JSON;
    tagVal.pData = pJson;
    tagVal.nData = strlen(pJson);
    if (NULL == taosArrayPush(pTagVals, &tagVal)) {
      taosMemoryFree(pJson);
      taosArrayDestroy(pTagVals);
      CTG_ERR_JRET(terrno);
    }
  } else {
    CTG_ERR_JRET(tTagToValArray((const STag*)pCfg->pTags, &pTagVals));
  }

  *pRes = pTagVals;

_return:

  tFreeSTableCfgRsp((STableCfgRsp*)pCfg);
  
  CTG_RET(code);
}
#endif

int32_t ctgGetTbDistVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pTableName, SArray** pVgList) {
  STableMeta*   tbMeta = NULL;
  int32_t       code = 0;
  SVgroupInfo   vgroupInfo = {0};
  SCtgDBCache*  dbCache = NULL;
  SArray*       vgList = NULL;
  SDBVgInfo*    vgInfo = NULL;
  SCtgTbMetaCtx ctx = {0};
  ctx.pName = pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;

  *pVgList = NULL;

  CTG_ERR_JRET(ctgGetTbMeta(pCtg, pConn, &ctx, &tbMeta));

  char db[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pTableName, db);

  SHashObj* vgHash = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, db, &dbCache, &vgInfo, NULL));

  if (dbCache) {
    vgHash = dbCache->vgCache.vgInfo->vgHash;
  } else {
    vgHash = vgInfo->vgHash;
  }

  if (tbMeta->tableType == TSDB_SUPER_TABLE) {
    CTG_ERR_JRET(ctgGenerateVgList(pCtg, vgHash, pVgList, db));
  } else {
    // USE HASH METHOD INSTEAD OF VGID IN TBMETA
    ctgError("tb:%s, invalid method to get none stb vgInfo, tbType:%d", pTableName->tname, tbMeta->tableType);
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);

#if 0  
    int32_t vgId = tbMeta->vgId;
    if (taosHashGetDup(vgHash, &vgId, sizeof(vgId), &vgroupInfo) != 0) {
      ctgWarn("table's vgId not found in vgroup list, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_VG_META_MISMATCH);
    }

    vgList = taosArrayInit(1, sizeof(SVgroupInfo));
    if (NULL == vgList) {
      ctgError("taosArrayInit %d failed", (int32_t)sizeof(SVgroupInfo));
      CTG_ERR_JRET(terrno);    
    }

    if (NULL == taosArrayPush(vgList, &vgroupInfo)) {
      ctgError("taosArrayPush vgroupInfo to array failed, vgId:%d, tbName:%s", vgId, tNameGetTableName(pTableName));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    *pVgList = vgList;
    vgList = NULL;
#endif
  }

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  taosMemoryFreeClear(tbMeta);

  if (vgInfo) {
    freeVgInfo(vgInfo);
  }

  if (vgList) {
    taosArrayDestroy(vgList);
    vgList = NULL;
  }

  CTG_RET(code);
}

int32_t ctgGetTbHashVgroup(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SVgroupInfo* pVgroup, bool* exists) {
  if (IS_SYS_DBNAME(pTableName->dbname)) {
    ctgError("db:%s, no valid vgInfo for db", pTableName->dbname);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;
  char         db[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pTableName, db);

  SDBVgInfo* vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, db, &dbCache, &vgInfo, exists));

  if (exists && false == *exists) {
    ctgDebug("db:%s, vgInfo not in cache", pTableName->dbname);
    return TSDB_CODE_SUCCESS;
  }
  
  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, pConn ? &pConn->mgmtEps : NULL, vgInfo ? vgInfo : dbCache->vgCache.vgInfo, pTableName, pVgroup));

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    freeVgInfo(vgInfo);
  }

  CTG_RET(code);
}

int32_t ctgGetTbsHashVgId(SCatalog* pCtg, SRequestConnInfo* pConn, int32_t acctId, const char* pDb, const char* pTbs[], int32_t tbNum, int32_t* vgId) {
  if (IS_SYS_DBNAME(pDb)) {
    ctgError("db:%s, no valid vgInfo for db", pDb);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  (void)snprintf(dbFName, TSDB_DB_FNAME_LEN, "%d.%s", acctId, pDb);

  SDBVgInfo* vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, dbFName, &dbCache, &vgInfo, NULL));
  
  CTG_ERR_JRET(ctgGetVgIdsFromHashValue(pCtg, vgInfo ? vgInfo : dbCache->vgCache.vgInfo, dbFName, pTbs, tbNum, vgId));

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    freeVgInfo(vgInfo);
  }

  CTG_RET(code);
}


int32_t ctgGetCachedTbVgMeta(SCatalog* pCtg, const SName* pTableName, SVgroupInfo* pVgroup, STableMeta** pTableMeta) {
  int32_t      code = 0;
  char         db[TSDB_DB_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(pTableName, db);
  SCtgDBCache *dbCache = NULL;
  SCtgTbCache *tbCache = NULL;

  CTG_ERR_RET(ctgAcquireVgMetaFromCache(pCtg, db, pTableName->tname, &dbCache, &tbCache));

  if (NULL == dbCache || NULL == tbCache) {
    *pTableMeta = NULL;
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgGetVgInfoFromHashValue(pCtg, NULL, dbCache->vgCache.vgInfo, pTableName, pVgroup));

  ctgRUnlockVgInfo(dbCache);

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;
  code = ctgCopyTbMeta(pCtg, &ctx, &dbCache, &tbCache, pTableMeta, db);

  ctgReleaseTbMetaToCache(pCtg, dbCache, tbCache);

  CTG_RET(code);

_return:
  
  ctgReleaseVgMetaToCache(pCtg, dbCache, tbCache);

  CTG_RET(code);
}


int32_t ctgRemoveTbMeta(SCatalog* pCtg, SName* pTableName, bool related) {
  int32_t code = 0;

  if (NULL == pCtg || NULL == pTableName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgRemoveTbMetaFromCache(pCtg, pTableName, true, related));

_return:

  CTG_RET(code);
}


int32_t ctgRemoveViewMeta(SCatalog* pCtg, const char* dbFName, uint64_t dbId, const char* viewName, uint64_t viewId) {
  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == viewName) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    return TSDB_CODE_SUCCESS;
  }

  CTG_ERR_JRET(ctgDropViewMetaEnqueue(pCtg, dbFName, 0, viewName, viewId, true));

_return:

  CTG_RET(code);
}


void ctgProcessTimerEvent(void *param, void *tmrId) {
  CTG_API_NENTER();

  (void)ctgdShowCacheInfo();
  (void)ctgdShowStatInfo();

  int32_t cacheMaxSize = atomic_load_32(&tsMetaCacheMaxSize);
  if (cacheMaxSize >= 0) {
    uint64_t cacheSize = 0;
    ctgGetGlobalCacheSize(&cacheSize);
    bool overflow = CTG_CACHE_OVERFLOW(cacheSize, cacheMaxSize);
    
    qDebug("catalog cache size: %" PRIu64"B, maxCaseSize:%dMB, %s", cacheSize, cacheMaxSize, overflow ? "overflow" : "NO overflow");

    if (overflow) {
      int32_t code = ctgClearCacheEnqueue(NULL, true, false, false, false);
      if (code) {
        qError("clear cache enqueue failed, error:%s", tstrerror(code));
        if (taosTmrReset(ctgProcessTimerEvent, CTG_DEFAULT_CACHE_MON_MSEC, NULL, gCtgMgmt.timer, &gCtgMgmt.cacheTimer)) {
          qError("reset catalog cache monitor timer error, timer stoppped");
        }
      }

      goto _return;
    }
  }

  qTrace("reset catalog timer");
  if (taosTmrReset(ctgProcessTimerEvent, CTG_DEFAULT_CACHE_MON_MSEC, NULL, gCtgMgmt.timer, &gCtgMgmt.cacheTimer)) {
    qError("reset catalog cache monitor timer error, timer stoppped");
  }

_return:

  CTG_API_NLEAVE();
}

int32_t ctgGetDBCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* pDbCfg) {
  CTG_ERR_RET(ctgReadDBCfgFromCache(pCtg, dbFName, pDbCfg));

  if (pDbCfg->cfgVersion < 0) {
    CTG_ERR_RET(ctgGetDBCfgFromMnode(pCtg, pConn, dbFName, pDbCfg, NULL));
    SDbCfgInfo *pCfg = NULL;
    CTG_ERR_RET(ctgCloneDbCfgInfo(pDbCfg, &pCfg));    
    
    CTG_ERR_RET(ctgUpdateDbCfgEnqueue(pCtg, dbFName, pDbCfg->dbId, pCfg, false));
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTbTsmas(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pTableName, SArray** ppRes) {
  STableTSMAInfoRsp tsmasRsp = {0};
  int32_t code = ctgGetTbTSMAFromMnode(pCtg, pConn, pTableName, &tsmasRsp, NULL, TDMT_MND_GET_TABLE_TSMA);
  if (code == TSDB_CODE_MND_SMA_NOT_EXIST) {
    code = 0;
    goto _return;
  }
  
  CTG_ERR_JRET(code);
  
  *ppRes = tsmasRsp.pTsmas;
  tsmasRsp.pTsmas = NULL;

  for (int32_t i = 0; i < (*ppRes)->size; ++i) {
    CTG_ERR_JRET(ctgUpdateTbTSMAEnqueue(pCtg, taosArrayGet((*ppRes), i), 0, false));
  }
  
  return TSDB_CODE_SUCCESS;

_return:

  if (tsmasRsp.pTsmas) {
    tFreeTableTSMAInfoRsp(&tsmasRsp);
  }
  CTG_RET(code);
}


int32_t ctgGetTsma(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTsmaName, STableTSMAInfo** pTsma) {
  STableTSMAInfoRsp tsmaRsp = {0};
  int32_t code = ctgGetTbTSMAFromMnode(pCtg, pConn, pTsmaName, &tsmaRsp, NULL, TDMT_MND_GET_TSMA);
  if (code == TSDB_CODE_MND_SMA_NOT_EXIST) {
    code = 0;
    goto _return;
  }

  CTG_ERR_JRET(code);
  
  *pTsma = taosArrayGetP(tsmaRsp.pTsmas, 0);
  taosArrayDestroy(tsmaRsp.pTsmas);
  tsmaRsp.pTsmas = NULL;

_return:

  if (tsmaRsp.pTsmas) {
    tFreeTableTSMAInfoRsp(&tsmaRsp);
  }
  CTG_RET(code);
}


int32_t catalogInit(SCatalogCfg* cfg) {
  qInfo("catalog init");
  if (gCtgMgmt.pCluster) {
    qError("catalog already initialized");
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  TAOS_MEMSET(&gCtgMgmt, 0, sizeof(gCtgMgmt));

  if (cfg) {
    TAOS_MEMCPY(&gCtgMgmt.cfg, cfg, sizeof(*cfg));

    if (gCtgMgmt.cfg.maxDBCacheNum == 0) {
      gCtgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    }

    if (gCtgMgmt.cfg.maxTblCacheNum == 0) {
      gCtgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TBLMETA_NUMBER;
    }

    if (gCtgMgmt.cfg.maxViewCacheNum == 0) {
      gCtgMgmt.cfg.maxViewCacheNum = CTG_DEFAULT_CACHE_VIEW_NUMBER;
    }

    if (gCtgMgmt.cfg.maxTSMACacheNum == 0) {
      gCtgMgmt.cfg.maxTSMACacheNum = CTG_DEFAULT_CACHE_TSMA_NUMBER;
    }

    if (gCtgMgmt.cfg.dbRentSec == 0) {
      gCtgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    }

    if (gCtgMgmt.cfg.stbRentSec == 0) {
      gCtgMgmt.cfg.stbRentSec = CTG_DEFAULT_RENT_SECOND;
    }

    if (gCtgMgmt.cfg.viewRentSec == 0) {
      gCtgMgmt.cfg.viewRentSec = CTG_DEFAULT_RENT_SECOND;
    }
    
    if (gCtgMgmt.cfg.tsmaRentSec == 0) {
      gCtgMgmt.cfg.tsmaRentSec = CTG_DEFAULT_RENT_SECOND;
    }
  } else {
    gCtgMgmt.cfg.maxDBCacheNum = CTG_DEFAULT_CACHE_DB_NUMBER;
    gCtgMgmt.cfg.maxTblCacheNum = CTG_DEFAULT_CACHE_TBLMETA_NUMBER;
    gCtgMgmt.cfg.maxViewCacheNum = CTG_DEFAULT_CACHE_VIEW_NUMBER;
    gCtgMgmt.cfg.maxTSMACacheNum = CTG_DEFAULT_CACHE_TSMA_NUMBER;
    gCtgMgmt.cfg.dbRentSec = CTG_DEFAULT_RENT_SECOND;
    gCtgMgmt.cfg.stbRentSec = CTG_DEFAULT_RENT_SECOND;
    gCtgMgmt.cfg.viewRentSec = CTG_DEFAULT_RENT_SECOND;
    gCtgMgmt.cfg.tsmaRentSec = CTG_DEFAULT_RENT_SECOND;
  }

  gCtgMgmt.pCluster = taosHashInit(CTG_DEFAULT_CACHE_CLUSTER_NUMBER, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT),
                                   false, HASH_ENTRY_LOCK);
  if (NULL == gCtgMgmt.pCluster) {
    qError("taosHashInit %d cluster cache failed", CTG_DEFAULT_CACHE_CLUSTER_NUMBER);
    CTG_ERR_RET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  if (tsem_init(&gCtgMgmt.queue.reqSem, 0, 0)) {
    qError("tsem_init failed, terror:%s", tstrerror(terrno));
    CTG_ERR_RET(TSDB_CODE_CTG_SYS_ERROR);
  }

  gCtgMgmt.queue.head = taosMemoryCalloc(1, sizeof(SCtgQNode));
  if (NULL == gCtgMgmt.queue.head) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgQNode));
    CTG_ERR_RET(terrno);
  }
  gCtgMgmt.queue.tail = gCtgMgmt.queue.head;

  gCtgMgmt.jobPool = taosOpenRef(200, ctgFreeJob);
  if (gCtgMgmt.jobPool < 0) {
    qError("taosOpenRef failed, error:%s", tstrerror(terrno));
    CTG_ERR_RET(terrno);
  }

  gCtgMgmt.timer = taosTmrInit(0, 0, 0, "catalog");
  if (NULL == gCtgMgmt.timer) {
    qError("init timer failed, error:%s", tstrerror(terrno));
    CTG_ERR_RET(terrno);
  }

  gCtgMgmt.cacheTimer = taosTmrStart(ctgProcessTimerEvent, CTG_DEFAULT_CACHE_MON_MSEC, NULL, gCtgMgmt.timer);
  if (NULL == gCtgMgmt.cacheTimer) {
    qError("start cache timer failed");
    CTG_ERR_RET(terrno);
  }

  CTG_ERR_RET(ctgStartUpdateThread());

  qInfo("catalog initialized, maxDb:%u, maxTbl:%u, dbRentSec:%u, stbRentSec:%u", gCtgMgmt.cfg.maxDBCacheNum,
        gCtgMgmt.cfg.maxTblCacheNum, gCtgMgmt.cfg.dbRentSec, gCtgMgmt.cfg.stbRentSec);

  return TSDB_CODE_SUCCESS;
}

int32_t catalogGetHandle(int64_t clusterId, SCatalog** catalogHandle) {
  if (NULL == catalogHandle) {
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_ENTER();

  if (NULL == gCtgMgmt.pCluster) {
    qError("catalog cluster cache are not ready, clusterId:0x%" PRIx64, clusterId);
    CTG_API_LEAVE(TSDB_CODE_CTG_NOT_READY);
  }

  int32_t   code = 0;
  SCatalog* clusterCtg = NULL;

  while (true) {
    SCatalog** ctg = (SCatalog**)taosHashGet(gCtgMgmt.pCluster, (char*)&clusterId, sizeof(clusterId));

    if (ctg && (*ctg)) {
      *catalogHandle = *ctg;
      CTG_STAT_HIT_INC(CTG_CI_CLUSTER, 1);
      qTrace("ctg:%p, get catalog handle from cache, clusterId:0x%" PRIx64, *ctg, clusterId);
      CTG_API_LEAVE(TSDB_CODE_SUCCESS);
    }

    CTG_STAT_NHIT_INC(CTG_CI_CLUSTER, 1);

    clusterCtg = taosMemoryCalloc(1, sizeof(SCatalog));
    if (NULL == clusterCtg) {
      qError("calloc %d failed", (int32_t)sizeof(SCatalog));
      CTG_API_LEAVE(terrno);
    }

    clusterCtg->clusterId = clusterId;

    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->dbRent, gCtgMgmt.cfg.dbRentSec, CTG_RENT_DB, sizeof(SDbCacheInfo)));
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->stbRent, gCtgMgmt.cfg.stbRentSec, CTG_RENT_STABLE, sizeof(SSTableVersion)));
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->viewRent, gCtgMgmt.cfg.viewRentSec, CTG_RENT_VIEW, sizeof(SViewVersion)));
    CTG_ERR_JRET(ctgMetaRentInit(&clusterCtg->tsmaRent, gCtgMgmt.cfg.tsmaRentSec, CTG_RENT_TSMA, sizeof(STSMAVersion)));

    clusterCtg->dbCache = taosHashInit(gCtgMgmt.cfg.maxDBCacheNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY),
                                       false, HASH_ENTRY_LOCK);
    if (NULL == clusterCtg->dbCache) {
      qError("taosHashInit %d dbCache failed", CTG_DEFAULT_CACHE_DB_NUMBER);
      CTG_ERR_JRET(terrno);
    }

    clusterCtg->userCache = taosHashInit(gCtgMgmt.cfg.maxUserCacheNum,
                                         taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
    if (NULL == clusterCtg->userCache) {
      qError("taosHashInit %d user cache failed", gCtgMgmt.cfg.maxUserCacheNum);
      CTG_ERR_JRET(terrno);
    }

    code = taosHashPut(gCtgMgmt.pCluster, &clusterId, sizeof(clusterId), &clusterCtg, POINTER_BYTES);
    if (code) {
      if (HASH_NODE_EXIST(code)) {
        ctgFreeHandleImpl(clusterCtg);
        continue;
      }

      qError("taosHashPut catalog to cache failed, clusterId:0x%" PRIx64, clusterId);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    qDebug("ctg:%p, add catalog to cache, clusterId:0x%" PRIx64, clusterCtg, clusterId);

    break;
  }

  *catalogHandle = clusterCtg;

  CTG_STAT_NUM_INC(CTG_CI_CLUSTER, 1);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  ctgFreeHandleImpl(clusterCtg);

  CTG_API_LEAVE(code);
}

int32_t catalogGetDBVgVersion(SCatalog* pCtg, const char* dbFName, int32_t* version, int64_t* dbId, int32_t* tableNum, int64_t* pStateTs) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == version || NULL == dbId || NULL == tableNum || NULL == pStateTs) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;

  CTG_ERR_JRET(ctgAcquireVgInfoFromCache(pCtg, dbFName, &dbCache));
  if (NULL == dbCache) {
    *version = CTG_DEFAULT_INVALID_VERSION;
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  *version = dbCache->vgCache.vgInfo->vgVersion;
  *dbId = dbCache->dbId;
  *tableNum = dbCache->vgCache.vgInfo->numOfTable;

  ctgReleaseVgInfoToCache(pCtg, dbCache);

  ctgDebug("db:%s, get db vgVersion from cache, vgVersion:%d", dbFName, *version);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetDBVgList(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SArray** vgroupList) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == pConn || NULL == vgroupList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;
  SArray*      vgList = NULL;
  SHashObj*    vgHash = NULL;
  SDBVgInfo*   vgInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, dbFName, &dbCache, &vgInfo, NULL));
  if (dbCache) {
    vgHash = dbCache->vgCache.vgInfo->vgHash;
  } else {
    vgHash = vgInfo->vgHash;
  }

  CTG_ERR_JRET(ctgGenerateVgList(pCtg, vgHash, &vgList, dbFName));

  *vgroupList = vgList;
  vgList = NULL;

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  }

  if (vgInfo) {
    freeVgInfo(vgInfo);
  }

  CTG_API_LEAVE(code);
}

int32_t catalogGetDBVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, TAOS_DB_ROUTE_INFO* pInfo) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbFName || NULL == pConn || NULL == pInfo) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgDBCache* dbCache = NULL;
  int32_t      code = 0;
  SDBVgInfo*   dbInfo = NULL;
  CTG_ERR_JRET(ctgGetDBVgInfo(pCtg, pConn, dbFName, &dbCache, &dbInfo, NULL));
  if (dbCache) {
    dbInfo = dbCache->vgCache.vgInfo;
  }

  pInfo->routeVersion = dbInfo->vgVersion;
  pInfo->hashPrefix = dbInfo->hashPrefix;
  pInfo->hashSuffix = dbInfo->hashSuffix;
  pInfo->hashMethod = dbInfo->hashMethod;
  pInfo->vgNum = taosHashGetSize(dbInfo->vgHash);
  if (pInfo->vgNum <= 0) {
    ctgError("db:%s, invalid vgNum %d in db vgHash", dbFName, pInfo->vgNum);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  pInfo->vgHash = taosMemoryCalloc(pInfo->vgNum, sizeof(TAOS_VGROUP_HASH_INFO));
  if (NULL == pInfo->vgHash) {
    CTG_ERR_JRET(terrno);
  }

  SVgroupInfo* vgInfo = NULL;  
  int32_t i = 0;
  void* pIter = taosHashIterate(dbInfo->vgHash, NULL);
  while (pIter) {
    vgInfo = pIter;

    pInfo->vgHash[i].vgId = vgInfo->vgId;
    pInfo->vgHash[i].hashBegin = vgInfo->hashBegin;
    pInfo->vgHash[i].hashEnd = vgInfo->hashEnd;
    
    pIter = taosHashIterate(dbInfo->vgHash, pIter);
    vgInfo = NULL;
    ++i;
  }

_return:

  if (dbCache) {
    ctgRUnlockVgInfo(dbCache);
    ctgReleaseDBCache(pCtg, dbCache);
  } else if (dbInfo) {
    freeVgInfo(dbInfo);
  }

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateDBVgInfo(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDBVgInfo* dbInfo) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == dbInfo) {
    freeVgInfo(dbInfo);
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  code = ctgUpdateVgroupEnqueue(pCtg, dbFName, dbId, dbInfo, false);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateDbCfg(SCatalog* pCtg, const char* dbFName, uint64_t dbId, SDbCfgInfo* cfgInfo) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == cfgInfo) {
    freeDbCfgInfo(cfgInfo);
    CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  code = ctgUpdateDbCfgEnqueue(pCtg, dbFName, dbId, cfgInfo, false);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogRemoveDB(SCatalog* pCtg, const char* dbFName, uint64_t dbId) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_ERR_JRET(ctgDropDbCacheEnqueue(pCtg, dbFName, dbId));

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateVgEpSet(SCatalog* pCtg, const char* dbFName, int32_t vgId, SEpSet* epSet) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == epSet) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_ERR_JRET(ctgUpdateVgEpsetEnqueue(pCtg, (char*)dbFName, vgId, epSet));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateTableIndex(SCatalog* pCtg, STableIndexRsp* pRsp) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == pRsp) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  STableIndex* pIndex = taosMemoryCalloc(1, sizeof(STableIndex));
  if (NULL == pIndex) {
    CTG_API_LEAVE(terrno);
  }

  TAOS_MEMCPY(pIndex, pRsp, sizeof(STableIndex));

  CTG_ERR_JRET(ctgUpdateTbIndexEnqueue(pCtg, &pIndex, false));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogRemoveTableMeta(SCatalog* pCtg, SName* pTableName) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgRemoveTbMeta(pCtg, pTableName, false));
}

int32_t catalogRemoveTableRelatedMeta(SCatalog* pCtg, SName* pTableName) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgRemoveTbMeta(pCtg, pTableName, true));
}


int32_t catalogRemoveStbMeta(SCatalog* pCtg, const char* dbFName, uint64_t dbId, const char* stbName, uint64_t suid) {
  CTG_API_ENTER();

  int32_t code = 0;

  if (NULL == pCtg || NULL == dbFName || NULL == stbName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (NULL == pCtg->dbCache) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  CTG_ERR_JRET(ctgDropStbMetaEnqueue(pCtg, dbFName, dbId, stbName, suid, true));

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetTableMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB;

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, pConn, &ctx, pTableMeta));
}

int32_t catalogGetCachedTableMeta(SCatalog* pCtg, const SName* pTableName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_UNKNOWN_STB | CTG_FLAG_ONLY_CACHE;

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, NULL, &ctx, pTableMeta));
}


int32_t catalogGetSTableMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                             STableMeta** pTableMeta) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_STB;

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, pConn, &ctx, pTableMeta));
}

int32_t catalogGetCachedSTableMeta(SCatalog* pCtg, const SName* pTableName,          STableMeta** pTableMeta) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_STB | CTG_FLAG_ONLY_CACHE;

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, NULL, &ctx, pTableMeta));
}


int32_t catalogUpdateTableMeta(SCatalog* pCtg, STableMetaRsp* pMsg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pMsg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateTbMeta(pCtg, pMsg, true));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogAsyncUpdateTableMeta(SCatalog* pCtg, STableMetaRsp* pMsg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pMsg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateTbMeta(pCtg, pMsg, false));

_return:

  CTG_API_LEAVE(code);
}


int32_t catalogChkTbMetaVersion(SCatalog* pCtg, SRequestConnInfo* pConn, SArray* pTables) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTables) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = TSDB_CODE_SUCCESS;
  SName   name = {0};
  int32_t sver = 0;
  int32_t tver = 0;
  int32_t rver = 0;
  int32_t tbNum = taosArrayGetSize(pTables);
  for (int32_t i = 0; i < tbNum; ++i) {
    STbSVersion* pTb = (STbSVersion*)taosArrayGet(pTables, i);
    if (NULL == pTb) {
      ctgError("fail to get the %dth table, tbNum:%d", i, tbNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }
    
    if (NULL == pTb->tbFName || 0 == pTb->tbFName[0]) {
      continue;
    }

    if (tNameFromString(&name, pTb->tbFName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) {
      ctgError("invalid tbFName format, tbFName:%s, idx:%d", pTb->tbFName, i);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    if (IS_SYS_DBNAME(name.dbname)) {
      continue;
    }

    int32_t  tbType = 0;
    uint64_t suid = 0;
    char     stbName[TSDB_TABLE_FNAME_LEN];
    CTG_ERR_JRET(ctgReadTbVerFromCache(pCtg, &name, &sver, &tver, &rver, &tbType, &suid, stbName));
    if ((sver >= 0 && sver < pTb->sver) || (tver >= 0 && tver < pTb->tver) || (rver >= 0 && rver < pTb->rver)) {
      switch (tbType) {
        case TSDB_CHILD_TABLE: {
          SName stb = name;
          tstrncpy(stb.tname, stbName, sizeof(stb.tname));
          CTG_ERR_JRET(ctgRemoveTbMeta(pCtg, &stb, false));
          break;
        }
        case TSDB_VIRTUAL_CHILD_TABLE: {
          CTG_ERR_JRET(ctgRemoveTbMeta(pCtg, &name, false));
          break;
        }
        case TSDB_SUPER_TABLE:
        case TSDB_NORMAL_TABLE:
        case TSDB_VIRTUAL_NORMAL_TABLE: {
          CTG_ERR_JRET(ctgRemoveTbMeta(pCtg, &name, false));
          break;
        }
        default:
          ctgError("ignore table type %d", tbType);
          break;
      }
    }
  }

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogRefreshDBVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == dbFName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgRefreshDBVgInfo(pCtg, pConn, dbFName));
}

int32_t catalogRefreshTableMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, int32_t isSTable) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_FORCE_UPDATE | CTG_FLAG_MAKE_STB(isSTable);
  if (IS_SYS_DBNAME(ctx.pName->dbname)) {
    CTG_FLAG_SET_SYS_DB(ctx.flag);
  }

  CTG_API_LEAVE(ctgRefreshTbMeta(pCtg, pConn, &ctx, NULL, true));
}

int32_t catalogRefreshGetTableMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                                   STableMeta** pTableMeta, int32_t isSTable) {
  CTG_API_ENTER();

  SCtgTbMetaCtx ctx = {0};
  ctx.pName = (SName*)pTableName;
  ctx.flag = CTG_FLAG_FORCE_UPDATE | CTG_FLAG_MAKE_STB(isSTable);

  CTG_API_LEAVE(ctgGetTbMeta(pCtg, pConn, &ctx, pTableMeta));
}

int32_t catalogGetTableDistVgInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SArray** pVgList) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pVgList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (IS_SYS_DBNAME(pTableName->dbname)) {
    ctgError("no valid vgInfo for db, dbname:%s", pTableName->dbname);
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgGetTbDistVgInfo(pCtg, pConn, (SName*)pTableName, pVgList));
}

int32_t catalogGetTableHashVgroup(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                                  SVgroupInfo* pVgroup) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTbHashVgroup(pCtg, pConn, pTableName, pVgroup, NULL));
}

int32_t catalogGetTablesHashVgId(SCatalog* pCtg, SRequestConnInfo* pConn, int32_t acctId, const char* pDb, const char* pTableName[],
                                  int32_t tableNum, int32_t *vgId) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTbsHashVgId(pCtg, pConn, acctId, pDb, pTableName, tableNum, vgId));
}

int32_t catalogGetCachedTableHashVgroup(SCatalog* pCtg, const SName* pTableName,           SVgroupInfo* pVgroup, bool* exists) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetTbHashVgroup(pCtg, NULL, pTableName, pVgroup, exists));
}

int32_t catalogGetCachedTableVgMeta(SCatalog* pCtg, const SName* pTableName,          SVgroupInfo* pVgroup, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgGetCachedTbVgMeta(pCtg, pTableName, pVgroup, pTableMeta));
}


#if 0
int32_t catalogGetAllMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SCatalogReq* pReq, SMetaData* pRsp) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pReq || NULL == pRsp) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  pRsp->pTableMeta = NULL;

  if (pReq->pTableMeta) {
    int32_t tbNum = (int32_t)taosArrayGetSize(pReq->pTableMeta);
    if (tbNum <= 0) {
      ctgError("empty table name list, tbNum:%d", tbNum);
      CTG_ERR_JRET(TSDB_CODE_CTG_INVALID_INPUT);
    }

    pRsp->pTableMeta = taosArrayInit(tbNum, POINTER_BYTES);
    if (NULL == pRsp->pTableMeta) {
      ctgError("taosArrayInit %d failed", tbNum);
      CTG_ERR_JRET(terrno);
    }

    for (int32_t i = 0; i < tbNum; ++i) {
      SName*        name = taosArrayGet(pReq->pTableMeta, i);
      STableMeta*   pTableMeta = NULL;
      SCtgTbMetaCtx ctx = {0};
      ctx.pName = name;
      ctx.flag = CTG_FLAG_UNKNOWN_STB;

      CTG_ERR_JRET(ctgGetTbMeta(pCtg, pConn, &ctx, &pTableMeta));

      if (NULL == taosArrayPush(pRsp->pTableMeta, &pTableMeta)) {
        ctgError("taosArrayPush failed, idx:%d", i);
        taosMemoryFreeClear(pTableMeta);
        CTG_ERR_JRET(terrno);
      }
    }
  }

  if (pReq->qNodeRequired) {
    pRsp->pQnodeList = taosArrayInit(10, sizeof(SQueryNodeLoad));
    CTG_ERR_JRET(ctgGetQnodeListFromMnode(pCtg, pConn, pRsp->pQnodeList, NULL));
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);

_return:

  if (pRsp->pTableMeta) {
    int32_t aSize = taosArrayGetSize(pRsp->pTableMeta);
    for (int32_t i = 0; i < aSize; ++i) {
      STableMeta* pMeta = taosArrayGetP(pRsp->pTableMeta, i);
      taosMemoryFreeClear(pMeta);
    }

    taosArrayDestroy(pRsp->pTableMeta);
    pRsp->pTableMeta = NULL;
  }

  CTG_API_LEAVE(code);
}
#endif

int32_t catalogAsyncGetAllMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SCatalogReq* pReq, catalogCallback fp,
                               void* param, int64_t* jobId) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pReq || NULL == fp || NULL == param) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t  code = 0;
  SCtgJob* pJob = NULL;
  CTG_ERR_JRET(ctgInitJob(pCtg, pConn, &pJob, pReq, fp, param));

  CTG_ERR_JRET(ctgLaunchJob(pJob));

  // NOTE: here the assignment of jobId is invalid, may over-write the true scheduler created query job.
  //  *jobId = pJob->refId;

_return:

  if (pJob) {
    int32_t code2 = taosReleaseRef(gCtgMgmt.jobPool, pJob->refId);
    if (TSDB_CODE_SUCCESS) {
      qError("release catalog job refId:%" PRId64 " falied, error:%s", pJob->refId, tstrerror(code2));
    }

    if (code) {
      code2 = taosRemoveRef(gCtgMgmt.jobPool, pJob->refId);
      if (TSDB_CODE_SUCCESS) {
        qError("remove catalog job refId:%" PRId64 " falied, error:%s", pJob->refId, tstrerror(code2));
      }
    }
  }

  CTG_API_LEAVE(code);
}

int32_t catalogGetQnodeList(SCatalog* pCtg, SRequestConnInfo* pConn, SArray* pQnodeList) {
  CTG_API_ENTER();

  int32_t code = 0;
  if (NULL == pCtg || NULL == pConn || NULL == pQnodeList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_CACHE_NHIT_INC(CTG_CI_QNODE, 1);
  CTG_ERR_JRET(ctgGetQnodeListFromMnode(pCtg, pConn, pQnodeList, NULL));

_return:

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetDnodeList(SCatalog* pCtg, SRequestConnInfo* pConn, SArray** pDnodeList) {
  CTG_API_ENTER();

  int32_t code = 0;
  if (NULL == pCtg || NULL == pConn || NULL == pDnodeList) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_CACHE_NHIT_INC(CTG_CI_DNODE, 1);
  CTG_ERR_JRET(ctgGetDnodeListFromMnode(pCtg, pConn, pDnodeList, NULL));

_return:

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetExpiredSTables(SCatalog* pCtg, SSTableVersion** stables, uint32_t* num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == stables || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->stbRent, (void**)stables, num, sizeof(SSTableVersion)));
}

int32_t catalogGetExpiredViews(SCatalog* pCtg, SViewVersion** views, uint32_t* num, SDynViewVersion** dynViewVersion) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == views || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  *dynViewVersion = taosMemoryMalloc(sizeof(SDynViewVersion));
  if (NULL == *dynViewVersion) {
    CTG_API_LEAVE(terrno);
  }

  (*dynViewVersion)->svrBootTs = atomic_load_64(&pCtg->dynViewVer.svrBootTs);
  (*dynViewVersion)->dynViewVer = atomic_load_64(&pCtg->dynViewVer.dynViewVer);
  
  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->viewRent, (void**)views, num, sizeof(SViewVersion)));
}


int32_t catalogGetExpiredDBs(SCatalog* pCtg, SDbCacheInfo** dbs, uint32_t* num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == dbs || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->dbRent, (void**)dbs, num, sizeof(SDbCacheInfo)));
}

int32_t catalogGetExpiredUsers(SCatalog* pCtg, SUserAuthVersion** users, uint32_t* num) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == users || NULL == num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  *num = taosHashGetSize(pCtg->userCache);
  if (*num <= 0) {
    CTG_API_LEAVE(TSDB_CODE_SUCCESS);
  }

  *users = taosMemoryCalloc(*num, sizeof(SUserAuthVersion));
  if (NULL == *users) {
    ctgError("calloc %d userAuthVersion failed", *num);
    CTG_API_LEAVE(terrno);
  }

  uint32_t      i = 0;
  SCtgUserAuth* pAuth = taosHashIterate(pCtg->userCache, NULL);
  while (pAuth != NULL) {
    size_t len = 0;
    void*  key = taosHashGetKey(pAuth, &len);
    TAOS_STRNCPY((*users)[i].user, key, len);
    (*users)[i].user[len] = 0;
    (*users)[i].version = pAuth->userAuth.version;
    ++i;
    if (i >= *num) {
      taosHashCancelIterate(pCtg->userCache, pAuth);
      break;
    }

    pAuth = taosHashIterate(pCtg->userCache, pAuth);
  }

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogGetExpiredTsmas(SCatalog* pCtg, STSMAVersion** tsmas, uint32_t* num) {
  CTG_API_ENTER();

  if (!pCtg || !tsmas || !num) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgMetaRentGet(&pCtg->tsmaRent, (void**)tsmas, num, sizeof(STSMAVersion)));
}

int32_t catalogGetDBCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* pDbCfg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == dbFName || NULL == pDbCfg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgGetDBCfg(pCtg, pConn, dbFName, pDbCfg));
}

int32_t catalogGetIndexMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const char* indexName, SIndexInfo* pInfo) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == indexName || NULL == pInfo) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgGetIndexInfoFromMnode(pCtg, pConn, indexName, pInfo, NULL));
}

int32_t catalogGetTableIndex(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SArray** pRes) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pRes) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetTbIndex(pCtg, pConn, (SName*)pTableName, pRes));

_return:

  CTG_API_LEAVE(code);
}

#if 0
int32_t catalogGetTableTag(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SArray** pRes) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pRes) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetTbTag(pCtg, pConn, (SName*)pTableName, pRes));

_return:

  CTG_API_LEAVE(code);
}
#endif

int32_t catalogRefreshGetTableCfg(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableCfg** pCfg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pCfg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgRemoveTbMeta(pCtg, (SName*)pTableName, false));

  CTG_ERR_JRET(ctgGetTbCfg(pCtg, pConn, (SName*)pTableName, pCfg));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetUdfInfo(SCatalog* pCtg, SRequestConnInfo* pConn, const char* funcName, SFuncInfo* pInfo) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == funcName || NULL == pInfo) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_CACHE_NHIT_INC(CTG_CI_UDF, 1);

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetUdfInfoFromMnode(pCtg, pConn, funcName, pInfo, NULL));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogChkAuth(SCatalog* pCtg, SRequestConnInfo* pConn, SUserAuthInfo *pAuth, SUserAuthRes* pRes) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pAuth || NULL == pRes) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgChkAuth(pCtg, pConn, pAuth, pRes, NULL));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogChkAuthFromCache(SCatalog* pCtg, SUserAuthInfo *pAuth,        SUserAuthRes* pRes, bool* exists) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pAuth || NULL == pRes || NULL == exists) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgChkAuth(pCtg, NULL, pAuth, pRes, exists));

_return:

  CTG_API_LEAVE(code);
}


int32_t catalogGetServerVersion(SCatalog* pCtg, SRequestConnInfo* pConn, char** pVersion) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pVersion) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_CACHE_NHIT_INC(CTG_CI_SVR_VER, 1);

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetSvrVerFromMnode(pCtg, pConn, pVersion, NULL));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateUserAuthInfo(SCatalog* pCtg, SGetUserAuthRsp* pAuth) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pAuth) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  CTG_API_LEAVE(ctgUpdateUserEnqueue(pCtg, pAuth, false));
}

void catalogFreeMetaData(SMetaData * pData) {
  ctgDestroySMetaData(pData);
}

int32_t catalogRemoveViewMeta(SCatalog* pCtg, const char* dbFName, uint64_t dbId, const char* viewName, uint64_t viewId) {
  CTG_API_ENTER();

  CTG_API_LEAVE(ctgRemoveViewMeta(pCtg, dbFName, dbId, viewName, viewId));
}


int32_t catalogUpdateDynViewVer(SCatalog* pCtg, SDynViewVersion* pVer) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pVer) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  atomic_store_64(&pCtg->dynViewVer.svrBootTs, pVer->svrBootTs);
  atomic_store_64(&pCtg->dynViewVer.dynViewVer, pVer->dynViewVer);

  ctgDebug("clusterId:0x%" PRIx64 ", svrBootTs updated to %" PRId64, pCtg->clusterId, pVer->svrBootTs);
  ctgDebug("clusterId:0x%" PRIx64 ", dynViewVer updated to %" PRId64, pCtg->clusterId, pVer->dynViewVer);

  CTG_API_LEAVE(TSDB_CODE_SUCCESS);
}

int32_t catalogUpdateViewMeta(SCatalog* pCtg, SViewMetaRsp* pMsg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pMsg) {
    tFreeSViewMetaRsp(pMsg);
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateViewMetaToCache(pCtg, pMsg, true));

_return:

  CTG_API_LEAVE(code);
}

#if 0
int32_t catalogAsyncUpdateViewMeta(SCatalog* pCtg, SViewMetaRsp* pMsg) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pMsg) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateViewMetaToCache(pCtg, pMsg, false));

_return:

  CTG_API_LEAVE(code);
}
#endif

int32_t catalogGetViewMeta(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pViewName, STableMeta** pTableMeta) {
  CTG_API_ENTER();

  CTG_API_LEAVE(TSDB_CODE_OPS_NOT_SUPPORT);
}

int32_t catalogAsyncUpdateTSMA(SCatalog* pCtg, STableTSMAInfo** ppTsma, int32_t tsmaVersion) {
  CTG_API_ENTER();
  if (!pCtg || !ppTsma) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateTbTSMAEnqueue(pCtg, ppTsma, tsmaVersion, false));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogUpdateTSMA(SCatalog* pCtg, STableTSMAInfo** pTsma) {
  CTG_API_ENTER();
  if (!pCtg || !pTsma) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }
  
  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateTbTSMAEnqueue(pCtg, pTsma, 0, true));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogRemoveTSMA(SCatalog* pCtg, const STableTSMAInfo* pTsma) {
  CTG_API_ENTER();
  int32_t code = 0;

  if (!pCtg || !pTsma) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  if (!pCtg->dbCache) {
    goto _return;
  }
  
  CTG_ERR_JRET(ctgDropTbTSMAEnqueue(pCtg, pTsma, true));
  
_return:

  CTG_API_LEAVE(code);
}

int32_t catalogGetTableTsmas(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SArray** pRes) {
  CTG_API_ENTER();

  if (NULL == pCtg || NULL == pConn || NULL == pTableName || NULL == pRes) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetTbTsmas(pCtg, pConn, (SName*)pTableName, pRes));

_return:

  CTG_API_LEAVE(code);
}


int32_t catalogGetTsma(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTsmaName, STableTSMAInfo** pTsma) {
  CTG_API_ENTER();

  if (!pCtg || !pConn || !pTsmaName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }

  int32_t code = 0;
  CTG_ERR_JRET(ctgGetTsma(pCtg, pConn, pTsmaName, pTsma));

_return:

  CTG_API_LEAVE(code);
}

int32_t catalogAsyncUpdateDbTsmaVersion(SCatalog* pCtg, int32_t tsmaVersion, const char* dbFName, int64_t dbId) {
  CTG_API_ENTER();
  if (!pCtg || !dbFName) {
    CTG_API_LEAVE(TSDB_CODE_CTG_INVALID_INPUT);
  }
  int32_t code = 0;
  CTG_ERR_JRET(ctgUpdateDbTsmaVersionEnqueue(pCtg, tsmaVersion, dbFName, dbId, false));

_return:
  CTG_API_LEAVE(code);
}

int32_t catalogClearCache(void) {
  CTG_API_ENTER_NOLOCK();

  qInfo("start to clear catalog cache");

  if (NULL == gCtgMgmt.pCluster) {
    CTG_API_LEAVE_NOLOCK(TSDB_CODE_SUCCESS);
  }

  int32_t code = ctgClearCacheEnqueue(NULL, false, false, false, true);

  qInfo("clear catalog cache end, code:%s", tstrerror(code));

  CTG_API_LEAVE_NOLOCK(code);
}

void catalogDestroy(void) {
  qInfo("start to destroy catalog");

  if (NULL == gCtgMgmt.pCluster || atomic_load_8((int8_t*)&gCtgMgmt.exit)) {
    return;
  }

  if (gCtgMgmt.cacheTimer) {
    if (!taosTmrStop(gCtgMgmt.cacheTimer)) {
/*
      qDebug("catalog cacheTimer %" PRIuPTR " not stopped", (uintptr_t)gCtgMgmt.cacheTimer);
    
      while (!taosTmrIsStopped(&gCtgMgmt.cacheTimer)) {
        taosMsleep(1);
      }
*/
    }

    qDebug("catalog cacheTimer %" PRIuPTR " is stopped", (uintptr_t)gCtgMgmt.cacheTimer);
    gCtgMgmt.cacheTimer = NULL;
    
    taosTmrCleanUp(gCtgMgmt.timer);
    gCtgMgmt.timer = NULL;
  }

  atomic_store_8((int8_t*)&gCtgMgmt.exit, true);

  if (!taosCheckCurrentInDll()) {
    (void)ctgClearCacheEnqueue(NULL, false, true, true, true);
    (void)taosThreadJoin(gCtgMgmt.updateThread, NULL);
  }

  taosHashCleanup(gCtgMgmt.pCluster);
  gCtgMgmt.pCluster = NULL;

  qInfo("catalog destroyed");
}
