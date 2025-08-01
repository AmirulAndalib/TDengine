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

#include "catalog.h"
#include "clientInt.h"
#include "clientLog.h"
#include "clientMonitor.h"
#include "scheduler.h"
#include "tglobal.h"
#include "trpc.h"

typedef struct {
  union {
    struct {
      SAppHbMgr *pAppHbMgr;
      int64_t    clusterId;
      int32_t    reqCnt;
      int8_t     connHbFlag;
    };
  };
} SHbParam;

SClientHbMgr clientHbMgr = {0};

static int32_t hbCreateThread();
static void    hbStopThread();
static int32_t hbUpdateUserAuthInfo(SAppHbMgr *pAppHbMgr, SUserAuthBatchRsp *batchRsp);

static int32_t hbMqHbReqHandle(SClientHbKey *connKey, void *param, SClientHbReq *req) { return 0; }

static int32_t hbMqHbRspHandle(SAppHbMgr *pAppHbMgr, SClientHbRsp *pRsp) { return 0; }

static int32_t hbProcessUserAuthInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog,
                                        SAppHbMgr *pAppHbMgr) {
  int32_t code = TSDB_CODE_SUCCESS;

  SUserAuthBatchRsp batchRsp = {0};
  if (tDeserializeSUserAuthBatchRsp(value, valueLen, &batchRsp) != 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  int32_t numOfBatchs = taosArrayGetSize(batchRsp.pArray);
  for (int32_t i = 0; i < numOfBatchs; ++i) {
    SGetUserAuthRsp *rsp = taosArrayGet(batchRsp.pArray, i);
    if (NULL == rsp) {
      code = terrno;
      goto _return;
    }
    tscDebug("hb to update user auth, user:%s, version:%d", rsp->user, rsp->version);

    TSC_ERR_JRET(catalogUpdateUserAuthInfo(pCatalog, rsp));
  }

  if (numOfBatchs > 0) {
    TSC_ERR_JRET(hbUpdateUserAuthInfo(pAppHbMgr, &batchRsp));
  }

  (void)atomic_val_compare_exchange_8(&pAppHbMgr->connHbFlag, 1, 2);

_return:
  taosArrayDestroy(batchRsp.pArray);
  return code;
}

static int32_t hbUpdateUserAuthInfo(SAppHbMgr *pAppHbMgr, SUserAuthBatchRsp *batchRsp) {
  int64_t clusterId = pAppHbMgr->pAppInstInfo->clusterId;
  for (int i = 0; i < TARRAY_SIZE(clientHbMgr.appHbMgrs); ++i) {
    SAppHbMgr *hbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (!hbMgr || hbMgr->pAppInstInfo->clusterId != clusterId) {
      continue;
    }

    SClientHbReq    *pReq = NULL;
    SGetUserAuthRsp *pRsp = NULL;
    while ((pReq = taosHashIterate(hbMgr->activeInfo, pReq))) {
      STscObj *pTscObj = (STscObj *)acquireTscObj(pReq->connKey.tscRid);
      if (!pTscObj) {
        continue;
      }

      if (!pRsp) {
        for (int32_t j = 0; j < TARRAY_SIZE(batchRsp->pArray); ++j) {
          SGetUserAuthRsp *rsp = TARRAY_GET_ELEM(batchRsp->pArray, j);
          if (0 == strncmp(rsp->user, pTscObj->user, TSDB_USER_LEN)) {
            pRsp = rsp;
            break;
          }
        }
        if (!pRsp) {
          releaseTscObj(pReq->connKey.tscRid);
          taosHashCancelIterate(hbMgr->activeInfo, pReq);
          break;
        }
      }

      if (pRsp->dropped == 1) {
        if (atomic_val_compare_exchange_8(&pTscObj->dropped, 0, 1) == 0) {
          if (pTscObj->userDroppedInfo.fp) {
            SPassInfo *dropInfo = &pTscObj->userDroppedInfo;
            if (dropInfo->fp) {
              (*dropInfo->fp)(dropInfo->param, NULL, TAOS_NOTIFY_USER_DROPPED);
            }
          }
        }
        releaseTscObj(pReq->connKey.tscRid);
        continue;
      }

      pTscObj->authVer = pRsp->version;

      if (pTscObj->sysInfo != pRsp->sysInfo) {
        tscDebug("update sysInfo of user %s from %" PRIi8 " to %" PRIi8 ", conn:%" PRIi64, pRsp->user,
                 pTscObj->sysInfo, pRsp->sysInfo, pTscObj->id);
        pTscObj->sysInfo = pRsp->sysInfo;
      }

      if (pTscObj->passInfo.fp) {
        SPassInfo *passInfo = &pTscObj->passInfo;
        int32_t    oldVer = atomic_load_32(&passInfo->ver);
        if (oldVer < pRsp->passVer) {
          atomic_store_32(&passInfo->ver, pRsp->passVer);
          if (passInfo->fp) {
            (*passInfo->fp)(passInfo->param, &pRsp->passVer, TAOS_NOTIFY_PASSVER);
          }
          tscDebug("update passVer of user %s from %d to %d, conn:%" PRIi64, pRsp->user, oldVer,
                   atomic_load_32(&passInfo->ver), pTscObj->id);
        }
      }

      if (pTscObj->whiteListInfo.fp) {
        SWhiteListInfo *whiteListInfo = &pTscObj->whiteListInfo;
        int64_t         oldVer = atomic_load_64(&whiteListInfo->ver);
        if (oldVer != pRsp->whiteListVer) {
          atomic_store_64(&whiteListInfo->ver, pRsp->whiteListVer);
          if (whiteListInfo->fp) {
            (*whiteListInfo->fp)(whiteListInfo->param, &pRsp->whiteListVer, TAOS_NOTIFY_WHITELIST_VER);
          }
          tscDebug("update whitelist version of user %s from %" PRId64 " to %" PRId64 ", conn:%" PRIi64, pRsp->user,
                   oldVer, atomic_load_64(&whiteListInfo->ver), pTscObj->id);
        }
      } else {
        // Need to update version information to prevent frequent fetching of authentication
        // information.
        SWhiteListInfo *whiteListInfo = &pTscObj->whiteListInfo;
        int64_t         oldVer = atomic_load_64(&whiteListInfo->ver);
        atomic_store_64(&whiteListInfo->ver, pRsp->whiteListVer);
        tscDebug("update whitelist version of user %s from %" PRId64 " to %" PRId64 ", conn:%" PRIi64, pRsp->user,
                 oldVer, atomic_load_64(&whiteListInfo->ver), pTscObj->id);
      }
      releaseTscObj(pReq->connKey.tscRid);
    }
  }
  return 0;
}

static int32_t hbGenerateVgInfoFromRsp(SDBVgInfo **pInfo, SUseDbRsp *rsp) {
  int32_t    code = 0;
  SDBVgInfo *vgInfo = taosMemoryCalloc(1, sizeof(SDBVgInfo));
  if (NULL == vgInfo) {
    return terrno;
  }

  vgInfo->vgVersion = rsp->vgVersion;
  vgInfo->stateTs = rsp->stateTs;
  vgInfo->flags = rsp->flags;
  vgInfo->hashMethod = rsp->hashMethod;
  vgInfo->hashPrefix = rsp->hashPrefix;
  vgInfo->hashSuffix = rsp->hashSuffix;
  vgInfo->vgHash = taosHashInit(rsp->vgNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (NULL == vgInfo->vgHash) {
    tscError("hash init[%d] failed", rsp->vgNum);
    code = terrno;
    goto _return;
  }

  for (int32_t j = 0; j < rsp->vgNum; ++j) {
    SVgroupInfo *pInfo = taosArrayGet(rsp->pVgroupInfos, j);
    if (taosHashPut(vgInfo->vgHash, &pInfo->vgId, sizeof(int32_t), pInfo, sizeof(SVgroupInfo)) != 0) {
      tscError("hash push failed, terrno:%d", terrno);
      code = terrno;
      goto _return;
    }
  }

_return:
  if (code) {
    taosHashCleanup(vgInfo->vgHash);
    taosMemoryFreeClear(vgInfo);
  }

  *pInfo = vgInfo;
  return code;
}

static int32_t hbProcessDBInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  SDbHbBatchRsp batchRsp = {0};
  if (tDeserializeSDbHbBatchRsp(value, valueLen, &batchRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    code = terrno;
    goto _return;
  }

  int32_t numOfBatchs = taosArrayGetSize(batchRsp.pArray);
  for (int32_t i = 0; i < numOfBatchs; ++i) {
    SDbHbRsp *rsp = taosArrayGet(batchRsp.pArray, i);
    if (NULL == rsp) {
      code = terrno;
      goto _return;
    }
    if (rsp->useDbRsp) {
      tscDebug("hb use db rsp, db:%s, vgVersion:%d, stateTs:%" PRId64 ", uid:%" PRIx64, rsp->useDbRsp->db,
               rsp->useDbRsp->vgVersion, rsp->useDbRsp->stateTs, rsp->useDbRsp->uid);

      if (rsp->useDbRsp->vgVersion < 0) {
        tscDebug("hb to remove db, db:%s", rsp->useDbRsp->db);
        code = catalogRemoveDB(pCatalog, rsp->useDbRsp->db, rsp->useDbRsp->uid);
      } else {
        SDBVgInfo *vgInfo = NULL;
        code = hbGenerateVgInfoFromRsp(&vgInfo, rsp->useDbRsp);
        if (TSDB_CODE_SUCCESS != code) {
          goto _return;
        }

        tscDebug("hb to update db vgInfo, db:%s", rsp->useDbRsp->db);

        TSC_ERR_JRET(catalogUpdateDBVgInfo(pCatalog, rsp->useDbRsp->db, rsp->useDbRsp->uid, vgInfo));

        if (IS_SYS_DBNAME(rsp->useDbRsp->db)) {
          code = hbGenerateVgInfoFromRsp(&vgInfo, rsp->useDbRsp);
          if (TSDB_CODE_SUCCESS != code) {
            goto _return;
          }

          TSC_ERR_JRET(catalogUpdateDBVgInfo(
              pCatalog, (rsp->useDbRsp->db[0] == 'i') ? TSDB_PERFORMANCE_SCHEMA_DB : TSDB_INFORMATION_SCHEMA_DB,
              rsp->useDbRsp->uid, vgInfo));
        }
      }
    }

    if (rsp->cfgRsp) {
      tscDebug("hb db cfg rsp, db:%s, cfgVersion:%d", rsp->cfgRsp->db, rsp->cfgRsp->cfgVersion);
      code = catalogUpdateDbCfg(pCatalog, rsp->cfgRsp->db, rsp->cfgRsp->dbId, rsp->cfgRsp);
      rsp->cfgRsp = NULL;
    }
    if (rsp->pTsmaRsp) {
      if (rsp->pTsmaRsp->pTsmas) {
        for (int32_t i = 0; i < rsp->pTsmaRsp->pTsmas->size; ++i) {
          STableTSMAInfo *pTsma = taosArrayGetP(rsp->pTsmaRsp->pTsmas, i);
          if (NULL == pTsma) {
            TSC_ERR_JRET(TSDB_CODE_OUT_OF_RANGE);
          }
          TSC_ERR_JRET(catalogAsyncUpdateTSMA(pCatalog, &pTsma, rsp->dbTsmaVersion));
        }
        taosArrayClear(rsp->pTsmaRsp->pTsmas);
      } else {
        TSC_ERR_JRET(catalogAsyncUpdateDbTsmaVersion(pCatalog, rsp->dbTsmaVersion, rsp->db, rsp->dbId));
      }
    }
  }

_return:

  tFreeSDbHbBatchRsp(&batchRsp);
  return code;
}

static int32_t hbProcessStbInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSTbHbRsp hbRsp = {0};
  if (tDeserializeSSTbHbRsp(value, valueLen, &hbRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfMeta = taosArrayGetSize(hbRsp.pMetaRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp *rsp = taosArrayGet(hbRsp.pMetaRsp, i);
    if (NULL == rsp) {
      code = terrno;
      goto _return;
    }
    if (rsp->numOfColumns < 0) {
      tscDebug("hb to remove stb, db:%s, stb:%s", rsp->dbFName, rsp->stbName);
      TSC_ERR_JRET(catalogRemoveStbMeta(pCatalog, rsp->dbFName, rsp->dbId, rsp->stbName, rsp->suid));
    } else {
      tscDebug("hb to update stb, db:%s, stb:%s", rsp->dbFName, rsp->stbName);
      if (rsp->pSchemas[0].colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
        tscError("invalid colId[%" PRIi16 "] for the first column in table meta rsp msg", rsp->pSchemas[0].colId);
        tFreeSSTbHbRsp(&hbRsp);
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      TSC_ERR_JRET(catalogAsyncUpdateTableMeta(pCatalog, rsp));
    }
  }

  int32_t numOfIndex = taosArrayGetSize(hbRsp.pIndexRsp);
  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp *rsp = taosArrayGet(hbRsp.pIndexRsp, i);
    if (NULL == rsp) {
      code = terrno;
      goto _return;
    }
    TSC_ERR_JRET(catalogUpdateTableIndex(pCatalog, rsp));
  }

_return:
  taosArrayDestroy(hbRsp.pIndexRsp);
  hbRsp.pIndexRsp = NULL;

  tFreeSSTbHbRsp(&hbRsp);
  return code;
}

static int32_t hbProcessDynViewRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  return catalogUpdateDynViewVer(pCatalog, (SDynViewVersion *)value);
}

static void hbFreeSViewMetaInRsp(void *p) {
  if (NULL == p || NULL == *(void **)p) {
    return;
  }
  SViewMetaRsp *pRsp = *(SViewMetaRsp **)p;
  tFreeSViewMetaRsp(pRsp);
  taosMemoryFreeClear(pRsp);
}

static int32_t hbProcessViewInfoRsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = TSDB_CODE_SUCCESS;

  SViewHbRsp hbRsp = {0};
  if (tDeserializeSViewHbRsp(value, valueLen, &hbRsp) != 0) {
    taosArrayDestroyEx(hbRsp.pViewRsp, hbFreeSViewMetaInRsp);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfMeta = taosArrayGetSize(hbRsp.pViewRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *rsp = taosArrayGetP(hbRsp.pViewRsp, i);
    if (NULL == rsp) {
      code = terrno;
      goto _return;
    }
    if (rsp->numOfCols < 0) {
      tscDebug("hb to remove view, db:%s, view:%s", rsp->dbFName, rsp->name);
      code = catalogRemoveViewMeta(pCatalog, rsp->dbFName, rsp->dbId, rsp->name, rsp->viewId);
      tFreeSViewMetaRsp(rsp);
      taosMemoryFreeClear(rsp);
    } else {
      tscDebug("hb to update view, db:%s, view:%s", rsp->dbFName, rsp->name);
      code = catalogUpdateViewMeta(pCatalog, rsp);
    }
    TSC_ERR_JRET(code);
  }

_return:
  taosArrayDestroy(hbRsp.pViewRsp);
  return code;
}

static int32_t hbprocessTSMARsp(void *value, int32_t valueLen, struct SCatalog *pCatalog) {
  int32_t code = 0;

  STSMAHbRsp hbRsp = {0};
  if (tDeserializeTSMAHbRsp(value, valueLen, &hbRsp)) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int32_t numOfTsma = taosArrayGetSize(hbRsp.pTsmas);
  for (int32_t i = 0; i < numOfTsma; ++i) {
    STableTSMAInfo *pTsmaInfo = taosArrayGetP(hbRsp.pTsmas, i);

    if (!pTsmaInfo->pFuncs) {
      tscDebug("hb to remove tsma:%s.%s", pTsmaInfo->dbFName, pTsmaInfo->name);
      code = catalogRemoveTSMA(pCatalog, pTsmaInfo);
      tFreeAndClearTableTSMAInfo(pTsmaInfo);
    } else {
      tscDebug("hb to update tsma:%s.%s", pTsmaInfo->dbFName, pTsmaInfo->name);
      code = catalogUpdateTSMA(pCatalog, &pTsmaInfo);
      tFreeAndClearTableTSMAInfo(pTsmaInfo);
    }
    TSC_ERR_JRET(code);
  }

_return:
  taosArrayDestroy(hbRsp.pTsmas);
  return code;
}

static void hbProcessQueryRspKvs(int32_t kvNum, SArray *pKvs, struct SCatalog *pCatalog, SAppHbMgr *pAppHbMgr) {
  for (int32_t i = 0; i < kvNum; ++i) {
    SKv *kv = taosArrayGet(pKvs, i);
    if (NULL == kv) {
      tscError("invalid hb kv, idx:%d", i);
      continue;
    }
    switch (kv->key) {
      case HEARTBEAT_KEY_USER_AUTHINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb user auth info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        if (TSDB_CODE_SUCCESS != hbProcessUserAuthInfoRsp(kv->value, kv->valueLen, pCatalog, pAppHbMgr)) {
          tscError("process user auth info response faild, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        break;
      }
      case HEARTBEAT_KEY_DBINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb db info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        if (TSDB_CODE_SUCCESS != hbProcessDBInfoRsp(kv->value, kv->valueLen, pCatalog)) {
          tscError("process db info response faild, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        break;
      }
      case HEARTBEAT_KEY_STBINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid hb stb info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        if (TSDB_CODE_SUCCESS != hbProcessStbInfoRsp(kv->value, kv->valueLen, pCatalog)) {
          tscError("process stb info response faild, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        break;
      }
#ifdef TD_ENTERPRISE
      case HEARTBEAT_KEY_DYN_VIEW: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid dyn view info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        if (TSDB_CODE_SUCCESS != hbProcessDynViewRsp(kv->value, kv->valueLen, pCatalog)) {
          tscError("Process dyn view response failed, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        break;
      }
      case HEARTBEAT_KEY_VIEWINFO: {
        if (kv->valueLen <= 0 || NULL == kv->value) {
          tscError("invalid view info, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        if (TSDB_CODE_SUCCESS != hbProcessViewInfoRsp(kv->value, kv->valueLen, pCatalog)) {
          tscError("Process view info response failed, len:%d, value:%p", kv->valueLen, kv->value);
          break;
        }
        break;
      }
#endif
      case HEARTBEAT_KEY_TSMA: {
        if (kv->valueLen <= 0 || !kv->value) {
          tscError("Invalid tsma info, len:%d, value:%p", kv->valueLen, kv->value);
        }
        if (TSDB_CODE_SUCCESS != hbprocessTSMARsp(kv->value, kv->valueLen, pCatalog)) {
          tscError("Process tsma info response failed, len:%d, value:%p", kv->valueLen, kv->value);
        }
        break;
      }
      default:
        tscError("invalid hb key type:%d", kv->key);
        break;
    }
  }
}

static int32_t hbQueryHbRspHandle(SAppHbMgr *pAppHbMgr, SClientHbRsp *pRsp) {
  SClientHbReq *pReq = taosHashAcquire(pAppHbMgr->activeInfo, &pRsp->connKey, sizeof(SClientHbKey));
  if (NULL == pReq) {
    tscWarn("pReq to get activeInfo, may be dropped, refId:%" PRIx64 ", type:%d", pRsp->connKey.tscRid,
            pRsp->connKey.connType);
    return TSDB_CODE_SUCCESS;
  }

  if (pRsp->query) {
    STscObj *pTscObj = (STscObj *)acquireTscObj(pRsp->connKey.tscRid);
    if (NULL == pTscObj) {
      tscDebug("tscObj rid %" PRIx64 " not exist", pRsp->connKey.tscRid);
    } else {
      if (pRsp->query->totalDnodes > 1) {
        SEpSet  originEpset = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
        if (!isEpsetEqual(&originEpset, &pRsp->query->epSet)) {
          SEpSet *pOrig = &originEpset;
          SEp    *pOrigEp = &pOrig->eps[pOrig->inUse];
          SEp    *pNewEp = &pRsp->query->epSet.eps[pRsp->query->epSet.inUse];
          tscDebug("mnode epset updated from %d/%d=>%s:%d to %d/%d=>%s:%d in hb", pOrig->inUse, pOrig->numOfEps,
                   pOrigEp->fqdn, pOrigEp->port, pRsp->query->epSet.inUse, pRsp->query->epSet.numOfEps, pNewEp->fqdn,
                   pNewEp->port);

          updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, &pRsp->query->epSet);
        }
      }

      pTscObj->pAppInfo->totalDnodes = pRsp->query->totalDnodes;
      pTscObj->pAppInfo->onlineDnodes = pRsp->query->onlineDnodes;
      pTscObj->connId = pRsp->query->connId;
      tscTrace("connId:%u, hb rsp, dnodes %d/%d", pTscObj->connId, pTscObj->pAppInfo->onlineDnodes,
               pTscObj->pAppInfo->totalDnodes);

      if (pRsp->query->killRid) {
        tscDebug("QID:0x%" PRIx64 ", need to be killed now", pRsp->query->killRid);
        SRequestObj *pRequest = acquireRequest(pRsp->query->killRid);
        if (NULL == pRequest) {
          tscDebug("QID:0x%" PRIx64 ", not exist to kill", pRsp->query->killRid);
        } else {
          taos_stop_query((TAOS_RES *)pRequest);
          (void)releaseRequest(pRsp->query->killRid);
        }
      }

      if (pRsp->query->killConnection) {
        taos_close_internal(pTscObj);
      }

      if (pRsp->query->pQnodeList) {
        if (TSDB_CODE_SUCCESS != updateQnodeList(pTscObj->pAppInfo, pRsp->query->pQnodeList)) {
          tscWarn("update qnode list failed");
        }
      }

      releaseTscObj(pRsp->connKey.tscRid);
    }
  }

  int32_t kvNum = pRsp->info ? taosArrayGetSize(pRsp->info) : 0;

  tscDebug("hb got %d rsp kv", kvNum);

  if (kvNum > 0) {
    struct SCatalog *pCatalog = NULL;
    int32_t          code = catalogGetHandle(pReq->clusterId, &pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      tscWarn("catalogGetHandle failed, clusterId:0x%" PRIx64 ", error:%s", pReq->clusterId, tstrerror(code));
    } else {
      hbProcessQueryRspKvs(kvNum, pRsp->info, pCatalog, pAppHbMgr);
    }
  }

  taosHashRelease(pAppHbMgr->activeInfo, pReq);

  return TSDB_CODE_SUCCESS;
}

static int32_t hbAsyncCallBack(void *param, SDataBuf *pMsg, int32_t code) {
  if (0 == atomic_load_8(&clientHbMgr.inited)) {
    goto _return;
  }

  static int32_t    emptyRspNum = 0;
  int32_t           idx = *(int32_t *)param;
  SClientHbBatchRsp pRsp = {0};
  if (TSDB_CODE_SUCCESS == code) {
    code = tDeserializeSClientHbBatchRsp(pMsg->pData, pMsg->len, &pRsp);
    if (TSDB_CODE_SUCCESS != code) {
      tscError("deserialize hb rsp failed");
    }
    int32_t now = taosGetTimestampSec();
    int32_t delta = abs(now - pRsp.svrTimestamp);
    if (delta > timestampDeltaLimit) {
      code = TSDB_CODE_TIME_UNSYNCED;
      tscError("time diff:%ds is too big", delta);
    }
  }

  int32_t rspNum = taosArrayGetSize(pRsp.rsps);

  (void)taosThreadMutexLock(&clientHbMgr.lock);

  SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, idx);
  if (pAppHbMgr == NULL) {
    (void)taosThreadMutexUnlock(&clientHbMgr.lock);
    tscError("appHbMgr not exist, idx:%d", idx);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    tFreeClientHbBatchRsp(&pRsp);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  SAppInstInfo *pInst = pAppHbMgr->pAppInstInfo;

  if (code != 0) {
    pInst->onlineDnodes = pInst->totalDnodes ? 0 : -1;
    tscDebug("hb rsp error %s, update server status %d/%d", tstrerror(code), pInst->onlineDnodes, pInst->totalDnodes);
    (void)taosThreadMutexUnlock(&clientHbMgr.lock);
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    tFreeClientHbBatchRsp(&pRsp);
    return code;
  }

  pInst->serverCfg.monitorParas = pRsp.monitorParas;
  pInst->serverCfg.enableAuditDelete = pRsp.enableAuditDelete;
  pInst->serverCfg.enableStrongPass = pRsp.enableStrongPass;
  tsEnableStrongPassword = pInst->serverCfg.enableStrongPass;
  tscDebug("monitor paras from hb, clusterId:0x%" PRIx64 ", threshold:%d scope:%d", pInst->clusterId,
           pRsp.monitorParas.tsSlowLogThreshold, pRsp.monitorParas.tsSlowLogScope);

  if (rspNum) {
    tscDebug("hb got %d rsp, %d empty rsp received before", rspNum,
             atomic_val_compare_exchange_32(&emptyRspNum, emptyRspNum, 0));
  } else {
    (void)atomic_add_fetch_32(&emptyRspNum, 1);
  }

  for (int32_t i = 0; i < rspNum; ++i) {
    SClientHbRsp *rsp = taosArrayGet(pRsp.rsps, i);
    code = (*clientHbMgr.rspHandle[rsp->connKey.connType])(pAppHbMgr, rsp);
    if (code) {
      break;
    }
  }

  (void)taosThreadMutexUnlock(&clientHbMgr.lock);

  tFreeClientHbBatchRsp(&pRsp);

_return:
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  return code;
}

int32_t hbBuildQueryDesc(SQueryHbReqBasic *hbBasic, STscObj *pObj) {
  int64_t    now = taosGetTimestampUs();
  SQueryDesc desc = {0};
  int32_t    code = 0;

  void *pIter = taosHashIterate(pObj->pRequests, NULL);
  while (pIter != NULL) {
    int64_t     *rid = pIter;
    SRequestObj *pRequest = acquireRequest(*rid);
    if (NULL == pRequest) {
      pIter = taosHashIterate(pObj->pRequests, pIter);
      continue;
    }

    if (pRequest->killed || 0 == pRequest->body.queryJob) {
      (void)releaseRequest(*rid);
      pIter = taosHashIterate(pObj->pRequests, pIter);
      continue;
    }

    tstrncpy(desc.sql, pRequest->sqlstr, sizeof(desc.sql));
    desc.stime = pRequest->metric.start / 1000;
    desc.queryId = pRequest->requestId;
    desc.useconds = now - pRequest->metric.start;
    desc.reqRid = pRequest->self;
    desc.stableQuery = pRequest->stableQuery;
    desc.isSubQuery = pRequest->isSubReq;
    code = taosGetFqdn(desc.fqdn);
    if (TSDB_CODE_SUCCESS != code) {
      (void)releaseRequest(*rid);
      tscError("get fqdn failed");
      return TSDB_CODE_FAILED;
    }
    desc.subPlanNum = pRequest->body.subplanNum;

    if (desc.subPlanNum) {
      desc.subDesc = taosArrayInit(desc.subPlanNum, sizeof(SQuerySubDesc));
      if (NULL == desc.subDesc) {
        (void)releaseRequest(*rid);
        return terrno;
      }

      code = schedulerGetTasksStatus(pRequest->body.queryJob, desc.subDesc);
      if (code) {
        taosArrayDestroy(desc.subDesc);
        desc.subDesc = NULL;
      }
      desc.subPlanNum = taosArrayGetSize(desc.subDesc);
    } else {
      desc.subDesc = NULL;
    }

    (void)releaseRequest(*rid);
    if (NULL == taosArrayPush(hbBasic->queryDesc, &desc)) {
      taosArrayDestroy(desc.subDesc);
      return terrno;
    }

    pIter = taosHashIterate(pObj->pRequests, pIter);
  }

  return code;
}

int32_t hbGetQueryBasicInfo(SClientHbKey *connKey, SClientHbReq *req) {
  STscObj *pTscObj = (STscObj *)acquireTscObj(connKey->tscRid);
  if (NULL == pTscObj) {
    tscWarn("tscObj rid 0x%" PRIx64 " not exist", connKey->tscRid);
    return terrno;
  }

  SQueryHbReqBasic *hbBasic = (SQueryHbReqBasic *)taosMemoryCalloc(1, sizeof(SQueryHbReqBasic));
  if (NULL == hbBasic) {
    tscError("calloc %d failed", (int32_t)sizeof(SQueryHbReqBasic));
    releaseTscObj(connKey->tscRid);
    return terrno;
  }

  hbBasic->connId = pTscObj->connId;

  int32_t numOfQueries = pTscObj->pRequests ? taosHashGetSize(pTscObj->pRequests) : 0;
  if (numOfQueries <= 0) {
    req->query = hbBasic;
    releaseTscObj(connKey->tscRid);
    tscDebug("no queries on connection");
    return TSDB_CODE_SUCCESS;
  }

  hbBasic->queryDesc = taosArrayInit(numOfQueries, sizeof(SQueryDesc));
  if (NULL == hbBasic->queryDesc) {
    tscWarn("taosArrayInit %d queryDesc failed", numOfQueries);
    releaseTscObj(connKey->tscRid);
    taosMemoryFree(hbBasic);
    return terrno;
  }

  int32_t code = hbBuildQueryDesc(hbBasic, pTscObj);
  if (code) {
    releaseTscObj(connKey->tscRid);
    if (hbBasic->queryDesc) {
      taosArrayDestroyEx(hbBasic->queryDesc, tFreeClientHbQueryDesc);
    }
    taosMemoryFree(hbBasic);
    return code;
  }

  req->query = hbBasic;
  releaseTscObj(connKey->tscRid);

  return TSDB_CODE_SUCCESS;
}

static int32_t hbGetUserAuthInfo(SClientHbKey *connKey, SHbParam *param, SClientHbReq *req) {
  STscObj *pTscObj = (STscObj *)acquireTscObj(connKey->tscRid);
  if (!pTscObj) {
    tscWarn("tscObj rid 0x%" PRIx64 " not exist", connKey->tscRid);
    return terrno;
  }

  int32_t code = 0;

  SKv  kv = {.key = HEARTBEAT_KEY_USER_AUTHINFO};
  SKv *pKv = NULL;
  if ((pKv = taosHashGet(req->info, &kv.key, sizeof(kv.key)))) {
    int32_t           userNum = pKv->valueLen / sizeof(SUserAuthVersion);
    SUserAuthVersion *userAuths = (SUserAuthVersion *)pKv->value;
    for (int32_t i = 0; i < userNum; ++i) {
      SUserAuthVersion *pUserAuth = userAuths + i;
      // both key and user exist, update version
      if (strncmp(pUserAuth->user, pTscObj->user, TSDB_USER_LEN) == 0) {
        pUserAuth->version = htonl(-1);  // force get userAuthInfo
        goto _return;
      }
    }
    // key exists, user not exist, append user
    SUserAuthVersion *qUserAuth =
        (SUserAuthVersion *)taosMemoryRealloc(pKv->value, (userNum + 1) * sizeof(SUserAuthVersion));
    if (qUserAuth) {
      tstrncpy((qUserAuth + userNum)->user, pTscObj->user, TSDB_USER_LEN);
      (qUserAuth + userNum)->version = htonl(-1);  // force get userAuthInfo
      pKv->value = qUserAuth;
      pKv->valueLen += sizeof(SUserAuthVersion);
    } else {
      code = terrno;
    }
    goto _return;
  }

  // key/user not exist, add user
  SUserAuthVersion *user = taosMemoryMalloc(sizeof(SUserAuthVersion));
  if (!user) {
    code = terrno;
    goto _return;
  }
  tstrncpy(user->user, pTscObj->user, TSDB_USER_LEN);
  user->version = htonl(-1);  // force get userAuthInfo
  kv.valueLen = sizeof(SUserAuthVersion);
  kv.value = user;

  tscDebug("hb got user auth info, valueLen:%d, user:%s, authVer:%d, tscRid:%" PRIi64, kv.valueLen, user->user,
           pTscObj->authVer, connKey->tscRid);

  if (!req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
    if (NULL == req->info) {
      code = terrno;
      goto _return;
    }
  }

  if (taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv)) != 0) {
    taosMemoryFree(user);
    code = terrno ? terrno : TSDB_CODE_APP_ERROR;
    goto _return;
  }

_return:
  releaseTscObj(connKey->tscRid);
  if (code) {
    tscError("hb got user auth info failed since %s", tstrerror(code));
  }

  return code;
}

int32_t hbGetExpiredUserInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SUserAuthVersion *users = NULL;
  uint32_t          userNum = 0;
  int32_t           code = 0;

  code = catalogGetExpiredUsers(pCatalog, &users, &userNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (userNum <= 0) {
    taosMemoryFree(users);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < userNum; ++i) {
    SUserAuthVersion *user = &users[i];
    user->version = htonl(user->version);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_USER_AUTHINFO,
      .valueLen = sizeof(SUserAuthVersion) * userNum,
      .value = users,
  };

  tscDebug("hb got %d expired users, valueLen:%d", userNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
    if (NULL == req->info) {
      taosMemoryFree(users);
      return terrno;
    }
  }

  code = taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(users);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetExpiredDBInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SDbCacheInfo *dbs = NULL;
  uint32_t      dbNum = 0;
  int32_t       code = 0;

  code = catalogGetExpiredDBs(pCatalog, &dbs, &dbNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (dbNum <= 0) {
    taosMemoryFree(dbs);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < dbNum; ++i) {
    SDbCacheInfo *db = &dbs[i];
    tscDebug("the %dth expired db:%s, dbId:%" PRId64
             ", vgVersion:%d, cfgVersion:%d, numOfTable:%d, startTs:%" PRId64,
             i, db->dbFName, db->dbId, db->vgVersion, db->cfgVersion, db->numOfTable, db->stateTs);

    db->dbId = htobe64(db->dbId);
    db->vgVersion = htonl(db->vgVersion);
    db->cfgVersion = htonl(db->cfgVersion);
    db->numOfTable = htonl(db->numOfTable);
    db->stateTs = htobe64(db->stateTs);
    db->tsmaVersion = htonl(db->tsmaVersion);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_DBINFO,
      .valueLen = sizeof(SDbCacheInfo) * dbNum,
      .value = dbs,
  };

  tscDebug("hb got %d expired db, valueLen:%d", dbNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
    if (NULL == req->info) {
      taosMemoryFree(dbs);
      return terrno;
    }
  }

  code = taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(dbs);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetExpiredStbInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SSTableVersion *stbs = NULL;
  uint32_t        stbNum = 0;
  int32_t         code = 0;

  code = catalogGetExpiredSTables(pCatalog, &stbs, &stbNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  if (stbNum <= 0) {
    taosMemoryFree(stbs);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < stbNum; ++i) {
    SSTableVersion *stb = &stbs[i];
    stb->suid = htobe64(stb->suid);
    stb->sversion = htonl(stb->sversion);
    stb->tversion = htonl(stb->tversion);
    stb->smaVer = htonl(stb->smaVer);
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_STBINFO,
      .valueLen = sizeof(SSTableVersion) * stbNum,
      .value = stbs,
  };

  tscDebug("hb got %d expired stb, valueLen:%d", stbNum, kv.valueLen);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
    if (NULL == req->info) {
      taosMemoryFree(stbs);
      return terrno;
    }
  }

  code = taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(stbs);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hbGetExpiredViewInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *req) {
  SViewVersion    *views = NULL;
  uint32_t         viewNum = 0;
  int32_t          code = 0;
  SDynViewVersion *pDynViewVer = NULL;

  TSC_ERR_JRET(catalogGetExpiredViews(pCatalog, &views, &viewNum, &pDynViewVer));

  if (viewNum <= 0) {
    taosMemoryFree(views);
    taosMemoryFree(pDynViewVer);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < viewNum; ++i) {
    SViewVersion *view = &views[i];
    view->dbId = htobe64(view->dbId);
    view->viewId = htobe64(view->viewId);
    view->version = htonl(view->version);
  }

  tscDebug("hb got %u expired view, valueLen:%lu", viewNum, sizeof(SViewVersion) * viewNum);

  if (NULL == req->info) {
    req->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
    if (NULL == req->info) {
      TSC_ERR_JRET(terrno);
    }
  }

  SKv kv = {
      .key = HEARTBEAT_KEY_DYN_VIEW,
      .valueLen = sizeof(SDynViewVersion),
      .value = pDynViewVer,
  };

  TSC_ERR_JRET(taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv)));

  kv.key = HEARTBEAT_KEY_VIEWINFO;
  kv.valueLen = sizeof(SViewVersion) * viewNum;
  kv.value = views;

  TSC_ERR_JRET(taosHashPut(req->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv)));
  return TSDB_CODE_SUCCESS;
_return:
  taosMemoryFree(views);
  taosMemoryFree(pDynViewVer);
  return code;
}

int32_t hbGetExpiredTSMAInfo(SClientHbKey *connKey, struct SCatalog *pCatalog, SClientHbReq *pReq) {
  int32_t       code = 0;
  uint32_t      tsmaNum = 0;
  STSMAVersion *tsmas = NULL;

  code = catalogGetExpiredTsmas(pCatalog, &tsmas, &tsmaNum);
  if (code) {
    taosMemoryFree(tsmas);
    return code;
  }

  if (tsmaNum <= 0) {
    taosMemoryFree(tsmas);
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t i = 0; i < tsmaNum; ++i) {
    STSMAVersion *tsma = &tsmas[i];
    tsma->dbId = htobe64(tsma->dbId);
    tsma->tsmaId = htobe64(tsma->tsmaId);
    tsma->version = htonl(tsma->version);
  }

  tscDebug("hb got %d expred tsmas, valueLen:%lu", tsmaNum, sizeof(STSMAVersion) * tsmaNum);

  if (!pReq->info) {
    pReq->info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);
    if (!pReq->info) {
      taosMemoryFree(tsmas);
      return terrno;
    }
  }

  SKv kv = {.key = HEARTBEAT_KEY_TSMA, .valueLen = sizeof(STSMAVersion) * tsmaNum, .value = tsmas};
  code = taosHashPut(pReq->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(tsmas);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t hbGetAppInfo(int64_t clusterId, SClientHbReq *req) {
  SAppHbReq *pApp = taosHashGet(clientHbMgr.appSummary, &clusterId, sizeof(clusterId));
  if (NULL != pApp) {
    (void)memcpy(&req->app, pApp, sizeof(*pApp));
  } else {
    (void)memset(&req->app.summary, 0, sizeof(req->app.summary));
    req->app.pid = taosGetPId();
    req->app.appId = clientHbMgr.appId;
    TSC_ERR_RET(taosGetAppName(req->app.name, NULL));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t hbQueryHbReqHandle(SClientHbKey *connKey, void *param, SClientHbReq *req) {
  int32_t   code = 0;
  SHbParam *hbParam = (SHbParam *)param;
  SCatalog *pCatalog = NULL;

  code = hbGetQueryBasicInfo(connKey, req);
  if (code != TSDB_CODE_SUCCESS) {
    tscWarn("hbGetQueryBasicInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
    return code;
  }

  if (hbParam->reqCnt == 0) {
    code = catalogGetHandle(hbParam->clusterId, &pCatalog);
    if (code != TSDB_CODE_SUCCESS) {
      tscWarn("catalogGetHandle failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }

    code = hbGetAppInfo(hbParam->clusterId, req);
    if (TSDB_CODE_SUCCESS != code) {
      tscWarn("getAppInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }

    if (!taosHashGet(clientHbMgr.appHbHash, &hbParam->clusterId, sizeof(hbParam->clusterId))) {
      code = hbGetExpiredUserInfo(connKey, pCatalog, req);
      if (TSDB_CODE_SUCCESS != code) {
        tscWarn("hbGetExpiredUserInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
        return code;
      }
      if (clientHbMgr.appHbHash) {
        code = taosHashPut(clientHbMgr.appHbHash, &hbParam->clusterId, sizeof(uint64_t), NULL, 0);
        if (TSDB_CODE_SUCCESS != code) {
          tscWarn("hbQueryHbReqHandle put clusterId failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId,
                  tstrerror(code));
          return code;
        }
      }
    }

    // invoke after hbGetExpiredUserInfo
    if (2 != atomic_load_8(&hbParam->pAppHbMgr->connHbFlag)) {
      code = hbGetUserAuthInfo(connKey, hbParam, req);
      if (TSDB_CODE_SUCCESS != code) {
        tscWarn("hbGetUserAuthInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
        return code;
      }
      atomic_store_8(&hbParam->pAppHbMgr->connHbFlag, 1);
    }

    code = hbGetExpiredDBInfo(connKey, pCatalog, req);
    if (TSDB_CODE_SUCCESS != code) {
      tscWarn("hbGetExpiredDBInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }

    code = hbGetExpiredStbInfo(connKey, pCatalog, req);
    if (TSDB_CODE_SUCCESS != code) {
      tscWarn("hbGetExpiredStbInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }

#ifdef TD_ENTERPRISE
    code = hbGetExpiredViewInfo(connKey, pCatalog, req);
    if (TSDB_CODE_SUCCESS != code) {
      tscWarn("hbGetExpiredViewInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }
#endif
    code = hbGetExpiredTSMAInfo(connKey, pCatalog, req);
    if (TSDB_CODE_SUCCESS != code) {
      tscWarn("hbGetExpiredTSMAInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }
  } else {
    code = hbGetAppInfo(hbParam->clusterId, req);
    if (TSDB_CODE_SUCCESS != code) {
      tscWarn("hbGetAppInfo failed, clusterId:0x%" PRIx64 ", error:%s", hbParam->clusterId, tstrerror(code));
      return code;
    }
  }

  ++hbParam->reqCnt;  // success to get catalog info

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE void hbMgrInitHandle() {
  // init all handle
  clientHbMgr.reqHandle[CONN_TYPE__QUERY] = hbQueryHbReqHandle;
  clientHbMgr.reqHandle[CONN_TYPE__TMQ] = hbMqHbReqHandle;

  clientHbMgr.rspHandle[CONN_TYPE__QUERY] = hbQueryHbRspHandle;
  clientHbMgr.rspHandle[CONN_TYPE__TMQ] = hbMqHbRspHandle;
}

int32_t hbGatherAllInfo(SAppHbMgr *pAppHbMgr, SClientHbBatchReq **pBatchReq) {
  *pBatchReq = taosMemoryCalloc(1, sizeof(SClientHbBatchReq));
  if (pBatchReq == NULL) {
    return terrno;
  }
  int32_t connKeyCnt = atomic_load_32(&pAppHbMgr->connKeyCnt);
  (*pBatchReq)->reqs = taosArrayInit(connKeyCnt, sizeof(SClientHbReq));
  if (!(*pBatchReq)->reqs) {
    tFreeClientHbBatchReq(*pBatchReq);
    return terrno;
  }

  int64_t  maxIpWhiteVer = 0;
  void    *pIter = NULL;
  SHbParam param = {0};
  while ((pIter = taosHashIterate(pAppHbMgr->activeInfo, pIter))) {
    SClientHbReq *pOneReq = pIter;
    SClientHbKey *connKey = &pOneReq->connKey;
    STscObj      *pTscObj = (STscObj *)acquireTscObj(connKey->tscRid);

    if (!pTscObj || atomic_load_8(&pTscObj->dropped) == 1) {
      if (pTscObj) releaseTscObj(connKey->tscRid);
      continue;
    }

    tstrncpy(pOneReq->userApp, pTscObj->optionInfo.userApp, sizeof(pOneReq->userApp));
    pOneReq->userIp = pTscObj->optionInfo.userIp;
    pOneReq->userDualIp = pTscObj->optionInfo.userDualIp;
    pOneReq = taosArrayPush((*pBatchReq)->reqs, pOneReq);
    if (NULL == pOneReq) {
      releaseTscObj(connKey->tscRid);
      continue;
    }

    switch (connKey->connType) {
      case CONN_TYPE__QUERY: {
        if (param.clusterId == 0) {
          // init
          param.clusterId = pOneReq->clusterId;
          param.pAppHbMgr = pAppHbMgr;
          param.connHbFlag = atomic_load_8(&pAppHbMgr->connHbFlag);
        }
        break;
      }
      default:
        break;
    }
    if (clientHbMgr.reqHandle[connKey->connType]) {
      int32_t code = (*clientHbMgr.reqHandle[connKey->connType])(connKey, &param, pOneReq);
      if (code) {
        tscWarn("hbGatherAllInfo failed since %s, tscRid:%" PRIi64 ", connType:%" PRIi8, tstrerror(code),
                connKey->tscRid, connKey->connType);
      }
    }

    int64_t ver = atomic_load_64(&pTscObj->whiteListInfo.ver);
    maxIpWhiteVer = TMAX(maxIpWhiteVer, ver);
    releaseTscObj(connKey->tscRid);
  }
  (*pBatchReq)->ipWhiteListVer = maxIpWhiteVer;

  return TSDB_CODE_SUCCESS;
}

void hbThreadFuncUnexpectedStopped(void) { atomic_store_8(&clientHbMgr.threadStop, 2); }

void hbMergeSummary(SAppClusterSummary *dst, SAppClusterSummary *src) {
  dst->numOfInsertsReq += src->numOfInsertsReq;
  dst->numOfInsertRows += src->numOfInsertRows;
  dst->insertElapsedTime += src->insertElapsedTime;
  dst->insertBytes += src->insertBytes;
  dst->fetchBytes += src->fetchBytes;
  dst->queryElapsedTime += src->queryElapsedTime;
  dst->numOfSlowQueries += src->numOfSlowQueries;
  dst->totalRequests += src->totalRequests;
  dst->currentRequests += src->currentRequests;
}

int32_t hbGatherAppInfo(void) {
  SAppHbReq req = {0};
  int32_t   code = TSDB_CODE_SUCCESS;
  int       sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
  if (sz > 0) {
    req.pid = taosGetPId();
    req.appId = clientHbMgr.appId;
    TSC_ERR_RET(taosGetAppName(req.name, NULL));
  }

  taosHashClear(clientHbMgr.appSummary);

  for (int32_t i = 0; i < sz; ++i) {
    SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (pAppHbMgr == NULL) continue;

    int64_t    clusterId = pAppHbMgr->pAppInstInfo->clusterId;
    SAppHbReq *pApp = taosHashGet(clientHbMgr.appSummary, &clusterId, sizeof(clusterId));
    if (NULL == pApp) {
      (void)memcpy(&req.summary, &pAppHbMgr->pAppInstInfo->summary, sizeof(req.summary));
      req.startTime = pAppHbMgr->startTime;
      TSC_ERR_RET(taosHashPut(clientHbMgr.appSummary, &clusterId, sizeof(clusterId), &req, sizeof(req)));
    } else {
      if (pAppHbMgr->startTime < pApp->startTime) {
        pApp->startTime = pAppHbMgr->startTime;
      }

      hbMergeSummary(&pApp->summary, &pAppHbMgr->pAppInstInfo->summary);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void *hbThreadFunc(void *param) {
  setThreadName("hb");
#ifdef WINDOWS
  if (taosCheckCurrentInDll()) {
    atexit(hbThreadFuncUnexpectedStopped);
  }
#endif
  while (1) {
    if (1 == clientHbMgr.threadStop) {
      break;
    }

    if (TSDB_CODE_SUCCESS != taosThreadMutexLock(&clientHbMgr.lock)) {
      tscError("taosThreadMutexLock failed");
      return NULL;
    }

    int sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
    if (sz > 0) {
      if (TSDB_CODE_SUCCESS != hbGatherAppInfo()) {
        tscError("hbGatherAppInfo failed");
        return NULL;
      }
      if (sz > 1 && !clientHbMgr.appHbHash) {
        clientHbMgr.appHbHash = taosHashInit(0, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_NO_LOCK);
        if (NULL == clientHbMgr.appHbHash) {
          tscError("taosHashInit failed");
          return NULL;
        }
      }
      taosHashClear(clientHbMgr.appHbHash);
    }

    for (int i = 0; i < sz; i++) {
      SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, i);
      if (pAppHbMgr == NULL) {
        continue;
      }

      int32_t connCnt = atomic_load_32(&pAppHbMgr->connKeyCnt);
      if (connCnt == 0) {
        continue;
      }
      SClientHbBatchReq *pReq = NULL;
      int32_t            code = hbGatherAllInfo(pAppHbMgr, &pReq);
      if (TSDB_CODE_SUCCESS != code || taosArrayGetP(clientHbMgr.appHbMgrs, i) == NULL) {
        terrno = code ? code : TSDB_CODE_OUT_OF_RANGE;
        tFreeClientHbBatchReq(pReq);
        continue;
      }
      int tlen = tSerializeSClientHbBatchReq(NULL, 0, pReq);
      if (tlen == -1) {
        tFreeClientHbBatchReq(pReq);
        break;
      }
      void *buf = taosMemoryMalloc(tlen);
      if (buf == NULL) {
        tFreeClientHbBatchReq(pReq);
        // hbClearReqInfo(pAppHbMgr);
        break;
      }

      if (tSerializeSClientHbBatchReq(buf, tlen, pReq) == -1) {
        tFreeClientHbBatchReq(pReq);
        taosMemoryFree(buf);
        break;
      }
      SMsgSendInfo *pInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));

      if (pInfo == NULL) {
        tFreeClientHbBatchReq(pReq);
        // hbClearReqInfo(pAppHbMgr);
        taosMemoryFree(buf);
        break;
      }
      pInfo->fp = hbAsyncCallBack;
      pInfo->msgInfo.pData = buf;
      pInfo->msgInfo.len = tlen;
      pInfo->msgType = TDMT_MND_HEARTBEAT;
      pInfo->param = taosMemoryMalloc(sizeof(int32_t));
      if (pInfo->param  == NULL) {
        tFreeClientHbBatchReq(pReq);
        // hbClearReqInfo(pAppHbMgr);
        taosMemoryFree(buf);
        taosMemoryFree(pInfo);
        break;
      }
      *(int32_t *)pInfo->param = i;
      pInfo->paramFreeFp = taosAutoMemoryFree;
      pInfo->requestId = generateRequestId();
      pInfo->requestObjRefId = 0;

      SAppInstInfo *pAppInstInfo = pAppHbMgr->pAppInstInfo;
      SEpSet        epSet = getEpSet_s(&pAppInstInfo->mgmtEp);
      if (TSDB_CODE_SUCCESS != asyncSendMsgToServer(pAppInstInfo->pTransporter, &epSet, NULL, pInfo)) {
        tscWarn("failed to async send msg to server");
      }
      tFreeClientHbBatchReq(pReq);
      // hbClearReqInfo(pAppHbMgr);
      (void)atomic_add_fetch_32(&pAppHbMgr->reportCnt, 1);
    }

    if (TSDB_CODE_SUCCESS != taosThreadMutexUnlock(&clientHbMgr.lock)) {
      tscError("taosThreadMutexLock failed");
      return NULL;
    }
    taosMsleep(HEARTBEAT_INTERVAL);
  }
  taosHashCleanup(clientHbMgr.appHbHash);
  return NULL;
}

static int32_t hbCreateThread() {
  int32_t      code = TSDB_CODE_SUCCESS;
  TdThreadAttr thAttr;
  TSC_ERR_JRET(taosThreadAttrInit(&thAttr));
  TSC_ERR_JRET(taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE));
#ifdef TD_COMPACT_OS
  TSC_ERR_JRET(taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL));
#endif

  if (taosThreadCreate(&clientHbMgr.thread, &thAttr, hbThreadFunc, NULL) != 0) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    TSC_ERR_RET(terrno);
  }
  (void)taosThreadAttrDestroy(&thAttr);
_return:

  if (code) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    TSC_ERR_RET(terrno);
  }

  return code;
}

static void hbStopThread() {
  if (0 == atomic_load_8(&clientHbMgr.inited)) {
    return;
  }
  if (atomic_val_compare_exchange_8(&clientHbMgr.threadStop, 0, 1)) {
    tscDebug("hb thread already stopped");
    return;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  // thread quit mode kill or inner exit from self-thread
  if (clientHbMgr.quitByKill) {
    code = taosThreadKill(clientHbMgr.thread, 0);
    if (TSDB_CODE_SUCCESS != code) {
      tscError("taosThreadKill failed since %s", tstrerror(code));
    }
  } else {
    code = taosThreadJoin(clientHbMgr.thread, NULL);
    if (TSDB_CODE_SUCCESS != code) {
      tscError("taosThreadJoin failed since %s", tstrerror(code));
    }
  }

  tscDebug("hb thread stopped");
}

int32_t appHbMgrInit(SAppInstInfo *pAppInstInfo, char *key, SAppHbMgr **pAppHbMgr) {
  int32_t code = TSDB_CODE_SUCCESS;
  TSC_ERR_RET(hbMgrInit());
  *pAppHbMgr = taosMemoryMalloc(sizeof(SAppHbMgr));
  if (*pAppHbMgr == NULL) {
    TSC_ERR_JRET(terrno);
  }
  // init stat
  (*pAppHbMgr)->startTime = taosGetTimestampMs();
  (*pAppHbMgr)->connKeyCnt = 0;
  (*pAppHbMgr)->connHbFlag = 0;
  (*pAppHbMgr)->reportCnt = 0;
  (*pAppHbMgr)->reportBytes = 0;
  (*pAppHbMgr)->key = taosStrdup(key);
  if ((*pAppHbMgr)->key == NULL) {
    TSC_ERR_JRET(terrno);
  }

  // init app info
  (*pAppHbMgr)->pAppInstInfo = pAppInstInfo;

  // init hash info
  (*pAppHbMgr)->activeInfo = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  if ((*pAppHbMgr)->activeInfo == NULL) {
    TSC_ERR_JRET(terrno);
  }

  // taosHashSetFreeFp(pAppHbMgr->activeInfo, tFreeClientHbReq);

  TSC_ERR_JRET(taosThreadMutexLock(&clientHbMgr.lock));
  if (taosArrayPush(clientHbMgr.appHbMgrs, &(*pAppHbMgr)) == NULL) {
    code = terrno;
    (void)taosThreadMutexUnlock(&clientHbMgr.lock);
    goto _return;
  }
  (*pAppHbMgr)->idx = taosArrayGetSize(clientHbMgr.appHbMgrs) - 1;
  TSC_ERR_JRET(taosThreadMutexUnlock(&clientHbMgr.lock));

  return TSDB_CODE_SUCCESS;
_return:
  taosMemoryFree(*pAppHbMgr);
  return code;
}

void hbFreeAppHbMgr(SAppHbMgr *pTarget) {
  void *pIter = taosHashIterate(pTarget->activeInfo, NULL);
  while (pIter != NULL) {
    SClientHbReq *pOneReq = pIter;
    tFreeClientHbReq(pOneReq);
    pIter = taosHashIterate(pTarget->activeInfo, pIter);
  }
  taosHashCleanup(pTarget->activeInfo);
  pTarget->activeInfo = NULL;

  taosMemoryFree(pTarget->key);
  taosMemoryFree(pTarget);
}

void hbRemoveAppHbMrg(SAppHbMgr **pAppHbMgr) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = taosThreadMutexLock(&clientHbMgr.lock);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to lock clientHbMgr, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }
  int32_t mgrSize = taosArrayGetSize(clientHbMgr.appHbMgrs);
  for (int32_t i = 0; i < mgrSize; ++i) {
    SAppHbMgr *pItem = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (pItem == *pAppHbMgr) {
      hbFreeAppHbMgr(*pAppHbMgr);
      *pAppHbMgr = NULL;
      taosArraySet(clientHbMgr.appHbMgrs, i, pAppHbMgr);
      break;
    }
  }
  code = taosThreadMutexUnlock(&clientHbMgr.lock);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to unlock clientHbMgr, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }
}

void appHbMgrCleanup(void) {
  int sz = taosArrayGetSize(clientHbMgr.appHbMgrs);
  for (int i = 0; i < sz; i++) {
    SAppHbMgr *pTarget = taosArrayGetP(clientHbMgr.appHbMgrs, i);
    if (pTarget == NULL) continue;
    hbFreeAppHbMgr(pTarget);
  }
}

int32_t hbMgrInit() {
  // init once
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 0, 1);
  if (old == 1) return 0;

  clientHbMgr.appId = tGenIdPI64();
  tscInfo("app initialized, appId:0x%" PRIx64, clientHbMgr.appId);

  clientHbMgr.appSummary = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (NULL == clientHbMgr.appSummary) {
    uError("hbMgrInit:taosHashInit error") return terrno;
  }
  clientHbMgr.appHbMgrs = taosArrayInit(0, sizeof(void *));
  if (NULL == clientHbMgr.appHbMgrs) {
    uError("hbMgrInit:taosArrayInit error") return terrno;
  }
  TdThreadMutexAttr attr = {0};

  int ret = taosThreadMutexAttrInit(&attr);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexAttrInit error") return ret;
  }

  ret = taosThreadMutexAttrSetType(&attr, PTHREAD_MUTEX_RECURSIVE);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexAttrSetType error") return ret;
  }

  ret = taosThreadMutexInit(&clientHbMgr.lock, &attr);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexInit error") return ret;
  }

  ret = taosThreadMutexAttrDestroy(&attr);
  if (ret != 0) {
    uError("hbMgrInit:taosThreadMutexAttrDestroy error") return ret;
  }

  // init handle funcs
  hbMgrInitHandle();

  // init backgroud thread
  ret = hbCreateThread();
  if (ret != 0) {
    uError("hbMgrInit:hbCreateThread error") return ret;
  }

  return 0;
}

void hbMgrCleanUp() {
  hbStopThread();

  // destroy all appHbMgr
  int8_t old = atomic_val_compare_exchange_8(&clientHbMgr.inited, 1, 0);
  if (old == 0) return;

  int32_t code = taosThreadMutexLock(&clientHbMgr.lock);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to lock clientHbMgr, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }
  appHbMgrCleanup();
  taosArrayDestroy(clientHbMgr.appHbMgrs);
  clientHbMgr.appHbMgrs = NULL;
  code = taosThreadMutexUnlock(&clientHbMgr.lock);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to unlock clientHbMgr, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }
}

int32_t hbRegisterConnImpl(SAppHbMgr *pAppHbMgr, SClientHbKey connKey, int64_t clusterId) {
  // init hash in activeinfo
  void *data = taosHashGet(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
  if (data != NULL) {
    return 0;
  }
  SClientHbReq hbReq = {0};
  hbReq.connKey = connKey;
  hbReq.clusterId = clusterId;
  // hbReq.info = taosHashInit(64, hbKeyHashFunc, 1, HASH_ENTRY_LOCK);

  TSC_ERR_RET(taosHashPut(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey), &hbReq, sizeof(SClientHbReq)));

  (void)atomic_add_fetch_32(&pAppHbMgr->connKeyCnt, 1);
  return 0;
}

int32_t hbRegisterConn(SAppHbMgr *pAppHbMgr, int64_t tscRefId, int64_t clusterId, int8_t connType) {
  SClientHbKey connKey = {
      .tscRid = tscRefId,
      .connType = connType,
  };

  switch (connType) {
    case CONN_TYPE__QUERY: {
      return hbRegisterConnImpl(pAppHbMgr, connKey, clusterId);
    }
    case CONN_TYPE__TMQ: {
      return 0;
    }
    default:
      return 0;
  }
}

void hbDeregisterConn(STscObj *pTscObj, SClientHbKey connKey) {
  int32_t code = taosThreadMutexLock(&clientHbMgr.lock);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to lock clientHbMgr, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }
  SAppHbMgr *pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, pTscObj->appHbMgrIdx);
  if (pAppHbMgr) {
    SClientHbReq *pReq = taosHashAcquire(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
    if (pReq) {
      tFreeClientHbReq(pReq);
      code = taosHashRemove(pAppHbMgr->activeInfo, &connKey, sizeof(SClientHbKey));
      if (TSDB_CODE_SUCCESS != code) {
        tscError("hbDeregisterConn taosHashRemove error, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
      }
      taosHashRelease(pAppHbMgr->activeInfo, pReq);
      (void)atomic_sub_fetch_32(&pAppHbMgr->connKeyCnt, 1);
    }
  }
  code = taosThreadMutexUnlock(&clientHbMgr.lock);
  if (TSDB_CODE_SUCCESS != code) {
    tscError("failed to unlock clientHbMgr, code:%s", tstrerror(TAOS_SYSTEM_ERROR(code)));
  }
}

// set heart beat thread quit mode , if quicByKill 1 then kill thread else quit from inner
void taos_set_hb_quit(int8_t quitByKill) { clientHbMgr.quitByKill = quitByKill; }
