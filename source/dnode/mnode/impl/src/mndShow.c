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

#define _DEFAULT_SOURCE
#include "mndShow.h"
#include "mndPrivilege.h"
#include "mndUser.h"
#include "systable.h"

#define SHOW_STEP_SIZE            100
#define SHOW_COLS_STEP_SIZE       4096
#define SHOW_PRIVILEGES_STEP_SIZE 2048

static SShowObj *mndCreateShowObj(SMnode *pMnode, SRetrieveTableReq *pReq);
static void      mndFreeShowObj(SShowObj *pShow);
static SShowObj *mndAcquireShowObj(SMnode *pMnode, int64_t showId);
static void      mndReleaseShowObj(SShowObj *pShow, bool forceRemove);
static bool      mndCheckRetrieveFinished(SShowObj *pShow);
static int32_t   mndProcessRetrieveSysTableReq(SRpcMsg *pReq);

int32_t mndInitShow(SMnode *pMnode) {
  int32_t    code = 0;
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  pMgmt->cache = taosCacheInit(TSDB_DATA_TYPE_INT, 5000, true, (__cache_free_fn_t)mndFreeShowObj, "show");
  if (pMgmt->cache == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc show cache since %s", tstrerror(code));
    TAOS_RETURN(code);
  }

  mndSetMsgHandle(pMnode, TDMT_MND_SYSTABLE_RETRIEVE, mndProcessRetrieveSysTableReq);
  TAOS_RETURN(code);
}

void mndCleanupShow(SMnode *pMnode) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  if (pMgmt->cache != NULL) {
    taosCacheCleanup(pMgmt->cache);
    pMgmt->cache = NULL;
  }
}

static int32_t convertToRetrieveType(char *name, int32_t len) {
  int32_t type = -1;

  if (strncasecmp(name, TSDB_INS_TABLE_DNODES, len) == 0) {
    type = TSDB_MGMT_TABLE_DNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_MNODES, len) == 0) {
    type = TSDB_MGMT_TABLE_MNODE;
    /*
      } else if (strncasecmp(name, TSDB_INS_TABLE_MODULES, len) == 0) {
        type = TSDB_MGMT_TABLE_MODULE;
    */
  } else if (strncasecmp(name, TSDB_INS_TABLE_QNODES, len) == 0) {
    type = TSDB_MGMT_TABLE_QNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_SNODES, len) == 0) {
    type = TSDB_MGMT_TABLE_SNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_ANODES, len) == 0) {
    type = TSDB_MGMT_TABLE_ANODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_ANODES_FULL, len) == 0) {
    type = TSDB_MGMT_TABLE_ANODE_FULL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_BNODES, len) == 0) {
    type = TSDB_MGMT_TABLE_BNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_ARBGROUPS, len) == 0) {
    type = TSDB_MGMT_TABLE_ARBGROUP;
  } else if (strncasecmp(name, TSDB_INS_TABLE_CLUSTER, len) == 0) {
    type = TSDB_MGMT_TABLE_CLUSTER;
  } else if (strncasecmp(name, TSDB_INS_TABLE_DATABASES, len) == 0) {
    type = TSDB_MGMT_TABLE_DB;
  } else if (strncasecmp(name, TSDB_INS_TABLE_FUNCTIONS, len) == 0) {
    type = TSDB_MGMT_TABLE_FUNC;
  } else if (strncasecmp(name, TSDB_INS_TABLE_INDEXES, len) == 0) {
    type = TSDB_MGMT_TABLE_INDEX;
  } else if (strncasecmp(name, TSDB_INS_TABLE_STABLES, len) == 0) {
    type = TSDB_MGMT_TABLE_STB;
  } else if (strncasecmp(name, TSDB_INS_TABLE_TABLES, len) == 0) {
    type = TSDB_MGMT_TABLE_TABLE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_TAGS, len) == 0) {
    type = TSDB_MGMT_TABLE_TAG;
  } else if (strncasecmp(name, TSDB_INS_TABLE_COLS, len) == 0) {
    type = TSDB_MGMT_TABLE_COL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_TABLE_DISTRIBUTED, len) == 0) {
    //    type = TSDB_MGMT_TABLE_DIST;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USERS, len) == 0) {
    type = TSDB_MGMT_TABLE_USER;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USERS_FULL, len) == 0) {
    type = TSDB_MGMT_TABLE_USER_FULL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_LICENCES, len) == 0) {
    type = TSDB_MGMT_TABLE_GRANTS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_VGROUPS, len) == 0) {
    type = TSDB_MGMT_TABLE_VGROUP;
  } else if (strncasecmp(name, TSDB_PERFS_TABLE_CONSUMERS, len) == 0) {
    type = TSDB_MGMT_TABLE_CONSUMERS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_SUBSCRIPTIONS, len) == 0) {
    type = TSDB_MGMT_TABLE_SUBSCRIPTIONS;
  } else if (strncasecmp(name, TSDB_PERFS_TABLE_TRANS, len) == 0) {
    type = TSDB_MGMT_TABLE_TRANS;
  } else if (strncasecmp(name, TSDB_PERFS_TABLE_SMAS, len) == 0) {
    type = TSDB_MGMT_TABLE_SMAS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_CONFIGS, len) == 0) {
    type = TSDB_MGMT_TABLE_CONFIGS;
  } else if (strncasecmp(name, TSDB_PERFS_TABLE_CONNECTIONS, len) == 0) {
    type = TSDB_MGMT_TABLE_CONNS;
  } else if (strncasecmp(name, TSDB_PERFS_TABLE_QUERIES, len) == 0) {
    type = TSDB_MGMT_TABLE_QUERIES;
  } else if (strncasecmp(name, TSDB_INS_TABLE_VNODES, len) == 0) {
    type = TSDB_MGMT_TABLE_VNODES;
  } else if (strncasecmp(name, TSDB_INS_TABLE_TOPICS, len) == 0) {
    type = TSDB_MGMT_TABLE_TOPICS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_STREAMS, len) == 0) {
    type = TSDB_MGMT_TABLE_STREAMS;
  } else if (strncasecmp(name, TSDB_PERFS_TABLE_APPS, len) == 0) {
    type = TSDB_MGMT_TABLE_APPS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_STREAM_TASKS, len) == 0) {
    type = TSDB_MGMT_TABLE_STREAM_TASKS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_STREAM_RECALCULATES, len) == 0) {
    type = TSDB_MGMT_TABLE_STREAM_RECALCULATES;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_PRIVILEGES, len) == 0) {
    type = TSDB_MGMT_TABLE_PRIVILEGES;
  } else if (strncasecmp(name, TSDB_INS_TABLE_VIEWS, len) == 0) {
    type = TSDB_MGMT_TABLE_VIEWS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_COMPACTS, len) == 0) {
    type = TSDB_MGMT_TABLE_COMPACT;
  } else if (strncasecmp(name, TSDB_INS_TABLE_COMPACT_DETAILS, len) == 0) {
    type = TSDB_MGMT_TABLE_COMPACT_DETAIL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_TRANSACTION_DETAILS, len) == 0) {
    type = TSDB_MGMT_TABLE_TRANSACTION_DETAIL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_GRANTS_FULL, len) == 0) {
    type = TSDB_MGMT_TABLE_GRANTS_FULL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_GRANTS_LOGS, len) == 0) {
    type = TSDB_MGMT_TABLE_GRANTS_LOGS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_MACHINES, len) == 0) {
    type = TSDB_MGMT_TABLE_MACHINES;
  } else if (strncasecmp(name, TSDB_INS_TABLE_ENCRYPTIONS, len) == 0) {
    type = TSDB_MGMT_TABLE_ENCRYPTIONS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_TSMAS, len) == 0) {
    type = TSDB_MGMT_TABLE_TSMAS;
  } else if (strncasecmp(name, TSDB_INS_DISK_USAGE, len) == 0) {
    type = TSDB_MGMT_TABLE_USAGE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_FILESETS, len) == 0) {
    type = TSDB_MGMT_TABLE_FILESETS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_VC_COLS, len) == 0) {
    type = TSDB_MGMT_TABLE_VC_COL;
  } else if (strncasecmp(name, TSDB_INS_TABLE_MOUNTS, len) == 0) {
    type = TSDB_MGMT_TABLE_MOUNT;
  } else {
    mError("invalid show name:%s len:%d", name, len);
  }

  return type;
}

static SShowObj *mndCreateShowObj(SMnode *pMnode, SRetrieveTableReq *pReq) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  int64_t showId = atomic_add_fetch_64(&pMgmt->showId, 1);
  if (showId == 0) atomic_add_fetch_64(&pMgmt->showId, 1);

  int32_t size = sizeof(SShowObj);

  SShowObj showObj = {0};

  showObj.id = showId;
  showObj.pMnode = pMnode;
  showObj.type = convertToRetrieveType(pReq->tb, tListLen(pReq->tb));
  (void)memcpy(showObj.db, pReq->db, TSDB_DB_FNAME_LEN);
  tstrncpy(showObj.filterTb, pReq->filterTb, TSDB_TABLE_NAME_LEN);

  int32_t   keepTime = tsShellActivityTimer * 6 * 1000;
  SShowObj *pShow = taosCachePut(pMgmt->cache, &showId, sizeof(int64_t), &showObj, size, keepTime);
  if (pShow == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("show:0x%" PRIx64 ", failed to put into cache since %s", showId, terrstr());
    return NULL;
  }

  mTrace("show:0x%" PRIx64 ", is created, data:%p", showId, pShow);
  return pShow;
}

static void mndFreeShowObj(SShowObj *pShow) {
  SMnode    *pMnode = pShow->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  ShowFreeIterFp freeFp = pMgmt->freeIterFps[pShow->type];
  if (freeFp != NULL) {
    if (pShow->pIter != NULL) {
      mTrace("show:0x%" PRIx64 ", is destroying, data:%p, pIter:%p, ", pShow->id, pShow, pShow->pIter);

      (*freeFp)(pMnode, pShow->pIter);

      pShow->pIter = NULL;
    }
  }

  mTrace("show:0x%" PRIx64 ", is destroyed, data:%p", pShow->id, pShow);
}

static SShowObj *mndAcquireShowObj(SMnode *pMnode, int64_t showId) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;

  SShowObj *pShow = taosCacheAcquireByKey(pMgmt->cache, &showId, sizeof(showId));
  if (pShow == NULL) {
    mError("show:0x%" PRIx64 ", already destroyed", showId);
    return NULL;
  }

  mTrace("show:0x%" PRIx64 ", acquired from cache, data:%p", pShow->id, pShow);
  return pShow;
}

static void mndReleaseShowObj(SShowObj *pShow, bool forceRemove) {
  if (pShow == NULL) return;
  mTrace("show:0x%" PRIx64 ", released from cache, data:%p force:%d", pShow->id, pShow, forceRemove);

  // A bug in tcache.c
  forceRemove = 0;

  SMnode    *pMnode = pShow->pMnode;
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  taosCacheRelease(pMgmt->cache, (void **)(&pShow), forceRemove);
}

static int32_t mndProcessRetrieveSysTableReq(SRpcMsg *pReq) {
  int32_t    code = 0;
  SMnode    *pMnode = pReq->info.node;
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  SShowObj  *pShow = NULL;
  int32_t    rowsToRead = SHOW_STEP_SIZE;
  int32_t    size = 0;
  int32_t    rowsRead = 0;
  mDebug("mndProcessRetrieveSysTableReq start");
  SRetrieveTableReq retrieveReq = {0};
  TAOS_CHECK_RETURN(tDeserializeSRetrieveTableReq(pReq->pCont, pReq->contLen, &retrieveReq));

  mDebug("process to retrieve systable req db:%s, tb:%s, compactId:%" PRId64, retrieveReq.db, retrieveReq.tb,
         retrieveReq.compactId);

  if (retrieveReq.showId == 0) {
    STableMetaRsp *pMeta = taosHashGet(pMnode->infosMeta, retrieveReq.tb, strlen(retrieveReq.tb));
    if (pMeta == NULL) {
      pMeta = taosHashGet(pMnode->perfsMeta, retrieveReq.tb, strlen(retrieveReq.tb));
      if (pMeta == NULL) {
        code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
        mError("failed to process show-retrieve req:%p since %s", pShow, tstrerror(code));
        TAOS_RETURN(code);
      }
    }

    pShow = mndCreateShowObj(pMnode, &retrieveReq);
    if (pShow == NULL) {
      code = terrno;
      mError("failed to process show-meta req since %s", tstrerror(code));
      TAOS_RETURN(code);
    }

    pShow->pMeta = pMeta;
    pShow->numOfColumns = pShow->pMeta->numOfColumns;
  } else {
    pShow = mndAcquireShowObj(pMnode, retrieveReq.showId);
    if (pShow == NULL) {
      code = TSDB_CODE_MND_INVALID_SHOWOBJ;
      mError("failed to process show-retrieve req:%p since %s", pShow, tstrerror(code));
      TAOS_RETURN(code);
    }
  }

  if (pShow->type == TSDB_MGMT_TABLE_COL) {  // expend capacity for ins_columns
    rowsToRead = SHOW_COLS_STEP_SIZE;
  } else if (pShow->type == TSDB_MGMT_TABLE_PRIVILEGES) {
    rowsToRead = SHOW_PRIVILEGES_STEP_SIZE;
  }
  ShowRetrieveFp retrieveFp = pMgmt->retrieveFps[pShow->type];
  if (retrieveFp == NULL) {
    mndReleaseShowObj(pShow, false);
    code = TSDB_CODE_MSG_NOT_PROCESSED;
    mError("show:0x%" PRIx64 ", failed to retrieve data since %s", pShow->id, tstrerror(code));
    TAOS_RETURN(code);
  }

  mDebug("show:0x%" PRIx64 ", start retrieve data, type:%d", pShow->id, pShow->type);
  if (retrieveReq.user[0] != 0) {
    (void)memcpy(pReq->info.conn.user, retrieveReq.user, TSDB_USER_LEN);
  } else {
    (void)memcpy(pReq->info.conn.user, TSDB_DEFAULT_USER, strlen(TSDB_DEFAULT_USER) + 1);
  }
  code = -1;
  if (retrieveReq.db[0] &&
      (code = mndCheckShowPrivilege(pMnode, pReq->info.conn.user, pShow->type, retrieveReq.db)) != 0) {
    TAOS_RETURN(code);
  }
  if (pShow->type == TSDB_MGMT_TABLE_USER_FULL) {
    if (strcmp(pReq->info.conn.user, "root") != 0) {
      mError("The operation is not permitted, user:%s, pShow->type:%d", pReq->info.conn.user, pShow->type);
      code = TSDB_CODE_MND_NO_RIGHTS;
      TAOS_RETURN(code);
    }
  }

  int32_t numOfCols = pShow->pMeta->numOfColumns;

  SSDataBlock *pBlock = NULL;
  code = createDataBlock(&pBlock);
  if (code) {
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData idata = {0};

    SSchema *p = &pShow->pMeta->pSchemas[i];

    idata.info.bytes = p->bytes;
    idata.info.type = p->type;
    idata.info.colId = p->colId;
    TAOS_CHECK_RETURN(blockDataAppendColInfo(pBlock, &idata));
  }

  TAOS_CHECK_RETURN(blockDataEnsureCapacity(pBlock, rowsToRead));

  if (mndCheckRetrieveFinished(pShow)) {
    mDebug("show:0x%" PRIx64 ", read finished, numOfRows:%d", pShow->id, pShow->numOfRows);
    rowsRead = 0;
  } else {
    rowsRead = (*retrieveFp)(pReq, pShow, pBlock, rowsToRead);
    if (rowsRead < 0) {
      code = rowsRead;
      mDebug("show:0x%" PRIx64 ", retrieve completed", pShow->id);
      mndReleaseShowObj(pShow, true);
      blockDataDestroy(pBlock);
      TAOS_RETURN(code);
    }

    pBlock->info.rows = rowsRead;
    mDebug("show:0x%" PRIx64 ", stop retrieve data, rowsRead:%d numOfRows:%d", pShow->id, rowsRead, pShow->numOfRows);
  }

  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);
  size = sizeof(SRetrieveMetaTableRsp) + sizeof(int32_t) + sizeof(SSysTableSchema) * pShow->pMeta->numOfColumns +
         dataEncodeBufSize;

  SRetrieveMetaTableRsp *pRsp = rpcMallocCont(size);
  if (pRsp == NULL) {
    mError("show:0x%" PRIx64 ", failed to retrieve data since %s", pShow->id, tstrerror(terrno));
    code = terrno;
    goto _exit;
  }

  pRsp->handle = htobe64(pShow->id);

  if (rowsRead > 0) {
    char    *pStart = pRsp->data;
    SSchema *ps = pShow->pMeta->pSchemas;

    *(int32_t *)pStart = htonl(pShow->pMeta->numOfColumns);
    pStart += sizeof(int32_t);  // number of columns

    for (int32_t i = 0; i < pShow->pMeta->numOfColumns; ++i) {
      SSysTableSchema *pSchema = (SSysTableSchema *)pStart;
      pSchema->bytes = htonl(ps[i].bytes);
      pSchema->colId = htons(ps[i].colId);
      pSchema->type = ps[i].type;

      pStart += sizeof(SSysTableSchema);
    }

    int32_t len = blockEncode(pBlock, pStart, dataEncodeBufSize, pShow->pMeta->numOfColumns);
    if (len < 0) {
      mError("show:0x%" PRIx64 ", failed to retrieve data since %s", pShow->id, tstrerror(terrno));
      code = terrno;
      return code;
    }
  }

  pRsp->numOfRows = htonl(rowsRead);
  pRsp->precision = TSDB_TIME_PRECISION_MILLI;  // millisecond time precision
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = size;

  if (rowsRead == 0 || mndCheckRetrieveFinished(pShow)) {
    pRsp->completed = 1;
    mDebug("show:0x%" PRIx64 ", retrieve completed", pShow->id);
    mndReleaseShowObj(pShow, true);
  } else {
    mDebug("show:0x%" PRIx64 ", retrieve not completed yet", pShow->id);
    mndReleaseShowObj(pShow, false);
  }

  blockDataDestroy(pBlock);
  return TSDB_CODE_SUCCESS;
_exit:
  mndReleaseShowObj(pShow, false);
  blockDataDestroy(pBlock);
  if (pRsp) {
    rpcFreeCont(pRsp);
  }
  return code;
}

static bool mndCheckRetrieveFinished(SShowObj *pShow) {
  if (pShow->pIter == NULL && pShow->numOfRows != 0) {
    return true;
  }
  return false;
}

void mndAddShowRetrieveHandle(SMnode *pMnode, EShowType showType, ShowRetrieveFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->retrieveFps[showType] = fp;
}

void mndAddShowFreeIterHandle(SMnode *pMnode, EShowType showType, ShowFreeIterFp fp) {
  SShowMgmt *pMgmt = &pMnode->showMgmt;
  pMgmt->freeIterFps[showType] = fp;
}
