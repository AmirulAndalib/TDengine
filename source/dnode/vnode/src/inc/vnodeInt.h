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

#ifndef _TD_VNODE_DEF_H_
#define _TD_VNODE_DEF_H_

#include "executor.h"
#include "filter.h"
#include "qworker.h"
#ifdef USE_ROCKSDB
#include "rocksdb/c.h"
#endif
#include "sync.h"
#include "tRealloc.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tcompare.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tdb.h"
#include "tencode.h"
#include "tfs.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tlosertree.h"
#include "tlrucache.h"
#include "tmsgcb.h"
#include "trbtree.h"
#include "tref.h"
#include "tskiplist.h"
#include "stream.h"
#include "ttime.h"
#include "ttimer.h"
#include "wal.h"

#include "bse.h"
#include "vnode.h"

#include "metrics.h"
#include "taos_monitor.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeInfo         SVnodeInfo;
typedef struct SSma               SSma;
typedef struct STsdb              STsdb;
typedef struct STQ                STQ;
typedef struct SVState            SVState;
typedef struct SVStatis           SVStatis;
typedef struct SVBufPool          SVBufPool;
typedef struct SQueueWorker       SQHandle;
typedef struct STsdbKeepCfg       STsdbKeepCfg;
typedef struct SMetaSnapReader    SMetaSnapReader;
typedef struct SMetaSnapWriter    SMetaSnapWriter;
typedef struct STsdbSnapReader    STsdbSnapReader;
typedef struct STsdbSnapWriter    STsdbSnapWriter;
typedef struct STsdbSnapRAWReader STsdbSnapRAWReader;
typedef struct STsdbSnapRAWWriter STsdbSnapRAWWriter;
typedef struct STqSnapReader      STqSnapReader;
typedef struct STqSnapWriter      STqSnapWriter;
typedef struct SStreamTaskReader  SStreamTaskReader;
typedef struct SStreamTaskWriter  SStreamTaskWriter;
typedef struct SStreamStateReader SStreamStateReader;
typedef struct SStreamStateWriter SStreamStateWriter;
typedef struct SRSmaSnapReader    SRSmaSnapReader;
typedef struct SRSmaSnapWriter    SRSmaSnapWriter;
typedef struct SSnapDataHdr       SSnapDataHdr;
typedef struct SCommitInfo        SCommitInfo;
typedef struct SCompactInfo       SCompactInfo;
typedef struct SQueryNode         SQueryNode;

typedef struct SStreamNotifyHandleMap SStreamNotifyHandleMap;

#define VNODE_META_TMP_DIR    "meta.tmp"
#define VNODE_META_BACKUP_DIR "meta.backup"


#define VNODE_TSDB_NAME_LEN  6
#define VNODE_META_DIR       "meta"
#define VNODE_TSDB_DIR       "tsdb"
#define VNODE_TQ_DIR         "tq"
#define VNODE_WAL_DIR        "wal"
#define VNODE_TSMA_DIR       "tsma"
#define VNODE_RSMA_DIR       "rsma"
#define VNODE_RSMA0_DIR      "tsdb"
#define VNODE_RSMA1_DIR      "rsma1"
#define VNODE_RSMA2_DIR      "rsma2"
#define VNODE_TQ_STREAM      "stream"
#define VNODE_CACHE_DIR      "cache.rdb"
#define VNODE_TSDB_CACHE_DIR VNODE_TSDB_DIR TD_DIRSEP VNODE_CACHE_DIR
#define VNODE_BSE_DIR   "bse"

#if SUSPEND_RESUME_TEST  // only for test purpose
#define VNODE_BUFPOOL_SEGMENTS 1
#else
#define VNODE_BUFPOOL_SEGMENTS 3
#endif

#define VND_INFO_FNAME     "vnode.json"
#define VND_INFO_FNAME_TMP "vnode_tmp.json"

// vnd.h
typedef int32_t (*_query_reseek_func_t)(void* pQHandle);
struct SQueryNode {
  SQueryNode*          pNext;
  SQueryNode**         ppNext;
  void*                pQHandle;
  _query_reseek_func_t reseek;
};

#if 1  // refact APIs below (TODO)
typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

SMTbCursor* metaOpenTbCursor(void* pVnode);
void        metaCloseTbCursor(SMTbCursor* pTbCur);
void        metaPauseTbCursor(SMTbCursor* pTbCur);
int32_t     metaResumeTbCursor(SMTbCursor* pTbCur, int8_t first, int8_t move);
int32_t     metaTbCursorNext(SMTbCursor* pTbCur, ETableType jumpTableType);
int32_t     metaTbCursorPrev(SMTbCursor* pTbCur, ETableType jumpTableType);

#endif

void* vnodeBufPoolMalloc(SVBufPool* pPool, int size);
void* vnodeBufPoolMallocAligned(SVBufPool* pPool, int size);
void  vnodeBufPoolFree(SVBufPool* pPool, void* p);
void  vnodeBufPoolRef(SVBufPool* pPool);
void  vnodeBufPoolUnRef(SVBufPool* pPool, bool proactive);
int   vnodeDecodeInfo(uint8_t* pData, SVnodeInfo* pInfo);

void vnodeBufPoolRegisterQuery(SVBufPool* pPool, SQueryNode* pQNode);
void vnodeBufPoolDeregisterQuery(SVBufPool* pPool, SQueryNode* pQNode, bool proactive);

void  initStorageAPI(SStorageAPI* pAPI);

// meta
typedef struct SMStbCursor SMStbCursor;
typedef struct STbUidStore STbUidStore;

#define META_BEGIN_HEAP_BUFFERPOOL 0
#define META_BEGIN_HEAP_OS         1
#define META_BEGIN_HEAP_NIL        2

int             metaOpen(SVnode* pVnode, SMeta** ppMeta, int8_t rollback);
int             metaUpgrade(SVnode* pVnode, SMeta** ppMeta);
void            metaClose(SMeta** pMeta);
int             metaBegin(SMeta* pMeta, int8_t fromSys);
TXN*            metaGetTxn(SMeta* pMeta);
int             metaCommit(SMeta* pMeta, TXN* txn);
int             metaFinishCommit(SMeta* pMeta, TXN* txn);
int             metaPrepareAsyncCommit(SMeta* pMeta);
int             metaAbort(SMeta* pMeta);
int             metaCreateSuperTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int32_t         metaAlterSuperTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int32_t         metaDropSuperTable(SMeta* pMeta, int64_t verison, SVDropStbReq* pReq);
int32_t         metaCreateTable2(SMeta* pMeta, int64_t version, SVCreateTbReq* pReq, STableMetaRsp** ppRsp);
int32_t         metaDropTable2(SMeta* pMeta, int64_t version, SVDropTbReq* pReq);
int32_t         metaTrimTables(SMeta* pMeta, int64_t version);
int32_t         metaDropMultipleTables(SMeta* pMeta, int64_t version, SArray* tbUids);
int             metaTtlFindExpired(SMeta* pMeta, int64_t timePointMs, SArray* tbUids, int32_t ttlDropMaxCount);
int             metaAlterTable(SMeta* pMeta, int64_t version, SVAlterTbReq* pReq, STableMetaRsp* pMetaRsp);
int             metaUpdateChangeTimeWithLock(SMeta* pMeta, tb_uid_t uid, int64_t changeTimeMs);
SSchemaWrapper* metaGetTableSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, int lock, SExtSchema** extSchema);
int64_t         metaGetTableCreateTime(SMeta* pMeta, tb_uid_t uid, int lock);
SExtSchema*     metaGetSExtSchema(const SMetaEntry* pME);
int32_t         metaGetTbTSchemaNotNull(SMeta* pMeta, tb_uid_t uid, int32_t sver, int lock, STSchema** ppTSchema);
int32_t         metaGetTbTSchemaMaybeNull(SMeta* pMeta, tb_uid_t uid, int32_t sver, int lock, STSchema** ppTSchema);
STSchema*       metaGetTbTSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, int lock);
int32_t         metaGetTbTSchemaEx(SMeta* pMeta, tb_uid_t suid, tb_uid_t uid, int32_t sver, STSchema** ppTSchema);
int             metaGetTableEntryByName(SMetaReader* pReader, const char* name);
int             metaAlterCache(SMeta* pMeta, int32_t nPage);

int32_t metaUidCacheClear(SMeta* pMeta, uint64_t suid);
int32_t metaTbGroupCacheClear(SMeta* pMeta, uint64_t suid);
int32_t metaRefDbsCacheClear(SMeta* pMeta, uint64_t suid);
void    metaCacheClear(SMeta* pMeta);

int32_t metaAddIndexToSuperTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int32_t metaDropIndexFromSuperTable(SMeta* pMeta, int64_t version, SDropIndexReq* pReq);

int64_t       metaGetTimeSeriesNum(SMeta* pMeta, int type);
void          metaUpdTimeSeriesNum(SMeta* pMeta);
SMCtbCursor*  metaOpenCtbCursor(void* pVnode, tb_uid_t uid, int lock);
int32_t       metaResumeCtbCursor(SMCtbCursor* pCtbCur, int8_t first);
void          metaPauseCtbCursor(SMCtbCursor* pCtbCur);
void          metaCloseCtbCursor(SMCtbCursor* pCtbCur);
tb_uid_t      metaCtbCursorNext(SMCtbCursor* pCtbCur);
SMStbCursor*  metaOpenStbCursor(SMeta* pMeta, tb_uid_t uid);
void          metaCloseStbCursor(SMStbCursor* pStbCur);
tb_uid_t      metaStbCursorNext(SMStbCursor* pStbCur);
STSma*        metaGetSmaInfoByIndex(SMeta* pMeta, int64_t indexUid);
STSmaWrapper* metaGetSmaInfoByTable(SMeta* pMeta, tb_uid_t uid, bool deepCopy);
SArray*       metaGetSmaIdsByTable(SMeta* pMeta, tb_uid_t uid);
SArray*       metaGetSmaTbUids(SMeta* pMeta);
void*         metaGetIdx(SMeta* pMeta);
void*         metaGetIvtIdx(SMeta* pMeta);
int32_t       metaFlagCache(SVnode* pVnode);

int64_t metaGetTbNum(SMeta* pMeta);
void    metaReaderDoInit(SMetaReader* pReader, SMeta* pMeta, int32_t flags);

int32_t metaCreateTSma(SMeta* pMeta, int64_t version, SSmaCfg* pCfg);
int32_t metaDropTSma(SMeta* pMeta, int64_t indexUid);

typedef struct SMetaInfo {
  int64_t uid;
  int64_t suid;
  int64_t version;
  int32_t skmVer;
} SMetaInfo;
int32_t metaGetInfo(SMeta* pMeta, int64_t uid, SMetaInfo* pInfo, SMetaReader* pReader);

// tsdb
int32_t tsdbOpen(SVnode* pVnode, STsdb** ppTsdb, const char* dir, STsdbKeepCfg* pKeepCfg, int8_t rollback, bool force);
void    tsdbClose(STsdb** pTsdb);
int32_t tsdbBegin(STsdb* pTsdb);
// int32_t tsdbPrepareCommit(STsdb* pTsdb);
// int32_t tsdbCommit(STsdb* pTsdb, SCommitInfo* pInfo);
int32_t tsdbCacheCommit(STsdb* pTsdb);
int32_t tsdbCacheNewTable(STsdb* pTsdb, int64_t uid, tb_uid_t suid, const SSchemaWrapper* pSchemaRow);
int32_t tsdbCacheDropTable(STsdb* pTsdb, int64_t uid, tb_uid_t suid, SSchemaWrapper* pSchemaRow);
int32_t tsdbCacheDropSubTables(STsdb* pTsdb, SArray* uids, tb_uid_t suid);
int32_t tsdbCacheNewSTableColumn(STsdb* pTsdb, SArray* uids, int16_t cid, int8_t col_type);
int32_t tsdbCacheDropSTableColumn(STsdb* pTsdb, SArray* uids, int16_t cid, bool hasPrimayKey);
int32_t tsdbCacheNewNTableColumn(STsdb* pTsdb, int64_t uid, int16_t cid, int8_t col_type);
int32_t tsdbCacheDropNTableColumn(STsdb* pTsdb, int64_t uid, int16_t cid, bool hasPrimayKey);
void    tsdbCacheInvalidateSchema(STsdb* pTsdb, tb_uid_t suid, tb_uid_t uid, int32_t sver);
int     tsdbScanAndConvertSubmitMsg(STsdb* pTsdb, SSubmitReq2* pMsg);
int     tsdbInsertData(STsdb* pTsdb, int64_t version, SSubmitReq2* pMsg, SSubmitRsp2* pRsp);
int32_t tsdbInsertTableData(STsdb* pTsdb, int64_t version, SSubmitTbData* pSubmitTbData, int32_t* affectedRows);
int32_t tsdbDeleteTableData(STsdb* pTsdb, int64_t version, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey);
void    tsdbSetKeepCfg(STsdb* pTsdb, STsdbCfg* pCfg);
int64_t tsdbGetEarliestTs(STsdb* pTsdb);

// tq
int32_t tqOpen(const char* path, SVnode* pVnode);
void    tqClose(STQ*);
int     tqPushMsg(STQ*, tmsg_t msgType);
int     tqRegisterPushHandle(STQ* pTq, void* handle, SRpcMsg* pMsg);
void    tqUnregisterPushHandle(STQ* pTq, void* pHandle);
void    tqScanWalAsync(STQ* pTq);
int32_t tqStopStreamTasksAsync(STQ* pTq);
int32_t tqProcessTaskResetReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessAllTaskStopReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskChkptReportRsp(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskCheckpointReadyRsp(STQ* pTq, SRpcMsg* pMsg);

// injection error
void streamMetaFreeTQDuringScanWalError(STQ* pTq);

int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd);
int32_t tqCheckColModifiable(STQ* pTq, int64_t tbUid, int32_t colId);
// tq-mq
int32_t tqProcessAddCheckInfoReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessDelCheckInfoReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessSubscribeReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessSeekReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessPollPush(STQ* pTq);
int32_t tqProcessVgWalInfoReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessVgCommittedInfoReq(STQ* pTq, SRpcMsg* pMsg);

// tq-stream
int32_t tqProcessTaskPauseReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessTaskResumeReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessTaskConsenChkptIdReq(STQ* pTq, SRpcMsg* pMsg);

// sma
int32_t smaInit();
void    smaCleanUp();
int32_t smaOpen(SVnode* pVnode, int8_t rollback, bool force);
void    smaClose(SSma* pSma);
int32_t smaBegin(SSma* pSma);
int32_t smaPrepareAsyncCommit(SSma* pSma);
int32_t smaCommit(SSma* pSma, SCommitInfo* pInfo);
int32_t smaFinishCommit(SSma* pSma);
int32_t smaPostCommit(SSma* pSma);
int32_t smaRetention(SSma* pSma, int64_t now);

int32_t tdProcessTSmaCreate(SSma* pSma, int64_t version, const char* msg);
int32_t tdProcessTSmaInsert(SSma* pSma, int64_t indexUid, const char* msg);

int32_t tdProcessRSmaCreate(SSma* pSma, SVCreateStbReq* pReq);
int32_t tdProcessRSmaSubmit(SSma* pSma, int64_t version, void* pReq, void* pMsg, int32_t len);
int32_t tdProcessRSmaDelete(SSma* pSma, int64_t version, void* pReq, void* pMsg, int32_t len);
int32_t tdProcessRSmaDrop(SSma* pSma, SVDropStbReq* pReq);
int32_t tdFetchTbUidList(SSma* pSma, STbUidStore** ppStore, tb_uid_t suid, tb_uid_t uid);
int32_t tdUpdateTbUidList(SSma* pSma, STbUidStore* pUidStore, bool isAdd);
void*   tdUidStoreFree(STbUidStore* pStore);

// SMetaSnapReader ========================================
int32_t metaSnapReaderOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapReader** ppReader);
void    metaSnapReaderClose(SMetaSnapReader** ppReader);
int32_t metaSnapRead(SMetaSnapReader* pReader, uint8_t** ppData);
// SMetaSnapWriter ========================================
int32_t metaSnapWriterOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapWriter** ppWriter);
int32_t metaSnapWrite(SMetaSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t metaSnapWriterClose(SMetaSnapWriter** ppWriter, int8_t rollback);
// STsdbSnapReader ========================================
int32_t tsdbSnapReaderOpen(STsdb* pTsdb, int64_t sver, int64_t ever, int8_t type, void* pRanges,
                           STsdbSnapReader** ppReader);
void    tsdbSnapReaderClose(STsdbSnapReader** ppReader);
int32_t tsdbSnapRead(STsdbSnapReader* pReader, uint8_t** ppData);
// STsdbSnapWriter ========================================
int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, void* pRanges, STsdbSnapWriter** ppWriter);
int32_t tsdbSnapWrite(STsdbSnapWriter* pWriter, SSnapDataHdr* pHdr);
int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* pWriter, bool rollback);
int32_t tsdbSnapWriterClose(STsdbSnapWriter** ppWriter, int8_t rollback);
// STsdbSnapRAWReader ========================================
int32_t tsdbSnapRAWReaderOpen(STsdb* pTsdb, int64_t ever, int8_t type, STsdbSnapRAWReader** ppReader);
void    tsdbSnapRAWReaderClose(STsdbSnapRAWReader** ppReader);
int32_t tsdbSnapRAWRead(STsdbSnapRAWReader* pReader, uint8_t** ppData);
// STsdbSnapRAWWriter ========================================
int32_t tsdbSnapRAWWriterOpen(STsdb* pTsdb, int64_t ever, STsdbSnapRAWWriter** ppWriter);
int32_t tsdbSnapRAWWrite(STsdbSnapRAWWriter* pWriter, SSnapDataHdr* pHdr);
int32_t tsdbSnapRAWWriterPrepareClose(STsdbSnapRAWWriter* pWriter);
int32_t tsdbSnapRAWWriterClose(STsdbSnapRAWWriter** ppWriter, int8_t rollback);
// STqSnapshotReader ==
int32_t tqSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, int8_t type, STqSnapReader** ppReader);
void    tqSnapReaderClose(STqSnapReader** ppReader);
int32_t tqSnapRead(STqSnapReader* pReader, uint8_t** ppData);
// STqSnapshotWriter ======================================
int32_t tqSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, STqSnapWriter** ppWriter);
int32_t tqSnapWriterClose(STqSnapWriter** ppWriter, int8_t rollback);
int32_t tqSnapHandleWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData);

int32_t tqSnapCheckInfoWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t tqSnapOffsetWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData);

// SStreamTaskWriter ======================================

int32_t streamTaskSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamTaskReader** ppReader);
int32_t streamTaskSnapReaderClose(SStreamTaskReader* pReader);
int32_t streamTaskSnapRead(SStreamTaskReader* pReader, uint8_t** ppData);

int32_t streamTaskSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamTaskWriter** ppWriter);
int32_t streamTaskSnapWriterClose(SStreamTaskWriter* ppWriter, int8_t rollback, int8_t loadTask);
int32_t streamTaskSnapWrite(SStreamTaskWriter* pWriter, uint8_t* pData, uint32_t nData);

int32_t streamStateSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamStateReader** ppReader);
int32_t streamStateSnapReaderClose(SStreamStateReader* pReader);
int32_t streamStateSnapRead(SStreamStateReader* pReader, uint8_t** ppData);

int32_t streamStateSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, SStreamStateWriter** ppWriter);
int32_t streamStateSnapWriterClose(SStreamStateWriter* pWriter, int8_t rollback);
int32_t streamStateSnapWrite(SStreamStateWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t streamStateRebuildFromSnap(SStreamStateWriter* pWriter, int64_t chkpId);

int32_t streamStateLoadTasks(SStreamStateWriter* pWriter);

// SStreamTaskReader ======================================
// SStreamStateWriter =====================================
// SStreamStateReader =====================================
// SRSmaSnapReader ========================================
int32_t rsmaSnapReaderOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapReader** ppReader);
void    rsmaSnapReaderClose(SRSmaSnapReader** ppReader);
int32_t rsmaSnapRead(SRSmaSnapReader* pReader, uint8_t** ppData);
// SRSmaSnapWriter ========================================
int32_t rsmaSnapWriterOpen(SSma* pSma, int64_t sver, int64_t ever, void** ppRanges, SRSmaSnapWriter** ppWriter);
int32_t rsmaSnapWrite(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t rsmaSnapWriterPrepareClose(SRSmaSnapWriter* pWriter, bool rollback);
int32_t rsmaSnapWriterClose(SRSmaSnapWriter** ppWriter, int8_t rollback);

typedef struct {
  int8_t  streamType;  // sma or other
  int8_t  dstType;
  int16_t padding;
  int32_t smaId;
  int64_t tbUid;
  int64_t lastReceivedVer;
  int64_t lastCommittedVer;
} SStreamSinkInfo;

typedef struct {
  SVnode*   pVnode;
  SHashObj* pHash;  // streamId -> SStreamSinkInfo
} SSink;

// SVState
struct SVState {
  int64_t committed;
  int64_t applied;
  int64_t applyTerm;
  int64_t commitID;
  int64_t commitTerm;
};

struct SVStatis {
  int64_t nInsert;              // delta
  int64_t nInsertSuccess;       // delta
  int64_t nBatchInsert;         // delta
  int64_t nBatchInsertSuccess;  // delta
};

struct SVnodeInfo {
  SVnodeCfg config;
  SVState   state;
  SVStatis  statis;
};

typedef enum {
  TSDB_TYPE_TSDB = 0,     // TSDB
  TSDB_TYPE_TSMA = 1,     // TSMA
  TSDB_TYPE_RSMA_L0 = 2,  // RSMA Level 0
  TSDB_TYPE_RSMA_L1 = 3,  // RSMA Level 1
  TSDB_TYPE_RSMA_L2 = 4,  // RSMA Level 2
} ETsdbType;

struct STsdbKeepCfg {
  int8_t  precision;  // precision always be used with below keep cfgs
  int32_t days;
  int32_t keep0;
  int32_t keep1;
  int32_t keep2;
  int32_t keepTimeOffset;
};

typedef struct SVCommitSched {
  int64_t commitMs;
  int64_t maxWaitMs;
} SVCommitSched;

typedef struct SVMonitorObj {
  char strClusterId[TSDB_CLUSTER_ID_LEN];
  char strDnodeId[TSDB_NODE_ID_LEN];
  char strVgId[TSDB_VGROUP_ID_LEN];
} SVMonitorObj;

typedef struct {
  int64_t async;
  int64_t id;
} SVAChannelID;

typedef struct {
  int64_t async;
  int64_t id;
} SVATaskID;

struct SVnodeWriteMetrics {
  int64_t total_requests;
  int64_t total_rows;
  int64_t total_bytes;
  int64_t cache_hit_ratio;
  int64_t rpc_queue_wait;
  int64_t preprocess_time;
  int64_t apply_time;
  int64_t apply_bytes;
  int64_t fetch_batch_meta_time;
  int64_t fetch_batch_meta_count;
  int64_t memory_table_size;
  int64_t commit_count;
  int64_t merge_count;
  int64_t commit_time;
  int64_t merge_time;
  int64_t block_commit_time;
  int64_t blocked_commit_count;
  int64_t memtable_wait_time;
  int64_t last_cache_commit_time;
  int64_t last_cache_commit_count;
};

struct SVnode {
  SVState   state;
  SVStatis  statis;
  char*     path;
  STfs*     pTfs;
  STfs*     pMountTfs;
  int32_t   diskPrimary;
  SVnodeCfg config;
  SMsgCb    msgCb;
  bool      disableWrite;
  bool      mounted;

  //  Metrics
  SVnodeWriteMetrics writeMetrics;

  // Buffer Pool
  TdThreadMutex mutex;
  TdThreadCond  poolNotEmpty;
  SVBufPool*    aBufPool[VNODE_BUFPOOL_SEGMENTS];
  SVBufPool*    freeList;
  SVBufPool*    inUse;
  SVBufPool*    onCommit;
  SVBufPool*    recycleHead;
  SVBufPool*    recycleTail;
  SVBufPool*    onRecycle;

  // commit variables
  SVATaskID commitTask;
  SVATaskID commitTask2;

  struct {
    TdThreadRwlock metaRWLock;
    SMeta*         pMeta;
    SMeta*         pNewMeta;
    SVATaskID      metaCompactTask;
  };

  SSma*         pSma;
  STsdb*        pTsdb;
  SWal*         pWal;
  STQ*          pTq;
  SBse*         pBse;
  SSink*        pSink;
  int64_t       sync;
  TdThreadMutex lock;
  bool          blocked;
  bool          restored;
  tsem_t        syncSem;
  int32_t       blockSec;
  int64_t       blockSeq;
  SQHandle*     pQuery;
  SVMonitorObj  monitor;
  uint32_t      applyQueueErrorCount;

  // Notification Handles
  SStreamNotifyHandleMap* pNotifyHandleMap;
};

#define TD_VID(PVNODE) ((PVNODE)->config.vgId)

#define VND_TSDB(vnd)       ((vnd)->pTsdb)
#define VND_RSMA0(vnd)      ((vnd)->pTsdb)
#define VND_RSMA1(vnd)      ((vnd)->pSma->pRSmaTsdb[TSDB_RETENTION_L0])
#define VND_RSMA2(vnd)      ((vnd)->pSma->pRSmaTsdb[TSDB_RETENTION_L1])
#define VND_RETENTIONS(vnd) (&(vnd)->config.tsdbCfg.retentions)
#define VND_IS_RSMA(v)      ((v)->config.isRsma == 1)
#define VND_IS_TSMA(v)      ((v)->config.isTsma == 1)

#define TSDB_CACHE_NO(c)       ((c).cacheLast == 0)
#define TSDB_CACHE_LAST_ROW(c) (((c).cacheLast & 1) > 0)
#define TSDB_CACHE_LAST(c)     (((c).cacheLast & 2) > 0)
#define TSDB_CACHE_RESET(c)    (((c).cacheLast & 4) > 0)

#define TSDB_TFS(v)            ((v)->pMountTfs ? (v)->pMountTfs : (v)->pTfs)
#define TSDB_VID(v)            ((v)->mounted ? (v)->config.mountVgId : (v)->config.vgId)

struct STbUidStore {
  tb_uid_t  suid;
  SArray*   tbUids;
  SHashObj* uidHash;
};

struct SSma {
  bool          locked;
  TdThreadMutex mutex;
  SVnode*       pVnode;
  STsdb*        pRSmaTsdb[TSDB_RETENTION_L2];
  void*         pTSmaEnv;
  void*         pRSmaEnv;
};

#define SMA_CFG(s)                       (&(s)->pVnode->config)
#define SMA_TSDB_CFG(s)                  (&(s)->pVnode->config.tsdbCfg)
#define SMA_RETENTION(s)                 ((SRetention*)&(s)->pVnode->config.tsdbCfg.retentions)
#define SMA_LOCKED(s)                    ((s)->locked)
#define SMA_META(s)                      ((s)->pVnode->pMeta)
#define SMA_VID(s)                       TD_VID((s)->pVnode)
#define SMA_TFS(s)                       ((s)->pVnode->pTfs)
#define SMA_TSMA_ENV(s)                  ((s)->pTSmaEnv)
#define SMA_RSMA_ENV(s)                  ((s)->pRSmaEnv)
#define SMA_RSMA_TSDB0(s)                ((s)->pVnode->pTsdb)
#define SMA_RSMA_TSDB1(s)                ((s)->pRSmaTsdb[TSDB_RETENTION_L0])
#define SMA_RSMA_TSDB2(s)                ((s)->pRSmaTsdb[TSDB_RETENTION_L1])
#define SMA_RSMA_GET_TSDB(pVnode, level) ((level == 0) ? pVnode->pTsdb : pVnode->pSma->pRSmaTsdb[level - 1])

// sma
void smaHandleRes(void* pVnode, int64_t smaId, const SArray* data);

enum {
  SNAP_DATA_CFG = 0,
  SNAP_DATA_META = 1,
  SNAP_DATA_TSDB = 2,
  SNAP_DATA_DEL = 3,
  SNAP_DATA_RSMA1 = 4,
  SNAP_DATA_RSMA2 = 5,
  SNAP_DATA_QTASK = 6,
  SNAP_DATA_TQ_HANDLE = 7,
  SNAP_DATA_TQ_OFFSET = 8,
  SNAP_DATA_STREAM_TASK = 9,
  SNAP_DATA_STREAM_TASK_CHECKPOINT = 10,
  SNAP_DATA_STREAM_STATE = 11,
  SNAP_DATA_STREAM_STATE_BACKEND = 12,
  SNAP_DATA_TQ_CHECKINFO = 13,
  SNAP_DATA_RAW = 14,
  SNAP_DATA_BSE = 15,
};

struct SSnapDataHdr {
  int8_t  type;
  int8_t  flag;
  int64_t index;
  int64_t size;
  uint8_t data[];
};

struct SCommitInfo {
  SVnodeInfo info;
  SVnode*    pVnode;
  TXN*       txn;
};

struct SCompactInfo {
  SVnode*     pVnode;
  int32_t     flag;
  int64_t     commitID;
  STimeWindow tw;
};

// a simple hash table impl
typedef struct SVHashTable SVHashTable;

struct SVHashTable {
  uint32_t (*hash)(const void*);
  int32_t (*compare)(const void*, const void*);
  int32_t              numEntries;
  uint32_t             numBuckets;
  struct SVHashEntry** buckets;
};

#define vHashNumEntries(ht) ((ht)->numEntries)
int32_t vHashInit(SVHashTable** ht, uint32_t (*hash)(const void*), int32_t (*compare)(const void*, const void*));
void    vHashDestroy(SVHashTable** ht);
int32_t vHashPut(SVHashTable* ht, void* obj);
int32_t vHashGet(SVHashTable* ht, const void* obj, void** retObj);
int32_t vHashDrop(SVHashTable* ht, const void* obj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/
