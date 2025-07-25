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
#ifndef TDENGINE_EXECUTORINT_H
#define TDENGINE_EXECUTORINT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tcommon.h"
#include "theap.h"
#include "tlosertree.h"
#include "tsort.h"
#include "tvariant.h"

#include "dataSinkMgt.h"
#include "executil.h"
#include "executor.h"
#include "planner.h"
#include "scalar.h"
#include "taosdef.h"
#include "tarray.h"
#include "tfill.h"
#include "thash.h"
#include "tlockfree.h"
#include "tmsg.h"
#include "tpagedbuf.h"
#include "tlrucache.h"
#include "tworker.h"

typedef int32_t (*__block_search_fn_t)(char* data, int32_t num, int64_t key, int32_t order);

typedef struct STsdbReader STsdbReader;
typedef struct STqReader   STqReader;

typedef enum SOperatorParamType { OP_GET_PARAM = 1, OP_NOTIFY_PARAM } SOperatorParamType;

typedef enum EExtWinMode {
  EEXT_MODE_SCALAR = 1,
  EEXT_MODE_AGG,
  EEXT_MODE_INDEFR_FUNC,
} EExtWinMode;

#define IS_VALID_SESSION_WIN(winInfo)        ((winInfo).sessionWin.win.skey > 0)
#define SET_SESSION_WIN_INVALID(winInfo)     ((winInfo).sessionWin.win.skey = INT64_MIN)
#define IS_INVALID_SESSION_WIN_KEY(winKey)   ((winKey).win.skey <= 0)
#define SET_SESSION_WIN_KEY_INVALID(pWinKey) ((pWinKey)->win.skey = INT64_MIN)

#define IS_STREAM_MODE(_task) ((_task)->execModel == OPTR_EXEC_MODEL_STREAM)

/**
 * If the number of generated results is greater than this value,
 * query query will be halt and return results to client immediate.
 */
typedef struct SResultInfo {  // TODO refactor
  int64_t totalRows;          // total generated result size in rows
  int64_t totalBytes;         // total results in bytes.
  int32_t capacity;           // capacity of current result output buffer
  int32_t threshold;          // result size threshold in rows.
} SResultInfo;

typedef struct STableQueryInfo {
  TSKEY              lastKey;  // last check ts, todo remove it later
  SResultRowPosition pos;      // current active time window
} STableQueryInfo;

typedef struct SLimit {
  int64_t limit;
  int64_t offset;
} SLimit;

typedef struct STableScanAnalyzeInfo SFileBlockLoadRecorder;

enum {
  STREAM_RECOVER_STEP__NONE = 0,
  STREAM_RECOVER_STEP__PREPARE1,
  STREAM_RECOVER_STEP__PREPARE2,
  STREAM_RECOVER_STEP__SCAN1,
};

extern int32_t exchangeObjRefPool;

typedef struct {
  char*   pData;
  bool    isNull;
  int16_t type;
  int32_t bytes;
} SGroupKeys, SStateKeys;

typedef struct {
  char*           tablename;
  char*           dbname;
  int32_t         tversion;
  int32_t         rversion;
  SSchemaWrapper* sw;
  SSchemaWrapper* qsw;
} SSchemaInfo;

typedef struct SExchangeOpStopInfo {
  int32_t operatorType;
  int64_t refId;
} SExchangeOpStopInfo;

typedef struct SGcOperatorParam {
  int64_t sessionId;
  int32_t downstreamIdx;
  int32_t vgId;
  int64_t tbUid;
  bool    needCache;
} SGcOperatorParam;

typedef struct SGcNotifyOperatorParam {
  int32_t downstreamIdx;
  int32_t vgId;
  int64_t tbUid;
} SGcNotifyOperatorParam;

typedef struct SExprSupp {
  SExprInfo*      pExprInfo;
  int32_t         numOfExprs;  // the number of scalar expression in group operator
  SqlFunctionCtx* pCtx;
  int32_t*        rowEntryInfoOffset;  // offset value for each row result cell info
  SFilterInfo*    pFilterInfo;
  bool            hasWindowOrGroup;
} SExprSupp;

typedef enum {
  EX_SOURCE_DATA_NOT_READY = 0x1,
  EX_SOURCE_DATA_STARTED,
  EX_SOURCE_DATA_READY,
  EX_SOURCE_DATA_EXHAUSTED,
} EX_SOURCE_STATUS;

#define COL_MATCH_FROM_COL_ID  0x1
#define COL_MATCH_FROM_SLOT_ID 0x2

typedef struct SLoadRemoteDataInfo {
  uint64_t totalSize;     // total load bytes from remote
  uint64_t totalRows;     // total number of rows
  uint64_t totalElapsed;  // total elapsed time
} SLoadRemoteDataInfo;

typedef struct SLimitInfo {
  SLimit   limit;
  SLimit   slimit;
  uint64_t currentGroupId;
  int64_t  remainGroupOffset;
  int64_t  numOfOutputGroups;
  int64_t  remainOffset;
  int64_t  numOfOutputRows;
} SLimitInfo;

typedef struct SSortMergeJoinOperatorParam {
  bool initDownstream;
} SSortMergeJoinOperatorParam;

typedef struct SExchangeOperatorBasicParam {
  int32_t        vgId;
  int32_t        srcOpType;
  bool           tableSeq;
  SArray*        uidList;
  bool           isVtbRefScan;
  bool           isVtbTagScan;
  SOrgTbInfo*    colMap;
  STimeWindow    window;
} SExchangeOperatorBasicParam;

typedef struct SExchangeOperatorBatchParam {
  bool       multiParams;
  SSHashObj* pBatchs;  // SExchangeOperatorBasicParam
} SExchangeOperatorBatchParam;

typedef struct SExchangeOperatorParam {
  bool                        multiParams;
  SExchangeOperatorBasicParam basic;
} SExchangeOperatorParam;

typedef struct SExchangeSrcIndex {
  int32_t srcIdx;
  int32_t inUseIdx;
} SExchangeSrcIndex;

typedef struct SExchangeInfo {
  int64_t    seqId;
  SArray*    pSources;
  SSHashObj* pHashSources;
  SArray*    pSourceDataInfo;
  tsem_t     ready;
  void*      pTransporter;

  // SArray<SSDataBlock*>, result block list, used to keep the multi-block that
  // passed by downstream operator
  SArray*      pResultBlockList;
  SArray*      pRecycledBlocks;  // build a pool for small data block to avoid to repeatly create and then destroy.
  SSDataBlock* pDummyBlock;      // dummy block, not keep data
  bool         seqLoadData;      // sequential load data or not, false by default
  bool         dynamicOp;
  bool         dynTbname;         // %%tbname for stream    
  int32_t      current;
  SLoadRemoteDataInfo loadInfo;
  uint64_t            self;
  SLimitInfo          limitInfo;
  int64_t             openedTs;  // start exec time stamp, todo: move to SLoadRemoteDataInfo
  char*               pTaskId;
  SArray*             pFetchRpcHandles;
} SExchangeInfo;

typedef struct SScanInfo {
  int32_t numOfAsc;
  int32_t numOfDesc;
} SScanInfo;

typedef struct SSampleExecInfo {
  double   sampleRatio;  // data block sample ratio, 1 by default
  uint32_t seed;         // random seed value
} SSampleExecInfo;

enum {
  TABLE_SCAN__TABLE_ORDER = 1,
  TABLE_SCAN__BLOCK_ORDER = 2,
};

typedef enum ETableCountState {
  TABLE_COUNT_STATE_NONE = 0,       // before start scan
  TABLE_COUNT_STATE_SCAN = 1,       // cur group scanning
  TABLE_COUNT_STATE_PROCESSED = 2,  // cur group processed
  TABLE_COUNT_STATE_END = 3,        // finish or noneed to process
} ETableCountState;

typedef struct SAggSupporter {
  SSHashObj*     pResultRowHashTable;  // quick locate the window object for each result
  char*          keyBuf;               // window key buffer
  SDiskbasedBuf* pResultBuf;           // query result buffer based on blocked-wised disk file
  int32_t        resultRowSize;  // the result buffer size for each result row, with the meta data size for each row
  int32_t        currentPageId;  // current write page id
} SAggSupporter;

typedef struct {
  // if the upstream is an interval operator, the interval info is also kept here to get the time window to check if
  // current data block needs to be loaded.
  SInterval      interval;
  SAggSupporter* pAggSup;
  SExprSupp*     pExprSup;  // expr supporter of aggregate operator
} SAggOptrPushDownInfo;

typedef struct STableMetaCacheInfo {
  SLRUCache* pTableMetaEntryCache;  // 100 by default
  uint64_t   metaFetch;
  uint64_t   cacheHit;
} STableMetaCacheInfo;

typedef struct STableScanBase {
  STsdbReader*           dataReader;
  SFileBlockLoadRecorder readRecorder;
  SQueryTableDataCond    cond;
  SQueryTableDataCond    orgCond; // use for virtual super table scan
  SAggOptrPushDownInfo   pdInfo;
  SColMatchInfo          matchInfo;
  SReadHandle            readHandle;
  SExprSupp              pseudoSup;
  STableMetaCacheInfo    metaCache;
  int32_t                scanFlag;  // table scan flag to denote if it is a repeat/reverse/main scan
  int32_t                dataBlockLoadFlag;
  SLimitInfo             limitInfo;
  // there are more than one table list exists in one task, if only one vnode exists.
  STableListInfo* pTableListInfo;
  TsdReader       readerAPI;
} STableScanBase;

typedef struct STableScanInfo {
  STableScanBase  base;
  SScanInfo       scanInfo;
  int32_t         scanTimes;
  SSDataBlock*    pResBlock;
  SHashObj*       pIgnoreTables;
  SSampleExecInfo sample;           // sample execution info
  int32_t         tableStartIndex;  // current group scan start
  int32_t         tableEndIndex;    // current group scan end
  int32_t         currentGroupId;
  int32_t         currentTable;
  int8_t          scanMode;
  int8_t          assignBlockUid;
  uint8_t         countState;  // empty table count state
  bool            hasGroupByTag;
  bool            filesetDelimited;
  bool            needCountEmptyTable;
  SSDataBlock*    pOrgBlock;
  bool            ignoreTag;
  bool            virtualStableScan;
} STableScanInfo;

typedef enum ESubTableInputType {
  SUB_TABLE_MEM_BLOCK,
  SUB_TABLE_EXT_PAGES,
} ESubTableInputType;

typedef struct STmsSubTableInput {
  STsdbReader*        pReader;
  SQueryTableDataCond tblCond;
  STableKeyInfo*      pKeyInfo;
  bool                bInMemReader;
  ESubTableInputType  type;
  SSDataBlock*        pReaderBlock;

  SArray*      aBlockPages;
  SSDataBlock* pPageBlock;
  int32_t      pageIdx;

  int32_t      rowIdx;
  int64_t*     aTs;
  SSDataBlock* pInputBlock;
} STmsSubTableInput;

typedef struct SBlockOrderInfo SBlockOrderInfo;
typedef struct STmsSubTablesMergeInfo {
  SBlockOrderInfo* pTsOrderInfo;
  SBlockOrderInfo* pPkOrderInfo;

  int32_t                 numSubTables;
  STmsSubTableInput*      aInputs;
  SMultiwayMergeTreeInfo* pTree;
  int32_t                 numSubTablesCompleted;

  int32_t        numTableBlocksInMem;
  SDiskbasedBuf* pBlocksBuf;

  int32_t numInMemReaders;
} STmsSubTablesMergeInfo;

typedef struct STableMergeScanInfo {
  int32_t         tableStartIndex;
  int32_t         tableEndIndex;
  bool            hasGroupId;
  uint64_t        groupId;
  STableScanBase  base;
  int32_t         bufPageSize;
  uint32_t        sortBufSize;  // max buffer size for in-memory sort
  SArray*         pSortInfo;
  SSortHandle*    pSortHandle;
  SSDataBlock*    pSortInputBlock;
  SSDataBlock*    pReaderBlock;
  int64_t         startTs;  // sort start time
  SLimitInfo      limitInfo;
  int64_t         numOfRows;
  SScanInfo       scanInfo;
  int32_t         scanTimes;
  int32_t         readIdx;
  SSDataBlock*    pResBlock;
  SSampleExecInfo sample;         // sample execution info
  SSHashObj*      mTableNumRows;  // uid->num of table rows
  SHashObj*       mSkipTables;
  int64_t         mergeLimit;
  SSortExecInfo   sortExecInfo;
  bool            needCountEmptyTable;
  bool            bGroupProcessed;  // the group return data means processed
  bool            filesetDelimited;
  bool            bNewFilesetEvent;
  bool            bNextDurationBlockEvent;
  int32_t         numNextDurationBlocks;
  SSDataBlock*    nextDurationBlocks[2];
  bool            rtnNextDurationBlocks;
  int32_t         nextDurationBlocksIdx;
  bool            bSortRowId;

  STmsSubTablesMergeInfo* pSubTablesMergeInfo;
} STableMergeScanInfo;

typedef struct STagScanFilterContext {
  SHashObj* colHash;
  int32_t   index;
  SArray*   cInfoList;
  int32_t   code;
} STagScanFilterContext;

typedef struct STagScanInfo {
  SColumnInfo*          pCols;
  SSDataBlock*          pRes;
  SColMatchInfo         matchInfo;
  int32_t               curPos;
  SReadHandle           readHandle;
  STableListInfo*       pTableListInfo;
  uint64_t              suid;
  void*                 pCtbCursor;
  SNode*                pTagCond;
  SNode*                pTagIndexCond;
  STagScanFilterContext filterCtx;
  SArray*               aUidTags;     // SArray<STUidTagInfo>
  SArray*               aFilterIdxs;  // SArray<int32_t>
  SStorageAPI*          pStorageAPI;
  SLimitInfo            limitInfo;
} STagScanInfo;

typedef enum EStreamScanMode {
  STREAM_SCAN_FROM_READERHANDLE = 1,
  STREAM_SCAN_FROM_RES,
  STREAM_SCAN_FROM_UPDATERES,
  STREAM_SCAN_FROM_DELETE_DATA,
  STREAM_SCAN_FROM_DATAREADER_RETRIEVE,
  STREAM_SCAN_FROM_DATAREADER_RANGE,
  STREAM_SCAN_FROM_CREATE_TABLERES,
} EStreamScanMode;

enum {
  PROJECT_RETRIEVE_CONTINUE = 0x1,
  PROJECT_RETRIEVE_DONE = 0x2,
};

typedef struct SStreamAggSupporter {
  int32_t         resultRowSize;  // the result buffer size for each result row, with the meta data size for each row
  SSDataBlock*    pScanBlock;
  SStreamState*   pState;
  int64_t         gap;        // stream session window gap
  SqlFunctionCtx* pDummyCtx;  // for combine
  SSHashObj*      pResultRows;
  int32_t         stateKeySize;
  int16_t         stateKeyType;
  SDiskbasedBuf*  pResultBuf;
  SStateStore     stateStore;
  STimeWindow     winRange;
  SStorageAPI*    pSessionAPI;
  struct SUpdateInfo* pUpdateInfo;
  int32_t             windowCount;
  int32_t             windowSliding;
  SStreamStateCur*    pCur;
} SStreamAggSupporter;

typedef struct SWindowSupporter {
  SStreamAggSupporter* pStreamAggSup;
  int64_t              gap;
  uint16_t             parentType;
  SAggSupporter*       pIntervalAggSup;
} SWindowSupporter;

typedef struct SPartitionBySupporter {
  SArray* pGroupCols;     // group by columns, SArray<SColumn>
  SArray* pGroupColVals;  // current group column values, SArray<SGroupKeys>
  char*   keyBuf;         // group by keys for hash
  bool    needCalc;       // partition by column
} SPartitionBySupporter;

typedef struct SPartitionDataInfo {
  uint64_t groupId;
  char*    tbname;
  SArray*  rowIds;
} SPartitionDataInfo;

typedef struct STimeWindowAggSupp {
  int8_t          calTrigger;
  int8_t          calTriggerSaved;
  int64_t         deleteMark;
  int64_t         deleteMarkSaved;
  int64_t         waterMark;
  TSKEY           maxTs;
  TSKEY           minTs;
  SColumnInfoData timeWindowData;  // query time window info for scalar function execution.
} STimeWindowAggSupp;

typedef struct SStreamNotifyEventSupp {
  SHashObj*    pWindowEventHashMap;  // Hash map from gorupid+skey+eventType to the list node of window event.
  SHashObj*    pTableNameHashMap;    // Hash map from groupid to the dest child table name.
  SSDataBlock* pEventBlock;          // The datablock contains all window events and results.
  SArray*      pSessionKeys;
  const char*  windowType;
} SStreamNotifyEventSupp;

typedef struct SSteamOpBasicInfo {
  int32_t                primaryPkIndex;
  int16_t                operatorFlag;
  SStreamNotifyEventSupp notifyEventSup;
  bool                   recvCkBlock;
  SSDataBlock*           pCheckpointRes;
  SSHashObj*             pSeDeleted;
  void*                  pDelIterator;
  SSDataBlock*           pDelRes;
  SArray*                pUpdated;
  STableTsDataState*     pTsDataState;
  int32_t                numOfRecv;
} SSteamOpBasicInfo;

typedef struct SStreamFillSupporter {
  int32_t        type;  // fill type
  SInterval      interval;
  SResultRowData prev;
  TSKEY          prevOriginKey;
  SResultRowData cur;
  SResultRowData next;
  TSKEY          nextOriginKey;
  SResultRowData nextNext;
  SFillColInfo*  pAllColInfo;  // fill exprs and not fill exprs
  SExprSupp      notFillExprSup;
  int32_t        numOfAllCols;  // number of all exprs, including the tags columns
  int32_t        numOfFillCols;
  int32_t        numOfNotFillCols;
  int32_t        rowSize;
  SSHashObj*     pResMap;
  bool           hasDelete;
  SStorageAPI*   pAPI;
  STimeWindow    winRange;
  int32_t        pkColBytes;
  __compar_fn_t  comparePkColFn;
  int32_t*       pOffsetInfo;
  bool           normalFill;
  void*          pEmptyRow;
  SArray*        pResultRange;
} SStreamFillSupporter;

typedef struct SStreamScanInfo {
  SSteamOpBasicInfo basic;
  SExprInfo*        pPseudoExpr;
  int32_t           numOfPseudoExpr;
  SExprSupp         tbnameCalSup;
  SExprSupp*        pPartTbnameSup;
  SExprSupp         tagCalSup;
  int32_t           primaryTsIndex;  // primary time stamp slot id
  int32_t           primaryKeyIndex;
  SReadHandle       readHandle;
  SInterval         interval;  // if the upstream is an interval operator, the interval info is also kept here.
  SColMatchInfo     matchInfo;

  SArray*      pBlockLists;  // multiple SSDatablock.
  SSDataBlock* pRes;         // result SSDataBlock
  SSDataBlock* pUpdateRes;   // update SSDataBlock
  int32_t      updateResIndex;
  int32_t      blockType;        // current block type
  int32_t      validBlockIndex;  // Is current data has returned?
  uint64_t     numOfExec;        // execution times
  STqReader*   tqReader;

  SHashObj*       pVtableMergeHandles;  // key: vtable uid, value: SStreamVtableMergeHandle
  SDiskbasedBuf*  pVtableMergeBuf;      // page buffer used by vtable merge
  SArray*         pVtableReadyHandles;
  STableListInfo* pTableListInfo;

  uint64_t            groupId;
  bool                igCheckGroupId;
  struct SUpdateInfo* pUpdateInfo;

  EStreamScanMode       scanMode;
  struct SOperatorInfo* pStreamScanOp;
  struct SOperatorInfo* pTableScanOp;
  SArray*               childIds;
  SWindowSupporter      windowSup;
  SPartitionBySupporter partitionSup;
  SExprSupp*            pPartScalarSup;
  bool                  assignBlockUid;  // assign block uid to groupId, temporarily used for generating rollup SMA.
  int32_t               scanWinIndex;    // for state operator
  SSDataBlock*          pDeleteDataRes;  // delete data SSDataBlock
  int32_t               deleteDataIndex;
  STimeWindow           updateWin;
  STimeWindowAggSupp    twAggSup;
  SSDataBlock*          pUpdateDataRes;
  SStreamFillSupporter* pFillSup;
  // status for tmq
  SNodeList* pGroupTags;
  SNode*     pTagCond;
  SNode*     pTagIndexCond;

  // recover
  int32_t      blockRecoverTotCnt;
  SSDataBlock* pRecoverRes;

  SSDataBlock*      pCreateTbRes;
  int8_t            igCheckUpdate;
  int8_t            igExpired;
  void*             pState;  // void
  SStoreTqReader    readerFn;
  SStateStore       stateStore;
  SSDataBlock*      pCheckpointRes;
  int8_t            pkColType;
  int32_t           pkColLen;
  bool              useGetResultRange;
  STimeWindow       lastScanRange;
  SSDataBlock*      pRangeScanRes;  // update SSDataBlock
  bool              hasPart;

  //nonblock data scan
  TSKEY                  recalculateInterval;
  __compar_fn_t          comparePkColFn;
  SScanRange             curRange;
  struct SOperatorInfo*  pRecTableScanOp;
  bool                   scanAllTables;
  SSHashObj*             pRecRangeMap;
  SArray*                pRecRangeRes;
} SStreamScanInfo;

typedef struct {
  struct SVnode*       vnode;  // todo remove this
  SSDataBlock          pRes;   // result SSDataBlock
  STsdbReader*         dataReader;
  struct SSnapContext* sContext;
  SStorageAPI*         pAPI;
  STableListInfo*      pTableListInfo;
} SStreamRawScanInfo;

typedef struct STableCountScanSupp {
  int16_t dbNameSlotId;
  int16_t stbNameSlotId;
  int16_t tbCountSlotId;
  bool    groupByDbName;
  bool    groupByStbName;
  char    dbNameFilter[TSDB_DB_NAME_LEN];
  char    stbNameFilter[TSDB_TABLE_NAME_LEN];
} STableCountScanSupp;

typedef struct SOptrBasicInfo {
  SResultRowInfo resultRowInfo;
  SSDataBlock*   pRes;
  bool           mergeResultBlock;
  int32_t        inputTsOrder;
  int32_t        outputTsOrder;
} SOptrBasicInfo;

typedef struct SIntervalAggOperatorInfo {
  SOptrBasicInfo     binfo;              // basic info
  SAggSupporter      aggSup;             // aggregate supporter
  SExprSupp          scalarSupp;         // supporter for perform scalar function
  SGroupResInfo      groupResInfo;       // multiple results build supporter
  SInterval          interval;           // interval info
  int32_t            primaryTsIndex;     // primary time stamp slot id from result of downstream operator.
  STimeWindow        win;                // query time range
  bool               timeWindowInterpo;  // interpolation needed or not
  SArray*            pInterpCols;        // interpolation columns
  EOPTR_EXEC_MODEL   execModel;          // operator execution model [batch model|stream model]
  STimeWindowAggSupp twAggSup;
  SArray*            pPrevValues;  //  SArray<SGroupKeys> used to keep the previous not null value for interpolation.
  bool               cleanGroupResInfo;
  struct SOperatorInfo* pOperator;
  // for limit optimization
  bool          limited;
  int64_t       limit;
  bool          slimited;
  int64_t       slimit;
  uint64_t      curGroupId;  // initialize to UINT64_MAX
  uint64_t      handledGroupNum;
  BoundedQueue* pBQ;
} SIntervalAggOperatorInfo;

typedef struct SMergeAlignedIntervalAggOperatorInfo {
  SIntervalAggOperatorInfo* intervalAggOperatorInfo;

  uint64_t     groupId;  // current groupId
  int64_t      curTs;    // current ts
  SSDataBlock* prefetchedBlock;
  SResultRow*  pResultRow;
} SMergeAlignedIntervalAggOperatorInfo;

typedef struct SOpCheckPointInfo {
  uint16_t  checkPointId;
  SHashObj* children;  // key:child id
} SOpCheckPointInfo;

typedef struct SDataGroupInfo {
  uint64_t groupId;
  int64_t  numOfRows;
  SArray*  pPageList;
  SArray*  blockForNotLoaded;   // SSDataBlock that data is not loaded
  int32_t  offsetForNotLoaded;  // read offset for SSDataBlock that data is not loaded
} SDataGroupInfo;

typedef struct SWindowRowsSup {
  STimeWindow win;
  TSKEY       prevTs;
  int32_t     startRowIndex;
  int32_t     numOfRows;
  uint64_t    groupId;
} SWindowRowsSup;

typedef int32_t (*AggImplFn)(struct SOperatorInfo* pOperator, SSDataBlock* pBlock);

typedef struct SSessionAggOperatorInfo {
  SOptrBasicInfo        binfo;
  SAggSupporter         aggSup;
  SExprSupp             scalarSupp;  // supporter for perform scalar function
  SGroupResInfo         groupResInfo;
  SWindowRowsSup        winSup;
  bool                  reptScan;  // next round scan
  int64_t               gap;       // session window gap
  int32_t               tsSlotId;  // primary timestamp slot id
  STimeWindowAggSupp    twAggSup;
  struct SOperatorInfo* pOperator;
  bool                  cleanGroupResInfo;
} SSessionAggOperatorInfo;

typedef struct SStateWindowOperatorInfo {
  SOptrBasicInfo        binfo;
  SAggSupporter         aggSup;
  SExprSupp             scalarSup;
  SGroupResInfo         groupResInfo;
  SWindowRowsSup        winSup;
  SColumn               stateCol;  // start row index
  bool                  hasKey;
  SStateKeys            stateKey;
  int32_t               tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp    twAggSup;
  struct SOperatorInfo* pOperator;
  bool                  cleanGroupResInfo;
  int64_t               trueForLimit;
} SStateWindowOperatorInfo;


typedef struct SEventWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  SWindowRowsSup     winSup;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
  uint64_t           groupId;  // current group id, used to identify the data block from different groups
  SFilterInfo*       pStartCondInfo;
  SFilterInfo*       pEndCondInfo;
  bool               inWindow;
  SResultRow*        pRow;
  SSDataBlock*       pPreDataBlock;
  struct SOperatorInfo*     pOperator;
  int64_t            trueForLimit;
} SEventWindowOperatorInfo;

#define OPTR_IS_OPENED(_optr)  (((_optr)->status & OP_OPENED) == OP_OPENED)
#define OPTR_SET_OPENED(_optr) ((_optr)->status |= OP_OPENED)
#define OPTR_CLR_OPENED(_optr) ((_optr)->status &= ~OP_OPENED)

SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode);

int32_t initQueriedTableSchemaInfo(SReadHandle* pHandle, SScanPhysiNode* pScanNode, const char* dbName,
                                   SExecTaskInfo* pTaskInfo);
void    cleanupQueriedTableScanInfo(void* p);

void initBasicInfo(SOptrBasicInfo* pInfo, SSDataBlock* pBlock);
void cleanupBasicInfo(SOptrBasicInfo* pInfo);

int32_t initExprSupp(SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfExpr, SFunctionStateStore* pStore);
void    cleanupExprSupp(SExprSupp* pSup);
void    cleanupExprSuppWithoutFilter(SExprSupp* pSupp);

void     cleanupResultInfoInStream(SExecTaskInfo* pTaskInfo, void* pState, SExprSupp* pSup,
                                   SGroupResInfo* pGroupResInfo);
void     cleanupResultInfoInHashMap(SExecTaskInfo* pTaskInfo, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                    SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap);
void     cleanupResultInfo(SExecTaskInfo* pTaskInfo, SExprSupp* pSup, SGroupResInfo* pGroupResInfo,
                           SAggSupporter *pAggSup, bool cleanHashmap);
void     cleanupResultInfoWithoutHash(SExecTaskInfo* pTaskInfo, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                                      SGroupResInfo* pGroupResInfo);

int32_t initAggSup(SExprSupp* pSup, SAggSupporter* pAggSup, SExprInfo* pExprInfo, int32_t numOfCols, size_t keyBufSize,
                   const char* pkey, void* pState, SFunctionStateStore* pStore);
void    cleanupAggSup(SAggSupporter* pAggSup);

void initResultSizeInfo(SResultInfo* pResultInfo, int32_t numOfRows);

void doBuildResultDatablock(struct SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                            SDiskbasedBuf* pBuf);

/**
 * @brief copydata from hash table, instead of copying from SGroupResInfo's pRow
 */
void doCopyToSDataBlockByHash(SExecTaskInfo* pTaskInfo, SSDataBlock* pBlock, SExprSupp* pSup, SDiskbasedBuf* pBuf,
                              SGroupResInfo* pGroupResInfo, SSHashObj* pHashmap, int32_t threshold, bool ignoreGroup);

bool hasLimitOffsetInfo(SLimitInfo* pLimitInfo);
bool hasSlimitOffsetInfo(SLimitInfo* pLimitInfo);
void initLimitInfo(const SNode* pLimit, const SNode* pSLimit, SLimitInfo* pLimitInfo);
void resetLimitInfoForNextGroup(SLimitInfo* pLimitInfo);
bool applyLimitOffset(SLimitInfo* pLimitInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo);

int32_t applyAggFunctionOnPartialTuples(SExecTaskInfo* taskInfo, SqlFunctionCtx* pCtx, SColumnInfoData* pTimeWindowData,
                                        int32_t offset, int32_t forwardStep, int32_t numOfTotal, int32_t numOfOutput);

int32_t setFunctionResultOutput(struct SOperatorInfo* pOperator, SOptrBasicInfo* pInfo, SAggSupporter* pSup, int32_t stage,
                             int32_t numOfExprs);
int32_t      setRowTsColumnOutputInfo(SqlFunctionCtx* pCtx, int32_t numOfCols, SArray** pResList);                             
int32_t extractDataBlockFromFetchRsp(SSDataBlock* pRes, char* pData, SArray* pColList, char** pNextStart);
void    updateLoadRemoteInfo(SLoadRemoteDataInfo* pInfo, int64_t numOfRows, int32_t dataLen, int64_t startTs,
                             struct SOperatorInfo* pOperator);

STimeWindow getFirstQualifiedTimeWindow(int64_t ts, STimeWindow* pWindow, SInterval* pInterval, int32_t order);
int32_t     getBufferPgSize(int32_t rowSize, uint32_t* defaultPgsz, int64_t* defaultBufsz);

extern void doDestroyExchangeOperatorInfo(void* param);

int32_t doFilter(SSDataBlock* pBlock, SFilterInfo* pFilterInfo, SColMatchInfo* pColMatchInfo);
int32_t addTagPseudoColumnData(SReadHandle* pHandle, const SExprInfo* pExpr, int32_t numOfExpr, SSDataBlock* pBlock,
                               int32_t rows, SExecTaskInfo* pTask, STableMetaCacheInfo* pCache);

int32_t appendOneRowToDataBlock(SSDataBlock* pBlock, STupleHandle* pTupleHandle);
int32_t setResultRowInitCtx(SResultRow* pResult, SqlFunctionCtx* pCtx, int32_t numOfOutput,
                            int32_t* rowEntryInfoOffset);
void    clearResultRowInitFlag(SqlFunctionCtx* pCtx, int32_t numOfOutput);

SResultRow* doSetResultOutBufByKey(SDiskbasedBuf* pResultBuf, SResultRowInfo* pResultRowInfo, char* pData,
                                   int32_t bytes, bool masterscan, uint64_t groupId, SExecTaskInfo* pTaskInfo,
                                   bool isIntervalQuery, SAggSupporter* pSup, bool keepGroup);

int32_t projectApplyFunctions(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock, SqlFunctionCtx* pCtx,
                              int32_t numOfOutput, SArray* pPseudoList, const void* pExtraParams);
int32_t projectApplyFunctionsWithSelect(SExprInfo* pExpr, SSDataBlock* pResult, SSDataBlock* pSrcBlock,
                                        SqlFunctionCtx* pCtx, int32_t numOfOutput, SArray* pPseudoList,
                                        const void* pExtraParams, bool doSelectFunc);

int32_t setInputDataBlock(SExprSupp* pExprSupp, SSDataBlock* pBlock, int32_t order, int32_t scanFlag,
                          bool createDummyCol);

int32_t checkForQueryBuf(size_t numOfTables);

int32_t createDataSinkParam(SDataSinkNode* pNode, void** pParam, SExecTaskInfo* pTask, SReadHandle* readHandle);

STimeWindow getActiveTimeWindow(SDiskbasedBuf* pBuf, SResultRowInfo* pResultRowInfo, int64_t ts, SInterval* pInterval,
                                int32_t order);
int32_t getNumOfRowsInTimeWindow(SDataBlockInfo* pDataBlockInfo, TSKEY* pPrimaryColumn, int32_t startPos, TSKEY ekey,
                                 __block_search_fn_t searchFn, STableQueryInfo* item, int32_t order);
int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
SResultRow* getNewResultRow(SDiskbasedBuf* pResultBuf, int32_t* currentPageId, int32_t interBufSize);
void getCurSessionWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId, SSessionKey* pKey);
bool isInTimeWindow(STimeWindow* pWin, TSKEY ts, int64_t gap);
bool functionNeedToExecute(SqlFunctionCtx* pCtx);
bool isOverdue(TSKEY ts, STimeWindowAggSupp* pSup);
bool isCloseWindow(STimeWindow* pWin, STimeWindowAggSupp* pSup);
bool isDeletedStreamWindow(STimeWindow* pWin, uint64_t groupId, void* pState, STimeWindowAggSupp* pTwSup,
                           SStateStore* pStore);

uint64_t calGroupIdByData(SPartitionBySupporter* pParSup, SExprSupp* pExprSup, SSDataBlock* pBlock, int32_t rowId);

void finalizeResultRows(SDiskbasedBuf* pBuf, SResultRowPosition* resultRowPosition, SExprSupp* pSup,
                        SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo);

bool    groupbyTbname(SNodeList* pGroupList);
void    getNextIntervalWindow(SInterval* pInterval, STimeWindow* tw, int32_t order);
int32_t getForwardStepsInBlock(int32_t numOfRows, __block_search_fn_t searchFn, TSKEY ekey, int32_t pos, int32_t order,
                               int64_t* pData);
SSDataBlock* buildCreateTableBlock(SExprSupp* tbName, SExprSupp* tag);
SExprInfo*   createExpr(SNodeList* pNodeList, int32_t* numOfExprs);

int32_t copyResultrowToDataBlock(SExprInfo* pExprInfo, int32_t numOfExprs, SResultRow* pRow, SqlFunctionCtx* pCtx,
                                 SSDataBlock* pBlock, const int32_t* rowEntryOffset, SExecTaskInfo* pTaskInfo);
void doUpdateNumOfRows(SqlFunctionCtx* pCtx, SResultRow* pRow, int32_t numOfExprs, const int32_t* rowEntryOffset);

void    streamOpReleaseState(struct SOperatorInfo* pOperator);
void    streamOpReloadState(struct SOperatorInfo* pOperator);
void    destroyStreamAggSupporter(SStreamAggSupporter* pSup);
void    clearGroupResInfo(SGroupResInfo* pGroupResInfo);
int32_t initBasicInfoEx(SOptrBasicInfo* pBasicInfo, SExprSupp* pSup, SExprInfo* pExprInfo, int32_t numOfCols,
                        SSDataBlock* pResultBlock, SFunctionStateStore* pStore);
int32_t initStreamAggSupporter(SStreamAggSupporter* pSup, SExprSupp* pExpSup, int32_t numOfOutput, int64_t gap,
                               SStreamState* pState, int32_t keySize, int16_t keyType, SStateStore* pStore,
                               SReadHandle* pHandle, STimeWindowAggSupp* pTwAggSup, const char* taskIdStr,
                               SStorageAPI* pApi, int32_t tsIndex, int8_t stateType, int32_t ratio);
int32_t initDownStream(struct SOperatorInfo* downstream, SStreamAggSupporter* pAggSup, uint16_t type,
                       int32_t tsColIndex, STimeWindowAggSupp* pTwSup, struct SSteamOpBasicInfo* pBasic, int64_t recalculateInterval);
int32_t getMaxTsWins(const SArray* pAllWins, SArray* pMaxWins);
void    initGroupResInfoFromArrayList(SGroupResInfo* pGroupResInfo, SArray* pArrayList);
void    getSessionHashKey(const SSessionKey* pKey, SSessionKey* pHashKey);
int32_t deleteSessionWinState(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SSHashObj* pMapUpdate,
                              SSHashObj* pMapDelete, SSHashObj* pPkDelete, bool needAdd);
int32_t getAllSessionWindow(SSHashObj* pHashMap, SSHashObj* pStUpdated);
int32_t closeSessionWindow(SSHashObj* pHashMap, STimeWindowAggSupp* pTwSup, SSHashObj* pClosed);
int32_t copyUpdateResult(SSHashObj** ppWinUpdated, SArray* pUpdated, __compar_fn_t compar);
int32_t sessionKeyCompareAsc(const void* pKey1, const void* pKey2);
void    removeSessionDeleteResults(SSHashObj* pHashMap, SArray* pWins);
int32_t doOneWindowAggImpl(SColumnInfoData* pTimeWindowData, SResultWindowInfo* pCurWin, SResultRow** pResult,
                           int32_t startIndex, int32_t winRows, int32_t rows, int32_t numOutput,
                           struct SOperatorInfo* pOperator, int64_t winDelta);
void    setSessionWinOutputInfo(SSHashObj* pStUpdated, SResultWindowInfo* pWinInfo);
int32_t saveSessionOutputBuf(SStreamAggSupporter* pAggSup, SResultWindowInfo* pWinInfo);
int32_t saveResult(SResultWindowInfo winInfo, SSHashObj* pStUpdated);
int32_t saveDeleteRes(SSHashObj* pStDelete, SSessionKey key);
void    removeSessionResult(SStreamAggSupporter* pAggSup, SSHashObj* pHashMap, SSHashObj* pResMap, SSessionKey* pKey);
void    doBuildDeleteDataBlock(struct SOperatorInfo* pOp, SSHashObj* pStDeleted, SSDataBlock* pBlock, void** Ite,
                               SGroupResInfo* pGroupResInfo);
void    doBuildSessionResult(struct SOperatorInfo* pOperator, void* pState, SGroupResInfo* pGroupResInfo,
                             SSDataBlock* pBlock, SArray* pSessionKeys);
int32_t getSessionWindowInfoByKey(SStreamAggSupporter* pAggSup, SSessionKey* pKey, SResultWindowInfo* pWinInfo);
void    getNextSessionWinInfo(SStreamAggSupporter* pAggSup, SSHashObj* pStUpdated, SResultWindowInfo* pCurWin,
                              SResultWindowInfo* pNextWin);
int32_t compactTimeWindow(SExprSupp* pSup, SStreamAggSupporter* pAggSup, STimeWindowAggSupp* pTwAggSup,
                          SExecTaskInfo* pTaskInfo, SResultWindowInfo* pCurWin, SResultWindowInfo* pNextWin,
                          SSHashObj* pStUpdated, SSHashObj* pStDeleted, bool addGap);
void    releaseOutputBuf(void* pState, SRowBuffPos* pPos, SStateStore* pAPI);
void    resetWinRange(STimeWindow* winRange);
int64_t getDeleteMark(SWindowPhysiNode* pWinPhyNode, int64_t interval);
void    resetUnCloseSessionWinInfo(SSHashObj* winMap);
void    setStreamOperatorCompleted(struct SOperatorInfo* pOperator);
void    reloadAggSupFromDownStream(struct SOperatorInfo* downstream, SStreamAggSupporter* pAggSup);
void    destroyFlusedPos(void* pRes);
bool    isIrowtsPseudoColumn(SExprInfo* pExprInfo);
bool    isIsfilledPseudoColumn(SExprInfo* pExprInfo);
bool    isInterpFunc(SExprInfo* pExprInfo);
bool    isIrowtsOriginPseudoColumn(SExprInfo* pExprInfo);

int32_t encodeSSessionKey(void** buf, SSessionKey* key);
void*   decodeSSessionKey(void* buf, SSessionKey* key);
int32_t encodeSResultWindowInfo(void** buf, SResultWindowInfo* key, int32_t outLen);
void*   decodeSResultWindowInfo(void* buf, SResultWindowInfo* key, int32_t outLen);
int32_t encodeSTimeWindowAggSupp(void** buf, STimeWindowAggSupp* pTwAggSup);
void*   decodeSTimeWindowAggSupp(void* buf, STimeWindowAggSupp* pTwAggSup);

void    destroyOperatorParamValue(void* pValues);
int32_t mergeOperatorParams(SOperatorParam* pDst, SOperatorParam* pSrc);
int32_t buildTableScanOperatorParam(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, bool tableSeq);
int32_t buildTableScanOperatorParamEx(SOperatorParam** ppRes, SArray* pUidList, int32_t srcOpType, SOrgTbInfo *pMap, bool tableSeq, STimeWindow *window);
void    freeExchangeGetBasicOperatorParam(void* pParam);
void    freeOperatorParam(SOperatorParam* pParam, SOperatorParamType type);
void    freeResetOperatorParams(struct SOperatorInfo* pOperator, SOperatorParamType type, bool allFree);
int32_t getNextBlockFromDownstreamImpl(struct SOperatorInfo* pOperator, int32_t idx, bool clearParam,
                                       SSDataBlock** pResBlock);
void getCountWinRange(SStreamAggSupporter* pAggSup, const SSessionKey* pKey, EStreamType mode, SSessionKey* pDelRange);
void    doDeleteSessionWindow(SStreamAggSupporter* pAggSup, SSessionKey* pKey);

int32_t saveDeleteInfo(SArray* pWins, SSessionKey key);
void    removeSessionResults(SStreamAggSupporter* pAggSup, SSHashObj* pHashMap, SArray* pWins);
int32_t copyDeleteWindowInfo(SArray* pResWins, SSHashObj* pStDeleted);
int32_t copyDeleteSessionKey(SSHashObj* source, SSHashObj* dest);

bool inSlidingWindow(SInterval* pInterval, STimeWindow* pWin, SDataBlockInfo* pBlockInfo);
bool inCalSlidingWindow(SInterval* pInterval, STimeWindow* pWin, TSKEY calStart, TSKEY calEnd, EStreamType blockType);
bool compareVal(const char* v, const SStateKeys* pKey);
bool inWinRange(STimeWindow* range, STimeWindow* cur);
int32_t doDeleteTimeWindows(SStreamAggSupporter* pAggSup, SSDataBlock* pBlock, SArray* result);

int32_t getNextQualifiedWindow(SInterval* pInterval, STimeWindow* pNext, SDataBlockInfo* pDataBlockInfo,
                               TSKEY* primaryKeys, int32_t prevPosition, int32_t order);
int32_t extractQualifiedTupleByFilterResult(SSDataBlock* pBlock, const SColumnInfoData* p, int32_t status);
bool    getIgoreNullRes(SExprSupp* pExprSup);
bool    checkNullRow(SExprSupp* pExprSup, SSDataBlock* pSrcBlock, int32_t index, bool ignoreNull);
int64_t getMinWindowSize(struct SOperatorInfo* pOperator);

void    destroyTmqScanOperatorInfo(void* param);
int32_t checkUpdateData(SStreamScanInfo* pInfo, bool invertible, SSDataBlock* pBlock, bool out);
void resetBasicOperatorState(SOptrBasicInfo* pBasicInfo);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EXECUTORINT_H
