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

#ifndef TDENGINE_TFILL_H
#define TDENGINE_TFILL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executil.h"
#include "os.h"
#include "taosdef.h"
#include "tcommon.h"
#include "tsimplehash.h"

#define GET_DEST_SLOT_ID(_p) ((_p)->pExpr->base.resSchema.slotId)

struct SSDataBlock;

typedef struct SFillColInfo {
  int32_t    numOfFillExpr;
  SExprInfo* pExpr;
  bool       notFillCol;  // denote if this column needs fill operation
  SVariant   fillVal;
  bool       fillNull;
} SFillColInfo;

typedef struct SFillLinearInfo {
  SPoint  start;
  SPoint  end;
  bool    isStartSet;
  bool    isEndSet;
  int16_t type;
  int32_t bytes;
} SFillLinearInfo;

typedef struct {
  SSchema col;
  char*   tagVal;
} SFillTagColInfo;

typedef struct {
  int64_t key;
  SArray* pRowVal;
  SArray* pNullValueFlag;
} SRowVal;

typedef struct SColumnFillProgress {
  SListNode* pBlockNode;
  int32_t    rowIdx;
} SColumnFillProgress;

typedef struct SBlockFillProgress {
  int32_t rowIdx;
} SBlockFillProgress;

typedef struct SFillBlock {
  SSDataBlock* pBlock;
  SArray*      pFillProgress;
  bool         allColFinished;
} SFillBlock;

typedef struct SFillInfo {
  TSKEY        start;         // start timestamp
  TSKEY        end;           // endKey for fill
  TSKEY        currentKey;    // current active timestamp, the value may be changed during the fill procedure.
  int32_t      tsSlotId;      // primary time stamp slot id
  int32_t      srcTsSlotId;   // timestamp column id in the source data block.
  int32_t      order;         // order [TSDB_ORDER_ASC|TSDB_ORDER_DESC]
  int32_t      type;          // fill type
  int32_t      numOfRows;     // number of rows in the input data block
  int32_t      index;         // active row index
  int32_t      numOfTotal;    // number of filled rows in one round
  int32_t      numOfCurrent;  // number of filled rows in current results
  int32_t      numOfCols;     // number of columns, including the tags columns
  SInterval    interval;
  SRowVal      prev;
  SRowVal      next;
  SSDataBlock* pSrcBlock;
  int32_t      alloc;  // data buffer size in rows

  SFillColInfo*    pFillCol;  // column info for fill operations
  SFillTagColInfo* pTags;     // tags value for filling gap
  const char*      id;
  SExecTaskInfo*   pTaskInfo;
  int8_t           isFilled;
  SList*           pFillSavedBlockList;
  SArray*          pColFillProgress;
} SFillInfo;

typedef struct SResultCellData {
  bool    isNull;
  int8_t  type;
  int32_t bytes;
  char    pData[];
} SResultCellData;

typedef struct SResultRowData {
  TSKEY            key;
  SResultCellData* pRowVal;
} SResultRowData;

typedef struct SStreamFillLinearInfo {
  TSKEY   nextEnd;
  SArray* pEndPoints;
  SArray* pNextEndPoints;
  int64_t winIndex;
  bool    hasNext;
} SStreamFillLinearInfo;

typedef struct SStreamFillInfo {
  TSKEY                  start;    // startKey for fill
  TSKEY                  end;      // endKey for fill
  TSKEY                  current;  // current Key for fill
  TSKEY                  preRowKey;
  TSKEY                  prePointKey;
  TSKEY                  nextRowKey;
  TSKEY                  nextPointKey;
  SResultRowData*        pResRow;
  SStreamFillLinearInfo* pLinearInfo;
  bool                   needFill;
  int32_t                type;  // fill type
  int32_t                pos;
  SArray*                delRanges;
  int32_t                delIndex;
  uint64_t               curGroupId;
  bool                   hasNext;
  SResultRowData*        pNonFillRow;
  void*                  pTempBuff;
} SStreamFillInfo;

int64_t getNumOfResultsAfterFillGap(SFillInfo* pFillInfo, int64_t ekey, int32_t maxNumOfRows);

void          taosFillSetStartInfo(struct SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey);
void          taosResetFillInfo(struct SFillInfo* pFillInfo, TSKEY startTimestamp);
void          taosFillSetInputDataBlock(struct SFillInfo* pFillInfo, const struct SSDataBlock* pInput);
void          taosFillUpdateStartTimestampInfo(SFillInfo* pFillInfo, int64_t ts);
bool          taosFillNotStarted(const SFillInfo* pFillInfo);
SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfFillExpr, SExprInfo* pNotFillExpr,
                                int32_t numOfNotFillCols, SExprInfo* pFillNullExpr, int32_t numOfFillNullExprs,
                                const struct SNodeListNode* val);
bool          taosFillHasMoreResults(struct SFillInfo* pFillInfo);

int32_t taosCreateFillInfo(TSKEY skey, int32_t numOfFillCols, int32_t numOfNotFillCols, int32_t fillNullCols,
                           int32_t capacity, SInterval* pInterval, int32_t fillType, struct SFillColInfo* pCol,
                           int32_t slotId, int32_t order, const char* id, SExecTaskInfo* pTaskInfo,
                           SFillInfo** ppFillInfo);

void*   taosDestroyFillInfo(struct SFillInfo* pFillInfo);
int32_t taosFillResultDataBlock(struct SFillInfo* pFillInfo, SSDataBlock* p, int32_t capacity);
int32_t taosFillResultDataBlock2(struct SFillInfo* pFillInfo, SSDataBlock* pDstBlock, int32_t capacity, bool *wantMoreBlock);
int64_t getFillInfoStart(struct SFillInfo* pFillInfo);

bool fillIfWindowPseudoColumn(SFillInfo* pFillInfo, SFillColInfo* pCol, SColumnInfoData* pDstColInfoData,
                              int32_t rowIndex);

SFillBlock*  tFillSaveBlock(SFillInfo* pFill, SSDataBlock* pBlock, SArray* pProgress);
void         destroyFillBlock(void* p);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TFILL_H
