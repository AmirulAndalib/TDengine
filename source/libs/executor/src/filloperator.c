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

#include "filter.h"
#include "os.h"
#include "query.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tmsg.h"
#include "ttypes.h"

#include "executorInt.h"
#include "tcommon.h"
#include "thash.h"
#include "ttime.h"

#include "function.h"
#include "operator.h"
#include "querynodes.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tfill.h"

typedef struct STimeRange {
  TSKEY    skey;
  TSKEY    ekey;
  uint64_t groupId;
} STimeRange;

typedef struct SFillOperatorInfo {
  struct SFillInfo* pFillInfo;
  SSDataBlock*      pRes;
  SSDataBlock*      pFinalRes;
  int64_t           totalInputRows;
  void**            p;
  SSDataBlock*      existNewGroupBlock;
  STimeWindow       win;
  SColMatchInfo     matchInfo;
  int32_t           primaryTsCol;
  int32_t           primarySrcSlotId;
  uint64_t          curGroupId;  // current handled group id
  SExprInfo*        pExprInfo;
  int32_t           numOfExpr;
  SExprSupp         noFillExprSupp;
  SExprSupp         fillNullExprSupp;
  SList*            pFillSavedBlockList;
  SNode*            pTimeRange;  // STimeRangeNode for stream fill
} SFillOperatorInfo;

static void destroyFillOperatorInfo(void* param);
static void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag);
static int32_t fillResetPrevForNewGroup(SFillInfo* pFillInfo);
static void reviseFillStartAndEndKey(SFillOperatorInfo* pInfo, int32_t order);

static void doHandleRemainBlockForNewGroupImpl(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                               SResultInfo* pResultInfo, int32_t order) {
  pInfo->totalInputRows = pInfo->existNewGroupBlock->info.rows;
  SSDataBlock*   pResBlock = pInfo->pFinalRes;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  //  int32_t order = TSDB_ORDER_ASC;
  int32_t scanFlag = MAIN_SCAN;
  //  getTableScanInfo(pOperator, &order, &scanFlag, false);
  taosResetFillInfo(pInfo->pFillInfo, getFillInfoStart(pInfo->pFillInfo));

  blockDataCleanup(pInfo->pRes);
  doApplyScalarCalculation(pOperator, pInfo->existNewGroupBlock, order, scanFlag);

  reviseFillStartAndEndKey(pOperator->info, order);

  int64_t ts = (order == TSDB_ORDER_ASC) ? pInfo->existNewGroupBlock->info.window.ekey
                                         : pInfo->existNewGroupBlock->info.window.skey;
  taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ts);

  taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);
  if (pInfo->pFillInfo->type == TSDB_FILL_PREV || pInfo->pFillInfo->type == TSDB_FILL_LINEAR) {
    int32_t code = fillResetPrevForNewGroup(pInfo->pFillInfo);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  int32_t numOfResultRows = pResultInfo->capacity - pResBlock->info.rows;
  int32_t code = taosFillResultDataBlock2(pInfo->pFillInfo, pResBlock, numOfResultRows, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  pInfo->curGroupId = pInfo->existNewGroupBlock->info.id.groupId;
  pInfo->existNewGroupBlock = NULL;
}

static void doHandleRemainBlockFromNewGroup(SOperatorInfo* pOperator, SFillOperatorInfo* pInfo,
                                            SResultInfo* pResultInfo, int32_t order) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (taosFillHasMoreResults(pInfo->pFillInfo)) {
    int32_t numOfResultRows = pResultInfo->capacity - pInfo->pFinalRes->info.rows;
    int32_t code = taosFillResultDataBlock2(pInfo->pFillInfo, pInfo->pFinalRes, numOfResultRows, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
    pInfo->pRes->info.id.groupId = pInfo->curGroupId;
    return;
  }

  // handle the cached new group data block
  if (pInfo->existNewGroupBlock) {
    doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, order);
  }
}

void doApplyScalarCalculation(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t order, int32_t scanFlag) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SFillOperatorInfo* pInfo = pOperator->info;
  SExprSupp*         pSup = &pOperator->exprSupp;
  code = setInputDataBlock(pSup, pBlock, order, scanFlag, false);
  QUERY_CHECK_CODE(code, lino, _end);
  code = projectApplyFunctions(pSup->pExprInfo, pInfo->pRes, pBlock, pSup->pCtx, pSup->numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _end);

  // reset the row value before applying the no-fill functions to the input data block, which is "pBlock" in this case.
  pInfo->pRes->info.rows = 0;
  SExprSupp* pNoFillSupp = &pInfo->noFillExprSupp;
  code = setInputDataBlock(pNoFillSupp, pBlock, order, scanFlag, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = projectApplyFunctions(pNoFillSupp->pExprInfo, pInfo->pRes, pBlock, pNoFillSupp->pCtx, pNoFillSupp->numOfExprs,
                               NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _end);

  if (pInfo->fillNullExprSupp.pExprInfo) {
    pInfo->pRes->info.rows = 0;
    code = setInputDataBlock(&pInfo->fillNullExprSupp, pBlock, order, scanFlag, false);
    QUERY_CHECK_CODE(code, lino, _end);
    code = projectApplyFunctions(pInfo->fillNullExprSupp.pExprInfo, pInfo->pRes, pBlock, pInfo->fillNullExprSupp.pCtx,
        pInfo->fillNullExprSupp.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo));
  }

  pInfo->pRes->info.id.groupId = pBlock->info.id.groupId;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t fillResetPrevForNewGroup(SFillInfo* pFillInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  for (int32_t colIdx = 0; colIdx < pFillInfo->numOfCols; ++colIdx) {
    if (!pFillInfo->pFillCol[colIdx].notFillCol) {
      SGroupKeys* key = taosArrayGet(pFillInfo->prev.pRowVal, colIdx);
      QUERY_CHECK_NULL(key, code, lino, _end, terrno);
      key->isNull = true;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* doFillImpl2(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  if (pInfo == NULL || pTaskInfo == NULL) {
    qError("%s failed at line %d since pInfo or pTaskInfo is NULL.", __func__, __LINE__);
    return NULL;
  }

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  SSDataBlock* pResBlock = pInfo->pFinalRes;
  if (pResBlock == NULL) {
    qError("%s failed at line %d since pResBlock is NULL.", __func__, __LINE__);
    return NULL;
  }
  blockDataCleanup(pResBlock);
  int32_t        order = pInfo->pFillInfo->order;
  SOperatorInfo* pDownstream = pOperator->pDownstream[0];

  doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, order);
  if (pResBlock->info.rows > 0) {
    pResBlock->info.id.groupId = pInfo->curGroupId;
    return pResBlock;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (pInfo->totalInputRows == 0 &&
          (pInfo->pFillInfo->type != TSDB_FILL_NULL_F && pInfo->pFillInfo->type != TSDB_FILL_SET_VALUE_F)) {
        setOperatorCompleted(pOperator);
        return NULL;
      } else if (pInfo->totalInputRows == 0 && taosFillNotStarted(pInfo->pFillInfo)) {
        reviseFillStartAndEndKey(pInfo, order);
      }

      taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
    } else {
      pResBlock->info.scanFlag = pBlock->info.scanFlag;
      pBlock->info.dataLoad = 1;
      code = blockDataUpdateTsWindow(pBlock, pInfo->primarySrcSlotId);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(pInfo->pRes);
      code = blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _end);
      code = blockDataEnsureCapacity(pInfo->pFinalRes, pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _end);
      doApplyScalarCalculation(pOperator, pBlock, order, pBlock->info.scanFlag);

      if (pInfo->curGroupId == 0 || (pInfo->curGroupId == pInfo->pRes->info.id.groupId)) {
        if (pInfo->curGroupId == 0 && taosFillNotStarted(pInfo->pFillInfo)) {
          reviseFillStartAndEndKey(pInfo, order);
        }

        pInfo->curGroupId = pInfo->pRes->info.id.groupId;  // the first data block
        pInfo->totalInputRows += pInfo->pRes->info.rows;

        int64_t ts = (order == TSDB_ORDER_ASC) ? pBlock->info.window.ekey : pBlock->info.window.skey;
        taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ts);
        taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);
      } else if (pInfo->curGroupId != pBlock->info.id.groupId) {  // the new group data block
        pInfo->existNewGroupBlock = pBlock;

        // Fill the previous group data block, before handle the data block of new group.
        // Close the fill operation for previous group data block
        taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
        pInfo->pFillInfo->prev.key = 0;
      }
    }

    int32_t numOfResultRows = pOperator->resultInfo.capacity - pResBlock->info.rows;
    bool wantMoreBlock = false;
    code = taosFillResultDataBlock2(pInfo->pFillInfo, pResBlock, numOfResultRows, &wantMoreBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    // current group has no more result to return
    if (pResBlock->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pResBlock->info.rows > pResultInfo->threshold || pBlock == NULL || pInfo->existNewGroupBlock != NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }

      doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, order);
      if (pResBlock->info.rows >= pOperator->resultInfo.threshold || pBlock == NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      blockDataCleanup(pResBlock);

      doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, order);
      if (pResBlock->info.rows > pResultInfo->threshold) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else {
      if (wantMoreBlock) continue;
      return NULL;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}

static SSDataBlock* doFillImpl(SOperatorInfo* pOperator) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  if (pInfo == NULL || pTaskInfo == NULL) {
    qError("%s failed at line %d since pInfo or pTaskInfo is NULL.", __func__, __LINE__);
    return NULL;
  }

  SResultInfo* pResultInfo = &pOperator->resultInfo;
  SSDataBlock* pResBlock = pInfo->pFinalRes;
  if (pResBlock == NULL) {
    qError("%s failed at line %d since pResBlock is NULL.", __func__, __LINE__);
    return NULL;
  }

  blockDataCleanup(pResBlock);

  int32_t order = pInfo->pFillInfo->order;

  SOperatorInfo* pDownstream = pOperator->pDownstream[0];
#if 0
  // the scan order may be different from the output result order for agg interval operator.
  if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL) {
    order = ((SIntervalAggOperatorInfo*) pDownstream->info)->resultTsOrder;
  } else {
    order = pInfo->pFillInfo->order;
  }
#endif

  doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, order);
  if (pResBlock->info.rows > 0) {
    pResBlock->info.id.groupId = pInfo->curGroupId;
    return pResBlock;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      if (pInfo->totalInputRows == 0 &&
          (pInfo->pFillInfo->type != TSDB_FILL_NULL_F && pInfo->pFillInfo->type != TSDB_FILL_SET_VALUE_F)) {
        setOperatorCompleted(pOperator);
        return NULL;
      } else if (pInfo->totalInputRows == 0 && taosFillNotStarted(pInfo->pFillInfo)) {
        reviseFillStartAndEndKey(pInfo, order);
      }

      taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
    } else {
      pResBlock->info.scanFlag = pBlock->info.scanFlag;
      pBlock->info.dataLoad = 1;
      code = blockDataUpdateTsWindow(pBlock, pInfo->primarySrcSlotId);
      QUERY_CHECK_CODE(code, lino, _end);

      blockDataCleanup(pInfo->pRes);
      code = blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _end);
      code = blockDataEnsureCapacity(pInfo->pFinalRes, pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _end);
      doApplyScalarCalculation(pOperator, pBlock, order, pBlock->info.scanFlag);

      if (pInfo->curGroupId == 0 || (pInfo->curGroupId == pInfo->pRes->info.id.groupId)) {
        if (pInfo->curGroupId == 0 && taosFillNotStarted(pInfo->pFillInfo)) {
          reviseFillStartAndEndKey(pInfo, order);
        }

        pInfo->curGroupId = pInfo->pRes->info.id.groupId;  // the first data block
        pInfo->totalInputRows += pInfo->pRes->info.rows;

        int64_t ts = (order == TSDB_ORDER_ASC) ? pBlock->info.window.ekey : pBlock->info.window.skey;
        taosFillSetStartInfo(pInfo->pFillInfo, pInfo->pRes->info.rows, ts);
        taosFillSetInputDataBlock(pInfo->pFillInfo, pInfo->pRes);
      } else if (pInfo->curGroupId != pBlock->info.id.groupId) {  // the new group data block
        pInfo->existNewGroupBlock = pBlock;

        // Fill the previous group data block, before handle the data block of new group.
        // Close the fill operation for previous group data block
        taosFillSetStartInfo(pInfo->pFillInfo, 0, pInfo->win.ekey);
        pInfo->pFillInfo->prev.key = 0;
      }
    }

    int32_t numOfResultRows = pOperator->resultInfo.capacity - pResBlock->info.rows;
    code = taosFillResultDataBlock(pInfo->pFillInfo, pResBlock, numOfResultRows);
    QUERY_CHECK_CODE(code, lino, _end);

    // current group has no more result to return
    if (pResBlock->info.rows > 0) {
      // 1. The result in current group not reach the threshold of output result, continue
      // 2. If multiple group results existing in one SSDataBlock is not allowed, return immediately
      if (pResBlock->info.rows > pResultInfo->threshold || pBlock == NULL || pInfo->existNewGroupBlock != NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }

      doHandleRemainBlockFromNewGroup(pOperator, pInfo, pResultInfo, order);
      if (pResBlock->info.rows >= pOperator->resultInfo.threshold || pBlock == NULL) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else if (pInfo->existNewGroupBlock) {  // try next group
      blockDataCleanup(pResBlock);

      doHandleRemainBlockForNewGroupImpl(pOperator, pInfo, pResultInfo, order);
      if (pResBlock->info.rows > pResultInfo->threshold) {
        pResBlock->info.id.groupId = pInfo->curGroupId;
        return pResBlock;
      }
    } else {
      return NULL;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return NULL;
}

static int32_t doFillNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SFillOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;

  if (pInfo->pTimeRange != NULL) {
    STimeWindow pWinRange = {0};
    bool        isWinRangeValid = false;
    calcTimeRange((STimeRangeNode*)pInfo->pTimeRange, &pTaskInfo->pStreamRuntimeInfo->funcInfo, &pWinRange,
                  &isWinRangeValid);

    if (isWinRangeValid) {
      pInfo->win.skey = pWinRange.skey;
      pInfo->win.ekey = pWinRange.ekey;
    }
  }

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SSDataBlock* fillResult = NULL;
  while (true) {
    fillResult = doFillImpl2(pOperator);
    if (fillResult == NULL) {
      setOperatorCompleted(pOperator);
      break;
    }

    code = doFilter(fillResult, pOperator->exprSupp.pFilterInfo, &pInfo->matchInfo);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (fillResult->info.rows > 0) {
      break;
    }
  }

  if (fillResult != NULL) {
    pOperator->resultInfo.totalRows += fillResult->info.rows;
  }

  (*ppRes) = fillResult;
  return code;
}

void destroyFillOperatorInfo(void* param) {
  SFillOperatorInfo* pInfo = (SFillOperatorInfo*)param;
  pInfo->pFillInfo = taosDestroyFillInfo(pInfo->pFillInfo);
  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;
  blockDataDestroy(pInfo->pFinalRes);
  pInfo->pFinalRes = NULL;

  cleanupExprSupp(&pInfo->noFillExprSupp);
  cleanupExprSupp(&pInfo->fillNullExprSupp);

  taosMemoryFreeClear(pInfo->p);
  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(param);
}

static int32_t initFillInfo(SFillOperatorInfo* pInfo, SExprInfo* pExpr, int32_t numOfCols, SExprInfo* pNotFillExpr,
                            int32_t numOfNotFillCols, SExprInfo* pFillNullExpr, int32_t numOfFillNullExprs,
                            SNodeListNode* pValNode, STimeWindow win, int32_t capacity, const char* id,
                            SInterval* pInterval, int32_t fillType, int32_t order, SExecTaskInfo* pTaskInfo) {
  SFillColInfo* pColInfo =
      createFillColInfo(pExpr, numOfCols, pNotFillExpr, numOfNotFillCols, pFillNullExpr, numOfFillNullExprs, pValNode);
  if (!pColInfo) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }

  int64_t startKey = (order == TSDB_ORDER_ASC) ? win.skey : win.ekey;

  //  STimeWindow w = {0};
  //  getInitialStartTimeWindow(pInterval, startKey, &w, order == TSDB_ORDER_ASC);
  pInfo->pFillInfo = NULL;
  int32_t code = taosCreateFillInfo(startKey, numOfCols, numOfNotFillCols, numOfFillNullExprs, capacity, pInterval,
                                    fillType, pColInfo, pInfo->primaryTsCol, order, id, pTaskInfo, &pInfo->pFillInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  if (order == TSDB_ORDER_ASC) {
    pInfo->win.skey = win.skey;
    pInfo->win.ekey = win.ekey;
  } else {
    pInfo->win.skey = win.ekey;
    pInfo->win.ekey = win.skey;
  }
  pInfo->p = taosMemoryCalloc(numOfCols, POINTER_BYTES);
  if (!pInfo->p) {
    return terrno;
  }

  if (pInfo->pFillInfo == NULL) {
    taosMemoryFree(pInfo->pFillInfo);
    taosMemoryFree(pInfo->p);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

static bool isWstartColumnExist(SFillOperatorInfo* pInfo) {
  if (pInfo->noFillExprSupp.numOfExprs == 0) {
    return false;
  }

  for (int32_t i = 0; i < pInfo->noFillExprSupp.numOfExprs; ++i) {
    SExprInfo* exprInfo = pInfo->noFillExprSupp.pExprInfo + i;
    if (exprInfo->pExpr->nodeType == QUERY_NODE_COLUMN && exprInfo->base.numOfParams == 1 &&
        exprInfo->base.pParam[0].pCol->colType == COLUMN_TYPE_WINDOW_START) {
      return true;
    }
  }
  return false;
}

static int32_t createPrimaryTsExprIfNeeded(SFillOperatorInfo* pInfo, SFillPhysiNode* pPhyFillNode, SExprSupp* pExprSupp,
                                           const char* idStr) {
  bool wstartExist = isWstartColumnExist(pInfo);

  if (wstartExist == false) {
    if (pPhyFillNode->pWStartTs->type != QUERY_NODE_TARGET) {
      qError("pWStartTs of fill physical node is not a target node, %s", idStr);
      return TSDB_CODE_QRY_SYS_ERROR;
    }

    SExprInfo* pExpr = taosMemoryRealloc(pExprSupp->pExprInfo, (pExprSupp->numOfExprs + 1) * sizeof(SExprInfo));
    if (pExpr == NULL) {
      return terrno;
    }

    int32_t code = createExprFromTargetNode(&pExpr[pExprSupp->numOfExprs], (STargetNode*)pPhyFillNode->pWStartTs);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pExprSupp->numOfExprs += 1;
      pExprSupp->pExprInfo = pExpr;
      return code;
    }

    pExprSupp->numOfExprs += 1;
    pExprSupp->pExprInfo = pExpr;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t resetFillOperState(SOperatorInfo* pOper) {
  SFillOperatorInfo* pFill = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  pOper->status = OP_NOT_OPENED;
  SFillPhysiNode* pPhyNode = (SFillPhysiNode*)pOper->pPhyNode;

  pFill->curGroupId = 0;
  pFill->totalInputRows = 0;
  blockDataCleanup(pFill->pRes);
  blockDataCleanup(pFill->pFinalRes);

  int64_t startKey = (pFill->pFillInfo->order == TSDB_ORDER_ASC) ? pPhyNode->timeRange.skey : pPhyNode->timeRange.ekey;
  pFill->pFillInfo->start = startKey;
  pFill->pFillInfo->currentKey = startKey;
  pFill->pFillInfo->end = startKey;

  pFill->pFillInfo->numOfRows = 0;
  pFill->pFillInfo->index = -1;
  pFill->pFillInfo->numOfTotal = 0;
  pFill->pFillInfo->numOfCurrent = 0;
  pFill->pFillInfo->isFilled = false;
  pFill->pFillInfo->prev.key = 0;
  pFill->pFillInfo->next.key = 0;
  int32_t size = taosArrayGetSize(pFill->pFillInfo->prev.pRowVal);
  for (int32_t i = 0; i < size; ++i) {
    SGroupKeys* pKey = taosArrayGet(pFill->pFillInfo->prev.pRowVal, i);
    pKey->isNull = true;
  }
  size = taosArrayGetSize(pFill->pFillInfo->next.pRowVal);
  for (int32_t i = 0; i < size; ++i) {
    SGroupKeys* pKey = taosArrayGet(pFill->pFillInfo->next.pRowVal, i);
    pKey->isNull = true;
  }

  taosMemoryFreeClear(pFill->pFillInfo->pTags);
  taosArrayDestroy(pFill->pFillInfo->pColFillProgress);
  pFill->pFillInfo->pColFillProgress = NULL;

  tdListFreeP(pFill->pFillInfo->pFillSavedBlockList, destroyFillBlock);
  pFill->pFillInfo->pFillSavedBlockList = NULL;

  int32_t order = (pPhyNode->node.inputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  if (order == TSDB_ORDER_ASC) {
    pFill->win.skey = pPhyNode->timeRange.skey;
    pFill->win.ekey = pPhyNode->timeRange.ekey;
  } else {
    pFill->win.skey = pPhyNode->timeRange.ekey;
    pFill->win.ekey = pPhyNode->timeRange.skey;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createFillOperatorInfo(SOperatorInfo* downstream, SFillPhysiNode* pPhyFillNode,
                                      SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);
  int32_t code = 0;
  int32_t lino = 0;

  SFillOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SFillOperatorInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pPhyFillNode;
  pInfo->pRes = createDataBlockFromDescNode(pPhyFillNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  SExprInfo* pExprInfo = NULL;

  code = createExprInfo(pPhyFillNode->pFillExprs, NULL, &pExprInfo, &pInfo->numOfExpr);
  QUERY_CHECK_CODE(code, lino, _error);

  pOperator->exprSupp.pExprInfo = pExprInfo;
  pOperator->exprSupp.numOfExprs = pInfo->numOfExpr;

  SExprSupp* pNoFillSupp = &pInfo->noFillExprSupp;
  code = createExprInfo(pPhyFillNode->pNotFillExprs, NULL, &pNoFillSupp->pExprInfo, &pNoFillSupp->numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = createPrimaryTsExprIfNeeded(pInfo, pPhyFillNode, pNoFillSupp, pTaskInfo->id.str);
  QUERY_CHECK_CODE(code, lino, _error);

  code =
      initExprSupp(pNoFillSupp, pNoFillSupp->pExprInfo, pNoFillSupp->numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  code = createExprInfo(pPhyFillNode->pFillNullExprs, NULL, &pInfo->fillNullExprSupp.pExprInfo,
                        &pInfo->fillNullExprSupp.numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);
  code = initExprSupp(&pInfo->fillNullExprSupp, pInfo->fillNullExprSupp.pExprInfo, pInfo->fillNullExprSupp.numOfExprs,
                      &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  SInterval* pInterval =
      QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL == downstream->operatorType
          ? &((SMergeAlignedIntervalAggOperatorInfo*)downstream->info)->intervalAggOperatorInfo->interval
          : &((SIntervalAggOperatorInfo*)downstream->info)->interval;

  int32_t order = (pPhyFillNode->node.inputTsOrder == ORDER_ASC) ? TSDB_ORDER_ASC : TSDB_ORDER_DESC;
  int32_t type = convertFillType(pPhyFillNode->mode);

  SResultInfo* pResultInfo = &pOperator->resultInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  code = initExprSupp(&pOperator->exprSupp, pExprInfo, pInfo->numOfExpr, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->primaryTsCol = ((STargetNode*)pPhyFillNode->pWStartTs)->slotId;
  pInfo->primarySrcSlotId = ((SColumnNode*)((STargetNode*)pPhyFillNode->pWStartTs)->pExpr)->slotId;

  int32_t numOfOutputCols = 0;
  code = extractColMatchInfo(pPhyFillNode->pFillExprs, pPhyFillNode->node.pOutputDataBlockDesc, &numOfOutputCols,
                             COL_MATCH_FROM_SLOT_ID, &pInfo->matchInfo);

  QUERY_CHECK_CODE(code, lino, _error);
  code = initFillInfo(pInfo, pExprInfo, pInfo->numOfExpr, pNoFillSupp->pExprInfo, pNoFillSupp->numOfExprs,
                      pInfo->fillNullExprSupp.pExprInfo, pInfo->fillNullExprSupp.numOfExprs,
                      (SNodeListNode*)pPhyFillNode->pValues, pPhyFillNode->timeRange, pResultInfo->capacity,
                      pTaskInfo->id.str, pInterval, type, order, pTaskInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  TSWAP(pInfo->pTimeRange, pPhyFillNode->pTimeRange);
  pInfo->pFinalRes = NULL;

  code = createOneDataBlock(pInfo->pRes, false, &pInfo->pFinalRes);
  if (code) {
    goto _error;
  }

  code = blockDataEnsureCapacity(pInfo->pFinalRes, pOperator->resultInfo.capacity);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = filterInitFromNode((SNode*)pPhyFillNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "FillOperator", QUERY_NODE_PHYSICAL_PLAN_FILL, false, OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doFillNext, NULL, destroyFillOperatorInfo, optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetFillOperState);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));

  if (pInfo != NULL) {
    destroyFillOperatorInfo(pInfo);
  }
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

static void reviseFillStartAndEndKey(SFillOperatorInfo* pInfo, int32_t order) {
  int64_t skey, ekey, next;
  if (order == TSDB_ORDER_ASC) {
    skey = taosTimeTruncate(pInfo->win.skey, &pInfo->pFillInfo->interval);
    taosFillUpdateStartTimestampInfo(pInfo->pFillInfo, skey);

    ekey = taosTimeTruncate(pInfo->win.ekey, &pInfo->pFillInfo->interval);
    next = ekey;
    while (next < pInfo->win.ekey) {
      next = taosTimeAdd(ekey, pInfo->pFillInfo->interval.sliding, pInfo->pFillInfo->interval.slidingUnit,
                         pInfo->pFillInfo->interval.precision, NULL);
      if (next == ekey) break;
      ekey = next > pInfo->win.ekey ? ekey : next;
    }
    pInfo->win.ekey = ekey;
  } else {
    skey = taosTimeTruncate(pInfo->win.skey, &pInfo->pFillInfo->interval);
    next = skey;
    while (next < pInfo->win.skey) {
      next = taosTimeAdd(skey, pInfo->pFillInfo->interval.sliding, pInfo->pFillInfo->interval.slidingUnit,
                         pInfo->pFillInfo->interval.precision, NULL);
      if (next == skey) break;
      skey = next > pInfo->win.skey ? skey : next;
    }
    taosFillUpdateStartTimestampInfo(pInfo->pFillInfo, skey);
    pInfo->win.ekey = taosTimeTruncate(pInfo->win.ekey, &pInfo->pFillInfo->interval);
  }
}
