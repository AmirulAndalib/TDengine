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
#include "function.h"
#include "functionMgt.h"
#include "operator.h"
#include "querytask.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "ttime.h"

typedef struct SCountWindowResult {
  int32_t    winRows;
  SResultRow row;
} SCountWindowResult;

typedef struct SCountWindowSupp {
  SArray* pWinStates;
  int32_t stateIndex;
} SCountWindowSupp;

typedef struct SCountWindowOperatorInfo {
  SOptrBasicInfo     binfo;
  SAggSupporter      aggSup;
  SExprSupp          scalarSup;
  int32_t            tsSlotId;  // primary timestamp column slot id
  STimeWindowAggSupp twAggSup;
  uint64_t           groupId;  // current group id, used to identify the data block from different groups
  SResultRow*        pRow;
  int32_t            windowCount;
  int32_t            windowSliding;
  SCountWindowSupp   countSup;
} SCountWindowOperatorInfo;

void destroyCountWindowOperatorInfo(void* param) {
  SCountWindowOperatorInfo* pInfo = (SCountWindowOperatorInfo*)param;
  if (pInfo == NULL) {
    return;
  }
  cleanupBasicInfo(&pInfo->binfo);
  colDataDestroy(&pInfo->twAggSup.timeWindowData);

  cleanupAggSup(&pInfo->aggSup);
  cleanupExprSupp(&pInfo->scalarSup);
  taosArrayDestroy(pInfo->countSup.pWinStates);
  taosMemoryFreeClear(param);
}

static void clearWinStateBuff(SCountWindowResult* pBuff) { pBuff->winRows = 0; }

static SCountWindowResult* getCountWinStateInfo(SCountWindowSupp* pCountSup) {
  SCountWindowResult* pBuffInfo = taosArrayGet(pCountSup->pWinStates, pCountSup->stateIndex);
  if (!pBuffInfo) {
    return NULL;
  }
  int32_t             size = taosArrayGetSize(pCountSup->pWinStates);
  // coverity scan
  ASSERTS(size > 0, "WinStates is empty");
  if (size > 0) {
    pCountSup->stateIndex = (pCountSup->stateIndex + 1) % size;
  }
  return pBuffInfo;
}

static int32_t setCountWindowOutputBuff(SExprSupp* pExprSup, SCountWindowSupp* pCountSup, SResultRow** pResult,
                                        SCountWindowResult** ppResBuff) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SCountWindowResult* pBuff = getCountWinStateInfo(pCountSup);
  (*pResult) = &pBuff->row;
  code = setResultRowInitCtx(*pResult, pExprSup->pCtx, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
  (*ppResBuff) = pBuff;
  return code;
}

static int32_t updateCountWindowInfo(int32_t start, int32_t blockRows, int32_t countWinRows, int32_t* pCurrentRows) {
  int32_t rows = TMIN(countWinRows - (*pCurrentRows), blockRows - start);
  (*pCurrentRows) += rows;
  return rows;
}

void doCountWindowAggImpl(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pExprSup = &pOperator->exprSupp;
  SCountWindowOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*              pRes = pInfo->binfo.pRes;
  SColumnInfoData*          pColInfoData = taosArrayGet(pBlock->pDataBlock, pInfo->tsSlotId);
  QUERY_CHECK_NULL(pColInfoData, code, lino, _end, terrno);
  TSKEY* tsCols = (TSKEY*)pColInfoData->pData;

  for (int32_t i = 0; i < pBlock->info.rows;) {
    SCountWindowResult* pBuffInfo = NULL;
    code = setCountWindowOutputBuff(pExprSup, &pInfo->countSup, &pInfo->pRow, &pBuffInfo);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
      T_LONG_JMP(pTaskInfo->env, code);
    }
    int32_t prevRows = pBuffInfo->winRows;
    int32_t num = updateCountWindowInfo(i, pBlock->info.rows, pInfo->windowCount, &pBuffInfo->winRows);
    int32_t step = num;
    if (prevRows == 0) {
      pInfo->pRow->win.skey = tsCols[i];
    }
    pInfo->pRow->win.ekey = tsCols[num + i - 1];

    updateTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pInfo->pRow->win, 0);
    applyAggFunctionOnPartialTuples(pTaskInfo, pExprSup->pCtx, &pInfo->twAggSup.timeWindowData, i, num,
                                    pBlock->info.rows, pExprSup->numOfExprs);
    if (pBuffInfo->winRows == pInfo->windowCount) {
      doUpdateNumOfRows(pExprSup->pCtx, pInfo->pRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
      copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, pInfo->pRow, pExprSup->pCtx, pRes,
                               pExprSup->rowEntryInfoOffset, pTaskInfo);
      pRes->info.rows += pInfo->pRow->numOfRows;
      clearWinStateBuff(pBuffInfo);
      clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
    }
    if (pInfo->windowCount != pInfo->windowSliding) {
      if (prevRows <= pInfo->windowSliding) {
        if (pBuffInfo->winRows > pInfo->windowSliding) {
          step = pInfo->windowSliding - prevRows;
        } else {
          step = pInfo->windowSliding;
        }
      } else {
        step = 0;
      }
    }
    i += step;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static void buildCountResult(SExprSupp* pExprSup, SCountWindowSupp* pCountSup, SExecTaskInfo* pTaskInfo,
                             SFilterInfo* pFilterInfo, SSDataBlock* pBlock) {
  SResultRow* pResultRow = NULL;
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pCountSup->pWinStates); i++) {
    SCountWindowResult* pBuff = NULL;
    code = setCountWindowOutputBuff(pExprSup, pCountSup, &pResultRow, &pBuff);
    QUERY_CHECK_CODE(code, lino, _end);
    if (pBuff->winRows == 0) {
      continue;
    }
    doUpdateNumOfRows(pExprSup->pCtx, pResultRow, pExprSup->numOfExprs, pExprSup->rowEntryInfoOffset);
    copyResultrowToDataBlock(pExprSup->pExprInfo, pExprSup->numOfExprs, pResultRow, pExprSup->pCtx, pBlock,
                             pExprSup->rowEntryInfoOffset, pTaskInfo);
    pBlock->info.rows += pResultRow->numOfRows;
    clearWinStateBuff(pBuff);
    clearResultRowInitFlag(pExprSup->pCtx, pExprSup->numOfExprs);
  }
  code = doFilter(pBlock, pFilterInfo, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t countWindowAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SCountWindowOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*            pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*                pExprSup = &pOperator->exprSupp;
  int32_t                   order = pInfo->binfo.inputTsOrder;
  SSDataBlock*              pRes = pInfo->binfo.pRes;
  SOperatorInfo*            downstream = pOperator->pDownstream[0];

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pRes->info.scanFlag = pBlock->info.scanFlag;
    code = setInputDataBlock(pExprSup, pBlock, order, MAIN_SCAN, true);
    QUERY_CHECK_CODE(code, lino, _end);

    code = blockDataUpdateTsWindow(pBlock, pInfo->tsSlotId);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->groupId == 0) {
      pInfo->groupId = pBlock->info.id.groupId;
    } else if (pInfo->groupId != pBlock->info.id.groupId) {
      buildCountResult(pExprSup, &pInfo->countSup, pTaskInfo, pOperator->exprSupp.pFilterInfo, pRes);
      pInfo->groupId = pBlock->info.id.groupId;
    }

    doCountWindowAggImpl(pOperator, pBlock);
    if (pRes->info.rows >= pOperator->resultInfo.threshold) {
      (*ppRes) = pRes;
      return code;
    }
  }

  buildCountResult(pExprSup, &pInfo->countSup, pTaskInfo, pOperator->exprSupp.pFilterInfo, pRes);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = pRes->info.rows == 0 ? NULL : pRes;
  return code;
}

static SSDataBlock* countWindowAggregate(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t code = countWindowAggregateNext(pOperator, &pRes);
  return pRes;
}

int32_t createCountwindowOperatorInfo(SOperatorInfo* downstream, SPhysiNode* physiNode,
                                             SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_OPTR_CHECK(pOptrInfo);

  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  SCountWindowOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SCountWindowOperatorInfo));
  SOperatorInfo*            pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pOperator->exprSupp.hasWindowOrGroup = true;

  SCountWinodwPhysiNode* pCountWindowNode = (SCountWinodwPhysiNode*)physiNode;

  pInfo->tsSlotId = ((SColumnNode*)pCountWindowNode->window.pTspk)->slotId;

  if (pCountWindowNode->window.pExprs != NULL) {
    int32_t    numOfScalarExpr = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pCountWindowNode->window.pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);
    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  size_t     keyBufSize = 0;
  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pCountWindowNode->window.pFuncs, NULL, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, keyBufSize, pTaskInfo->id.str,
                    pTaskInfo->streamInfo.pState, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pCountWindowNode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pResBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  initBasicInfo(&pInfo->binfo, pResBlock);
  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  pInfo->binfo.inputTsOrder = physiNode->inputTsOrder;
  pInfo->binfo.outputTsOrder = physiNode->outputTsOrder;
  pInfo->windowCount = pCountWindowNode->windowCount;
  pInfo->windowSliding = pCountWindowNode->windowSliding;
  // sizeof(SCountWindowResult)
  int32_t itemSize = sizeof(int32_t) + pInfo->aggSup.resultRowSize;
  int32_t numOfItem = 1;
  if (pInfo->windowCount != pInfo->windowSliding) {
    numOfItem = pInfo->windowCount / pInfo->windowSliding + 1;
  }

  pInfo->countSup.pWinStates = taosArrayInit_s(itemSize, numOfItem);
  if (!pInfo->countSup.pWinStates) {
    goto _error;
  }

  pInfo->countSup.stateIndex = 0;

  code = filterInitFromNode((SNode*)pCountWindowNode->window.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExecTimeWindowInfo(&pInfo->twAggSup.timeWindowData, &pTaskInfo->window);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "CountWindowOperator", QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT, true, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, countWindowAggregate, NULL, destroyCountWindowOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destroyCountWindowOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return code;
}
