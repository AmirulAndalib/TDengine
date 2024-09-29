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
#include "storageapi.h"
#include "tcommon.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "ttime.h"

typedef struct STimeSliceOperatorInfo {
  SSDataBlock*         pRes;
  STimeWindow          win;
  SInterval            interval;
  int64_t              current;
  SArray*              pPrevRow;     // SArray<SGroupValue>
  SArray*              pNextRow;     // SArray<SGroupValue>
  SArray*              pLinearInfo;  // SArray<SFillLinearInfo>
  bool                 isPrevRowSet;
  bool                 isNextRowSet;
  int32_t              fillType;      // fill type
  SColumn              tsCol;         // primary timestamp column
  SExprSupp            scalarSup;     // scalar calculation
  struct SFillColInfo* pFillColInfo;  // fill column info
  SRowKey              prevKey;
  bool                 prevTsSet;
  uint64_t             groupId;
  SGroupKeys*          pPrevGroupKey;
  SSDataBlock*         pNextGroupRes;
  SSDataBlock*         pRemainRes;   // save block unfinished processing
  int32_t              remainIndex;  // the remaining index in the block to be processed
  bool                 hasPk;
  SColumn              pkCol;
} STimeSliceOperatorInfo;

static void destroyTimeSliceOperatorInfo(void* param);

static FORCE_INLINE int32_t timeSliceEnsureBlockCapacity(STimeSliceOperatorInfo* pSliceInfo, SSDataBlock* pBlock) {
  if (pBlock->info.rows < pBlock->info.capacity) {
    return TSDB_CODE_SUCCESS;
  }

  uint32_t winNum = (pSliceInfo->win.ekey - pSliceInfo->win.skey) / pSliceInfo->interval.interval;
  uint32_t newRowsNum = pBlock->info.rows + TMIN(winNum / 4 + 1, 1048576);
  int32_t  code = blockDataEnsureCapacity(pBlock, newRowsNum);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

static bool isIrowtsPseudoColumn(SExprInfo* pExprInfo) {
  char* name = pExprInfo->pExpr->_function.functionName;
  return (IS_TIMESTAMP_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_irowts") == 0) ||
         (strcasecmp(name, "_flow") == 0) || (strcasecmp(name, "_fhigh") == 0);
}

static bool isIsfilledPseudoColumn(SExprInfo* pExprInfo) {
  char* name = pExprInfo->pExpr->_function.functionName;
  return (IS_BOOLEAN_TYPE(pExprInfo->base.resSchema.type) && strcasecmp(name, "_isfilled") == 0);
}

static int32_t doTimesliceNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  STimeSliceOperatorInfo* pSliceInfo = pOperator->info;
  SSDataBlock*            pRes = pSliceInfo->pRes;
  SExprSupp*              pExprSup = &pOperator->exprSupp;
  SSDataBlock*            pResBlock = pRes;
  int32_t                 index;

  blockDataCleanup(pRes);

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    code = timeSliceEnsureBlockCapacity(pSliceInfo, pResBlock);
    uInfo("===> block:%p rows:%" PRId64, pBlock, pBlock->info.rows);

    for (int32_t j = 0; j < pBlock->info.rows; ++j) {
      SColumnInfoData* pValCol = taosArrayGet(pBlock->pDataBlock, 1);
      if (pValCol == NULL) break;
      SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
      if (pTsCol == NULL) break;

      int64_t ts = ((TSKEY*)pTsCol->pData)[j];
      char*   val = colDataGetData(pValCol, j);
      int16_t valType = pValCol->info.type;

      switch (valType) {
        case TSDB_DATA_TYPE_BOOL:
          uInfo("%" PRId64 ",%d", ts, (*((int8_t*)val) == 1) ? 1 : 0);
          break;
        case TSDB_DATA_TYPE_TINYINT:
          uInfo("%" PRId64 ",%d", ts, *(int8_t*)val);
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          uInfo("%" PRId64 ",%u", ts, *(uint8_t*)val);
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          uInfo("%" PRId64 ",%d", ts, *(int16_t*)val);
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          uInfo("%" PRId64 ",%u", ts, *(uint16_t*)val);
          break;
        case TSDB_DATA_TYPE_INT:
          uInfo("%" PRId64 ",%d", ts, *(int32_t*)val);
          break;
        case TSDB_DATA_TYPE_UINT:
          uInfo("%" PRId64 ",%u", ts, *(uint32_t*)val);
          break;
        case TSDB_DATA_TYPE_BIGINT:
          uInfo("%" PRId64 ",%" PRId64, ts, *(int64_t*)val);
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          uInfo("%" PRId64 ",%" PRIu64, ts, *(uint64_t*)val);
          break;
        case TSDB_DATA_TYPE_FLOAT:
          uInfo("%" PRId64 ",%f", ts, GET_FLOAT_VAL(val));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          uInfo("%" PRId64 ",%f", ts, GET_DOUBLE_VAL(val));
          break;
      }
    }

    for (int32_t j = 0; j < pExprSup->numOfExprs; ++j) {
      SExprInfo* pExprInfo = &pExprSup->pExprInfo[j];

      int32_t          dstSlot = pExprInfo->base.resSchema.slotId;
      SColumnInfoData* pDst = taosArrayGet(pResBlock->pDataBlock, dstSlot);

      if (isIrowtsPseudoColumn(pExprInfo)) {
        if (dstSlot == 0) {
          double a = 12;
          code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&a, false);
        } else {
          code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&pSliceInfo->current, false);
        }
        QUERY_CHECK_CODE(code, lino, _finished);
      } else if (isIsfilledPseudoColumn(pExprInfo)) {
        bool isFilled = false;
        code = colDataSetVal(pDst, pResBlock->info.rows, (char*)&isFilled, false);
        QUERY_CHECK_CODE(code, lino, _finished);
      } else {
        int32_t v = 13;
        code = colDataSetVal(pDst, pResBlock->info.rows, (const char*)&v, false);
        QUERY_CHECK_CODE(code, lino, _finished);
      }
    }

    pResBlock->info.rows += 1;

    if (pRes->info.rows > 0) {
      (*ppRes) = pRes;
      qInfo("group:%" PRId64 ", return to upstream,", pRes->info.id.groupId);
      return code;
    }
  }

_finished:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = (pRes->info.rows == 0) ? NULL : pRes;
  return code;
}

static int32_t extractPkColumnFromFuncs(SNodeList* pFuncs, bool* pHasPk, SColumn* pPkColumn) {
  SNode* pNode;
  int32_t code = 0;
  FOREACH(pNode, pFuncs) {
    if ((nodeType(pNode) == QUERY_NODE_TARGET) && (nodeType(((STargetNode*)pNode)->pExpr) == QUERY_NODE_FUNCTION)) {
      SFunctionNode* pFunc = (SFunctionNode*)((STargetNode*)pNode)->pExpr;
      int32_t        numOfParam = LIST_LENGTH(pFunc->pParameterList);
      for (int32_t j = 0; j < numOfParam; ++j) {
        SNode* p1 = nodesListGetNode(pFunc->pParameterList, j);
        if (p1->type == QUERY_NODE_COLUMN) {
          SColumnNode* pcn = (SColumnNode*)p1;

          code = 0;
        } else if (p1->type == QUERY_NODE_VALUE) {
          SValueNode* pvn = (SValueNode*)p1;
          // pExp->base.pParam[j].type = FUNC_PARAM_TYPE_VALUE;
          // code = nodesValueNodeToVariant(pvn, &pExp->base.pParam[j].param);
          code = 0;
        }

        // if (fmIsInterpFunc(pFunc->funcId) && pFunc->hasPk) {
        //   SNode* pNode2 = (pFunc->pParameterList->pTail->pNode);
        //   if ((nodeType(pNode2) == QUERY_NODE_COLUMN) && ((SColumnNode*)pNode2)->isPk) {
        //     *pHasPk = true;
        //     *pPkColumn = extractColumnFromColumnNode((SColumnNode*)pNode2);
        //     break;
        //   }
        // }
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t createTimeSliceOperatorInfo(SOperatorInfo* downstream, SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo,
                                    SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                 code = 0;
  int32_t                 lino = 0;
  STimeSliceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STimeSliceOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pOperator == NULL || pInfo == NULL) {
    code = terrno;
    goto _error;
  }

  SInterpFuncPhysiNode* pInterpPhyNode = (SInterpFuncPhysiNode*)pPhyNode;
  SExprSupp*            pSup = &pOperator->exprSupp;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pInterpPhyNode->pFuncs, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(pSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pInterpPhyNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pScalarExprInfo = NULL;
    code = createExprInfo(pInterpPhyNode->pExprs, NULL, &pScalarExprInfo, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pInterpPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  QUERY_CHECK_CODE(code, lino, _error);

  // pInfo->tsCol = extractColumnFromColumnNode((SColumnNode*)pInterpPhyNode->pTimeSeries);
  code = extractPkColumnFromFuncs(pInterpPhyNode->pFuncs, &pInfo->hasPk, &pInfo->pkCol);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->fillType = convertFillType(pInterpPhyNode->fillMode);
  initResultSizeInfo(&pOperator->resultInfo, 4096);

  pInfo->pFillColInfo = createFillColInfo(pExprInfo, numOfExprs, NULL, 0, (SNodeListNode*)pInterpPhyNode->pFillValues);
  QUERY_CHECK_NULL(pInfo->pFillColInfo, code, lino, _error, terrno);

  pInfo->pLinearInfo = NULL;
  pInfo->pRes = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  pInterpPhyNode->timeRange.skey = 0;
  pInterpPhyNode->timeRange.ekey = 1677808088000;
  
  pInfo->win = pInterpPhyNode->timeRange;
  pInfo->interval.interval = pInterpPhyNode->interval;
  pInfo->current = pInfo->win.skey;
  pInfo->prevTsSet = false;
  pInfo->prevKey.ts = INT64_MIN;
  pInfo->groupId = 0;
  pInfo->pPrevGroupKey = NULL;
  pInfo->pNextGroupRes = NULL;
  pInfo->pRemainRes = NULL;
  pInfo->remainIndex = 0;

  if (pInfo->hasPk) {
    pInfo->prevKey.numOfPKs = 1;
    pInfo->prevKey.ts = INT64_MIN;
    pInfo->prevKey.pks[0].type = pInfo->pkCol.type;

    if (IS_VAR_DATA_TYPE(pInfo->pkCol.type)) {
      pInfo->prevKey.pks[0].pData = taosMemoryCalloc(1, pInfo->pkCol.bytes);
      QUERY_CHECK_NULL(pInfo->prevKey.pks[0].pData, code, lino, _error, terrno);
    }
  }

  if (downstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = (STableScanInfo*)downstream->info;
    pScanInfo->base.cond.twindows = pInfo->win;
    pScanInfo->base.cond.type = TIMEWINDOW_RANGE_EXTERNAL;
  }

  setOperatorInfo(pOperator, "TimeSliceOperator", QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTimesliceNext, NULL, destroyTimeSliceOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  //  int32_t code = initKeeperInfo(pSliceInfo, pBlock, &pOperator->exprSupp);

  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) destroyTimeSliceOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

void destroyTimeSliceOperatorInfo(void* param) {
  STimeSliceOperatorInfo* pInfo = (STimeSliceOperatorInfo*)param;

  blockDataDestroy(pInfo->pRes);
  pInfo->pRes = NULL;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pPrevRow); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pPrevRow, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pInfo->pPrevRow);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pNextRow); ++i) {
    SGroupKeys* pKey = taosArrayGet(pInfo->pNextRow, i);
    taosMemoryFree(pKey->pData);
  }
  taosArrayDestroy(pInfo->pNextRow);

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pLinearInfo); ++i) {
    SFillLinearInfo* pKey = taosArrayGet(pInfo->pLinearInfo, i);
    taosMemoryFree(pKey->start.val);
    taosMemoryFree(pKey->end.val);
  }
  taosArrayDestroy(pInfo->pLinearInfo);

  if (pInfo->pPrevGroupKey) {
    taosMemoryFree(pInfo->pPrevGroupKey->pData);
    taosMemoryFree(pInfo->pPrevGroupKey);
  }
  if (pInfo->hasPk && IS_VAR_DATA_TYPE(pInfo->pkCol.type)) {
    taosMemoryFreeClear(pInfo->prevKey.pks[0].pData);
  }

  cleanupExprSupp(&pInfo->scalarSup);
  if (pInfo->pFillColInfo != NULL) {
    for (int32_t i = 0; i < pInfo->pFillColInfo->numOfFillExpr; ++i) {
      taosVariantDestroy(&pInfo->pFillColInfo[i].fillVal);
    }
    taosMemoryFree(pInfo->pFillColInfo);
  }
  taosMemoryFreeClear(param);
}
