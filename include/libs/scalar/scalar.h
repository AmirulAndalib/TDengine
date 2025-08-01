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
#ifndef TDENGINE_SCALAR_H
#define TDENGINE_SCALAR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "function.h"
#include "nodes.h"
#include "querynodes.h"

typedef struct SFilterInfo SFilterInfo;

int32_t scalarGetOperatorResultType(SOperatorNode *pOp);

/*
pNode will be freed in API;
*pRes need to freed in caller
*/
int32_t scalarCalculateConstants(SNode *pNode, SNode **pRes);
int32_t scalarCalculateConstantsFromDual(SNode *pNode, SNode **pRes);
int32_t scalarConvertOpValueNodeTs(SOperatorNode *node);

/*
pDst need to freed in caller
*/
int32_t scalarCalculate(SNode *pNode, SArray *pBlockList, SScalarParam *pDst, const void* pExtraParam, void* streamTsRange);
int32_t scalarCalculateInRange(SNode *pNode, SArray *pBlockList, SScalarParam *pDst, int32_t rowStartIdx, int32_t rowEndIdx, const void* pExtraParam, void* streamTsRange);
void    sclFreeParam(SScalarParam* param);

int32_t scalarGetOperatorParamNum(EOperatorType type);
int32_t scalarGenerateSetFromList(void **data, void *pNode, uint32_t type, STypeMod typeMod, int8_t processType);

int32_t vectorGetConvertType(int32_t type1, int32_t type2);
int32_t vectorConvertSingleColImpl(const SScalarParam *pIn, SScalarParam *pOut, int32_t *overflow, int32_t startIndex, int32_t numOfRows);
int32_t vectorConvertSingleCol(SScalarParam *input, SScalarParam *output, int32_t type, STypeMod typeMod, int32_t startIndex, int32_t numOfRows);
STypeMod getConvertTypeMod(int32_t type, const SColumnInfo *pCol1, const SColumnInfo *pCol2);
uint32_t base64BufSize(size_t inputLenBytes);

/* Math functions */
int32_t absFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t logFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t powFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t sqrtFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

int32_t sinFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t cosFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t tanFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t asinFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t acosFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t atanFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

int32_t ceilFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t floorFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t roundFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t truncFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

int32_t piFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t expFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t lnFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t modFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t signFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t degreesFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t radiansFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t randFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t greatestFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t leastFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

/* String functions */
int32_t lengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t charLengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t concatFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t concatWsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t lowerFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t upperFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t ltrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t rtrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t substrFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t md5Function(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput);
int32_t charFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t asciiFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t positionFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t trimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t replaceFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t repeatFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t substrIdxFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t base64Function(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput);
int32_t crc32Function(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput);

/* Conversion functions */
int32_t castFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

/* Time related functions */
int32_t toISO8601Function(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t toUnixtimestampFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t toJsonFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t toTimestampFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t toCharFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int64_t offsetFromTz(char *timezone, int64_t factor);
int32_t timeTruncateFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t timeDiffFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t nowFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t todayFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t timezoneFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t weekdayFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t dayofweekFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t weekFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t weekofyearFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

bool getTimePseudoFuncEnv(struct SFunctionNode *pFunc, SFuncExecEnv *pEnv);

int32_t winStartTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t winEndTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t winDurFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t qStartTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t qEndTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t isWinFilledFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

int32_t qPseudoTagFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

/* Aggregation functions */
int32_t countScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t sumScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t minScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t maxScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t avgScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t stdScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t leastSQRScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t percentileScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t apercentileScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t spreadScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t derivativeScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t irateScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t twaScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t mavgScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t hllScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t csumScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t diffScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t forecastScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t stateCountScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t stateDurationScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t histogramScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t topBotScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t firstLastScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t sampleScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t tailScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t uniqueScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
int32_t modeScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

// stream pseudo functions
int32_t streamPseudoScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

 void calcTimeRange(STimeRangeNode* node, void* pStRtFuncInfo, STimeWindow* pWinRange, bool* winRangeValid);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SCALAR_H
