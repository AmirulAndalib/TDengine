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
#ifndef TDENGINE_FILTER_H
#define TDENGINE_FILTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"
#include "tcommon.h"

typedef struct SFilterInfo SFilterInfo;
typedef int32_t (*filer_get_col_from_id)(void *, int32_t, void **);

enum {
  FLT_OPTION_NO_REWRITE = 1,
  FLT_OPTION_TIMESTAMP = 2,
  FLT_OPTION_NEED_UNIQE = 4,
  FLT_OPTION_SCALAR_MODE = 8,
};


typedef enum EConditionType {
  COND_TYPE_PRIMARY_KEY = 1,
  COND_TYPE_TAG_INDEX,
  COND_TYPE_TAG,
  COND_TYPE_NORMAL,
  COND_TYPE_PRIMARY_KEY_EXPR
} EConditionType;


#define FILTER_RESULT_ALL_QUALIFIED     0x1
#define FILTER_RESULT_NONE_QUALIFIED    0x2
#define FILTER_RESULT_PARTIAL_QUALIFIED 0x3

typedef struct SFilterColumnParam {
  int32_t numOfCols;
  SArray *pDataBlock;
} SFilterColumnParam;

extern int32_t filterInitFromNode(SNode *pNode, SFilterInfo **pinfo, uint32_t options, void* pSclExtraParams);
extern int32_t filterExecute(SFilterInfo *info, SSDataBlock *pSrc, SColumnInfoData **p, SColumnDataAgg *statis,
                             int16_t numOfCols, int32_t *pFilterResStatus);
extern int32_t filterSetDataFromSlotId(SFilterInfo *info, void *param);
extern int32_t filterSetDataFromColId(SFilterInfo *info, void *param);
extern int32_t filterGetTimeRange(SNode *pNode, STimeWindow *win, bool *isStrict);
extern int32_t filterConverNcharColumns(SFilterInfo *pFilterInfo, int32_t rows, bool *gotNchar);
extern int32_t filterFreeNcharColumns(SFilterInfo *pFilterInfo);
extern void    filterFreeInfo(SFilterInfo *info);
extern int32_t filterRangeExecute(SFilterInfo *info, SColumnDataAgg *pDataStatis, int32_t numOfCols, int32_t numOfRows,
                                  bool *keep);

/* condition split interface */
int32_t filterPartitionCond(SNode **pCondition, SNode **pPrimaryKeyCond, SNode **pTagIndexCond, SNode **pTagCond,
                            SNode **pOtherCond);
int32_t filterIsMultiTableColsCond(SNode *pCond, bool *res);
EConditionType filterClassifyCondition(SNode *pNode);
int32_t        filterGetCompFunc(__compar_fn_t *func, int32_t type, int32_t optr);
bool           filterDoCompare(__compar_fn_t func, uint8_t optr, void *left, void *right);
const void *filterInfoGetSclExtraParans(const SFilterInfo *pFilterInfo);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FILTER_H
