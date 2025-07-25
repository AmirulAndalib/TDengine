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
#include "mndGrant.h"
#include "mndShow.h"

#ifndef _GRANT

#define GRANT_ITEM_SHOW(display)                               \
  do {                                                         \
    if ((++cols) >= nCols) goto _end;                          \
    if ((pColInfo = taosArrayGet(pBlock->pDataBlock, cols))) { \
      src = (display);                                         \
      STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);                \
      COL_DATA_SET_VAL_GOTO(tmp, false, NULL, NULL, _exit);    \
    }                                                          \
  } while (0)

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  int32_t code = 0;
  int32_t lino = 0;
  char    tmp[32];
  int32_t nCols = taosArrayGetSize(pBlock->pDataBlock);

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = TD_PRODUCT_NAME;
    STR_WITH_MAXSIZE_TO_VARSTR(tmp, src, 32);
    COL_DATA_SET_VAL_GOTO(tmp, false, NULL, NULL, _exit);

    GRANT_ITEM_SHOW("unlimited");
    GRANT_ITEM_SHOW("limited");
    GRANT_ITEM_SHOW("false");
    GRANT_ITEM_SHOW("ungranted");

    for (int32_t i = 0; i < nCols - 5; ++i) {
      GRANT_ITEM_SHOW("unlimited");
    }
  _end:
    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;
_exit:
  if (code != 0) {
    mError("failed to retrieve grant at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static int32_t mndRetrieveGrantFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }
static int32_t mndRetrieveGrantLogs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }
static int32_t mndRetrieveMachines(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }
static int32_t mndRetrieveEncryptions(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) { return 0; }

static int32_t mndProcessGrantHB(SRpcMsg *pReq) { return TSDB_CODE_SUCCESS; }

int32_t mndInitGrant(SMnode *pMnode) {
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_FULL, mndRetrieveGrantFull);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_LOGS, mndRetrieveGrantLogs);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MACHINES, mndRetrieveMachines);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPTIONS, mndRetrieveEncryptions);
  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  return 0;
}

void    mndCleanupGrant() {}
void    grantParseParameter() { mError("can't parsed parameter k"); }
void    grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {}
void    grantAdd(EGrantType grant, uint64_t value) {}
void    grantRestore(EGrantType grant, uint64_t value) {}
int64_t grantRemain(EGrantType grant) { return 0; }
int32_t tGetMachineId(char **result) {
  *result = NULL;
  return 0;
}
bool    grantCheckDualReplicaDnodes(void *pMnode) { return false; }
int32_t dmProcessGrantReq(void *pInfo, SRpcMsg *pMsg) { return TSDB_CODE_SUCCESS; }
int32_t dmProcessGrantNotify(void *pInfo, SRpcMsg *pMsg) { return TSDB_CODE_SUCCESS; }
int32_t mndProcessConfigGrantReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) { return 0; }
#endif

void mndGenerateMachineCode() { grantParseParameter(); }