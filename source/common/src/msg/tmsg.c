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
#include "tmsg.h"
#include "tglobal.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_RANGE_CODE_
#define TD_MSG_INFO_
#undef TD_MSG_TYPE_INFO_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_INFO_
#undef TD_MSG_TYPE_INFO_
#undef TD_MSG_RANGE_CODE_
#define TD_MSG_DICT_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_INFO_
#undef TD_MSG_TYPE_INFO_
#undef TD_MSG_DICT_
#undef TD_MSG_SEG_CODE_
#define TD_MSG_RANGE_CODE_
#include "tmsgdef.h"

#include "tanalytics.h"
#include "tcol.h"
#include "tlog.h"
#include "streamMsg.h"

#if defined(WINDOWS)
#include <IPHlpApi.h>
#include <WS2tcpip.h>
#include <Winsock2.h>
#endif

#define DECODESQL()                                                               \
  do {                                                                            \
    if (!tDecodeIsEnd(&decoder)) {                                                \
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sqlLen));                       \
      if (pReq->sqlLen > 0) {                                                     \
        TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->sql, NULL)); \
      }                                                                           \
    }                                                                             \
  } while (0)

#define ENCODESQL()                                                                       \
  do {                                                                                    \
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sqlLen));                                  \
    if (pReq->sqlLen > 0) {                                                               \
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->sql, pReq->sqlLen)); \
    }                                                                                     \
  } while (0)

#define FREESQL()                \
  do {                           \
    if (pReq->sql != NULL) {     \
      taosMemoryFree(pReq->sql); \
    }                            \
    pReq->sql = NULL;            \
  } while (0)

static int32_t tSerializeSMonitorParas(SEncoder *encoder, const SMonitorParas *pMonitorParas) {
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pMonitorParas->tsEnableMonitor));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pMonitorParas->tsMonitorInterval));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pMonitorParas->tsSlowLogScope));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pMonitorParas->tsSlowLogMaxLen));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pMonitorParas->tsSlowLogThreshold));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pMonitorParas->tsSlowLogThresholdTest));  // Obsolete
  TAOS_CHECK_RETURN(tEncodeCStr(encoder, pMonitorParas->tsSlowLogExceptDb));
  return 0;
}

static int32_t tDeserializeSMonitorParas(SDecoder *decoder, SMonitorParas *pMonitorParas) {
  TAOS_CHECK_RETURN(tDecodeI8(decoder, (int8_t *)&pMonitorParas->tsEnableMonitor));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pMonitorParas->tsMonitorInterval));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pMonitorParas->tsSlowLogScope));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pMonitorParas->tsSlowLogMaxLen));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pMonitorParas->tsSlowLogThreshold));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pMonitorParas->tsSlowLogThresholdTest));  // Obsolete
  TAOS_CHECK_RETURN(tDecodeCStrTo(decoder, pMonitorParas->tsSlowLogExceptDb));
  return 0;
}

static int32_t tDecodeSVAlterTbReqCommon(SDecoder *pDecoder, SVAlterTbReq *pReq);
static int32_t tDecodeSBatchDeleteReqCommon(SDecoder *pDecoder, SBatchDeleteReq *pReq);
static int32_t tEncodeTableTSMAInfoRsp(SEncoder *pEncoder, const STableTSMAInfoRsp *pRsp);
static int32_t tDecodeTableTSMAInfoRsp(SDecoder *pDecoder, STableTSMAInfoRsp *pRsp);

int32_t tInitSubmitMsgIter(const SSubmitReq *pMsg, SSubmitMsgIter *pIter) {
  if (pMsg == NULL) {
    return terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
  }

  pIter->totalLen = htonl(pMsg->length);
  pIter->numOfBlocks = htonl(pMsg->numOfBlocks);
  if (!(pIter->totalLen > 0)) {
    return terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
  }
  pIter->len = 0;
  pIter->pMsg = pMsg;
  if (pIter->totalLen <= sizeof(SSubmitReq)) {
    return terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
  }

  return 0;
}

int32_t tGetSubmitMsgNext(SSubmitMsgIter *pIter, SSubmitBlk **pPBlock) {
  if (!(pIter->len >= 0)) {
    return terrno = TSDB_CODE_INVALID_MSG_LEN;
  }

  if (pIter->len == 0) {
    pIter->len += sizeof(SSubmitReq);
  } else {
    if (pIter->len >= pIter->totalLen) {
      return terrno = TSDB_CODE_INVALID_MSG_LEN;
    }

    pIter->len += (sizeof(SSubmitBlk) + pIter->dataLen + pIter->schemaLen);
    if (!(pIter->len > 0)) {
      return terrno = TSDB_CODE_INVALID_MSG_LEN;
    }
  }

  if (pIter->len > pIter->totalLen) {
    *pPBlock = NULL;
    return terrno = TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP;
  }

  if (pIter->len == pIter->totalLen) {
    *pPBlock = NULL;
  } else {
    *pPBlock = (SSubmitBlk *)POINTER_SHIFT(pIter->pMsg, pIter->len);
    pIter->uid = htobe64((*pPBlock)->uid);
    pIter->suid = htobe64((*pPBlock)->suid);
    pIter->sversion = htonl((*pPBlock)->sversion);
    pIter->dataLen = htonl((*pPBlock)->dataLen);
    pIter->schemaLen = htonl((*pPBlock)->schemaLen);
    pIter->numOfRows = htonl((*pPBlock)->numOfRows);
  }
  return 0;
}

int32_t tInitSubmitBlkIter(SSubmitMsgIter *pMsgIter, SSubmitBlk *pBlock, SSubmitBlkIter *pIter) {
  if (pMsgIter->dataLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  pIter->totalLen = pMsgIter->dataLen;
  pIter->len = 0;
  pIter->row = (STSRow *)(pBlock->data + pMsgIter->schemaLen);
  return 0;
}

STSRow *tGetSubmitBlkNext(SSubmitBlkIter *pIter) {
  STSRow *row = pIter->row;

  if (pIter->len >= pIter->totalLen) {
    return NULL;
  } else {
    pIter->len += TD_ROW_LEN(row);
    if (pIter->len < pIter->totalLen) {
      pIter->row = POINTER_SHIFT(row, TD_ROW_LEN(row));
    }
    return row;
  }
}

int32_t tEncodeSEpSet(SEncoder *pEncoder, const SEpSet *pEp) {
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pEp->inUse));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pEp->numOfEps));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    TAOS_CHECK_RETURN(tEncodeU16(pEncoder, pEp->eps[i].port));
    TAOS_CHECK_RETURN(tEncodeCStrWithLen(pEncoder, pEp->eps[i].fqdn, TSDB_FQDN_LEN));
  }
  return 0;
}

int32_t tDecodeSEpSet(SDecoder *pDecoder, SEpSet *pEp) {
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pEp->inUse));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pEp->numOfEps));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    TAOS_CHECK_RETURN(tDecodeU16(pDecoder, &pEp->eps[i].port));
    TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pEp->eps[i].fqdn));
  }
  return 0;
}

int32_t tEncodeSQueryNodeAddr(SEncoder *pEncoder, SQueryNodeAddr *pAddr) {
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pAddr->nodeId));
  TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pAddr->epSet));
  return 0;
}

int32_t tEncodeSQueryNodeLoad(SEncoder *pEncoder, SQueryNodeLoad *pLoad) {
  TAOS_CHECK_RETURN(tEncodeSQueryNodeAddr(pEncoder, &pLoad->addr));
  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pLoad->load));
  return 0;
}

int32_t tDecodeSQueryNodeAddr(SDecoder *pDecoder, SQueryNodeAddr *pAddr) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pAddr->nodeId));
  TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &pAddr->epSet));
  return 0;
}

int32_t tDecodeSQueryNodeLoad(SDecoder *pDecoder, SQueryNodeLoad *pLoad) {
  TAOS_CHECK_RETURN(tDecodeSQueryNodeAddr(pDecoder, &pLoad->addr));
  TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pLoad->load));
  return 0;
}

int32_t taosEncodeSEpSet(void **buf, const SEpSet *pEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pEp->inUse);
  tlen += taosEncodeFixedI8(buf, pEp->numOfEps);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    tlen += taosEncodeFixedU16(buf, pEp->eps[i].port);
    tlen += taosEncodeString(buf, pEp->eps[i].fqdn);
  }
  return tlen;
}

void *taosDecodeSEpSet(const void *buf, SEpSet *pEp) {
  buf = taosDecodeFixedI8(buf, &pEp->inUse);
  buf = taosDecodeFixedI8(buf, &pEp->numOfEps);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; i++) {
    buf = taosDecodeFixedU16(buf, &pEp->eps[i].port);
    buf = taosDecodeStringTo(buf, pEp->eps[i].fqdn);
  }
  return (void *)buf;
}

static int32_t tSerializeSClientHbReq(SEncoder *pEncoder, const SClientHbReq *pReq) {
  TAOS_CHECK_RETURN(tEncodeSClientHbKey(pEncoder, &pReq->connKey));

  if (pReq->connKey.connType == CONN_TYPE__QUERY) {
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->app.appId));
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->app.pid));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->app.name));
    TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->app.startTime));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.numOfInsertsReq));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.numOfInsertRows));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.insertElapsedTime));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.insertBytes));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.fetchBytes));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.queryElapsedTime));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.numOfSlowQueries));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.totalRequests));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pReq->app.summary.currentRequests));

    int32_t queryNum = 0;
    if (pReq->query) {
      queryNum = 1;
      TAOS_CHECK_RETURN(tEncodeI32(pEncoder, queryNum));
      TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pReq->query->connId));

      int32_t num = taosArrayGetSize(pReq->query->queryDesc);
      TAOS_CHECK_RETURN(tEncodeI32(pEncoder, num));

      for (int32_t i = 0; i < num; ++i) {
        SQueryDesc *desc = taosArrayGet(pReq->query->queryDesc, i);
        TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, desc->sql));
        TAOS_CHECK_RETURN(tEncodeU64(pEncoder, desc->queryId));
        TAOS_CHECK_RETURN(tEncodeI64(pEncoder, desc->useconds));
        TAOS_CHECK_RETURN(tEncodeI64(pEncoder, desc->stime));
        TAOS_CHECK_RETURN(tEncodeI64(pEncoder, desc->reqRid));
        TAOS_CHECK_RETURN(tEncodeI8(pEncoder, desc->stableQuery));
        TAOS_CHECK_RETURN(tEncodeI8(pEncoder, desc->isSubQuery));
        TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, desc->fqdn));
        TAOS_CHECK_RETURN(tEncodeI32(pEncoder, desc->subPlanNum));

        int32_t snum = desc->subDesc ? taosArrayGetSize(desc->subDesc) : 0;
        TAOS_CHECK_RETURN(tEncodeI32(pEncoder, snum));
        for (int32_t m = 0; m < snum; ++m) {
          SQuerySubDesc *sDesc = taosArrayGet(desc->subDesc, m);
          TAOS_CHECK_RETURN(tEncodeI64(pEncoder, sDesc->tid));
          TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, sDesc->status));
        }
      }
    } else {
      TAOS_CHECK_RETURN(tEncodeI32(pEncoder, queryNum));
    }
  }

  int32_t kvNum = taosHashGetSize(pReq->info);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, kvNum));
  void *pIter = taosHashIterate(pReq->info, NULL);
  while (pIter != NULL) {
    SKv *kv = pIter;
    TAOS_CHECK_RETURN(tEncodeSKv(pEncoder, kv));
    pIter = taosHashIterate(pReq->info, pIter);
  }
  TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pReq->userIp));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->userApp));

  return 0;
}

static int32_t tDeserializeSClientHbReq(SDecoder *pDecoder, SClientHbReq *pReq) {
  int32_t code = 0;
  int32_t line = 0;
  TAOS_CHECK_RETURN(tDecodeSClientHbKey(pDecoder, &pReq->connKey));

  if (pReq->connKey.connType == CONN_TYPE__QUERY) {
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->app.appId));
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReq->app.pid));
    TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pReq->app.name));
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->app.startTime));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.numOfInsertsReq));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.numOfInsertRows));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.insertElapsedTime));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.insertBytes));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.fetchBytes));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.queryElapsedTime));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.numOfSlowQueries));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.totalRequests));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pReq->app.summary.currentRequests));

    int32_t queryNum = 0;
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &queryNum));
    if (queryNum) {
      pReq->query = taosMemoryCalloc(1, sizeof(*pReq->query));
      if (NULL == pReq->query) {
        return terrno;
      }
      code = tDecodeU32(pDecoder, &pReq->query->connId);
      TAOS_CHECK_GOTO(code, &line, _error);

      int32_t num = 0;
      code = tDecodeI32(pDecoder, &num);
      TAOS_CHECK_GOTO(code, &line, _error);

      if (num > 0) {
        pReq->query->queryDesc = taosArrayInit(num, sizeof(SQueryDesc));
        if (NULL == pReq->query->queryDesc) {
          return terrno;
        }

        for (int32_t i = 0; i < num; ++i) {
          SQueryDesc desc = {0};
          code = tDecodeCStrTo(pDecoder, desc.sql);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeU64(pDecoder, &desc.queryId);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeI64(pDecoder, &desc.useconds);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeI64(pDecoder, &desc.stime);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeI64(pDecoder, &desc.reqRid);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeI8(pDecoder, (int8_t *)&desc.stableQuery);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeI8(pDecoder, (int8_t *)&desc.isSubQuery);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeCStrTo(pDecoder, desc.fqdn);
          TAOS_CHECK_GOTO(code, &line, _error);

          code = tDecodeI32(pDecoder, &desc.subPlanNum);
          TAOS_CHECK_GOTO(code, &line, _error);

          int32_t snum = 0;
          code = tDecodeI32(pDecoder, &snum);
          if (snum > 0) {
            desc.subDesc = taosArrayInit(snum, sizeof(SQuerySubDesc));
            if (NULL == desc.subDesc) {
              code = terrno;
              TAOS_CHECK_GOTO(code, &line, _error);
            }

            for (int32_t m = 0; m < snum; ++m) {
              SQuerySubDesc sDesc = {0};
              code = tDecodeI64(pDecoder, &sDesc.tid);
              TAOS_CHECK_GOTO(code, &line, _error);

              code = (tDecodeCStrTo(pDecoder, sDesc.status));
              TAOS_CHECK_GOTO(code, &line, _error);
              if (!taosArrayPush(desc.subDesc, &sDesc)) {
                code = terrno;
                TAOS_CHECK_GOTO(code, &line, _error);
              }
            }
          }

          if (!(desc.subPlanNum == taosArrayGetSize(desc.subDesc))) {
            code = TSDB_CODE_INVALID_MSG;
            TAOS_CHECK_GOTO(code, &line, _error);
          }

          if (!taosArrayPush(pReq->query->queryDesc, &desc)) {
            code = terrno;
            TAOS_CHECK_GOTO(code, &line, _error);
          }
        }
      }
    }
  }

  int32_t kvNum = 0;
  TAOS_CHECK_GOTO(tDecodeI32(pDecoder, &kvNum), &line, _error);
  if (pReq->info == NULL) {
    pReq->info = taosHashInit(kvNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }
  if (pReq->info == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &line, _error);
  }
  for (int32_t i = 0; i < kvNum; i++) {
    SKv kv = {0};
    TAOS_CHECK_GOTO(tDecodeSKv(pDecoder, &kv), &line, _error);

    int32_t code = taosHashPut(pReq->info, &kv.key, sizeof(kv.key), &kv, sizeof(kv));
    TAOS_CHECK_GOTO(terrno = code, &line, _error);
  }
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_GOTO(tDecodeU32(pDecoder, &pReq->userIp), &line, _error);
    TAOS_CHECK_GOTO(tDecodeCStrTo(pDecoder, pReq->userApp), &line, _error);
  }

_error:
  if (code != 0) {
    tFreeClientHbReq(pReq);
  }
  return code;
}

static int32_t tSerializeSClientHbRsp(SEncoder *pEncoder, const SClientHbRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeSClientHbKey(pEncoder, &pRsp->connKey));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->status));

  int32_t queryNum = 0;
  if (pRsp->query) {
    queryNum = 1;
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, queryNum));
    TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pRsp->query->connId));
    TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pRsp->query->killRid));
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->query->totalDnodes));
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->query->onlineDnodes));
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->query->killConnection));
    TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pRsp->query->epSet));
    int32_t num = taosArrayGetSize(pRsp->query->pQnodeList);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, num));
    for (int32_t i = 0; i < num; ++i) {
      SQueryNodeLoad *pLoad = taosArrayGet(pRsp->query->pQnodeList, i);
      TAOS_CHECK_RETURN(tEncodeSQueryNodeLoad(pEncoder, pLoad));
    }
  } else {
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, queryNum));
  }

  int32_t kvNum = taosArrayGetSize(pRsp->info);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, kvNum));
  for (int32_t i = 0; i < kvNum; i++) {
    SKv *kv = taosArrayGet(pRsp->info, i);
    TAOS_CHECK_RETURN(tEncodeSKv(pEncoder, kv));
  }

  return 0;
}

static int32_t tDeserializeSClientHbRsp(SDecoder *pDecoder, SClientHbRsp *pRsp) {
  TAOS_CHECK_RETURN(tDecodeSClientHbKey(pDecoder, &pRsp->connKey));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->status));

  int32_t queryNum = 0;
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &queryNum));
  if (queryNum) {
    pRsp->query = taosMemoryCalloc(1, sizeof(*pRsp->query));
    if (NULL == pRsp->query) {
      return terrno;
    }
    TAOS_CHECK_RETURN(tDecodeU32(pDecoder, &pRsp->query->connId));
    TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pRsp->query->killRid));
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->query->totalDnodes));
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->query->onlineDnodes));
    TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pRsp->query->killConnection));
    TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &pRsp->query->epSet));
    int32_t pQnodeNum = 0;
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pQnodeNum));
    if (pQnodeNum > 0) {
      pRsp->query->pQnodeList = taosArrayInit(pQnodeNum, sizeof(SQueryNodeLoad));
      if (NULL == pRsp->query->pQnodeList) return terrno;
      for (int32_t i = 0; i < pQnodeNum; ++i) {
        SQueryNodeLoad load = {0};
        TAOS_CHECK_RETURN(tDecodeSQueryNodeLoad(pDecoder, &load));
        if (!taosArrayPush(pRsp->query->pQnodeList, &load)) return terrno;
      }
    }
  }

  int32_t kvNum = 0;
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &kvNum));
  pRsp->info = taosArrayInit(kvNum, sizeof(SKv));
  if (pRsp->info == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < kvNum; i++) {
    SKv kv = {0};
    TAOS_CHECK_RETURN(tDecodeSKv(pDecoder, &kv));
    if (!taosArrayPush(pRsp->info, &kv)) return terrno;
  }

  return 0;
}

int32_t tSerializeSClientHbBatchReq(void *buf, int32_t bufLen, const SClientHbBatchReq *pBatchReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen = 0;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pBatchReq->reqId));

  int32_t reqNum = taosArrayGetSize(pBatchReq->reqs);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, reqNum));
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq *pReq = taosArrayGet(pBatchReq->reqs, i);
    TAOS_CHECK_EXIT(tSerializeSClientHbReq(&encoder, pReq));
  }

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pBatchReq->ipWhiteListVer));

  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq *pReq = taosArrayGet(pBatchReq->reqs, i);
    TAOS_CHECK_EXIT(tSerializeIpRange(&encoder, (SIpRange *)&pReq->userDualIp));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSClientHbBatchReq(void *buf, int32_t bufLen, SClientHbBatchReq *pBatchReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBatchReq->reqId));

  int32_t reqNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &reqNum));
  if (reqNum > 0) {
    pBatchReq->reqs = taosArrayInit(reqNum, sizeof(SClientHbReq));
    if (NULL == pBatchReq->reqs) {
      return terrno;
    }
  }
  for (int32_t i = 0; i < reqNum; i++) {
    SClientHbReq req = {0};
    TAOS_CHECK_EXIT(tDeserializeSClientHbReq(&decoder, &req));
    if (!taosArrayPush(pBatchReq->reqs, &req)) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBatchReq->ipWhiteListVer));
  }

  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < reqNum; i++) {
      SClientHbReq *pReq = taosArrayGet(pBatchReq->reqs, i);
      TAOS_CHECK_EXIT(tDeserializeIpRange(&decoder, (SIpRange *)&pReq->userDualIp));
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSClientHbBatchRsp(void *buf, int32_t bufLen, const SClientHbBatchRsp *pBatchRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pBatchRsp->reqId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pBatchRsp->rspId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pBatchRsp->svrTimestamp));

  int32_t rspNum = taosArrayGetSize(pBatchRsp->rsps);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, rspNum));
  for (int32_t i = 0; i < rspNum; i++) {
    SClientHbRsp *pRsp = taosArrayGet(pBatchRsp->rsps, i);
    TAOS_CHECK_EXIT(tSerializeSClientHbRsp(&encoder, pRsp));
  }
  TAOS_CHECK_EXIT(tSerializeSMonitorParas(&encoder, &pBatchRsp->monitorParas));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pBatchRsp->enableAuditDelete));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pBatchRsp->enableStrongPass));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSClientHbBatchRsp(void *buf, int32_t bufLen, SClientHbBatchRsp *pBatchRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBatchRsp->reqId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pBatchRsp->rspId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pBatchRsp->svrTimestamp));

  int32_t rspNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &rspNum));
  if (pBatchRsp->rsps == NULL) {
    if ((pBatchRsp->rsps = taosArrayInit(rspNum, sizeof(SClientHbRsp))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  for (int32_t i = 0; i < rspNum; i++) {
    SClientHbRsp rsp = {0};
    TAOS_CHECK_EXIT(tDeserializeSClientHbRsp(&decoder, &rsp));
    if (taosArrayPush(pBatchRsp->rsps, &rsp) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDeserializeSMonitorParas(&decoder, &pBatchRsp->monitorParas));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pBatchRsp->enableAuditDelete));
  } else {
    pBatchRsp->enableAuditDelete = 0;
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pBatchRsp->enableStrongPass));
  } else {
    pBatchRsp->enableStrongPass = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMCreateStbReq(void *buf, int32_t bufLen, SMCreateStbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->source));
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->reserved[i]));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->suid));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->delay1));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->delay2));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->watermark1));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->watermark2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ttl));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->colVer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tagVer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfColumns));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfTags));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfFuncs));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->commentLen));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ast1Len));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ast2Len));

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SFieldWithOptions *pField = taosArrayGet(pReq->pColumns, i);
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->flags));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->bytes));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pField->compress));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->typeMod));
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField *pField = taosArrayGet(pReq->pTags, i);
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->flags));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->bytes));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
  }

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    const char *pFunc = taosArrayGet(pReq->pFuncs, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pFunc));
  }

  if (pReq->commentLen > 0) {
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->pComment));
  }
  if (pReq->ast1Len > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->pAst1, pReq->ast1Len));
  }
  if (pReq->ast2Len > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->pAst2, pReq->ast2Len));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->deleteMark1));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->deleteMark2));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->keep));

  ENCODESQL();

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->virtualStb));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateStbReq(void *buf, int32_t bufLen, SMCreateStbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->source));
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->reserved[i]));
  }
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->suid));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->delay1));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->delay2));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->watermark1));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->watermark2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ttl));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->colVer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tagVer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfColumns));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfTags));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfFuncs));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->commentLen));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ast1Len));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ast2Len));

  if ((pReq->pColumns = taosArrayInit(pReq->numOfColumns, sizeof(SFieldWithOptions))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((pReq->pTags = taosArrayInit(pReq->numOfTags, sizeof(SField))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((pReq->pFuncs = taosArrayInit(pReq->numOfFuncs, TSDB_FUNC_NAME_LEN)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    SFieldWithOptions field = {0};
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.flags));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &field.compress));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.typeMod));
    if (taosArrayPush(pReq->pColumns, &field) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    SField field = {0};
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.flags));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
    if (taosArrayPush(pReq->pTags, &field) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char pFunc[TSDB_FUNC_NAME_LEN] = {0};
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pFunc));
    if (taosArrayPush(pReq->pFuncs, pFunc) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (pReq->commentLen > 0) {
    pReq->pComment = taosMemoryMalloc(pReq->commentLen + 1);
    if (pReq->pComment == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pComment));
  }

  if (pReq->ast1Len > 0) {
    pReq->pAst1 = taosMemoryMalloc(pReq->ast1Len);
    if (pReq->pAst1 == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pAst1));
  }

  if (pReq->ast2Len > 0) {
    pReq->pAst2 = taosMemoryMalloc(pReq->ast2Len);
    if (pReq->pAst2 == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pAst2));
  }

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->deleteMark1));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->deleteMark2));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->keep));
  }

  DECODESQL();

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->virtualStb));
  } else {
    pReq->virtualStb = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCreateStbReq(SMCreateStbReq *pReq) {
  taosArrayDestroy(pReq->pColumns);
  taosArrayDestroy(pReq->pTags);
  taosArrayDestroy(pReq->pFuncs);
  taosMemoryFreeClear(pReq->pComment);
  taosMemoryFreeClear(pReq->pAst1);
  taosMemoryFreeClear(pReq->pAst2);
  FREESQL();
}

int32_t tSerializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->source));
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->reserved[i]));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->suid));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropStbReq(void *buf, int32_t bufLen, SMDropStbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->source));
  for (int32_t i = 0; i < sizeof(pReq->reserved) / sizeof(int8_t); ++i) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->reserved[i]));
  }
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->suid));

  DECODESQL();

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMDropStbReq(SMDropStbReq *pReq) { FREESQL(); }

int32_t tSerializeSMAlterStbReq(void *buf, int32_t bufLen, SMAlterStbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->alterType));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfFields));

  // if (pReq->alterType == )
  for (int32_t i = 0; i < pReq->numOfFields; ++i) {
    if (pReq->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION) {
      SFieldWithOptions *pField = taosArrayGet(pReq->pFields, i);
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->bytes));
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
      TAOS_CHECK_EXIT(tEncodeU32(&encoder, pField->compress));

    } else {
      SField *pField = taosArrayGet(pReq->pFields, i);
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pField->type));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pField->bytes));
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pField->name));
    }
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ttl));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->commentLen));
  if (pReq->commentLen > 0) {
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->comment));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->keep));
  ENCODESQL();
  if (pReq->alterType == TSDB_ALTER_TABLE_ADD_COLUMN ||
      pReq->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION) {
    if (taosArrayGetSize(pReq->pTypeMods) > 0) {
      int8_t hasTypeMod = 1;
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, hasTypeMod));
      for (int32_t i = 0; i < pReq->pTypeMods->size; ++i) {
        const STypeMod *pTypeMod = taosArrayGet(pReq->pTypeMods, i);
        TAOS_CHECK_ERRNO(tEncodeI32(&encoder, *pTypeMod));
      }
    } else {
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, 0));
    }
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMAlterStbReq(void *buf, int32_t bufLen, SMAlterStbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->alterType));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfFields));
  pReq->pFields = taosArrayInit(pReq->numOfFields, sizeof(SField));
  if (pReq->pFields == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < pReq->numOfFields; ++i) {
    if (pReq->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION) {
      taosArrayDestroy(pReq->pFields);
      if ((pReq->pFields = taosArrayInit(pReq->numOfFields, sizeof(SFieldWithOptions))) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      SFieldWithOptions field = {0};
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
      TAOS_CHECK_EXIT(tDecodeU32(&decoder, &field.compress));
      if (taosArrayPush(pReq->pFields, &field) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    } else {
      SField field = {0};
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &field.type));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &field.bytes));
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, field.name));
      if (taosArrayPush(pReq->pFields, &field) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ttl));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->commentLen));
  if (pReq->commentLen > 0) {
    pReq->comment = taosMemoryMalloc(pReq->commentLen + 1);
    if (pReq->comment == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->comment));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->keep));
  }
  DECODESQL();
  if (!tDecodeIsEnd(&decoder) && (pReq->alterType == TSDB_ALTER_TABLE_ADD_COLUMN ||
                                  pReq->alterType == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION)) {
    int8_t hasTypeMod = 0;
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &hasTypeMod));
    if (hasTypeMod == 1) {
      pReq->pTypeMods = taosArrayInit(pReq->numOfFields, sizeof(STypeMod));
      if (!pReq->pTypeMods) {
        TAOS_CHECK_EXIT(terrno);
      }
      for (int32_t i = 0; i < pReq->numOfFields; ++i) {
        STypeMod typeMod = 0;
        TAOS_CHECK_EXIT(tDecodeI32(&decoder, &typeMod));
        if (taosArrayPush(pReq->pTypeMods, &typeMod) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMAltertbReq(SMAlterStbReq *pReq) {
  taosArrayDestroy(pReq->pFields);
  pReq->pFields = NULL;
  taosMemoryFreeClear(pReq->comment);
  FREESQL();
  taosArrayDestroy(pReq->pTypeMods);
}

int32_t tSerializeSEpSet(void *buf, int32_t bufLen, const SEpSet *pEpset) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeSEpSet(&encoder, pEpset));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSEpSet(void *buf, int32_t bufLen, SEpSet *pEpset) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeSEpSet(&decoder, pEpset));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMCreateSmaReq(void *buf, int32_t bufLen, SMCreateSmaReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->stb));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->intervalUnit));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->slidingUnit));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->timezone));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dstVgId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->interval));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->offset));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->sliding));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->watermark));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->maxDelay));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->exprLen));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tagsFilterLen));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sqlLen));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->astLen));
  if (pReq->exprLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->expr, pReq->exprLen));
  }
  if (pReq->tagsFilterLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->tagsFilter, pReq->tagsFilterLen));
  }
  if (pReq->sqlLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->sql, pReq->sqlLen));
  }
  if (pReq->astLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->ast, pReq->astLen));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->deleteMark));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->lastTs));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->normSourceTbUid));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, taosArrayGetSize(pReq->pVgroupVerList)));

  for (int32_t i = 0; i < taosArrayGetSize(pReq->pVgroupVerList); ++i) {
    SVgroupVer *p = taosArrayGet(pReq->pVgroupVerList, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, p->vgId));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, p->ver));
  }
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->recursiveTsma));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->baseTsmaName));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->streamReqLen));
  if (pReq->streamReqLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->createStreamReq, pReq->streamReqLen));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dropStreamReqLen));
  if (pReq->dropStreamReqLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->dropStreamReq, pReq->dropStreamReqLen));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->uid));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateSmaReq(void *buf, int32_t bufLen, SMCreateSmaReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->stb));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->intervalUnit));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->slidingUnit));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->timezone));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dstVgId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->interval));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->offset));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->sliding));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->watermark));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->maxDelay));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->exprLen));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tagsFilterLen));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sqlLen));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->astLen));
  if (pReq->exprLen > 0) {
    pReq->expr = taosMemoryMalloc(pReq->exprLen);
    if (pReq->expr == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->expr));
  }
  if (pReq->tagsFilterLen > 0) {
    pReq->tagsFilter = taosMemoryMalloc(pReq->tagsFilterLen);
    if (pReq->tagsFilter == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->tagsFilter));
  }
  if (pReq->sqlLen > 0) {
    pReq->sql = taosMemoryMalloc(pReq->sqlLen);
    if (pReq->sql == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->sql));
  }
  if (pReq->astLen > 0) {
    pReq->ast = taosMemoryMalloc(pReq->astLen);
    if (pReq->ast == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->ast));
  }
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->deleteMark));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->lastTs));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->normSourceTbUid));

  int32_t numOfVgVer;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfVgVer));
  if (numOfVgVer > 0) {
    pReq->pVgroupVerList = taosArrayInit(numOfVgVer, sizeof(SVgroupVer));
    if (pReq->pVgroupVerList == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < numOfVgVer; ++i) {
      SVgroupVer v = {0};
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &v.vgId));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &v.ver));
      if (taosArrayPush(pReq->pVgroupVerList, &v) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->recursiveTsma));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->baseTsmaName));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->streamReqLen));
  if (pReq->streamReqLen > 0) {
    pReq->createStreamReq = taosMemoryMalloc(pReq->streamReqLen);
    if (pReq->createStreamReq == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->createStreamReq));
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dropStreamReqLen));
  if (pReq->dropStreamReqLen > 0) {
    pReq->dropStreamReq = taosMemoryMalloc(pReq->dropStreamReqLen);
    if (pReq->dropStreamReq == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dropStreamReq));
  }
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->uid));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCreateSmaReq(SMCreateSmaReq *pReq) {
  taosMemoryFreeClear(pReq->expr);
  taosMemoryFreeClear(pReq->tagsFilter);
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->ast);
  taosArrayDestroy(pReq->pVgroupVerList);
}

int32_t tSerializeSMDropSmaReq(void *buf, int32_t bufLen, SMDropSmaReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dropStreamReqLen));
  if (pReq->dropStreamReqLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->dropStreamReq, pReq->dropStreamReqLen));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropSmaReq(void *buf, int32_t bufLen, SMDropSmaReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dropStreamReqLen));
  if (pReq->dropStreamReqLen > 0) {
    pReq->dropStreamReq = taosMemoryMalloc(pReq->dropStreamReqLen);
    if (pReq->dropStreamReq == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dropStreamReq));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSCreateTagIdxReq(void *buf, int32_t bufLen, SCreateTagIndexReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->stbName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->colName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->idxName));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->idxType));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateTagIdxReq(void *buf, int32_t bufLen, SCreateTagIndexReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->stbName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->colName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->idxName));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->idxType));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tDeserializeSDropTagIdxReq(void *buf, int32_t bufLen, SDropTagIndexReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMCreateFullTextReq(void *buf, int32_t bufLen, SMCreateFullTextReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  tEndEncode(&encoder);
_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateFullTextReq(void *buf, int32_t bufLen, SMCreateFullTextReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSMCreateFullTextReq(SMCreateFullTextReq *pReq) {
  // impl later
  return;
}

int32_t tSerializeSNotifyReq(void *buf, int32_t bufLen, SNotifyReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->clusterId));

  int32_t nVgroup = taosArrayGetSize(pReq->pVloads);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, nVgroup));
  for (int32_t i = 0; i < nVgroup; ++i) {
    SVnodeLoadLite *vload = TARRAY_GET_ELEM(pReq->pVloads, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, vload->vgId));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, vload->nTimeSeries));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSNotifyReq(void *buf, int32_t bufLen, SNotifyReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->clusterId));
  int32_t nVgroup = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nVgroup));
  if (nVgroup > 0) {
    pReq->pVloads = taosArrayInit_s(sizeof(SVnodeLoadLite), nVgroup);
    if (!pReq->pVloads) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < nVgroup; ++i) {
      SVnodeLoadLite *vload = TARRAY_GET_ELEM(pReq->pVloads, i);
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &(vload->vgId)));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &(vload->nTimeSeries)));
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSNotifyReq(SNotifyReq *pReq) {
  if (pReq) {
    taosArrayDestroy(pReq->pVloads);
  }
}

int32_t tSerializeSStatusReq(void *buf, int32_t bufLen, SStatusReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  // status
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sver));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->dnodeVer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->clusterId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->rebootTime));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->updateTime));
  TAOS_CHECK_EXIT(tEncodeFloat(&encoder, pReq->numOfCores));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfSupportVnodes));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->numOfDiskCfg));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->memTotal));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->memAvail));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dnodeEp));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->machineId));

  // cluster cfg
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->clusterCfg.statusInterval));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->clusterCfg.checkTime));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->clusterCfg.timezone));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->clusterCfg.locale));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->clusterCfg.charset));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->clusterCfg.enableWhiteList));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->clusterCfg.encryptionKeyStat));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->clusterCfg.encryptionKeyChksum));

  // vnode loads
  int32_t vlen = (int32_t)taosArrayGetSize(pReq->pVloads);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, vlen));
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    int64_t     reserved = 0;
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pload->vgId));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pload->syncState));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pload->syncRestore));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pload->syncCanRead));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->cacheUsage));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->numOfTables));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->numOfTimeSeries));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->totalStorage));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->compStorage));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->pointsWritten));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pload->numOfCachedTables));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pload->learnerProgress));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->roleTimeMs));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->startTimeMs));
  }

  // mnode loads
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->mload.syncState));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->mload.syncRestore));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->qload.dnodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedQuery));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedCQuery));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedFetch));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedDrop));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedNotify));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedHb));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfProcessedDelete));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.cacheDataSize));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfQueryInQueue));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.numOfFetchInQueue));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.timeInQueryQueue));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->qload.timeInFetchQueue));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->statusSeq));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->mload.syncTerm));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->mload.roleTimeMs));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->clusterCfg.ttlChangeOnWrite));

  // vnode extra
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    int64_t     reserved = 0;
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->syncTerm));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, reserved));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, reserved));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, reserved));
  }

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->ipWhiteVer));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->analVer));
  TAOS_CHECK_EXIT(tSerializeSMonitorParas(&encoder, &pReq->clusterCfg.monitorParas));

  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->syncAppliedIndex));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->syncCommitIndex));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timestamp));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timestamp));

  // Encode buffer info
  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad *pload = taosArrayGet(pReq->pVloads, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->bufferSegmentUsed));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pload->bufferSegmentSize));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatusReq(void *buf, int32_t bufLen, SStatusReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  // status
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sver));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->dnodeVer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->clusterId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->rebootTime));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->updateTime));
  TAOS_CHECK_EXIT(tDecodeFloat(&decoder, &pReq->numOfCores));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfSupportVnodes));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->numOfDiskCfg));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->memTotal));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->memAvail));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dnodeEp));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->machineId));

  // cluster cfg
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->clusterCfg.statusInterval));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->clusterCfg.checkTime));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->clusterCfg.timezone));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->clusterCfg.locale));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->clusterCfg.charset));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->clusterCfg.enableWhiteList));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->clusterCfg.encryptionKeyStat));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->clusterCfg.encryptionKeyChksum));

  // vnode loads
  int32_t vlen = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vlen));
  pReq->pVloads = taosArrayInit(vlen, sizeof(SVnodeLoad));
  if (pReq->pVloads == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < vlen; ++i) {
    SVnodeLoad vload = {0};
    vload.syncTerm = -1;

    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vload.vgId));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &vload.syncState));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &vload.syncRestore));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &vload.syncCanRead));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.cacheUsage));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.numOfTables));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.numOfTimeSeries));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.totalStorage));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.compStorage));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.pointsWritten));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vload.numOfCachedTables));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &vload.learnerProgress));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.roleTimeMs));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &vload.startTimeMs));
    if (taosArrayPush(pReq->pVloads, &vload) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  // mnode loads
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->mload.syncState));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->mload.syncRestore));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->qload.dnodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedQuery));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedCQuery));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedFetch));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedDrop));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedNotify));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedHb));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfProcessedDelete));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.cacheDataSize));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfQueryInQueue));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.numOfFetchInQueue));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.timeInQueryQueue));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->qload.timeInFetchQueue));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->statusSeq));

  pReq->mload.syncTerm = -1;
  pReq->mload.roleTimeMs = 0;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->mload.syncTerm));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->mload.roleTimeMs));
  }

  pReq->clusterCfg.ttlChangeOnWrite = false;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->clusterCfg.ttlChangeOnWrite));
  }

  // vnode extra
  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < vlen; ++i) {
      SVnodeLoad *pLoad = taosArrayGet(pReq->pVloads, i);
      int64_t     reserved = 0;
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pLoad->syncTerm));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &reserved));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &reserved));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &reserved));
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->ipWhiteVer));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->analVer));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDeserializeSMonitorParas(&decoder, &pReq->clusterCfg.monitorParas));
  }

  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < vlen; ++i) {
      SVnodeLoad *pLoad = taosArrayGet(pReq->pVloads, i);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pLoad->syncAppliedIndex));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pLoad->syncCommitIndex));
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timestamp));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timestamp));
  }

  // Decode buffer info
  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < vlen; ++i) {
      SVnodeLoad *pLoad = taosArrayGet(pReq->pVloads, i);
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pLoad->bufferSegmentUsed));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pLoad->bufferSegmentSize));
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSStatusReq(SStatusReq *pReq) { taosArrayDestroy(pReq->pVloads); }

int32_t tSerializeSConfigReq(void *buf, int32_t bufLen, SConfigReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->cver));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->forceReadConfig));
  if (pReq->forceReadConfig) {
    TAOS_CHECK_EXIT(tSerializeSConfigArray(&encoder, pReq->array));
  }
  tEndEncode(&encoder);
_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConfigReq(void *buf, int32_t bufLen, SConfigReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->cver));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->forceReadConfig));
  if (pReq->forceReadConfig) {
    pReq->array = taosArrayInit(128, sizeof(SConfigItem));
    if (pReq->array == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDeserializeSConfigArray(&decoder, pReq->array));
  }
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSConfigReq(SConfigReq *pReq) { taosArrayDestroy(pReq->array); }

int32_t tSerializeSConfigRsp(void *buf, int32_t bufLen, SConfigRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->forceReadConfig));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->isConifgVerified));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->isVersionVerified));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->cver));
  if ((!pRsp->isConifgVerified) || (!pRsp->isVersionVerified)) {
    TAOS_CHECK_EXIT(tSerializeSConfigArray(&encoder, pRsp->array));
  }
  tEndEncode(&encoder);
_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConfigRsp(void *buf, int32_t bufLen, SConfigRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->forceReadConfig));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->isConifgVerified));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->isVersionVerified));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->cver));
  if ((!pRsp->isConifgVerified) || (!pRsp->isVersionVerified)) {
    pRsp->array = taosArrayInit(128, sizeof(SConfigItem));
    TAOS_CHECK_EXIT(tDeserializeSConfigArray(&decoder, pRsp->array));
  }
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

void tFreeSConfigRsp(SConfigRsp *pRsp) { taosArrayDestroy(pRsp->array); }

int32_t tSerializeSDnodeInfoReq(void *buf, int32_t bufLen, SDnodeInfoReq *pReq) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->machineId));

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  return code < 0 ? code : tlen;
}

int32_t tDeserializeSDnodeInfoReq(void *buf, int32_t bufLen, SDnodeInfoReq *pReq) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->machineId));

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSStatusRsp(void *buf, int32_t bufLen, SStatusRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  // status
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->dnodeVer));

  // dnode cfg
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->dnodeCfg.dnodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->dnodeCfg.clusterId));

  // dnode eps
  int32_t dlen = (int32_t)taosArrayGetSize(pRsp->pDnodeEps);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, dlen));
  for (int32_t i = 0; i < dlen; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pRsp->pDnodeEps, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pDnodeEp->id));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pDnodeEp->isMnode));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pDnodeEp->ep.fqdn));
    TAOS_CHECK_EXIT(tEncodeU16(&encoder, pDnodeEp->ep.port));
  }

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->statusSeq));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->ipWhiteVer));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->analVer));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatusRsp(void *buf, int32_t bufLen, SStatusRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  // status
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->dnodeVer));

  // cluster cfg
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->dnodeCfg.dnodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->dnodeCfg.clusterId));

  // dnode eps
  int32_t dlen = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &dlen));
  pRsp->pDnodeEps = taosArrayInit(dlen, sizeof(SDnodeEp));
  if (pRsp->pDnodeEps == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < dlen; ++i) {
    SDnodeEp dnodeEp = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &dnodeEp.id));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &dnodeEp.isMnode));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, dnodeEp.ep.fqdn));
    TAOS_CHECK_EXIT(tDecodeU16(&decoder, &dnodeEp.ep.port));
    if (taosArrayPush(pRsp->pDnodeEps, &dnodeEp) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->statusSeq));

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->ipWhiteVer));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->analVer));
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSStatusRsp(SStatusRsp *pRsp) { taosArrayDestroy(pRsp->pDnodeEps); }

int32_t tSerializeSStatisReq(void *buf, int32_t bufLen, SStatisReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->contLen));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->pCont));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->type));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSStatisReq(void *buf, int32_t bufLen, SStatisReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->contLen));
  if (pReq->contLen > 0) {
    pReq->pCont = taosMemoryMalloc(pReq->contLen + 1);
    if (pReq->pCont == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pCont));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, (int8_t *)&pReq->type));
  }
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSStatisReq(SStatisReq *pReq) { taosMemoryFreeClear(pReq->pCont); }

int32_t tSerializeSDropUserReq(void *buf, int32_t bufLen, SDropUserReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropUserReq(void *buf, int32_t bufLen, SDropUserReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSDropUserReq(SDropUserReq *pReq) { FREESQL(); }

int32_t tSerializeSAuditReq(void *buf, int32_t bufLen, SAuditReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->operation));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->table));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sqlLen));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->pSql));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAuditReq(void *buf, int32_t bufLen, SAuditReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->operation));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->table));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sqlLen));
  if (pReq->sqlLen > 0) {
    pReq->pSql = taosMemoryMalloc(pReq->sqlLen + 1);
    if (pReq->pSql == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pSql));
  }
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSAuditReq(SAuditReq *pReq) { taosMemoryFreeClear(pReq->pSql); }

SIpWhiteListDual *cloneIpWhiteList(SIpWhiteListDual *pIpWhiteList) {
  if (pIpWhiteList == NULL) return NULL;

  int32_t sz = sizeof(SIpWhiteListDual) + pIpWhiteList->num * sizeof(SIpRange);

  SIpWhiteListDual *pNew = taosMemoryCalloc(1, sz);
  if (pNew) {
    memcpy(pNew, pIpWhiteList, sz);
  }
  return pNew;
}

int32_t cvtIpWhiteListToDual(SIpWhiteList *pWhiteList, SIpWhiteListDual **pWhiteListDual) {
  int32_t           code = 0;
  int32_t           lino = 0;
  SIpWhiteListDual *pList = NULL;
  SIpRange          p6 = {0};

  pList = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + sizeof(SIpRange) * (pWhiteList->num + 1));
  if (pList == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  pList->num = pWhiteList->num;
  for (int i = 0; i < pWhiteList->num; i++) {
    SIpV4Range *pIp4 = &(pWhiteList->pIpRange[i]);
    SIpRange   *pRange = &(pList->pIpRanges[i]);

    pRange->type = 0;
    memcpy(&pRange->ipV4, pIp4, sizeof(SIpV4Range));
  }

  code = createDefaultIp6Range(&p6);
  TAOS_CHECK_GOTO(code, &lino, _OVER);

  memcpy(pList->pIpRanges + pList->num, &p6, sizeof(SIpRange));
  pList->num++;

_OVER:
  if (code != 0) {
    taosMemoryFree(pList);
  } else {
    *pWhiteListDual = pList;
  }
  return code;
}

int32_t createDefaultIp6Range(SIpRange *pRange) {
  int32_t code = 0;
  SIpAddr add6 = {.type = 1, .ipv6 = {"::1"}, .mask = 128};
  return tIpStrToUint(&add6, pRange);
}

int32_t createDefaultIp4Range(SIpRange *pRange) {
  int32_t code = 0;
  SIpAddr add4 = {.type = 0, .ipv4 = {"127.0.0.1"}, .mask = 32};
  return tIpStrToUint(&add4, pRange);
}
int32_t cvtIpWhiteListDualToV4(SIpWhiteListDual *pWhiteListDual, SIpWhiteList **pWhiteList) {
  int32_t code = 0;

  int32_t       num = 0;
  SIpWhiteList *p = taosMemCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * pWhiteListDual->num);
  if (p == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < pWhiteListDual->num; ++i) {
    SIpRange *pRange = &pWhiteListDual->pIpRanges[i];
    if (pRange->type == 0) {
      memcpy(&p->pIpRange[num], &pRange->ipV4, sizeof(SIpV4Range));
    }
    num++;
  }
  p->num = num;

  *pWhiteList = p;

  return code;
}
int32_t tSerializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->createType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->superUser));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->sysInfo));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->enable));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->pass));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numIpRanges));
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->pIpRanges[i].ip));
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->pIpRanges[i].mask));
  }
  ENCODESQL();
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->isImport));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->createDb));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->passIsMd5));

  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    SIpRange *pRange = &pReq->pIpDualRanges[i];
    code = tSerializeIpRange(&encoder, pRange);
    TAOS_CHECK_EXIT(code);
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateUserReq(void *buf, int32_t bufLen, SCreateUserReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->createType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->superUser));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->sysInfo));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->enable));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pass));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numIpRanges));
  pReq->pIpRanges = taosMemoryMalloc(pReq->numIpRanges * sizeof(SIpV4Range));
  if (pReq->pIpRanges == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &(pReq->pIpRanges[i].ip)));
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &(pReq->pIpRanges[i].mask)));
  }
  DECODESQL();
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->isImport));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->createDb));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->passIsMd5));
  }

  if (!tDecodeIsEnd(&decoder)) {
    pReq->pIpDualRanges = taosMemoryMalloc(pReq->numIpRanges * sizeof(SIpRange));
    if (pReq->pIpDualRanges == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
      SIpRange *pRange = &pReq->pIpDualRanges[i];
      code = tDeserializeIpRange(&decoder, pRange);
      TAOS_CHECK_EXIT(code);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t getIpv4Range(SUpdateUserIpWhite *pReq, SIpV4Range **pIpRange, int32_t *num) {
  int32_t code = 0;
  if (pReq->numOfRange <= 0) {
    return code;
  }

  SIpV4Range *p = taosMemoryCalloc(1, pReq->numOfRange * sizeof(SIpV4Range));
  if (p == NULL) {
    return terrno;
  }

  int32_t cnt = 0;
  for (int32_t i = 0; i < pReq->numOfRange; i++) {
    SIpRange *pRange = &pReq->pIpDualRanges[i];
    if (pRange->type == 0) {
      SIpV4Range *pIp4 = (SIpV4Range *)&pRange->ipV4;
      memcpy(&p[cnt], pIp4, sizeof(SIpV4Range));
    } else {
      continue;
    }
  }

  *pIpRange = p;
  *num = cnt;

  return code;
}
int32_t tSerializeSUpdateIpWhite(void *buf, int32_t bufLen, SUpdateIpWhite *pReq) {
  SEncoder    encoder = {0};
  int32_t     code = 0;
  int32_t     lino;
  int32_t     tlen;
  int32_t     num = 0;
  SIpV4Range *p = NULL;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->ver));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfUser));
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUser = &(pReq->pUserIpWhite[i]);

    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pUser->ver));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pUser->user));

    TAOS_CHECK_EXIT(getIpv4Range(pUser, &p, &num));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
    for (int32_t i = 0; i < num; i++) {
      SIpV4Range *pRange = &p[i];
      TAOS_CHECK_EXIT(tEncodeU32(&encoder, pRange->ip));
      TAOS_CHECK_EXIT(tEncodeU32(&encoder, pRange->mask));
    }
    taosMemFreeClear(p);
  }

  tEndEncode(&encoder);

_exit:
  taosMemFreeClear(p);
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSUpdateIpWhite(void *buf, int32_t bufLen, SUpdateIpWhite *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  // impl later
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->ver));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfUser));

  if ((pReq->pUserIpWhite = taosMemoryCalloc(1, sizeof(SUpdateUserIpWhite) * pReq->numOfUser)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUserWhite = &pReq->pUserIpWhite[i];
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pUserWhite->ver));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pUserWhite->user));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pUserWhite->numOfRange));

    if ((pUserWhite->pIpRanges = taosMemoryCalloc(1, pUserWhite->numOfRange * sizeof(SIpV4Range))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int j = 0; j < pUserWhite->numOfRange; j++) {
      SIpV4Range *pRange = &pUserWhite->pIpRanges[j];
      TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pRange->ip));
      TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pRange->mask));
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSUpdateIpWhiteReq(SUpdateIpWhite *pReq) {
  if (pReq == NULL) return;

  if (pReq->pUserIpWhite) {
    for (int i = 0; i < pReq->numOfUser; i++) {
      SUpdateUserIpWhite *pUserWhite = &pReq->pUserIpWhite[i];
      taosMemoryFree(pUserWhite->pIpRanges);
    }
  }
  taosMemoryFree(pReq->pUserIpWhite);
  return;
}

int32_t tSerializeIpRange(SEncoder *encoder, SIpRange *pRange) {
  int32_t lino;
  int32_t code = 0;

  TAOS_CHECK_EXIT(tEncodeI8(encoder, pRange->type));
  if (pRange->type == 0) {
    SIpV4Range *pIp4 = (SIpV4Range *)&pRange->ipV4;
    TAOS_CHECK_EXIT(tEncodeU32(encoder, pIp4->ip));
    TAOS_CHECK_EXIT(tEncodeU32(encoder, pIp4->mask));
  } else {
    SIpV6Range *pIp6 = (SIpV6Range *)&pRange->ipV6;
    TAOS_CHECK_EXIT(tEncodeU64(encoder, pIp6->addr[0]));
    TAOS_CHECK_EXIT(tEncodeU64(encoder, pIp6->addr[1]));
    TAOS_CHECK_EXIT(tEncodeU32(encoder, pIp6->mask));
  }
_exit:
  return code;
}

int32_t tDeserializeIpRange(SDecoder *decoder, SIpRange *pRange) {
  int32_t lino = 0;
  int32_t code = 0;

  TAOS_CHECK_EXIT(tDecodeI8(decoder, &pRange->type));
  if (pRange->type == 0) {
    SIpV4Range *pIp4 = (SIpV4Range *)&pRange->ipV4;
    TAOS_CHECK_EXIT(tDecodeU32(decoder, &pIp4->ip));
    TAOS_CHECK_EXIT(tDecodeU32(decoder, &pIp4->mask));
  } else {
    SIpV6Range *pIp6 = (SIpV6Range *)&pRange->ipV6;
    TAOS_CHECK_EXIT(tDecodeU64(decoder, &pIp6->addr[0]));
    TAOS_CHECK_EXIT(tDecodeU64(decoder, &pIp6->addr[1]));
    TAOS_CHECK_EXIT(tDecodeU32(decoder, &pIp6->mask));
  }
_exit:
  return code;
}
int32_t tSerializeSUpdateIpWhiteDual(void *buf, int32_t bufLen, SUpdateIpWhite *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->ver));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfUser));
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUser = &(pReq->pUserIpWhite[i]);

    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pUser->ver));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pUser->user));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pUser->numOfRange));
    for (int j = 0; j < pUser->numOfRange; j++) {
      SIpRange *pRange = &pUser->pIpDualRanges[j];
      TAOS_CHECK_EXIT(tSerializeIpRange(&encoder, pRange));
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSUpdateIpWhiteDual(void *buf, int32_t bufLen, SUpdateIpWhite *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  // impl later
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->ver));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfUser));

  if ((pReq->pUserIpWhite = taosMemoryCalloc(1, sizeof(SUpdateUserIpWhite) * pReq->numOfUser)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pUserWhite = &pReq->pUserIpWhite[i];
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pUserWhite->ver));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pUserWhite->user));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pUserWhite->numOfRange));

    if ((pUserWhite->pIpRanges = taosMemoryCalloc(1, pUserWhite->numOfRange * sizeof(SIpRange))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int j = 0; j < pUserWhite->numOfRange; j++) {
      SIpRange *pRange = &pUserWhite->pIpDualRanges[j];
      TAOS_CHECK_EXIT(tDeserializeIpRange(&decoder, pRange));
    }
  }

  tEndDecode(&decoder);
_exit:
  if (code < 0) {
    uError("Failed to deserialize SUpdateIpWhiteDual at line %d, code: %s", lino, tstrerror(code));
  }
  tDecoderClear(&decoder);
  return code;
}
void tFreeSUpdateIpWhiteDualReq(SUpdateIpWhite *pReq) {
  if (pReq == NULL) return;

  if (pReq->pUserIpWhite) {
    for (int i = 0; i < pReq->numOfUser; i++) {
      SUpdateUserIpWhite *pUserWhite = &pReq->pUserIpWhite[i];
      taosMemoryFree(pUserWhite->pIpDualRanges);
    }
  }
  taosMemoryFree(pReq->pUserIpWhite);
  return;
}

int32_t cloneSUpdateIpWhiteReq(SUpdateIpWhite *pReq, SUpdateIpWhite **pUpdateMsg) {
  int32_t code = 0;
  if (pReq == NULL) {
    return 0;
  }
  SUpdateIpWhite *pClone = taosMemoryCalloc(1, sizeof(SUpdateIpWhite));
  if (pClone == NULL) {
    return terrno;
  }

  pClone->numOfUser = pReq->numOfUser;
  pClone->ver = pReq->ver;
  pClone->pUserIpWhite = taosMemoryCalloc(1, sizeof(SUpdateUserIpWhite) * pReq->numOfUser);
  if (pClone->pUserIpWhite == NULL) {
    taosMemoryFree(pClone);
    return terrno;
  }

  for (int i = 0; i < pReq->numOfUser; i++) {
    SUpdateUserIpWhite *pNew = &pClone->pUserIpWhite[i];
    SUpdateUserIpWhite *pOld = &pReq->pUserIpWhite[i];

    pNew->ver = pOld->ver;
    memcpy(pNew->user, pOld->user, strlen(pOld->user));
    pNew->numOfRange = pOld->numOfRange;

    int32_t sz = pOld->numOfRange * sizeof(SIpRange);
    pNew->pIpDualRanges = taosMemoryCalloc(1, sz);
    if (pNew->pIpDualRanges == NULL) {
      code = terrno;
      break;
    }
    memcpy(pNew->pIpDualRanges, pOld->pIpDualRanges, sz);
  }
_return:
  if (code < 0) {
    tFreeSUpdateIpWhiteReq(pClone);
    taosMemoryFree(pClone);
  } else {
    *pUpdateMsg = pClone;
  }
  return code;
}
int32_t tSerializeRetrieveIpWhite(void *buf, int32_t bufLen, SRetrieveIpWhiteReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->ipWhiteVer));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeRetrieveIpWhite(void *buf, int32_t bufLen, SRetrieveIpWhiteReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->ipWhiteVer));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeRetrieveAnalyticAlgoReq(void *buf, int32_t bufLen, SRetrieveAnalyticsAlgoReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->analVer));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeRetrieveAnalyticAlgoReq(void *buf, int32_t bufLen, SRetrieveAnalyticsAlgoReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->analVer));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeRetrieveAnalyticAlgoRsp(void *buf, int32_t bufLen, SRetrieveAnalyticAlgoRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  int32_t numOfAlgos = 0;
  void   *pIter = taosHashIterate(pRsp->hash, NULL);
  while (pIter != NULL) {
    SAnalyticsUrl *pUrl = pIter;
    size_t         nameLen = 0;
    const char    *name = taosHashGetKey(pIter, &nameLen);
    if (nameLen > 0 && nameLen <= TSDB_ANALYTIC_ALGO_KEY_LEN && pUrl->urlLen > 0) {
      numOfAlgos++;
    }
    pIter = taosHashIterate(pRsp->hash, pIter);
  }

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->ver));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfAlgos));

  pIter = taosHashIterate(pRsp->hash, NULL);
  while (pIter != NULL) {
    SAnalyticsUrl *pUrl = pIter;
    size_t         nameLen = 0;
    const char    *name = taosHashGetKey(pIter, &nameLen);
    if (nameLen > 0 && pUrl->urlLen > 0) {
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, nameLen));
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)name, nameLen));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pUrl->anode));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pUrl->type));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pUrl->urlLen));
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pUrl->url, pUrl->urlLen));
    }
    pIter = taosHashIterate(pRsp->hash, pIter);
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeRetrieveAnalyticAlgoRsp(void *buf, int32_t bufLen, SRetrieveAnalyticAlgoRsp *pRsp) {
  if (pRsp->hash == NULL) {
    pRsp->hash = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
    if (pRsp->hash == NULL) {
      terrno = TSDB_CODE_OUT_OF_BUFFER;
      return terrno;
    }
  }

  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  int32_t       numOfAlgos = 0;
  int32_t       nameLen;
  int32_t       type;
  char          name[TSDB_ANALYTIC_ALGO_KEY_LEN];
  SAnalyticsUrl url = {0};

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->ver));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfAlgos));

  for (int32_t f = 0; f < numOfAlgos; ++f) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &nameLen));
    if (nameLen > 0 && nameLen <= TSDB_ANALYTIC_ALGO_NAME_LEN) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, name));
    }

    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &url.anode));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &type));
    url.type = (EAnalAlgoType)type;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &url.urlLen));
    if (url.urlLen > 0) {
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&url.url, NULL) < 0);
    }

    char dstName[TSDB_ANALYTIC_ALGO_NAME_LEN] = {0};
    strntolower(dstName, name, nameLen);

    TAOS_CHECK_EXIT(taosHashPut(pRsp->hash, dstName, nameLen, &url, sizeof(SAnalyticsUrl)));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeRetrieveAnalyticAlgoRsp(SRetrieveAnalyticAlgoRsp *pRsp) {
  void *pIter = taosHashIterate(pRsp->hash, NULL);
  while (pIter != NULL) {
    SAnalyticsUrl *pUrl = (SAnalyticsUrl *)pIter;
    taosMemoryFree(pUrl->url);
    pIter = taosHashIterate(pRsp->hash, pIter);
  }
  taosHashCleanup(pRsp->hash);

  pRsp->hash = NULL;
}

void tFreeSCreateUserReq(SCreateUserReq *pReq) {
  FREESQL();
  taosMemoryFreeClear(pReq->pIpRanges);
  taosMemoryFreeClear(pReq->pIpDualRanges);
}

int32_t tSerializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->alterType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->superUser));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->sysInfo));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->enable));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->isView));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->pass));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->objname));
  int32_t len = strlen(pReq->tabName);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, len));
  if (len > 0) {
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->tabName));
  }
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->tagCond, pReq->tagCondLen));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numIpRanges));
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->pIpRanges[i].ip));
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->pIpRanges[i].mask));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->privileges));
  ENCODESQL();
  TAOS_CHECK_EXIT(tEncodeU8(&encoder, pReq->flag));
  TAOS_CHECK_EXIT(tEncodeU8(&encoder, pReq->passIsMd5));

  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    SIpRange *pRange = &pReq->pIpDualRanges[i];
    code = tSerializeIpRange(&encoder, pRange);
    TAOS_CHECK_EXIT(code);
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterUserReq(void *buf, int32_t bufLen, SAlterUserReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->alterType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->superUser));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->sysInfo));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->enable));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->isView));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pass));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->objname));
  if (!tDecodeIsEnd(&decoder)) {
    int32_t len = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &len));
    if (len > 0) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->tabName));
    }
    uint64_t tagCondLen = 0;
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->tagCond, &tagCondLen));
    pReq->tagCondLen = tagCondLen;
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numIpRanges));
  pReq->pIpRanges = taosMemoryMalloc(pReq->numIpRanges * sizeof(SIpV4Range));
  if (pReq->pIpRanges == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &(pReq->pIpRanges[i].ip)));
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &(pReq->pIpRanges[i].mask)));
  }
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->privileges));
  DECODESQL();
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU8(&decoder, &pReq->flag));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU8(&decoder, &pReq->passIsMd5));
  }

  if (!tDecodeIsEnd(&decoder)) {
    pReq->pIpDualRanges = taosMemoryMalloc(pReq->numIpRanges * sizeof(SIpRange));
    if (pReq->pIpDualRanges == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < pReq->numIpRanges; ++i) {
      SIpRange *pRange = &pReq->pIpDualRanges[i];
      code = tDeserializeIpRange(&decoder, pRange);
      TAOS_CHECK_EXIT(code);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSAlterUserReq(SAlterUserReq *pReq) {
  taosMemoryFreeClear(pReq->tagCond);
  taosMemoryFree(pReq->pIpRanges);
  taosMemoryFree(pReq->pIpDualRanges);
  FREESQL();
}

int32_t tSerializeSGetUserAuthReq(void *buf, int32_t bufLen, SGetUserAuthReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserAuthReq(void *buf, int32_t bufLen, SGetUserAuthReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSGetUserAuthRspImpl(SEncoder *pEncoder, SGetUserAuthRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pRsp->user));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->superAuth));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->sysInfo));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->enable));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->dropped));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->version));

  int32_t numOfCreatedDbs = taosHashGetSize(pRsp->createdDbs);
  int32_t numOfReadDbs = taosHashGetSize(pRsp->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pRsp->writeDbs);

  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfCreatedDbs));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfReadDbs));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfWriteDbs));

  char *db = taosHashIterate(pRsp->createdDbs, NULL);
  while (db != NULL) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, db));
    db = taosHashIterate(pRsp->createdDbs, db);
  }

  db = taosHashIterate(pRsp->readDbs, NULL);
  while (db != NULL) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, db));
    db = taosHashIterate(pRsp->readDbs, db);
  }

  db = taosHashIterate(pRsp->writeDbs, NULL);
  while (db != NULL) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, db));
    db = taosHashIterate(pRsp->writeDbs, db);
  }

  int32_t numOfReadTbs = taosHashGetSize(pRsp->readTbs);
  int32_t numOfWriteTbs = taosHashGetSize(pRsp->writeTbs);
  int32_t numOfAlterTbs = taosHashGetSize(pRsp->alterTbs);
  int32_t numOfReadViews = taosHashGetSize(pRsp->readViews);
  int32_t numOfWriteViews = taosHashGetSize(pRsp->writeViews);
  int32_t numOfAlterViews = taosHashGetSize(pRsp->alterViews);
  int32_t numOfUseDbs = taosHashGetSize(pRsp->useDbs);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfReadTbs));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfWriteTbs));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfAlterTbs));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfReadViews));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfWriteViews));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfAlterViews));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, numOfUseDbs));

  char *tb = taosHashIterate(pRsp->readTbs, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(tb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));

    size_t valueLen = 0;
    valueLen = strlen(tb);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, valueLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, tb));

    tb = taosHashIterate(pRsp->readTbs, tb);
  }

  tb = taosHashIterate(pRsp->writeTbs, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(tb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));

    size_t valueLen = 0;
    valueLen = strlen(tb);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, valueLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, tb));

    tb = taosHashIterate(pRsp->writeTbs, tb);
  }

  tb = taosHashIterate(pRsp->alterTbs, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(tb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));

    size_t valueLen = 0;
    valueLen = strlen(tb);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, valueLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, tb));

    tb = taosHashIterate(pRsp->alterTbs, tb);
  }

  tb = taosHashIterate(pRsp->readViews, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(tb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));

    size_t valueLen = 0;
    valueLen = strlen(tb);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, valueLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, tb));

    tb = taosHashIterate(pRsp->readViews, tb);
  }

  tb = taosHashIterate(pRsp->writeViews, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(tb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));

    size_t valueLen = 0;
    valueLen = strlen(tb);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, valueLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, tb));

    tb = taosHashIterate(pRsp->writeViews, tb);
  }

  tb = taosHashIterate(pRsp->alterViews, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(tb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));

    size_t valueLen = 0;
    valueLen = strlen(tb);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, valueLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, tb));

    tb = taosHashIterate(pRsp->alterViews, tb);
  }

  int32_t *useDb = taosHashIterate(pRsp->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, keyLen));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, key));
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, *useDb));
    useDb = taosHashIterate(pRsp->useDbs, useDb);
  }

  // since 3.0.7.0
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->passVer));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->whiteListVer));
  return 0;
}

int32_t tSerializeSGetUserAuthRsp(void *buf, int32_t bufLen, SGetUserAuthRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tSerializeSGetUserAuthRspImpl(&encoder, pRsp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserAuthRspImpl(SDecoder *pDecoder, SGetUserAuthRsp *pRsp) {
  char *key = NULL, *value = NULL;
  pRsp->createdDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readTbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeTbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->alterTbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->readViews = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->writeViews = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->alterViews = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pRsp->useDbs = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pRsp->createdDbs == NULL || pRsp->readDbs == NULL || pRsp->writeDbs == NULL || pRsp->readTbs == NULL ||
      pRsp->writeTbs == NULL || pRsp->alterTbs == NULL || pRsp->readViews == NULL || pRsp->writeViews == NULL ||
      pRsp->alterViews == NULL || pRsp->useDbs == NULL) {
    goto _err;
  }

  if (tDecodeCStrTo(pDecoder, pRsp->user) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->superAuth) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->sysInfo) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->enable) < 0) goto _err;
  if (tDecodeI8(pDecoder, &pRsp->dropped) < 0) goto _err;
  if (tDecodeI32(pDecoder, &pRsp->version) < 0) goto _err;

  int32_t numOfCreatedDbs = 0;
  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  if (tDecodeI32(pDecoder, &numOfCreatedDbs) < 0) goto _err;
  if (tDecodeI32(pDecoder, &numOfReadDbs) < 0) goto _err;
  if (tDecodeI32(pDecoder, &numOfWriteDbs) < 0) goto _err;

  for (int32_t i = 0; i < numOfCreatedDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) goto _err;
    int32_t len = strlen(db);
    if (taosHashPut(pRsp->createdDbs, db, len + 1, db, len + 1) < 0) goto _err;
  }

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) goto _err;
    int32_t len = strlen(db);
    if (taosHashPut(pRsp->readDbs, db, len + 1, db, len + 1) < 0) goto _err;
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    if (tDecodeCStrTo(pDecoder, db) < 0) goto _err;
    int32_t len = strlen(db);
    if (taosHashPut(pRsp->writeDbs, db, len + 1, db, len + 1) < 0) goto _err;
  }

  if (!tDecodeIsEnd(pDecoder)) {
    int32_t numOfReadTbs = 0;
    int32_t numOfWriteTbs = 0;
    int32_t numOfAlterTbs = 0;
    int32_t numOfReadViews = 0;
    int32_t numOfWriteViews = 0;
    int32_t numOfAlterViews = 0;
    int32_t numOfUseDbs = 0;
    if (tDecodeI32(pDecoder, &numOfReadTbs) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfWriteTbs) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfAlterTbs) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfReadViews) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfWriteViews) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfAlterViews) < 0) goto _err;
    if (tDecodeI32(pDecoder, &numOfUseDbs) < 0) goto _err;

    for (int32_t i = 0; i < numOfReadTbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      if ((value = taosMemoryCalloc(valuelen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      if (taosHashPut(pRsp->readTbs, key, keyLen, value, valuelen + 1) < 0) goto _err;

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfWriteTbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      if ((value = taosMemoryCalloc(valuelen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      if (taosHashPut(pRsp->writeTbs, key, keyLen, value, valuelen + 1) < 0) goto _err;

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfAlterTbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      if ((value = taosMemoryCalloc(valuelen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      if (taosHashPut(pRsp->alterTbs, key, keyLen, value, valuelen + 1) < 0) goto _err;

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfReadViews; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      if ((value = taosMemoryCalloc(valuelen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      if (taosHashPut(pRsp->readViews, key, keyLen, value, valuelen + 1) < 0) goto _err;

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfWriteViews; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      if ((value = taosMemoryCalloc(valuelen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      if (taosHashPut(pRsp->writeViews, key, keyLen, value, valuelen + 1) < 0) goto _err;

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfAlterViews; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t valuelen = 0;
      if (tDecodeI32(pDecoder, &valuelen) < 0) goto _err;

      if ((value = taosMemoryCalloc(valuelen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, value) < 0) goto _err;

      if (taosHashPut(pRsp->alterViews, key, keyLen, value, valuelen + 1) < 0) goto _err;

      taosMemoryFreeClear(key);
      taosMemoryFreeClear(value);
    }

    for (int32_t i = 0; i < numOfUseDbs; ++i) {
      int32_t keyLen = 0;
      if (tDecodeI32(pDecoder, &keyLen) < 0) goto _err;

      if ((key = taosMemoryCalloc(keyLen + 1, sizeof(char))) == NULL) goto _err;
      if (tDecodeCStrTo(pDecoder, key) < 0) goto _err;

      int32_t ref = 0;
      if (tDecodeI32(pDecoder, &ref) < 0) goto _err;

      if (taosHashPut(pRsp->useDbs, key, keyLen, &ref, sizeof(ref)) < 0) goto _err;
      taosMemoryFreeClear(key);
    }
    // since 3.0.7.0
    if (!tDecodeIsEnd(pDecoder)) {
      if (tDecodeI32(pDecoder, &pRsp->passVer) < 0) goto _err;
    } else {
      pRsp->passVer = 0;
    }
    if (!tDecodeIsEnd(pDecoder)) {
      if (tDecodeI64(pDecoder, &pRsp->whiteListVer) < 0) goto _err;
    } else {
      pRsp->whiteListVer = 0;
    }
  }
  return 0;
_err:
  taosHashCleanup(pRsp->createdDbs);
  taosHashCleanup(pRsp->readDbs);
  taosHashCleanup(pRsp->writeDbs);
  taosHashCleanup(pRsp->readTbs);
  taosHashCleanup(pRsp->writeTbs);
  taosHashCleanup(pRsp->alterTbs);
  taosHashCleanup(pRsp->readViews);
  taosHashCleanup(pRsp->writeViews);
  taosHashCleanup(pRsp->alterViews);
  taosHashCleanup(pRsp->useDbs);

  taosMemoryFreeClear(key);
  taosMemoryFreeClear(value);
  return -1;
}

int32_t tDeserializeSGetUserAuthRsp(void *buf, int32_t bufLen, SGetUserAuthRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDeserializeSGetUserAuthRspImpl(&decoder, pRsp));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSGetUserAuthRsp(SGetUserAuthRsp *pRsp) {
  taosHashCleanup(pRsp->createdDbs);
  taosHashCleanup(pRsp->readDbs);
  taosHashCleanup(pRsp->writeDbs);
  taosHashCleanup(pRsp->readTbs);
  taosHashCleanup(pRsp->writeTbs);
  taosHashCleanup(pRsp->alterTbs);
  taosHashCleanup(pRsp->readViews);
  taosHashCleanup(pRsp->writeViews);
  taosHashCleanup(pRsp->alterViews);
  taosHashCleanup(pRsp->useDbs);
}

int32_t tSerializeSGetUserWhiteListReq(void *buf, int32_t bufLen, SGetUserWhiteListReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserWhiteListReq(void *buf, int32_t bufLen, SGetUserWhiteListReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSGetUserWhiteListRsp(void *buf, int32_t bufLen, SGetUserWhiteListRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->user));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->numWhiteLists));
  for (int i = 0; i < pRsp->numWhiteLists; ++i) {
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pRsp->pWhiteLists[i].ip));
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, pRsp->pWhiteLists[i].mask));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSGetUserWhiteListRsp(void *buf, int32_t bufLen, SGetUserWhiteListRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->user));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->numWhiteLists));
  pRsp->pWhiteLists = taosMemoryMalloc(pRsp->numWhiteLists * sizeof(SIpV4Range));
  if (pRsp->pWhiteLists == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pRsp->numWhiteLists; ++i) {
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &(pRsp->pWhiteLists[i].ip)));
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &(pRsp->pWhiteLists[i].mask)));
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSGetUserWhiteListRsp(SGetUserWhiteListRsp *pRsp) { taosMemoryFree(pRsp->pWhiteLists); }

int32_t tSerializeSGetUserWhiteListDualRsp(void *buf, int32_t bufLen, SGetUserWhiteListRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->user));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->numWhiteLists));
  for (int i = 0; i < pRsp->numWhiteLists; ++i) {
    SIpRange *range = &pRsp->pWhiteListsDual[i];
    TAOS_CHECK_EXIT(tSerializeIpRange(&encoder, range));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSGetUserWhiteListDualRsp(void *buf, int32_t bufLen, SGetUserWhiteListRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->user));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->numWhiteLists));

  pRsp->pWhiteListsDual = taosMemoryMalloc(pRsp->numWhiteLists * sizeof(SIpRange));
  if (pRsp->pWhiteListsDual == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pRsp->numWhiteLists; ++i) {
    SIpRange *range = &pRsp->pWhiteListsDual[i];
    TAOS_CHECK_EXIT(tDeserializeIpRange(&decoder, range));
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tIpStrToUint(const SIpAddr *addr, SIpRange *range) {
  int32_t code = 0;
  range->type = addr->type;
  const char *buf = IP_ADDR_STR(addr);
  if (addr->type == 0) {
    struct in_addr taddr;
    if (inet_pton(AF_INET, buf, &taddr) <= 0) {
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }

    SIpV4Range *ipv4 = &range->ipV4;
    ipv4->ip = taddr.s_addr;
    ipv4->mask = addr->mask;
  } else if (addr->type == 1) {
    struct in6_addr taddr;
    if (inet_pton(AF_INET6, buf, &(taddr)) <= 0) {
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }

    SIpV6Range *ipv6 = &range->ipV6;
    memcpy(&ipv6->addr[0], taddr.s6_addr, 8);
    memcpy(&ipv6->addr[1], taddr.s6_addr + 8, 8);

    ipv6->mask = addr->mask;
  }
  return code;
}

int32_t tIpUintToStr(const SIpRange *range, SIpAddr *addr) {
  int32_t code = 0;
  addr->type = range->type;
  if (addr->type == 0) {
    struct in_addr taddr;
    memcpy(&taddr.s_addr, &range->ipV4.ip, sizeof(taddr.s_addr));
    if (inet_ntop(AF_INET, &taddr, addr->ipv4, sizeof(addr->ipv4)) == NULL) {
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }
    addr->mask = range->ipV4.mask;
  } else {
    struct in6_addr taddr;
    memcpy(taddr.s6_addr, &range->ipV6.addr[0], 8);
    memcpy(taddr.s6_addr + 8, &range->ipV6.addr[1], 8);
    if (inet_ntop(AF_INET6, &taddr, addr->ipv6, sizeof(addr->ipv6)) == NULL) {
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }
    addr->mask = range->ipV6.mask;
  }

  return code;
}
int32_t tIpRangeSetMask(SIpRange *range, int32_t mask) {
  if (range->type == 0) {
    SIpV4Range *p4 = (SIpV4Range *)&range->ipV4;
    if (mask < 0 || mask > 32) {
      return TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
    p4->mask = mask;
  } else {
    SIpV6Range *p6 = (SIpV6Range *)&range->ipV6;
    if (mask < 0 || mask > 128) {
      return TSDB_CODE_PAR_INVALID_IP_RANGE;
    }
    p6->mask = mask;
  }
  return 0;
}

void tIpRangeSetDefaultMask(SIpRange *range) {
  if (range->type == 0) {
    SIpV4Range *p4 = (SIpV4Range *)&range->ipV4;
    p4->mask = 128;
  } else {
    SIpV6Range *p6 = (SIpV6Range *)&range->ipV6;
    p6->mask = 32;
  }
}
void tFreeSGetUserWhiteListDualRsp(SGetUserWhiteListRsp *pRsp) { taosMemoryFree(pRsp->pWhiteListsDual); }

int32_t tSerializeSMCfgClusterReq(void *buf, int32_t bufLen, SMCfgClusterReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->config));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->value));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCfgClusterReq(void *buf, int32_t bufLen, SMCfgClusterReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->config));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->value));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCfgClusterReq(SMCfgClusterReq *pReq) { FREESQL(); }

int32_t tSerializeSCreateDropMQSNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDropMQSNodeReq(void *buf, int32_t bufLen, SMCreateQnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tEncodeSNodeEpSet(SEncoder* pEncoder, SNodeEpSet *pNode) {
  int32_t code = 0;
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pNode->nodeId));
  TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pNode->epSet));

_exit:

  return code;
}

int32_t tDecodeSNodeEpSet(SDecoder *pDecoder, SNodeEpSet *pNode) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pNode->nodeId));
  TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &pNode->epSet));

  return 0;
}



int32_t tSerializeSDCreateSNodeReq(void *buf, int32_t bufLen, SDCreateSnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->snodeId));
  TAOS_CHECK_EXIT(tEncodeSNodeEpSet(&encoder, &pReq->leaders[0]));
  TAOS_CHECK_EXIT(tEncodeSNodeEpSet(&encoder, &pReq->leaders[1]));
  TAOS_CHECK_EXIT(tEncodeSNodeEpSet(&encoder, &pReq->replica));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDCreateSNodeReq(void *buf, int32_t bufLen, SDCreateSnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->snodeId));
  TAOS_CHECK_EXIT(tDecodeSNodeEpSet(&decoder, &pReq->leaders[0]));
  TAOS_CHECK_EXIT(tDecodeSNodeEpSet(&decoder, &pReq->leaders[1]));
  TAOS_CHECK_EXIT(tDecodeSNodeEpSet(&decoder, &pReq->replica));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}


void tFreeSMCreateQnodeReq(SMCreateQnodeReq *pReq) { FREESQL(); }

void tFreeSDCreateSnodeReq(SDCreateSnodeReq *pReq) { FREESQL(); }

void tFreeSDDropQnodeReq(SDDropQnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->fqdn));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->port));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->force));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->unsafe));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDnodeReq(void *buf, int32_t bufLen, SDropDnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->fqdn));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->port));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->force));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->unsafe));
  } else {
    pReq->unsafe = false;
  }

  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSDropDnodeReq(SDropDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSRestoreDnodeReq(void *buf, int32_t bufLen, SRestoreDnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->restoreType));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRestoreDnodeReq(void *buf, int32_t bufLen, SRestoreDnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->restoreType));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSRestoreDnodeReq(SRestoreDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->config));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->value));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCfgDnodeReq(void *buf, int32_t bufLen, SMCfgDnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->config));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->value));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCfgDnodeReq(SMCfgDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSDCfgDnodeReq(void *buf, int32_t bufLen, SDCfgDnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->version));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->config));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->value));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDCfgDnodeReq(void *buf, int32_t bufLen, SDCfgDnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->version));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->config));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->value));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMCreateAnodeReq(void *buf, int32_t bufLen, SMCreateAnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->urlLen));
  if (pReq->urlLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->url, pReq->urlLen));
  }
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateAnodeReq(void *buf, int32_t bufLen, SMCreateAnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->urlLen));
  if (pReq->urlLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->url, NULL));
  }

  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCreateAnodeReq(SMCreateAnodeReq *pReq) {
  taosMemoryFreeClear(pReq->url);
  FREESQL();
}

int32_t tSerializeSMDropAnodeReq(void *buf, int32_t bufLen, SMDropAnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->anodeId));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropAnodeReq(void *buf, int32_t bufLen, SMDropAnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->anodeId));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMDropAnodeReq(SMDropAnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSMUpdateAnodeReq(void *buf, int32_t bufLen, SMUpdateAnodeReq *pReq) {
  return tSerializeSMDropAnodeReq(buf, bufLen, pReq);
}

int32_t tDeserializeSMUpdateAnodeReq(void *buf, int32_t bufLen, SMUpdateAnodeReq *pReq) {
  return tDeserializeSMDropAnodeReq(buf, bufLen, pReq);
}

void tFreeSMUpdateAnodeReq(SMUpdateAnodeReq *pReq) { tFreeSMDropAnodeReq(pReq); }

int32_t tSerializeSMCreateBnodeReq(void *buf, int32_t bufLen, SMCreateBnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->bnodeProto));

  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMCreateBnodeReq(void *buf, int32_t bufLen, SMCreateBnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->bnodeProto));

  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMCreateBnodeReq(SMCreateBnodeReq *pReq) {
  // May free options
  FREESQL();
}

int32_t tSerializeSMDropBnodeReq(void *buf, int32_t bufLen, SMDropBnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropBnodeReq(void *buf, int32_t bufLen, SMDropBnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMDropBnodeReq(SMDropBnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSCreateDnodeReq(void *buf, int32_t bufLen, SCreateDnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->fqdn));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->port));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDnodeReq(void *buf, int32_t bufLen, SCreateDnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->fqdn));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->port));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCreateDnodeReq(SCreateDnodeReq *pReq) { FREESQL(); }

int32_t tSerializeSCreateFuncReq(void *buf, int32_t bufLen, SCreateFuncReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->funcType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->scriptType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->outputType));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->outputLen));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->bufSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->codeLen));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->signature));

  if (pReq->pCode != NULL) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->pCode, pReq->codeLen));
  }

  int32_t commentSize = 0;
  if (pReq->pComment != NULL) {
    commentSize = strlen(pReq->pComment) + 1;
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, commentSize));
  if (pReq->pComment != NULL) {
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->pComment));
  }

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->orReplace));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateFuncReq(void *buf, int32_t bufLen, SCreateFuncReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->funcType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->scriptType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->outputType));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->outputLen));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->bufSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->codeLen));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->signature));

  if (pReq->codeLen > 0) {
    pReq->pCode = taosMemoryCalloc(1, pReq->codeLen);
    if (pReq->pCode == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pCode));
  }

  int32_t commentSize = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &commentSize));
  if (commentSize > 0) {
    pReq->pComment = taosMemoryCalloc(1, commentSize);
    if (pReq->pComment == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->pComment));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->orReplace));
  } else {
    pReq->orReplace = false;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCreateFuncReq(SCreateFuncReq *pReq) {
  taosMemoryFree(pReq->pCode);
  taosMemoryFree(pReq->pComment);
}

int32_t tSerializeSDropFuncReq(void *buf, int32_t bufLen, SDropFuncReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropFuncReq(void *buf, int32_t bufLen, SDropFuncReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSRetrieveFuncReq(void *buf, int32_t bufLen, SRetrieveFuncReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfFuncs));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ignoreCodeComment));

  if (pReq->numOfFuncs != (int32_t)taosArrayGetSize(pReq->pFuncNames)) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char *fname = taosArrayGet(pReq->pFuncNames, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, fname));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveFuncReq(void *buf, int32_t bufLen, SRetrieveFuncReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfFuncs));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, (int8_t *)&pReq->ignoreCodeComment));

  pReq->pFuncNames = taosArrayInit(pReq->numOfFuncs, TSDB_FUNC_NAME_LEN);
  if (pReq->pFuncNames == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < pReq->numOfFuncs; ++i) {
    char fname[TSDB_FUNC_NAME_LEN] = {0};
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, fname));
    if (taosArrayPush(pReq->pFuncNames, fname) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSRetrieveFuncReq(SRetrieveFuncReq *pReq) { taosArrayDestroy(pReq->pFuncNames); }

int32_t tSerializeSRetrieveFuncRsp(void *buf, int32_t bufLen, SRetrieveFuncRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->numOfFuncs));

  if (pRsp->numOfFuncs != (int32_t)taosArrayGetSize(pRsp->pFuncInfos)) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
    SFuncInfo *pInfo = taosArrayGet(pRsp->pFuncInfos, i);

    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pInfo->name));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pInfo->funcType));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pInfo->scriptType));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pInfo->outputType));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->outputLen));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->bufSize));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pInfo->signature));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->codeSize));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pInfo->commentSize));
    if (pInfo->codeSize) {
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pInfo->pCode, pInfo->codeSize));
    }
    if (pInfo->commentSize) {
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pInfo->pComment));
    }
  }

  if (pRsp->numOfFuncs != (int32_t)taosArrayGetSize(pRsp->pFuncExtraInfos)) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
    SFuncExtraInfo *extraInfo = taosArrayGet(pRsp->pFuncExtraInfos, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, extraInfo->funcVersion));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, extraInfo->funcCreatedTime));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveFuncRsp(void *buf, int32_t bufLen, SRetrieveFuncRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->numOfFuncs));

  pRsp->pFuncInfos = taosArrayInit(pRsp->numOfFuncs, sizeof(SFuncInfo));
  if (pRsp->pFuncInfos == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
    SFuncInfo fInfo = {0};
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, fInfo.name));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &fInfo.funcType));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &fInfo.scriptType));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &fInfo.outputType));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &fInfo.outputLen));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &fInfo.bufSize));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &fInfo.signature));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &fInfo.codeSize));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &fInfo.commentSize));
    if (fInfo.codeSize) {
      fInfo.pCode = taosMemoryCalloc(1, fInfo.codeSize);
      if (fInfo.pCode == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, fInfo.pCode));
    }
    if (fInfo.commentSize) {
      fInfo.pComment = taosMemoryCalloc(1, fInfo.commentSize);
      if (fInfo.pComment == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, fInfo.pComment));
    }

    if (taosArrayPush(pRsp->pFuncInfos, &fInfo) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  pRsp->pFuncExtraInfos = taosArrayInit(pRsp->numOfFuncs, sizeof(SFuncExtraInfo));
  if (pRsp->pFuncExtraInfos == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if (tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
      SFuncExtraInfo extraInfo = {0};
      if (taosArrayPush(pRsp->pFuncExtraInfos, &extraInfo) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  } else {
    for (int32_t i = 0; i < pRsp->numOfFuncs; ++i) {
      SFuncExtraInfo extraInfo = {0};
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &extraInfo.funcVersion));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &extraInfo.funcCreatedTime));
      if (taosArrayPush(pRsp->pFuncExtraInfos, &extraInfo) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSFuncInfo(SFuncInfo *pInfo) {
  if (NULL == pInfo) {
    return;
  }

  taosMemoryFree(pInfo->pCode);
  taosMemoryFree(pInfo->pComment);
}

void tFreeSRetrieveFuncRsp(SRetrieveFuncRsp *pRsp) {
  int32_t size = taosArrayGetSize(pRsp->pFuncInfos);
  for (int32_t i = 0; i < size; ++i) {
    SFuncInfo *pInfo = taosArrayGet(pRsp->pFuncInfos, i);
    tFreeSFuncInfo(pInfo);
  }
  taosArrayDestroy(pRsp->pFuncInfos);
  taosArrayDestroy(pRsp->pFuncExtraInfos);
}

int32_t tSerializeSTableCfgReq(void *buf, int32_t bufLen, STableCfgReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->tbName));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSTableCfgReq(void *buf, int32_t bufLen, STableCfgReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);

  int32_t   code = 0;
  int32_t   lino;
  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->tbName));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTableCfgRsp(void *buf, int32_t bufLen, STableCfgRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->tbName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->stbName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->numOfTags));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->numOfColumns));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->tableType));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->delay1));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->delay2));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->watermark1));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->watermark2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->ttl));

  int32_t numOfFuncs = taosArrayGetSize(pRsp->pFuncs);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfFuncs));
  for (int32_t i = 0; i < numOfFuncs; ++i) {
    const char *pFunc = taosArrayGet(pRsp->pFuncs, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pFunc));
  }

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->commentLen));
  if (pRsp->commentLen > 0) {
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->pComment));
  }

  for (int32_t i = 0; i < pRsp->numOfColumns + pRsp->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    TAOS_CHECK_EXIT(tEncodeSSchema(&encoder, pSchema));
  }

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->tagsLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pRsp->pTags, pRsp->tagsLen));

  if (withExtSchema(pRsp->tableType)) {
    for (int32_t i = 0; i < pRsp->numOfColumns; ++i) {
      SSchemaExt *pSchemaExt = &pRsp->pSchemaExt[i];
      TAOS_CHECK_EXIT(tEncodeSSchemaExt(&encoder, pSchemaExt));
    }
  }

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->virtualStb));
  if (hasRefCol(pRsp->tableType)) {
    for (int32_t i = 0; i < pRsp->numOfColumns; ++i) {
      SColRef *pColRef = &pRsp->pColRefs[i];
      TAOS_CHECK_EXIT(tEncodeSColRef(&encoder, pColRef));
    }
  }

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->keep));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableCfgRsp(void *buf, int32_t bufLen, STableCfgRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->tbName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->stbName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->numOfTags));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->numOfColumns));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->tableType));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->delay1));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->delay2));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->watermark1));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->watermark2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->ttl));

  int32_t numOfFuncs = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfFuncs));
  if (numOfFuncs > 0) {
    pRsp->pFuncs = taosArrayInit(numOfFuncs, TSDB_FUNC_NAME_LEN);
    if (NULL == pRsp->pFuncs) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  for (int32_t i = 0; i < numOfFuncs; ++i) {
    char pFunc[TSDB_FUNC_NAME_LEN];
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pFunc));
    if (taosArrayPush(pRsp->pFuncs, pFunc) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->commentLen));
  if (pRsp->commentLen > 0) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pRsp->pComment));
  } else {
    pRsp->pComment = NULL;
  }

  int32_t totalCols = pRsp->numOfTags + pRsp->numOfColumns;
  pRsp->pSchemas = taosMemoryMalloc(sizeof(SSchema) * totalCols);
  if (pRsp->pSchemas == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    TAOS_CHECK_EXIT(tDecodeSSchema(&decoder, pSchema));
  }

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->tagsLen));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pRsp->pTags, NULL));

  if (!tDecodeIsEnd(&decoder)) {
    if (withExtSchema(pRsp->tableType) && pRsp->numOfColumns > 0) {
      pRsp->pSchemaExt = taosMemoryMalloc(sizeof(SSchemaExt) * pRsp->numOfColumns);
      if (pRsp->pSchemaExt == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }

      for (int32_t i = 0; i < pRsp->numOfColumns; ++i) {
        SSchemaExt *pSchemaExt = &pRsp->pSchemaExt[i];
        TAOS_CHECK_EXIT(tDecodeSSchemaExt(&decoder, pSchemaExt));
      }
    } else {
      pRsp->pSchemaExt = NULL;
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->virtualStb));
    if (hasRefCol(pRsp->tableType) && pRsp->numOfColumns > 0) {
      pRsp->pColRefs = taosMemoryMalloc(sizeof(SColRef) * pRsp->numOfColumns);
      if (pRsp->pColRefs == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }

      for (int32_t i = 0; i < pRsp->numOfColumns; ++i) {
        SColRef *pColRef = &pRsp->pColRefs[i];
        TAOS_CHECK_EXIT(tDecodeSColRef(&decoder, pColRef));
      }
    } else {
      pRsp->pColRefs = NULL;
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->keep));
  } else {
    pRsp->keep = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSTableCfgRsp(STableCfgRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosMemoryFreeClear(pRsp->pComment);
  taosMemoryFreeClear(pRsp->pSchemas);
  taosMemoryFreeClear(pRsp->pSchemaExt);
  taosMemoryFreeClear(pRsp->pColRefs);
  taosMemoryFreeClear(pRsp->pTags);

  taosArrayDestroy(pRsp->pFuncs);
}

int32_t tSerializeSCreateDbReq(void *buf, int32_t bufLen, SCreateDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfVgroups));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfStables));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->buffer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pageSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pages));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysPerFile));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->minRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->maxRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->walLevel));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->precision));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->compression));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->replications));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->strict));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->cacheLast));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->schemaless));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRetentionPeriod));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->walRetentionSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRollPeriod));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->walSegmentSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sstTrigger));
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->hashPrefix));
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->hashSuffix));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ignoreExist));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfRetensions));
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRetension->freq));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRetension->keep));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRetension->freqUnit));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRetension->keepUnit));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tsdbPageSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->keepTimeOffset));

  ENCODESQL();

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->withArbitrator));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->encryptAlgorithm));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssChunkSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssKeepLocal));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ssCompact));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dnodeListStr));

  // auto-compact parameters
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->compactInterval));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->compactStartTime));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->compactEndTime));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->compactTimeOffset));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateDbReq(void *buf, int32_t bufLen, SCreateDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfVgroups));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfStables));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->buffer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pageSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pages));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysPerFile));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->minRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->maxRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->walLevel));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->precision));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->compression));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->replications));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->strict));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->cacheLast));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->schemaless));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRetentionPeriod));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->walRetentionSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRollPeriod));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->walSegmentSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sstTrigger));
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->hashPrefix));
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->hashSuffix));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ignoreExist));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfRetensions));
  pReq->pRetensions = taosArrayInit(pReq->numOfRetensions, sizeof(SRetention));
  if (pReq->pRetensions == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention rentension = {0};
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &rentension.freq));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &rentension.keep));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &rentension.freqUnit));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &rentension.keepUnit));
    if (taosArrayPush(pReq->pRetensions, &rentension) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tsdbPageSize));

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->keepTimeOffset));
  } else {
    pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  }

  DECODESQL();

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->withArbitrator));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->encryptAlgorithm));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssChunkSize));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssKeepLocal));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ssCompact));
  } else {
    pReq->withArbitrator = TSDB_DEFAULT_DB_WITH_ARBITRATOR;
    pReq->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
    pReq->ssChunkSize = TSDB_DEFAULT_SS_CHUNK_SIZE;
    pReq->ssKeepLocal = TSDB_DEFAULT_SS_KEEP_LOCAL;
    pReq->ssCompact = TSDB_DEFAULT_SS_COMPACT;
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dnodeListStr));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->compactInterval));
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->compactStartTime));
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->compactEndTime));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->compactTimeOffset));
  } else {
    pReq->compactInterval = TSDB_DEFAULT_COMPACT_INTERVAL;
    pReq->compactStartTime = TSDB_DEFAULT_COMPACT_START_TIME;
    pReq->compactEndTime = TSDB_DEFAULT_COMPACT_END_TIME;
    pReq->compactTimeOffset = TSDB_DEFAULT_COMPACT_TIME_OFFSET;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCreateDbReq(SCreateDbReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
  FREESQL();
}

int32_t tSerializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->buffer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pageSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pages));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysPerFile));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->walLevel));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->strict));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->cacheLast));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->replications));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->sstTrigger));

  // 1st modification
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->minRows));
  // 2nd modification
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRetentionPeriod));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRetentionSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->keepTimeOffset));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssKeepLocal));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ssCompact));

  ENCODESQL();
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->withArbitrator));
  // auto compact config
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->compactInterval));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->compactStartTime));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->compactEndTime));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->compactTimeOffset));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterDbReq(void *buf, int32_t bufLen, SAlterDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->buffer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pageSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pages));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysPerFile));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->walLevel));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->strict));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->cacheLast));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->replications));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->sstTrigger));

  // 1st modification
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->minRows));
  } else {
    pReq->minRows = -1;
  }

  // 2nd modification
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRetentionPeriod));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRetentionSize));
  } else {
    pReq->walRetentionPeriod = -1;
    pReq->walRetentionSize = -1;
  }
  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->keepTimeOffset));
  }

  pReq->ssKeepLocal = TSDB_DEFAULT_SS_KEEP_LOCAL;
  pReq->ssCompact = TSDB_DEFAULT_SS_COMPACT;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssKeepLocal));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ssCompact));
  }

  DECODESQL();
  pReq->withArbitrator = TSDB_DEFAULT_DB_WITH_ARBITRATOR;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->withArbitrator));
  }

  // auto compact config
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->compactInterval));
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->compactStartTime));
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->compactEndTime));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->compactTimeOffset));
  } else {
    pReq->compactInterval = TSDB_DEFAULT_COMPACT_INTERVAL;
    pReq->compactStartTime = TSDB_DEFAULT_COMPACT_START_TIME;
    pReq->compactEndTime = TSDB_DEFAULT_COMPACT_END_TIME;
    pReq->compactTimeOffset = TSDB_DEFAULT_COMPACT_TIME_OFFSET;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSAlterDbReq(SAlterDbReq *pReq) { FREESQL(); }

int32_t tSerializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ignoreNotExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->force));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDbReq(void *buf, int32_t bufLen, SDropDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ignoreNotExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->force));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSDropDbReq(SDropDbReq *pReq) { FREESQL(); }

int32_t tSerializeSDropDbRsp(void *buf, int32_t bufLen, SDropDbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->db));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->uid));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropDbRsp(void *buf, int32_t bufLen, SDropDbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->db));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->uid));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->dbId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgVersion));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfTable));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->stateTs));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUseDbReq(void *buf, int32_t bufLen, SUseDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->dbId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgVersion));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfTable));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->stateTs));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVSubTablesReq(void *buf, int32_t bufLen, SVSubTablesReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->suid));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVSubTablesReq(void *buf, int32_t bufLen, SVSubTablesReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->suid));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVSubTablesRspImpl(SEncoder *pEncoder, SVSubTablesRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->vgId));
  int32_t numOfTables = taosArrayGetSize(pRsp->pTables);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, numOfTables));
  for (int32_t i = 0; i < numOfTables; ++i) {
    SVCTableRefCols *pTb = (SVCTableRefCols *)taosArrayGetP(pRsp->pTables, i);
    TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTb->uid));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTb->numOfSrcTbls));
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTb->numOfColRefs));
    for (int32_t n = 0; n < pTb->numOfColRefs; ++n) {
      SRefColInfo *pCol = pTb->refCols + n;
      TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pCol->colId));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pCol->refDbName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pCol->refTableName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pCol->refColName));
    }
  }

_exit:

  return code;
}

int32_t tSerializeSVSubTablesRsp(void *buf, int32_t bufLen, SVSubTablesRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tSerializeSVSubTablesRspImpl(&encoder, pRsp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVSubTablesRspImpl(SDecoder *pDecoder, SVSubTablesRsp *pRsp) {
  int32_t         code = 0;
  int32_t         lino;
  SVCTableRefCols tb = {0};
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->vgId));
  int32_t numOfTables = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &numOfTables));
  if (numOfTables > 0) {
    pRsp->pTables = taosArrayInit(numOfTables, POINTER_BYTES);
    if (NULL == pRsp->pTables) {
      code = terrno;
      return code;
    }

    for (int32_t i = 0; i < numOfTables; ++i) {
      TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &tb.uid));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &tb.numOfSrcTbls));
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &tb.numOfColRefs));
      if (tb.numOfColRefs > 0) {
        SVCTableRefCols *pTb = taosMemoryCalloc(1, sizeof(tb) + tb.numOfColRefs * sizeof(SRefColInfo));
        if (NULL == pTb) {
          code = terrno;
          return code;
        }
        if (NULL == taosArrayPush(pRsp->pTables, &pTb)) {
          code = terrno;
          taosMemoryFree(pTb);
          return code;
        }

        pTb->uid = tb.uid;
        pTb->numOfSrcTbls = tb.numOfSrcTbls;
        pTb->numOfColRefs = tb.numOfColRefs;
        pTb->refCols = (SRefColInfo *)(pTb + 1);
        for (int32_t n = 0; n < tb.numOfColRefs; ++n) {
          TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &pTb->refCols[n].colId));
          TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTb->refCols[n].refDbName));
          TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTb->refCols[n].refTableName));
          TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTb->refCols[n].refColName));
        }
      }
    }
  }

_exit:

  return code;
}

int32_t tDeserializeSVSubTablesRsp(void *buf, int32_t bufLen, SVSubTablesRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDeserializeSVSubTablesRspImpl(&decoder, pRsp));

  tEndDecode(&decoder);

_exit:

  tDecoderClear(&decoder);
  return code;
}

void tFreeSVCTableRefCols(void *pParam) {
  SVCTableRefCols *pCols = *(SVCTableRefCols **)pParam;
  if (NULL == pCols) {
    return;
  }

  taosMemoryFree(pCols);
}

void tDestroySVSubTablesRsp(void *rsp) {
  if (NULL == rsp) {
    return;
  }

  SVSubTablesRsp *pRsp = (SVSubTablesRsp *)rsp;

  taosArrayDestroyEx(pRsp->pTables, tFreeSVCTableRefCols);
}

int32_t tSerializeSVStbRefDbsReq(void *buf, int32_t bufLen, SVStbRefDbsReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->suid));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVStbRefDbsReq(void *buf, int32_t bufLen, SVStbRefDbsReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->suid));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVStbRefDbsRspImpl(SEncoder *pEncoder, SVStbRefDbsRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->vgId));
  int32_t numOfDbs = taosArrayGetSize(pRsp->pDbs);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, numOfDbs));
  for (int32_t i = 0; i < numOfDbs; ++i) {
    char *pDbName = (char *)taosArrayGetP(pRsp->pDbs, i);
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pDbName));
  }

_exit:

  return code;
}

int32_t tSerializeSVStbRefDbsRsp(void *buf, int32_t bufLen, SVStbRefDbsRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tSerializeSVStbRefDbsRspImpl(&encoder, pRsp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVStbRefDbsRspImpl(SDecoder *pDecoder, SVStbRefDbsRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->vgId));
  int32_t numOfDbs = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &numOfDbs));
  if (numOfDbs > 0) {
    pRsp->pDbs = taosArrayInit(numOfDbs, sizeof(void *));
    if (NULL == pRsp->pDbs) {
      code = terrno;
      return code;
    }

    for (int32_t i = 0; i < numOfDbs; ++i) {
      char *tbName;
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &tbName));
      if (taosArrayPush(pRsp->pDbs, &tbName) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

_exit:

  return code;
}

int32_t tDeserializeSVStbRefDbsRsp(void *buf, int32_t bufLen, SVStbRefDbsRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDeserializeSVStbRefDbsRspImpl(&decoder, pRsp));

  tEndDecode(&decoder);

_exit:

  tDecoderClear(&decoder);
  return code;
}

void tDestroySVStbRefDbsRsp(void *rsp) {
  if (NULL == rsp) {
    return;
  }

  SVStbRefDbsRsp *pRsp = (SVStbRefDbsRsp *)rsp;

  taosArrayDestroyP(pRsp->pDbs, NULL);
}

int32_t tSerializeSQnodeListReq(void *buf, int32_t bufLen, SQnodeListReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->rowNum));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeListReq(void *buf, int32_t bufLen, SQnodeListReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->rowNum));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSDnodeListReq(void *buf, int32_t bufLen, SDnodeListReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->rowNum));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSServerVerReq(void *buf, int32_t bufLen, SServerVerReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->useless));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSServerVerRsp(void *buf, int32_t bufLen, SServerVerRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->ver));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSServerVerRsp(void *buf, int32_t bufLen, SServerVerRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->ver));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  int32_t num = taosArrayGetSize(pRsp->qnodeList);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
  for (int32_t i = 0; i < num; ++i) {
    SQueryNodeLoad *pLoad = taosArrayGet(pRsp->qnodeList, i);
    TAOS_CHECK_EXIT(tEncodeSQueryNodeLoad(&encoder, pLoad));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeListRsp(void *buf, int32_t bufLen, SQnodeListRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (NULL == pRsp->qnodeList) {
    pRsp->qnodeList = taosArrayInit(num, sizeof(SQueryNodeLoad));
    if (NULL == pRsp->qnodeList) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  for (int32_t i = 0; i < num; ++i) {
    SQueryNodeLoad load = {0};
    TAOS_CHECK_EXIT(tDecodeSQueryNodeLoad(&decoder, &load));
    if (taosArrayPush(pRsp->qnodeList, &load) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSQnodeListRsp(SQnodeListRsp *pRsp) { taosArrayDestroy(pRsp->qnodeList); }

int32_t tSerializeSDnodeListRsp(void *buf, int32_t bufLen, SDnodeListRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  int32_t num = taosArrayGetSize(pRsp->dnodeList);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
  for (int32_t i = 0; i < num; ++i) {
    SDNodeAddr *pAddr = taosArrayGet(pRsp->dnodeList, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pAddr->nodeId));
    TAOS_CHECK_EXIT(tEncodeSEpSet(&encoder, &pAddr->epSet));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDnodeListRsp(void *buf, int32_t bufLen, SDnodeListRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (NULL == pRsp->dnodeList) {
    pRsp->dnodeList = taosArrayInit(num, sizeof(SDNodeAddr));
    if (NULL == pRsp->dnodeList) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  for (int32_t i = 0; i < num; ++i) {
    SDNodeAddr addr = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &addr.nodeId));
    TAOS_CHECK_EXIT(tDecodeSEpSet(&decoder, &addr.epSet));
    if (taosArrayPush(pRsp->dnodeList, &addr) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSDnodeListRsp(SDnodeListRsp *pRsp) { taosArrayDestroy(pRsp->dnodeList); }

int32_t tSerializeSCompactDbReq(void *buf, int32_t bufLen, SCompactDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timeRange.skey));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timeRange.ekey));
  ENCODESQL();

  // encode vgroup list
  int32_t numOfVgroups = taosArrayGetSize(pReq->vgroupIds);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfVgroups));
  if (numOfVgroups > 0) {
    for (int32_t i = 0; i < numOfVgroups; ++i) {
      int64_t vgid = *(int64_t *)taosArrayGet(pReq->vgroupIds, i);
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, vgid));
    }
  }

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->metaOnly));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactDbReq(void *buf, int32_t bufLen, SCompactDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timeRange.skey));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timeRange.ekey));
  DECODESQL();

  // decode vgroup list
  if (!tDecodeIsEnd(&decoder)) {
    int32_t numOfVgroups = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfVgroups));
    if (numOfVgroups > 0) {
      pReq->vgroupIds = taosArrayInit(numOfVgroups, sizeof(int64_t));
      if (NULL == pReq->vgroupIds) {
        TAOS_CHECK_EXIT(terrno);
      }

      for (int32_t i = 0; i < numOfVgroups; ++i) {
        int64_t vgid;
        TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &vgid));
        if (taosArrayPush(pReq->vgroupIds, &vgid) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->metaOnly));
  } else {
    pReq->metaOnly = false;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCompactDbReq(SCompactDbReq *pReq) {
  FREESQL();
  taosArrayDestroy(pReq->vgroupIds);
  pReq->vgroupIds = NULL;
}

int32_t tSerializeSCompactDbRsp(void *buf, int32_t bufLen, SCompactDbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->compactId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->bAccepted));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactDbRsp(void *buf, int32_t bufLen, SCompactDbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->compactId));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->bAccepted));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSKillCompactReq(void *buf, int32_t bufLen, SKillCompactReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->compactId));
  ENCODESQL();

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillCompactReq(void *buf, int32_t bufLen, SKillCompactReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->compactId));
  DECODESQL();

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSKillCompactReq(SKillCompactReq *pReq) { FREESQL(); }

int32_t tSerializeSUseDbRspImp(SEncoder *pEncoder, const SUseDbRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pRsp->db));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->uid));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->vgVersion));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->vgNum));
  TAOS_CHECK_RETURN(tEncodeI16(pEncoder, pRsp->hashPrefix));
  TAOS_CHECK_RETURN(tEncodeI16(pEncoder, pRsp->hashSuffix));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->hashMethod));

  for (int32_t i = 0; i < pRsp->vgNum; ++i) {
    SVgroupInfo *pVgInfo = taosArrayGet(pRsp->pVgroupInfos, i);
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pVgInfo->vgId));
    TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pVgInfo->hashBegin));
    TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pVgInfo->hashEnd));
    TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pVgInfo->epSet));
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pVgInfo->numOfTable));
  }

  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->errCode));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->stateTs));
  TAOS_CHECK_RETURN(tEncodeU8(pEncoder, pRsp->flags));
  return 0;
}

int32_t tSerializeSUseDbRsp(void *buf, int32_t bufLen, const SUseDbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tSerializeSUseDbRspImp(&encoder, pRsp));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSDbHbRspImp(SEncoder *pEncoder, const SDbHbRsp *pRsp) {
  if (pRsp->useDbRsp) {
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, 1));
    TAOS_CHECK_RETURN(tSerializeSUseDbRspImp(pEncoder, pRsp->useDbRsp));
  } else {
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, 0));
  }

  if (pRsp->cfgRsp) {
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, 1));
    TAOS_CHECK_RETURN(tSerializeSDbCfgRspImpl(pEncoder, pRsp->cfgRsp));
  } else {
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, 0));
  }

  if (pRsp->pTsmaRsp) {
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, 1));
    TAOS_CHECK_RETURN(tEncodeTableTSMAInfoRsp(pEncoder, pRsp->pTsmaRsp));
  } else {
    TAOS_CHECK_RETURN(tEncodeI8(pEncoder, 0));
  }
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->dbTsmaVersion));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pRsp->db));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->dbId));
  return 0;
}

int32_t tSerializeSDbHbBatchRsp(void *buf, int32_t bufLen, SDbHbBatchRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfBatch));
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SDbHbRsp *pDbRsp = taosArrayGet(pRsp->pArray, i);
    TAOS_CHECK_EXIT(tSerializeSDbHbRspImp(&encoder, pDbRsp));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUseDbRspImp(SDecoder *pDecoder, SUseDbRsp *pRsp) {
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pRsp->db));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pRsp->uid));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->vgVersion));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->vgNum));
  TAOS_CHECK_RETURN(tDecodeI16(pDecoder, &pRsp->hashPrefix));
  TAOS_CHECK_RETURN(tDecodeI16(pDecoder, &pRsp->hashSuffix));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pRsp->hashMethod));

  if (pRsp->vgNum > 0) {
    pRsp->pVgroupInfos = taosArrayInit(pRsp->vgNum, sizeof(SVgroupInfo));
    if (pRsp->pVgroupInfos == NULL) {
      TAOS_CHECK_RETURN(terrno);
    }

    for (int32_t i = 0; i < pRsp->vgNum; ++i) {
      SVgroupInfo vgInfo = {0};
      TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &vgInfo.vgId));
      TAOS_CHECK_RETURN(tDecodeU32(pDecoder, &vgInfo.hashBegin));
      TAOS_CHECK_RETURN(tDecodeU32(pDecoder, &vgInfo.hashEnd));
      TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &vgInfo.epSet));
      TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &vgInfo.numOfTable));
      if (taosArrayPush(pRsp->pVgroupInfos, &vgInfo) == NULL) {
        TAOS_CHECK_RETURN(terrno);
      }
    }
  }

  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->errCode));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pRsp->stateTs));
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeU8(pDecoder, &pRsp->flags));
  }
  return 0;
}

int32_t tDeserializeSUseDbRsp(void *buf, int32_t bufLen, SUseDbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDeserializeSUseDbRspImp(&decoder, pRsp));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tDeserializeSDbHbRspImp(SDecoder *decoder, SDbHbRsp *pRsp) {
  int8_t flag = 0;
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &flag));
  if (flag) {
    pRsp->useDbRsp = taosMemoryCalloc(1, sizeof(SUseDbRsp));
    if (NULL == pRsp->useDbRsp) {
      TAOS_CHECK_RETURN(terrno);
    }
    TAOS_CHECK_RETURN(tDeserializeSUseDbRspImp(decoder, pRsp->useDbRsp));
  }
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &flag));
  if (flag) {
    pRsp->cfgRsp = taosMemoryCalloc(1, sizeof(SDbCfgRsp));
    if (NULL == pRsp->cfgRsp) {
      TAOS_CHECK_RETURN(terrno);
    }
    TAOS_CHECK_RETURN(tDeserializeSDbCfgRspImpl(decoder, pRsp->cfgRsp));
  }
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &flag));
    if (flag) {
      pRsp->pTsmaRsp = taosMemoryCalloc(1, sizeof(STableTSMAInfoRsp));
      if (!pRsp->pTsmaRsp) {
        TAOS_CHECK_RETURN(terrno);
      }
      TAOS_CHECK_RETURN(tDecodeTableTSMAInfoRsp(decoder, pRsp->pTsmaRsp));
    }
  }
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->dbTsmaVersion));
  }
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeCStrTo(decoder, pRsp->db));
    TAOS_CHECK_RETURN(tDecodeI64(decoder, &pRsp->dbId));
  }

  return 0;
}

int32_t tDeserializeSDbHbBatchRsp(void *buf, int32_t bufLen, SDbHbBatchRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfBatch));

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(SDbHbRsp));
  if (pRsp->pArray == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < numOfBatch; ++i) {
    SDbHbRsp rsp = {0};
    TAOS_CHECK_EXIT(tDeserializeSDbHbRspImp(&decoder, &rsp));

    if (taosArrayPush(pRsp->pArray, &rsp) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSUsedbRsp(SUseDbRsp *pRsp) { taosArrayDestroy(pRsp->pVgroupInfos); }

void tFreeSDbHbRsp(SDbHbRsp *pDbRsp) {
  if (NULL == pDbRsp) {
    return;
  }

  if (pDbRsp->useDbRsp) {
    tFreeSUsedbRsp(pDbRsp->useDbRsp);
    taosMemoryFree(pDbRsp->useDbRsp);
  }

  if (pDbRsp->cfgRsp) {
    tFreeSDbCfgRsp(pDbRsp->cfgRsp);
    taosMemoryFree(pDbRsp->cfgRsp);
  }
  if (pDbRsp->pTsmaRsp) {
    tFreeTableTSMAInfoRsp(pDbRsp->pTsmaRsp);
    taosMemoryFree(pDbRsp->pTsmaRsp);
  }
}

void tFreeSDbHbBatchRsp(SDbHbBatchRsp *pRsp) {
  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SDbHbRsp *pDbRsp = taosArrayGet(pRsp->pArray, i);
    tFreeSDbHbRsp(pDbRsp);
  }

  taosArrayDestroy(pRsp->pArray);
}

int32_t tSerializeSUserAuthBatchRsp(void *buf, int32_t bufLen, SUserAuthBatchRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfBatch));
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SGetUserAuthRsp *pUserAuthRsp = taosArrayGet(pRsp->pArray, i);
    TAOS_CHECK_EXIT(tSerializeSGetUserAuthRspImpl(&encoder, pUserAuthRsp));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserAuthBatchRsp(void *buf, int32_t bufLen, SUserAuthBatchRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfBatch));

  pRsp->pArray = taosArrayInit(numOfBatch, sizeof(SGetUserAuthRsp));
  if (pRsp->pArray == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < numOfBatch; ++i) {
    SGetUserAuthRsp rsp = {0};
    TAOS_CHECK_EXIT(tDeserializeSGetUserAuthRspImpl(&decoder, &rsp));
    if (taosArrayPush(pRsp->pArray, &rsp) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSUserAuthBatchRsp(SUserAuthBatchRsp *pRsp) {
  int32_t numOfBatch = taosArrayGetSize(pRsp->pArray);
  for (int32_t i = 0; i < numOfBatch; ++i) {
    SGetUserAuthRsp *pUserAuthRsp = taosArrayGet(pRsp->pArray, i);
    tFreeSGetUserAuthRsp(pUserAuthRsp);
  }

  taosArrayDestroy(pRsp->pArray);
}

int32_t tSerializeSDbCfgReq(void *buf, int32_t bufLen, SDbCfgReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgReq(void *buf, int32_t bufLen, SDbCfgReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTrimDbReq(void *buf, int32_t bufLen, STrimDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->maxSpeed));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTrimDbReq(void *buf, int32_t bufLen, STrimDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->maxSpeed));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVTrimDbReq(void *buf, int32_t bufLen, SVTrimDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->timestamp));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVTrimDbReq(void *buf, int32_t bufLen, SVTrimDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->timestamp));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSSsMigrateDbReq(void *buf, int32_t bufLen, SSsMigrateDbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSsMigrateDbReq(void *buf, int32_t bufLen, SSsMigrateDbReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSSsMigrateDbRsp(void *buf, int32_t bufLen, SSsMigrateDbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->ssMigrateId));
  TAOS_CHECK_EXIT(tEncodeBool(&encoder, pRsp->bAccepted));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSsMigrateDbRsp(void *buf, int32_t bufLen, SSsMigrateDbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->ssMigrateId));
  TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pRsp->bAccepted));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSSsMigrateVgroupReq(void *buf, int32_t bufLen, SSsMigrateVgroupReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssMigrateId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->nodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timestamp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSsMigrateVgroupReq(void *buf, int32_t bufLen, SSsMigrateVgroupReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssMigrateId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->nodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timestamp));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSSsMigrateVgroupRsp(void *buf, int32_t bufLen, SSsMigrateVgroupRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->ssMigrateId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->nodeId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSsMigrateVgroupRsp(void *buf, int32_t bufLen, SSsMigrateVgroupRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->ssMigrateId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->nodeId));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}



int32_t tSerializeSQuerySsMigrateProgressReq(void* buf, int32_t bufLen, SQuerySsMigrateProgressReq* pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssMigrateId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  //TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timestamp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQuerySsMigrateProgressReq(void* buf, int32_t bufLen, SQuerySsMigrateProgressReq* pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssMigrateId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  //TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timestamp));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVnodeSsMigrateState(void* buf, int32_t bufLen, SVnodeSsMigrateState* pState) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pState->mnodeMigrateId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pState->vnodeMigrateId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pState->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pState->vgId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pState->startTimeSec));

  int32_t numFs = taosArrayGetSize(pState->pFileSetStates);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numFs));
  for (int32_t i = 0; i < numFs; ++i) {
    SFileSetSsMigrateState *fs = taosArrayGet(pState->pFileSetStates, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, fs->fid));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, fs->state));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVnodeSsMigrateState(void* buf, int32_t bufLen, SVnodeSsMigrateState* pState) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t numFs = 0;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pState->mnodeMigrateId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pState->vnodeMigrateId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pState->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pState->vgId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pState->startTimeSec));

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numFs));
  if (numFs > 0) {
    pState->pFileSetStates = taosArrayInit(numFs, sizeof(SFileSetSsMigrateState));
    if (pState->pFileSetStates == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  for (int32_t i = 0; i < numFs; ++i) {
    SFileSetSsMigrateState state = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &state.fid));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &state.state));
    taosArrayPush(pState->pFileSetStates, &state);
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVnodeSsMigrateState(SVnodeSsMigrateState* pState) {
  if (pState->pFileSetStates) {
    taosArrayDestroy(pState->pFileSetStates);
    pState->pFileSetStates = NULL;
  }
}

int32_t tSerializeSVDropTtlTableReq(void *buf, int32_t bufLen, SVDropTtlTableReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->timestampSec));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ttlDropMaxCount));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->nUids));
  for (int32_t i = 0; i < pReq->nUids; ++i) {
    tb_uid_t *pTbUid = taosArrayGet(pReq->pTbUids, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, *pTbUid));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVDropTtlTableReq(void *buf, int32_t bufLen, SVDropTtlTableReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->timestampSec));
  pReq->ttlDropMaxCount = INT32_MAX;
  pReq->nUids = 0;
  pReq->pTbUids = NULL;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ttlDropMaxCount));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->nUids));

    if (pReq->nUids > 0) {
      pReq->pTbUids = taosArrayInit(pReq->nUids, sizeof(tb_uid_t));
      if (pReq->pTbUids == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }

    tb_uid_t tbUid = 0;
    for (int32_t i = 0; i < pReq->nUids; ++i) {
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &tbUid));
      if (taosArrayPush(pReq->pTbUids, &tbUid) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSDbCfgRspImpl(SEncoder *encoder, const SDbCfgRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeCStr(encoder, pRsp->db));
  TAOS_CHECK_RETURN(tEncodeI64(encoder, pRsp->dbId));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->cfgVersion));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->numOfVgroups));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->numOfStables));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->buffer));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->cacheSize));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->pageSize));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->pages));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->daysPerFile));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->daysToKeep0));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->daysToKeep1));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->daysToKeep2));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->minRows));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->maxRows));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->walFsyncPeriod));
  TAOS_CHECK_RETURN(tEncodeI16(encoder, pRsp->hashPrefix));
  TAOS_CHECK_RETURN(tEncodeI16(encoder, pRsp->hashSuffix));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->walLevel));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->precision));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->compression));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->replications));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->strict));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->cacheLast));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->tsdbPageSize));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->walRetentionPeriod));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->walRollPeriod));
  TAOS_CHECK_RETURN(tEncodeI64(encoder, pRsp->walRetentionSize));
  TAOS_CHECK_RETURN(tEncodeI64(encoder, pRsp->walSegmentSize));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->numOfRetensions));
  for (int32_t i = 0; i < pRsp->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pRsp->pRetensions, i);
    TAOS_CHECK_RETURN(tEncodeI64(encoder, pRetension->freq));
    TAOS_CHECK_RETURN(tEncodeI64(encoder, pRetension->keep));
    TAOS_CHECK_RETURN(tEncodeI8(encoder, pRetension->freqUnit));
    TAOS_CHECK_RETURN(tEncodeI8(encoder, pRetension->keepUnit));
  }
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->schemaless));
  TAOS_CHECK_RETURN(tEncodeI16(encoder, pRsp->sstTrigger));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->keepTimeOffset));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->withArbitrator));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->encryptAlgorithm));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->ssChunkSize));
  TAOS_CHECK_RETURN(tEncodeI32(encoder, pRsp->ssKeepLocal));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->ssCompact));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->hashMethod));
  TAOS_CHECK_RETURN(tEncodeI32v(encoder, pRsp->compactInterval));
  TAOS_CHECK_RETURN(tEncodeI32v(encoder, pRsp->compactStartTime));
  TAOS_CHECK_RETURN(tEncodeI32v(encoder, pRsp->compactEndTime));
  TAOS_CHECK_RETURN(tEncodeI8(encoder, pRsp->compactTimeOffset));
  TAOS_CHECK_RETURN(tEncodeU8(encoder, pRsp->flags));

  return 0;
}

int32_t tSerializeSDbCfgRsp(void *buf, int32_t bufLen, const SDbCfgRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tSerializeSDbCfgRspImpl(&encoder, pRsp));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDbCfgRspImpl(SDecoder *decoder, SDbCfgRsp *pRsp) {
  TAOS_CHECK_RETURN(tDecodeCStrTo(decoder, pRsp->db));
  TAOS_CHECK_RETURN(tDecodeI64(decoder, &pRsp->dbId));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->cfgVersion));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->numOfVgroups));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->numOfStables));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->buffer));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->cacheSize));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->pageSize));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->pages));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->daysPerFile));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->daysToKeep0));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->daysToKeep1));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->daysToKeep2));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->minRows));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->maxRows));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->walFsyncPeriod));
  TAOS_CHECK_RETURN(tDecodeI16(decoder, &pRsp->hashPrefix));
  TAOS_CHECK_RETURN(tDecodeI16(decoder, &pRsp->hashSuffix));
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->walLevel));
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->precision));
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->compression));
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->replications));
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->strict));
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->cacheLast));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->tsdbPageSize));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->walRetentionPeriod));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->walRollPeriod));
  TAOS_CHECK_RETURN(tDecodeI64(decoder, &pRsp->walRetentionSize));
  TAOS_CHECK_RETURN(tDecodeI64(decoder, &pRsp->walSegmentSize));
  TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->numOfRetensions));
  if (pRsp->numOfRetensions > 0) {
    pRsp->pRetensions = taosArrayInit(pRsp->numOfRetensions, sizeof(SRetention));
    if (pRsp->pRetensions == NULL) {
      TAOS_CHECK_RETURN(terrno);
    }
  }

  for (int32_t i = 0; i < pRsp->numOfRetensions; ++i) {
    SRetention rentension = {0};
    TAOS_CHECK_RETURN(tDecodeI64(decoder, &rentension.freq));
    TAOS_CHECK_RETURN(tDecodeI64(decoder, &rentension.keep));
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &rentension.freqUnit));
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &rentension.keepUnit));
    if (taosArrayPush(pRsp->pRetensions, &rentension) == NULL) {
      TAOS_CHECK_RETURN(terrno);
    }
  }
  TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->schemaless));
  TAOS_CHECK_RETURN(tDecodeI16(decoder, &pRsp->sstTrigger));
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->keepTimeOffset));
  } else {
    pRsp->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  }
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->withArbitrator));
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->encryptAlgorithm));
    TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->ssChunkSize));
    TAOS_CHECK_RETURN(tDecodeI32(decoder, &pRsp->ssKeepLocal));
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->ssCompact));
  } else {
    pRsp->withArbitrator = TSDB_DEFAULT_DB_WITH_ARBITRATOR;
    pRsp->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
    pRsp->ssChunkSize = TSDB_DEFAULT_SS_CHUNK_SIZE;
    pRsp->ssKeepLocal = TSDB_DEFAULT_SS_KEEP_LOCAL;
    pRsp->ssCompact = TSDB_DEFAULT_SS_COMPACT;
  }
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->hashMethod));
  } else {
    pRsp->hashMethod = 1;  // default value
  }
  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeI32v(decoder, &pRsp->compactInterval));
    TAOS_CHECK_RETURN(tDecodeI32v(decoder, &pRsp->compactStartTime));
    TAOS_CHECK_RETURN(tDecodeI32v(decoder, &pRsp->compactEndTime));
    TAOS_CHECK_RETURN(tDecodeI8(decoder, &pRsp->compactTimeOffset));
  } else {
    pRsp->compactInterval = TSDB_DEFAULT_COMPACT_INTERVAL;
    pRsp->compactStartTime = TSDB_DEFAULT_COMPACT_START_TIME;
    pRsp->compactEndTime = TSDB_DEFAULT_COMPACT_END_TIME;
    pRsp->compactTimeOffset = TSDB_DEFAULT_COMPACT_TIME_OFFSET;
  }

  if (!tDecodeIsEnd(decoder)) {
    TAOS_CHECK_RETURN(tDecodeU8(decoder, &pRsp->flags));
  }

  return 0;
}

int32_t tDeserializeSDbCfgRsp(void *buf, int32_t bufLen, SDbCfgRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDeserializeSDbCfgRspImpl(&decoder, pRsp));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSDbCfgRsp(SDbCfgRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosArrayDestroy(pRsp->pRetensions);
}

int32_t tSerializeSUserIndexReq(void *buf, int32_t bufLen, SUserIndexReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->indexFName));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserIndexReq(void *buf, int32_t bufLen, SUserIndexReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->indexFName));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSUserIndexRsp(void *buf, int32_t bufLen, const SUserIndexRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->tblFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->colName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->indexType));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->indexExts));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSUserIndexRsp(void *buf, int32_t bufLen, SUserIndexRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->tblFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->colName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->indexType));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->indexExts));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTableIndexReq(void *buf, int32_t bufLen, STableIndexReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->tbFName));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableIndexReq(void *buf, int32_t bufLen, STableIndexReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->tbFName));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTableIndexInfo(SEncoder *pEncoder, STableIndexInfo *pInfo) {
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pInfo->intervalUnit));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pInfo->slidingUnit));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pInfo->interval));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pInfo->offset));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pInfo->sliding));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pInfo->dstTbUid));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pInfo->dstVgId));
  TAOS_CHECK_RETURN(tEncodeSEpSet(pEncoder, &pInfo->epSet));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pInfo->expr));
  return 0;
}

int32_t tSerializeSTableIndexRsp(void *buf, int32_t bufLen, const STableIndexRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->tbName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pRsp->suid));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->version));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->indexSize));
  int32_t num = taosArrayGetSize(pRsp->pIndex);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
  if (num > 0) {
    for (int32_t i = 0; i < num; ++i) {
      STableIndexInfo *pInfo = (STableIndexInfo *)taosArrayGet(pRsp->pIndex, i);
      TAOS_CHECK_EXIT(tSerializeSTableIndexInfo(&encoder, pInfo));
    }
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

void tFreeSerializeSTableIndexRsp(STableIndexRsp *pRsp) {
  if (pRsp->pIndex != NULL) {
    tFreeSTableIndexRsp(pRsp);
    pRsp->pIndex = NULL;
  }
}

int32_t tDeserializeSTableIndexInfo(SDecoder *pDecoder, STableIndexInfo *pInfo) {
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pInfo->intervalUnit));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pInfo->slidingUnit));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pInfo->interval));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pInfo->offset));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pInfo->sliding));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pInfo->dstTbUid));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pInfo->dstVgId));
  TAOS_CHECK_RETURN(tDecodeSEpSet(pDecoder, &pInfo->epSet));
  TAOS_CHECK_RETURN(tDecodeCStrAlloc(pDecoder, &pInfo->expr));
  return 0;
}

int32_t tDeserializeSTableIndexRsp(void *buf, int32_t bufLen, STableIndexRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->tbName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pRsp->suid));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->version));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->indexSize));
  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (num > 0) {
    pRsp->pIndex = taosArrayInit(num, sizeof(STableIndexInfo));
    if (NULL == pRsp->pIndex) {
      TAOS_CHECK_EXIT(terrno);
    }
    STableIndexInfo info;
    for (int32_t i = 0; i < num; ++i) {
      TAOS_CHECK_EXIT(tDeserializeSTableIndexInfo(&decoder, &info));
      if (NULL == taosArrayPush(pRsp->pIndex, &info)) {
        taosMemoryFree(info.expr);
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSTableIndexInfo(void *info) {
  if (NULL == info) {
    return;
  }

  STableIndexInfo *pInfo = (STableIndexInfo *)info;

  taosMemoryFree(pInfo->expr);
}

int32_t tSerializeSShowVariablesReq(void *buf, int32_t bufLen, SShowVariablesReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->opType));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->valLen));
  if (pReq->valLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->val, pReq->valLen));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowVariablesReq(void *buf, int32_t bufLen, SShowVariablesReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->opType));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->valLen));

  if (pReq->valLen > 0) {
    pReq->val = taosMemoryCalloc(1, pReq->valLen + 1);
    if (pReq->val == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->val));
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSShowVariablesReq(SShowVariablesReq *pReq) {
  if (NULL != pReq && NULL != pReq->val) {
    taosMemoryFree(pReq->val);
    pReq->val = NULL;
  }
}

int32_t tEncodeSVariablesInfo(SEncoder *pEncoder, SVariablesInfo *pInfo) {
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pInfo->name));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pInfo->value));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pInfo->scope));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pInfo->category));
  return 0;
}

int32_t tDecodeSVariablesInfo(SDecoder *pDecoder, SVariablesInfo *pInfo) {
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pInfo->name));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pInfo->value));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pInfo->scope));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pInfo->category));
  return 0;
}

int32_t tSerializeSShowVariablesRsp(void *buf, int32_t bufLen, SShowVariablesRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  int32_t varNum = taosArrayGetSize(pRsp->variables);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, varNum));
  for (int32_t i = 0; i < varNum; ++i) {
    SVariablesInfo *pInfo = taosArrayGet(pRsp->variables, i);
    TAOS_CHECK_EXIT(tEncodeSVariablesInfo(&encoder, pInfo));
  }

  for (int32_t i = 0; i < varNum; ++i) {
    SVariablesInfo *pInfo = taosArrayGet(pRsp->variables, i);
    TAOS_CHECK_RETURN(tEncodeCStr(&encoder, pInfo->info));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSShowVariablesRsp(void *buf, int32_t bufLen, SShowVariablesRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  int32_t varNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &varNum));
  if (varNum > 0) {
    pRsp->variables = taosArrayInit(varNum, sizeof(SVariablesInfo));
    if (NULL == pRsp->variables) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < varNum; ++i) {
      SVariablesInfo info = {0};
      TAOS_CHECK_EXIT(tDecodeSVariablesInfo(&decoder, &info));
      if (NULL == taosArrayPush(pRsp->variables, &info)) {
        TAOS_CHECK_EXIT(terrno);
      }
    }

    if (!tDecodeIsEnd(&decoder)) {
      for (int32_t i = 0; i < varNum; ++i) {
        SVariablesInfo *pInfo = taosArrayGet(pRsp->variables, i);
        TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pInfo->info));
      }
    }
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSShowVariablesRsp(SShowVariablesRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosArrayDestroy(pRsp->variables);
}

int32_t tSerializeSShowReq(void *buf, int32_t bufLen, SShowReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->payloadLen));
  if (pReq->payloadLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->payload, pReq->payloadLen));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

void tFreeSShowReq(SShowReq *pReq) { taosMemoryFreeClear(pReq->payload); }

int32_t tSerializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->showId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->tb));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->filterTb));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->compactId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->withFull));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveTableReq(void *buf, int32_t bufLen, SRetrieveTableReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->showId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->tb));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->filterTb));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->compactId));
  } else {
    pReq->compactId = -1;
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, (int8_t *)&pReq->withFull));
  }
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t tEncodeSTableMetaRsp(SEncoder *pEncoder, STableMetaRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pRsp->tbName));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pRsp->stbName));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pRsp->dbFName));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->dbId));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->numOfTags));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->numOfColumns));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->precision));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->tableType));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->sversion));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->tversion));
  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pRsp->suid));
  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pRsp->tuid));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->vgId));
  for (int32_t i = 0; i < pRsp->numOfColumns + pRsp->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    TAOS_CHECK_RETURN(tEncodeSSchema(pEncoder, pSchema));
  }

  if (withExtSchema(pRsp->tableType)) {
    for (int32_t i = 0; i < pRsp->numOfColumns; ++i) {
      SSchemaExt *pSchemaExt = &pRsp->pSchemaExt[i];
      TAOS_CHECK_RETURN(tEncodeSSchemaExt(pEncoder, pSchemaExt));
    }
  }

  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pRsp->virtualStb));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->numOfColRefs));
  if (hasRefCol(pRsp->tableType)) {
    for (int32_t i = 0; i < pRsp->numOfColRefs; ++i) {
      SColRef *pColRef = &pRsp->pColRefs[i];
      TAOS_CHECK_RETURN(tEncodeSColRef(pEncoder, pColRef));
    }
  }
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pRsp->rversion));

  return 0;
}

static int32_t tDecodeSTableMetaRsp(SDecoder *pDecoder, STableMetaRsp *pRsp) {
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pRsp->tbName));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pRsp->stbName));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pRsp->dbFName));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pRsp->dbId));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->numOfTags));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->numOfColumns));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pRsp->precision));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pRsp->tableType));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->sversion));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->tversion));
  TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pRsp->suid));
  TAOS_CHECK_RETURN(tDecodeU64(pDecoder, &pRsp->tuid));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->vgId));

  int32_t totalCols = pRsp->numOfTags + pRsp->numOfColumns;
  if (totalCols > 0) {
    pRsp->pSchemas = taosMemoryMalloc(sizeof(SSchema) * totalCols);
    if (pRsp->pSchemas == NULL) {
      TAOS_CHECK_RETURN(terrno);
    }

    for (int32_t i = 0; i < totalCols; ++i) {
      SSchema *pSchema = &pRsp->pSchemas[i];
      TAOS_CHECK_RETURN(tDecodeSSchema(pDecoder, pSchema));
    }
  } else {
    pRsp->pSchemas = NULL;
  }

  if (!tDecodeIsEnd(pDecoder)) {
    if (withExtSchema(pRsp->tableType) && pRsp->numOfColumns > 0) {
      pRsp->pSchemaExt = taosMemoryMalloc(sizeof(SSchemaExt) * pRsp->numOfColumns);
      if (pRsp->pSchemaExt == NULL) {
        TAOS_CHECK_RETURN(terrno);
      }

      for (int32_t i = 0; i < pRsp->numOfColumns; ++i) {
        SSchemaExt *pSchemaExt = &pRsp->pSchemaExt[i];
        TAOS_CHECK_RETURN(tDecodeSSchemaExt(pDecoder, pSchemaExt));
      }
    } else {
      pRsp->pSchemaExt = NULL;
    }
  }
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pRsp->virtualStb));
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->numOfColRefs));
    if (hasRefCol(pRsp->tableType) && pRsp->numOfColRefs > 0) {
      pRsp->pColRefs = taosMemoryMalloc(sizeof(SColRef) * pRsp->numOfColRefs);
      if (pRsp->pColRefs == NULL) {
        TAOS_CHECK_RETURN(terrno);
      }

      for (int32_t i = 0; i < pRsp->numOfColRefs; ++i) {
        SColRef *pColRef = &pRsp->pColRefs[i];
        TAOS_CHECK_RETURN(tDecodeSColRef(pDecoder, pColRef));
      }
    } else {
      pRsp->pColRefs = NULL;
    }
  }
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pRsp->rversion));
  }

  return 0;
}

int32_t tSerializeSTableMetaRsp(void *buf, int32_t bufLen, STableMetaRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeSTableMetaRsp(&encoder, pRsp));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSSTbHbRsp(void *buf, int32_t bufLen, SSTbHbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t numOfMeta = taosArrayGetSize(pRsp->pMetaRsp);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfMeta));
  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pMetaRsp, i);
    TAOS_CHECK_EXIT(tEncodeSTableMetaRsp(&encoder, pMetaRsp));
  }

  int32_t numOfIndex = taosArrayGetSize(pRsp->pIndexRsp);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfIndex));
  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp *pIndexRsp = taosArrayGet(pRsp->pIndexRsp, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pIndexRsp->tbName));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pIndexRsp->dbFName));
    TAOS_CHECK_EXIT(tEncodeU64(&encoder, pIndexRsp->suid));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pIndexRsp->version));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pIndexRsp->indexSize));
    int32_t num = taosArrayGetSize(pIndexRsp->pIndex);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
    for (int32_t j = 0; j < num; ++j) {
      STableIndexInfo *pInfo = (STableIndexInfo *)taosArrayGet(pIndexRsp->pIndex, j);
      TAOS_CHECK_EXIT(tSerializeSTableIndexInfo(&encoder, pInfo));
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSTableMetaRsp(void *buf, int32_t bufLen, STableMetaRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeSTableMetaRsp(&decoder, pRsp));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tDeserializeSSTbHbRsp(void *buf, int32_t bufLen, SSTbHbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t numOfMeta = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfMeta));
  pRsp->pMetaRsp = taosArrayInit(numOfMeta, sizeof(STableMetaRsp));
  if (pRsp->pMetaRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp tableMetaRsp = {0};
    TAOS_CHECK_EXIT(tDecodeSTableMetaRsp(&decoder, &tableMetaRsp));
    if (taosArrayPush(pRsp->pMetaRsp, &tableMetaRsp) == NULL) {
      taosMemoryFree(tableMetaRsp.pSchemas);
      taosMemoryFree(tableMetaRsp.pSchemaExt);
      taosMemoryFree(tableMetaRsp.pColRefs);
      TAOS_CHECK_EXIT(terrno);
    }
  }

  int32_t numOfIndex = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfIndex));

  pRsp->pIndexRsp = taosArrayInit(numOfIndex, sizeof(STableIndexRsp));
  if (pRsp->pIndexRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp tableIndexRsp = {0};
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, tableIndexRsp.tbName));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, tableIndexRsp.dbFName));
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &tableIndexRsp.suid));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &tableIndexRsp.version));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &tableIndexRsp.indexSize));
    int32_t num = 0;
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
    if (num > 0) {
      tableIndexRsp.pIndex = taosArrayInit(num, sizeof(STableIndexInfo));
      if (NULL == tableIndexRsp.pIndex) {
        TAOS_CHECK_EXIT(terrno);
      }
      STableIndexInfo info;
      for (int32_t j = 0; j < num; ++j) {
        TAOS_CHECK_EXIT(tDeserializeSTableIndexInfo(&decoder, &info));
        if (NULL == taosArrayPush(tableIndexRsp.pIndex, &info)) {
          taosMemoryFree(info.expr);
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
    if (taosArrayPush(pRsp->pIndexRsp, &tableIndexRsp) == NULL) {
      taosArrayDestroyEx(tableIndexRsp.pIndex, tFreeSTableIndexInfo);
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSTableMetaRsp(void *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosMemoryFreeClear(((STableMetaRsp *)pRsp)->pSchemas);
  taosMemoryFreeClear(((STableMetaRsp *)pRsp)->pSchemaExt);
  taosMemoryFreeClear(((STableMetaRsp *)pRsp)->pColRefs);
}

void tFreeSTableIndexRsp(void *info) {
  if (NULL == info) {
    return;
  }

  STableIndexRsp *pInfo = (STableIndexRsp *)info;

  taosArrayDestroyEx(pInfo->pIndex, tFreeSTableIndexInfo);
}

void tFreeSSTbHbRsp(SSTbHbRsp *pRsp) {
  int32_t numOfMeta = taosArrayGetSize(pRsp->pMetaRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    STableMetaRsp *pMetaRsp = taosArrayGet(pRsp->pMetaRsp, i);
    tFreeSTableMetaRsp(pMetaRsp);
  }

  taosArrayDestroy(pRsp->pMetaRsp);

  int32_t numOfIndex = taosArrayGetSize(pRsp->pIndexRsp);
  for (int32_t i = 0; i < numOfIndex; ++i) {
    STableIndexRsp *pIndexRsp = taosArrayGet(pRsp->pIndexRsp, i);
    tFreeSTableIndexRsp(pIndexRsp);
  }

  taosArrayDestroy(pRsp->pIndexRsp);
}

int32_t tSerializeSTableInfoReq(void *buf, int32_t bufLen, STableInfoReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  int32_t code = 0;
  int32_t lino;
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->tbName));
  TAOS_CHECK_EXIT(tEncodeU8(&encoder, pReq->option));
  TAOS_CHECK_EXIT(tEncodeU8(&encoder, pReq->autoCreateCtb));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSTableInfoReq(void *buf, int32_t bufLen, STableInfoReq *pReq) {
  int32_t   headLen = sizeof(SMsgHead);
  int32_t   code = 0;
  int32_t   lino;
  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->tbName));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU8(&decoder, &pReq->option));
  } else {
    pReq->option = 0;
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU8(&decoder, &pReq->autoCreateCtb));
  } else {
    pReq->autoCreateCtb = 0;
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMDropTopicReq(void *buf, int32_t bufLen, SMDropTopicReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));
  ENCODESQL();
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->force));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropTopicReq(void *buf, int32_t bufLen, SMDropTopicReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));
  DECODESQL();
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->force));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMDropTopicReq(SMDropTopicReq *pReq) { FREESQL(); }

int32_t tSerializeSMDropCgroupReq(void *buf, int32_t bufLen, SMDropCgroupReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->topic));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->cgroup));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->force));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropCgroupReq(void *buf, int32_t bufLen, SMDropCgroupReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->topic));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->cgroup));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->force));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSCMCreateTopicReq(void *buf, int32_t bufLen, const SCMCreateTopicReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igExists));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->subType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->withMeta));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->subDbName));
  if (TOPIC_SUB_TYPE__DB == pReq->subType) {
  } else {
    if (TOPIC_SUB_TYPE__TABLE == pReq->subType) {
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->subStbName));
    }
    if (pReq->ast && strlen(pReq->ast) > 0) {
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, strlen(pReq->ast)));
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->ast));
    } else {
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, 0));
    }
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, strlen(pReq->sql)));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->sql));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateTopicReq(void *buf, int32_t bufLen, SCMCreateTopicReq *pReq) {
  int32_t  sqlLen = 0;
  int32_t  astLen = 0;
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igExists));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->subType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->withMeta));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->subDbName));
  if (TOPIC_SUB_TYPE__DB == pReq->subType) {
  } else {
    if (TOPIC_SUB_TYPE__TABLE == pReq->subType) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->subStbName));
    }
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &astLen));
    if (astLen > 0) {
      pReq->ast = taosMemoryCalloc(1, astLen + 1);
      if (pReq->ast == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->ast));
    }
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &sqlLen));
  if (sqlLen > 0) {
    pReq->sql = taosMemoryCalloc(1, sqlLen + 1);
    if (pReq->sql == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->sql));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCMCreateTopicReq(SCMCreateTopicReq *pReq) {
  taosMemoryFreeClear(pReq->sql);
  if (TOPIC_SUB_TYPE__DB != pReq->subType) {
    taosMemoryFreeClear(pReq->ast);
  }
}

int32_t tSerializeSConnectReq(void *buf, int32_t bufLen, SConnectReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->connType));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pid));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->app));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->user));
  TAOS_CHECK_EXIT(tEncodeCStrWithLen(&encoder, pReq->passwd, TSDB_PASSWORD_LEN));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->startTime));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->sVer));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConnectReq(void *buf, int32_t bufLen, SConnectReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->connType));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pid));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->app));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->user));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->passwd));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->startTime));
  // Check the client version from version 3.0.3.0
  if (tDecodeIsEnd(&decoder)) {
    tDecoderClear(&decoder);
    TAOS_CHECK_EXIT(TSDB_CODE_VERSION_NOT_COMPATIBLE);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->sVer));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSConnectRsp(void *buf, int32_t bufLen, SConnectRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->acctId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->clusterId));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pRsp->connId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->dnodeNum));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->superUser));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->sysInfo));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->connType));
  TAOS_CHECK_EXIT(tEncodeSEpSet(&encoder, &pRsp->epSet));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->svrTimestamp));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->sVer));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->sDetailVer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->passVer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->authVer));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->whiteListVer));
  TAOS_CHECK_EXIT(tSerializeSMonitorParas(&encoder, &pRsp->monitorParas));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRsp->enableAuditDelete));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSConnectRsp(void *buf, int32_t bufLen, SConnectRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->acctId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->clusterId));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pRsp->connId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->dnodeNum));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->superUser));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->sysInfo));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->connType));
  TAOS_CHECK_EXIT(tDecodeSEpSet(&decoder, &pRsp->epSet));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->svrTimestamp));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->sVer));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->sDetailVer));

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->passVer));
  } else {
    pRsp->passVer = 0;
  }
  // since 3.0.7.0
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->authVer));
  } else {
    pRsp->authVer = 0;
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->whiteListVer));
  } else {
    pRsp->whiteListVer = 0;
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDeserializeSMonitorParas(&decoder, &pRsp->monitorParas));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pRsp->enableAuditDelete));
  } else {
    pRsp->enableAuditDelete = 0;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMTimerMsg(void *buf, int32_t bufLen, SMTimerReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->reserved));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeDropOrphanTaskMsg(void *buf, int32_t bufLen, SMStreamDropOrphanMsg *pMsg) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t size = taosArrayGetSize(pMsg->pList);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));

  for (int32_t i = 0; i < size; i++) {
    SOrphanTask *pTask = taosArrayGet(pMsg->pList, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pTask->streamId));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pTask->taskId));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pTask->nodeId));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeDropOrphanTaskMsg(void *buf, int32_t bufLen, SMStreamDropOrphanMsg *pMsg) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));

  if (num > 0) {
    pMsg->pList = taosArrayInit(num, sizeof(SOrphanTask));
    if (NULL == pMsg->pList) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < num; ++i) {
      SOrphanTask info = {0};
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &info.streamId));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &info.taskId));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &info.nodeId));

      if (taosArrayPush(pMsg->pList, &info) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroyDropOrphanTaskMsg(SMStreamDropOrphanMsg *pMsg) {
  if (pMsg == NULL) {
    return;
  }

  taosArrayDestroy(pMsg->pList);
}

int32_t tEncodeSReplica(SEncoder *pEncoder, SReplica *pReplica) {
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReplica->id));
  TAOS_CHECK_RETURN(tEncodeU16(pEncoder, pReplica->port));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReplica->fqdn));
  return 0;
}

int32_t tDecodeSReplica(SDecoder *pDecoder, SReplica *pReplica) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReplica->id));
  TAOS_CHECK_RETURN(tDecodeU16(pDecoder, &pReplica->port));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pReplica->fqdn));
  return 0;
}

int32_t tSerializeSCreateVnodeReq(void *buf, int32_t bufLen, SCreateVnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->dbUid));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgVersion));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfStables));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->buffer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pageSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pages));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysPerFile));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->minRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->maxRows));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->hashBegin));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->hashEnd));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->hashMethod));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->walLevel));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->precision));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->compression));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->strict));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->cacheLast));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->replica));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->selfIndex));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    TAOS_CHECK_EXIT(tEncodeSReplica(&encoder, pReplica));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfRetensions));
  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention *pRetension = taosArrayGet(pReq->pRetensions, i);
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRetension->freq));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRetension->keep));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRetension->freqUnit));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pRetension->keepUnit));
  }

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->isTsma));
  if (pReq->isTsma) {
    uint32_t tsmaLen = (uint32_t)(htonl(((SMsgHead *)pReq->pTsma)->contLen));
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pReq->pTsma, tsmaLen));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRetentionPeriod));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->walRetentionSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRollPeriod));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->walSegmentSize));
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->sstTrigger));
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->hashPrefix));
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->hashSuffix));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->tsdbPageSize));
  for (int32_t i = 0; i < 6; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->reserved[i]));
  }
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->learnerReplica));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->learnerSelfIndex));
  for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
    SReplica *pReplica = &pReq->learnerReplicas[i];
    TAOS_CHECK_EXIT(tEncodeSReplica(&encoder, pReplica));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->changeVersion));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->keepTimeOffset));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->encryptAlgorithm));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssChunkSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssKeepLocal));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ssCompact));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateVnodeReq(void *buf, int32_t bufLen, SCreateVnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->dbUid));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgVersion));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfStables));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->buffer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pageSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pages));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysPerFile));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->minRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->maxRows));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->hashBegin));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->hashEnd));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->hashMethod));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->walLevel));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->precision));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->compression));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->strict));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->cacheLast));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->replica));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->selfIndex));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    TAOS_CHECK_EXIT(tDecodeSReplica(&decoder, pReplica));
  }
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfRetensions));
  pReq->pRetensions = taosArrayInit(pReq->numOfRetensions, sizeof(SRetention));
  if (pReq->pRetensions == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < pReq->numOfRetensions; ++i) {
    SRetention rentension = {0};
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &rentension.freq));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &rentension.keep));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &rentension.freqUnit));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &rentension.keepUnit));
    if (taosArrayPush(pReq->pRetensions, &rentension) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->isTsma));
  if (pReq->isTsma) {
    TAOS_CHECK_EXIT(tDecodeBinary(&decoder, (uint8_t **)&pReq->pTsma, NULL));
  }

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRetentionPeriod));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->walRetentionSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRollPeriod));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->walSegmentSize));
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->sstTrigger));
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->hashPrefix));
  TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->hashSuffix));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->tsdbPageSize));
  for (int32_t i = 0; i < 6; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->reserved[i]));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->learnerReplica));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->learnerSelfIndex));
    for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
      SReplica *pReplica = &pReq->learnerReplicas[i];
      TAOS_CHECK_EXIT(tDecodeSReplica(&decoder, pReplica));
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->changeVersion));
  }
  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->keepTimeOffset));
  }
  pReq->encryptAlgorithm = TSDB_DEFAULT_ENCRYPT_ALGO;
  pReq->ssChunkSize = TSDB_DEFAULT_SS_CHUNK_SIZE;
  pReq->ssKeepLocal = TSDB_DEFAULT_SS_KEEP_LOCAL;
  pReq->ssCompact = TSDB_DEFAULT_SS_COMPACT;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->encryptAlgorithm));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssChunkSize));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssKeepLocal));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ssCompact));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tFreeSCreateVnodeReq(SCreateVnodeReq *pReq) {
  taosArrayDestroy(pReq->pRetensions);
  pReq->pRetensions = NULL;
  return 0;
}

int32_t tSerializeSQueryCompactProgressReq(void *buf, int32_t bufLen, SQueryCompactProgressReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->compactId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQueryCompactProgressReq(void *buf, int32_t bufLen, SQueryCompactProgressReq *pReq) {
  int32_t  headLen = sizeof(SMsgHead);
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, ((uint8_t *)buf) + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->compactId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSQueryCompactProgressRsp(void *buf, int32_t bufLen, SQueryCompactProgressRsp *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->compactId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numberFileset));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->finished));
  // 1. add progress and remaining time
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->progress));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->remainingTime));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}
int32_t tDeserializeSQueryCompactProgressRsp(void *buf, int32_t bufLen, SQueryCompactProgressRsp *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->compactId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numberFileset));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->finished));
  // 1. decode progress and remaining time
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->progress));
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->remainingTime));
  } else {
    pReq->progress = 0;
    pReq->remainingTime = 0;
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSDropVnodeReq(void *buf, int32_t bufLen, SDropVnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->dbUid));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  for (int32_t i = 0; i < 8; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->reserved[i]));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropVnodeReq(void *buf, int32_t bufLen, SDropVnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->dbUid));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  for (int32_t i = 0; i < 8; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->reserved[i]));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
int32_t tSerializeSDropIdxReq(void *buf, int32_t bufLen, SDropIndexReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->colName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->stb));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->stbUid));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->dbUid));
  for (int32_t i = 0; i < 8; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->reserved[i]));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropIdxReq(void *buf, int32_t bufLen, SDropIndexReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->colName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->stb));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->stbUid));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->dbUid));
  for (int32_t i = 0; i < 8; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->reserved[i]));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSCompactVnodeReq(void *buf, int32_t bufLen, SCompactVnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->dbUid));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->compactStartTime));

  // 1.1 add tw.skey and tw.ekey
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->tw.skey));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->tw.ekey));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->compactId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->metaOnly));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCompactVnodeReq(void *buf, int32_t bufLen, SCompactVnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->dbUid));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->compactStartTime));

  // 1.1
  if (tDecodeIsEnd(&decoder)) {
    pReq->tw.skey = TSKEY_MIN;
    pReq->tw.ekey = TSKEY_MAX;
  } else {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->tw.skey));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->tw.ekey));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->compactId));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->metaOnly));
  } else {
    pReq->metaOnly = false;
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVKillCompactReq(void *buf, int32_t bufLen, SVKillCompactReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->compactId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVKillCompactReq(void *buf, int32_t bufLen, SVKillCompactReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->compactId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSAlterVnodeConfigReq(void *buf, int32_t bufLen, SAlterVnodeConfigReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  SEncoder encoder = {0};

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgVersion));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->buffer));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pageSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->pages));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysPerFile));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->walLevel));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->strict));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->cacheLast));
  for (int32_t i = 0; i < 7; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->reserved[i]));
  }

  // 1st modification
  TAOS_CHECK_EXIT(tEncodeI16(&encoder, pReq->sttTrigger));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->minRows));
  // 2nd modification
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRetentionPeriod));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->walRetentionSize));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->keepTimeOffset));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->ssKeepLocal));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ssCompact));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeConfigReq(void *buf, int32_t bufLen, SAlterVnodeConfigReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgVersion));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->buffer));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pageSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->pages));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->cacheLastSize));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysPerFile));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep0));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep1));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->daysToKeep2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walFsyncPeriod));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->walLevel));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->strict));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->cacheLast));
  for (int32_t i = 0; i < 7; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->reserved[i]));
  }

  // 1st modification
  if (tDecodeIsEnd(&decoder)) {
    pReq->sttTrigger = -1;
    pReq->minRows = -1;
  } else {
    TAOS_CHECK_EXIT(tDecodeI16(&decoder, &pReq->sttTrigger));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->minRows));
  }

  // 2n modification
  if (tDecodeIsEnd(&decoder)) {
    pReq->walRetentionPeriod = -1;
    pReq->walRetentionSize = -1;
  } else {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRetentionPeriod));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->walRetentionSize));
  }
  pReq->keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->keepTimeOffset));
  }

  pReq->ssKeepLocal = TSDB_DEFAULT_SS_KEEP_LOCAL;
  pReq->ssCompact = TSDB_DEFAULT_SS_COMPACT;
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->ssKeepLocal) < 0);
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ssCompact) < 0);
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSAlterVnodeReplicaReq(void *buf, int32_t bufLen, SAlterVnodeReplicaReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->strict));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->selfIndex));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->replica));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    TAOS_CHECK_EXIT(tEncodeSReplica(&encoder, pReplica));
  }
  for (int32_t i = 0; i < 8; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->reserved[i]));
  }
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->learnerSelfIndex));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->learnerReplica));
  for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
    SReplica *pReplica = &pReq->learnerReplicas[i];
    TAOS_CHECK_EXIT(tEncodeSReplica(&encoder, pReplica));
  }
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->changeVersion));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeReplicaReq(void *buf, int32_t bufLen, SAlterVnodeReplicaReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->strict));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->selfIndex));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->replica));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    TAOS_CHECK_EXIT(tDecodeSReplica(&decoder, pReplica));
  }
  for (int32_t i = 0; i < 8; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->reserved[i]));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->learnerSelfIndex));
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->learnerReplica));
    for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
      SReplica *pReplica = &pReq->learnerReplicas[i];
      TAOS_CHECK_EXIT(tDecodeSReplica(&decoder, pReplica));
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->changeVersion));
  }

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSDisableVnodeWriteReq(void *buf, int32_t bufLen, SDisableVnodeWriteReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->disable));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDisableVnodeWriteReq(void *buf, int32_t bufLen, SDisableVnodeWriteReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->disable));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSAlterVnodeHashRangeReq(void *buf, int32_t bufLen, SAlterVnodeHashRangeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->srcVgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dstVgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->hashBegin));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->hashEnd));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->changeVersion));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->reserved));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAlterVnodeHashRangeReq(void *buf, int32_t bufLen, SAlterVnodeHashRangeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->srcVgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dstVgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->hashBegin));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->hashEnd));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->changeVersion));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->reserved));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->queryStrId));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillQueryReq(void *buf, int32_t bufLen, SKillQueryReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->queryStrId));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->connId));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillConnReq(void *buf, int32_t bufLen, SKillConnReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->connId));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSKillTransReq(void *buf, int32_t bufLen, SKillTransReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->transId));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSKillTransReq(void *buf, int32_t bufLen, SKillTransReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->transId));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSBalanceVgroupReq(void *buf, int32_t bufLen, SBalanceVgroupReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->useless));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSBalanceVgroupReq(void *buf, int32_t bufLen, SBalanceVgroupReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->useless));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSBalanceVgroupReq(SBalanceVgroupReq *pReq) { FREESQL(); }

int32_t tSerializeSAssignLeaderReq(void *buf, int32_t bufLen, SAssignLeaderReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->useless));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSAssignLeaderReq(void *buf, int32_t bufLen, SAssignLeaderReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->useless));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSAssignLeaderReq(SAssignLeaderReq *pReq) { FREESQL(); }

int32_t tSerializeSBalanceVgroupLeaderReq(void *buf, int32_t bufLen, SBalanceVgroupLeaderReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->reserved));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  ENCODESQL();
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->db));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSBalanceVgroupLeaderReq(void *buf, int32_t bufLen, SBalanceVgroupLeaderReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->reserved));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  }
  DECODESQL();
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->db));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSBalanceVgroupLeaderReq(SBalanceVgroupLeaderReq *pReq) { FREESQL(); }

int32_t tSerializeSMergeVgroupReq(void *buf, int32_t bufLen, SMergeVgroupReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId1));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId2));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMergeVgroupReq(void *buf, int32_t bufLen, SMergeVgroupReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId1));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId2));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSRedistributeVgroupReq(void *buf, int32_t bufLen, SRedistributeVgroupReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId1));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId2));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId3));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRedistributeVgroupReq(void *buf, int32_t bufLen, SRedistributeVgroupReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId1));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId2));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId3));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSRedistributeVgroupReq(SRedistributeVgroupReq *pReq) { FREESQL(); }

int32_t tSerializeSSplitVgroupReq(void *buf, int32_t bufLen, SSplitVgroupReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  TAOS_CHECK_EXIT(tEncodeBool(&encoder, pReq->force));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSplitVgroupReq(void *buf, int32_t bufLen, SSplitVgroupReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pReq->force));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSForceBecomeFollowerReq(void *buf, int32_t bufLen, SForceBecomeFollowerReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->vgId));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tSerializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->replica));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    TAOS_CHECK_EXIT(tEncodeSReplica(&encoder, pReplica));
  }
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->learnerReplica));
  for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
    SReplica *pReplica = &pReq->learnerReplicas[i];
    TAOS_CHECK_EXIT(tEncodeSReplica(&encoder, pReplica));
  }
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->lastIndex));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDCreateMnodeReq(void *buf, int32_t bufLen, SDCreateMnodeReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->replica));
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SReplica *pReplica = &pReq->replicas[i];
    TAOS_CHECK_EXIT(tDecodeSReplica(&decoder, pReplica));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->learnerReplica));
    for (int32_t i = 0; i < TSDB_MAX_LEARNER_REPLICA; ++i) {
      SReplica *pReplica = &pReq->learnerReplicas[i];
      TAOS_CHECK_EXIT(tDecodeSReplica(&decoder, pReplica));
    }
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->lastIndex));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSVArbHeartBeatReq(void *buf, int32_t bufLen, SVArbHeartBeatReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->arbToken));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->arbTerm));

  int32_t size = taosArrayGetSize(pReq->hbMembers);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; i++) {
    SVArbHbReqMember *pMember = taosArrayGet(pReq->hbMembers, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMember->vgId));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMember->hbSeq));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVArbHeartBeatReq(void *buf, int32_t bufLen, SVArbHeartBeatReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->dnodeId));
  if ((pReq->arbToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->arbToken));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->arbTerm));

  if ((pReq->hbMembers = taosArrayInit(16, sizeof(SVArbHbReqMember))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
  for (int32_t i = 0; i < size; i++) {
    SVArbHbReqMember member = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &member.vgId));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &member.hbSeq));
    if (taosArrayPush(pReq->hbMembers, &member) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVArbHeartBeatReq(SVArbHeartBeatReq *pReq) {
  if (!pReq) return;
  taosMemoryFree(pReq->arbToken);
  taosArrayDestroy(pReq->hbMembers);
}

int32_t tSerializeSVArbHeartBeatRsp(void *buf, int32_t bufLen, SVArbHeartBeatRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->arbToken));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->dnodeId));
  int32_t sz = taosArrayGetSize(pRsp->hbMembers);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, sz));
  for (int32_t i = 0; i < sz; i++) {
    SVArbHbRspMember *pMember = taosArrayGet(pRsp->hbMembers, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMember->vgId));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMember->hbSeq));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pMember->memberToken));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVArbHeartBeatRsp(void *buf, int32_t bufLen, SVArbHeartBeatRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->arbToken));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->dnodeId));
  int32_t sz = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &sz));
  if ((pRsp->hbMembers = taosArrayInit(sz, sizeof(SVArbHbRspMember))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < sz; i++) {
    SVArbHbRspMember hbMember = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &hbMember.vgId));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &hbMember.hbSeq));
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, hbMember.memberToken));
    if (taosArrayPush(pRsp->hbMembers, &hbMember) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVArbHeartBeatRsp(SVArbHeartBeatRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }
  taosArrayDestroy(pRsp->hbMembers);
}

int32_t tSerializeSVArbCheckSyncReq(void *buf, int32_t bufLen, SVArbCheckSyncReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->arbToken));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->arbTerm));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->member0Token));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->member1Token));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVArbCheckSyncReq(void *buf, int32_t bufLen, SVArbCheckSyncReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  if ((pReq->arbToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->arbToken));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->arbTerm));
  if ((pReq->member0Token = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->member0Token));
  if ((pReq->member1Token = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->member1Token));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVArbCheckSyncReq(SVArbCheckSyncReq *pRsp) {
  if (NULL == pRsp) {
    return;
  }
  taosMemoryFreeClear(pRsp->arbToken);
  taosMemoryFreeClear(pRsp->member0Token);
  taosMemoryFreeClear(pRsp->member1Token);
}

int32_t tSerializeSVArbCheckSyncRsp(void *buf, int32_t bufLen, SVArbCheckSyncRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->arbToken));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->member0Token));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->member1Token));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->vgId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->errCode));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVArbCheckSyncRsp(void *buf, int32_t bufLen, SVArbCheckSyncRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  if ((pRsp->arbToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->arbToken));
  if ((pRsp->member0Token = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->member0Token));
  if ((pRsp->member1Token = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->member1Token));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->vgId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->errCode));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVArbCheckSyncRsp(SVArbCheckSyncRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }
  taosMemoryFreeClear(pRsp->arbToken);
  taosMemoryFreeClear(pRsp->member0Token);
  taosMemoryFreeClear(pRsp->member1Token);
}

int32_t tSerializeSVArbSetAssignedLeaderReq(void *buf, int32_t bufLen, SVArbSetAssignedLeaderReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->arbToken));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->arbTerm));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->memberToken));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->force));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVArbSetAssignedLeaderReq(void *buf, int32_t bufLen, SVArbSetAssignedLeaderReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  if ((pReq->arbToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->arbToken));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->arbTerm));
  if ((pReq->memberToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->memberToken));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->force));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVArbSetAssignedLeaderReq(SVArbSetAssignedLeaderReq *pReq) {
  if (NULL == pReq) {
    return;
  }
  taosMemoryFreeClear(pReq->arbToken);
  taosMemoryFreeClear(pReq->memberToken);
}

int32_t tSerializeSVArbSetAssignedLeaderRsp(void *buf, int32_t bufLen, SVArbSetAssignedLeaderRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->arbToken));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->memberToken));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->vgId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSVArbSetAssignedLeaderRsp(void *buf, int32_t bufLen, SVArbSetAssignedLeaderRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  if ((pRsp->arbToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->arbToken));
  if ((pRsp->memberToken = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->memberToken));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->vgId));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSVArbSetAssignedLeaderRsp(SVArbSetAssignedLeaderRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }
  taosMemoryFreeClear(pRsp->arbToken);
  taosMemoryFreeClear(pRsp->memberToken);
}

int32_t tSerializeSMArbUpdateGroupBatchReq(void *buf, int32_t bufLen, SMArbUpdateGroupBatchReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t sz = taosArrayGetSize(pReq->updateArray);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, sz));

  for (int32_t i = 0; i < sz; i++) {
    SMArbUpdateGroup *pGroup = taosArrayGet(pReq->updateArray, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pGroup->vgId));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pGroup->dbUid));
    for (int j = 0; j < TSDB_ARB_GROUP_MEMBER_NUM; j++) {
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pGroup->members[j].dnodeId));
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pGroup->members[j].token));
    }
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pGroup->isSync));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pGroup->assignedLeader.dnodeId));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pGroup->assignedLeader.token));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pGroup->version));
  }

  for (int32_t i = 0; i < sz; i++) {
    SMArbUpdateGroup *pGroup = taosArrayGet(pReq->updateArray, i);
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, pGroup->assignedLeader.acked));
  }

  for (int32_t i = 0; i < sz; i++) {
    SMArbUpdateGroup *pGroup = taosArrayGet(pReq->updateArray, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pGroup->code));
    TAOS_CHECK_EXIT(tEncodeI64(&encoder, pGroup->updateTimeMs));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMArbUpdateGroupBatchReq(void *buf, int32_t bufLen, SMArbUpdateGroupBatchReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  int32_t sz = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &sz));

  SArray *updateArray = taosArrayInit(sz, sizeof(SMArbUpdateGroup));
  if (!updateArray) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < sz; i++) {
    SMArbUpdateGroup group = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &group.vgId));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &group.dbUid));
    for (int j = 0; j < TSDB_ARB_GROUP_MEMBER_NUM; j++) {
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &group.members[j].dnodeId));
      if ((group.members[j].token = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, group.members[j].token));
    }
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &group.isSync));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &group.assignedLeader.dnodeId));
    if ((group.assignedLeader.token = taosMemoryMalloc(TSDB_ARB_TOKEN_SIZE)) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, group.assignedLeader.token));
    TAOS_CHECK_EXIT(tDecodeI64(&decoder, &group.version));
    group.assignedLeader.acked = false;

    if (taosArrayPush(updateArray, &group) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < sz; i++) {
      SMArbUpdateGroup *pGroup = taosArrayGet(updateArray, i);
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pGroup->assignedLeader.acked));
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    for (int32_t i = 0; i < sz; i++) {
      SMArbUpdateGroup *pGroup = taosArrayGet(updateArray, i);
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pGroup->code));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pGroup->updateTimeMs));
    }
  }

  pReq->updateArray = updateArray;

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMArbUpdateGroupBatchReq(SMArbUpdateGroupBatchReq *pReq) {
  if (NULL == pReq || NULL == pReq->updateArray) {
    return;
  }

  int32_t sz = taosArrayGetSize(pReq->updateArray);
  for (int32_t i = 0; i < sz; i++) {
    SMArbUpdateGroup *pGroup = taosArrayGet(pReq->updateArray, i);
    for (int j = 0; j < TSDB_ARB_GROUP_MEMBER_NUM; j++) {
      taosMemoryFreeClear(pGroup->members[j].token);
    }
    taosMemoryFreeClear(pGroup->assignedLeader.token);
  }
  taosArrayDestroy(pReq->updateArray);
}

int32_t tSerializeSServerStatusRsp(void *buf, int32_t bufLen, SServerStatusRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->statusCode));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->details));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSServerStatusRsp(void *buf, int32_t bufLen, SServerStatusRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->statusCode));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->details));

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSExplainRsp(void *buf, int32_t bufLen, SExplainRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->numOfPlans));
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    SExplainExecInfo *info = &pRsp->subplanInfo[i];
    TAOS_CHECK_EXIT(tEncodeDouble(&encoder, info->startupCost));
    TAOS_CHECK_EXIT(tEncodeDouble(&encoder, info->totalCost));
    TAOS_CHECK_EXIT(tEncodeU64(&encoder, info->numOfRows));
    TAOS_CHECK_EXIT(tEncodeU32(&encoder, info->verboseLen));
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, info->verboseInfo, info->verboseLen));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSExplainRsp(void *buf, int32_t bufLen, SExplainRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->numOfPlans));
  if (pRsp->numOfPlans > 0) {
    pRsp->subplanInfo = taosMemoryCalloc(pRsp->numOfPlans, sizeof(SExplainExecInfo));
    if (pRsp->subplanInfo == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    TAOS_CHECK_EXIT(tDecodeDouble(&decoder, &pRsp->subplanInfo[i].startupCost));
    TAOS_CHECK_EXIT(tDecodeDouble(&decoder, &pRsp->subplanInfo[i].totalCost));
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pRsp->subplanInfo[i].numOfRows));
    TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pRsp->subplanInfo[i].verboseLen));
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, &pRsp->subplanInfo[i].verboseInfo, NULL));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSExplainRsp(SExplainRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  for (int32_t i = 0; i < pRsp->numOfPlans; ++i) {
    SExplainExecInfo *pExec = pRsp->subplanInfo + i;
    taosMemoryFree(pExec->verboseInfo);
  }

  taosMemoryFreeClear(pRsp->subplanInfo);
}

int32_t tSerializeSBatchReq(void *buf, int32_t bufLen, SBatchReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t num = taosArrayGetSize(pReq->pMsgs);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
  for (int32_t i = 0; i < num; ++i) {
    SBatchMsg *pMsg = taosArrayGet(pReq->pMsgs, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->msgIdx));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->msgType));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->msgLen));
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pMsg->msg, pMsg->msgLen));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSBatchReq(void *buf, int32_t bufLen, SBatchReq *pReq) {
  int32_t   headLen = sizeof(SMsgHead);
  int32_t   code = 0;
  int32_t   lino;
  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (num <= 0) {
    pReq->pMsgs = NULL;
    tEndDecode(&decoder);
    tDecoderClear(&decoder);
    return 0;
  }

  pReq->pMsgs = taosArrayInit(num, sizeof(SBatchMsg));
  if (NULL == pReq->pMsgs) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < num; ++i) {
    SBatchMsg msg = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.msgIdx));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.msgType));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.msgLen));
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, &msg.msg, NULL));
    if (NULL == taosArrayPush(pReq->pMsgs, &msg)) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSBatchRsp(void *buf, int32_t bufLen, SBatchRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t num = taosArrayGetSize(pRsp->pRsps);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
  for (int32_t i = 0; i < num; ++i) {
    SBatchRspMsg *pMsg = taosArrayGet(pRsp->pRsps, i);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->reqType));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->msgIdx));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->msgLen));
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, pMsg->rspCode));
    TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pMsg->msg, pMsg->msgLen));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSBatchRsp(void *buf, int32_t bufLen, SBatchRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, (char *)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (num <= 0) {
    pRsp->pRsps = NULL;
    tEndDecode(&decoder);

    tDecoderClear(&decoder);
    return 0;
  }

  pRsp->pRsps = taosArrayInit(num, sizeof(SBatchRspMsg));
  if (NULL == pRsp->pRsps) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < num; ++i) {
    SBatchRspMsg msg = {0};
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.reqType));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.msgIdx));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.msgLen));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &msg.rspCode));
    TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, &msg.msg, NULL));
    if (NULL == taosArrayPush(pRsp->pRsps, &msg)) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMqAskEpReq(void *buf, int32_t bufLen, SMqAskEpReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->consumerId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->epoch));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->cgroup));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMqAskEpReq(void *buf, int32_t bufLen, SMqAskEpReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, (char *)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->consumerId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->epoch));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->cgroup));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroySMqHbRsp(SMqHbRsp *pRsp) { taosArrayDestroy(pRsp->topicPrivileges); }

int32_t tSerializeSMqHbRsp(void *buf, int32_t bufLen, SMqHbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t sz = taosArrayGetSize(pRsp->topicPrivileges);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, sz));
  for (int32_t i = 0; i < sz; ++i) {
    STopicPrivilege *privilege = (STopicPrivilege *)taosArrayGet(pRsp->topicPrivileges, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, privilege->topic));
    TAOS_CHECK_EXIT(tEncodeI8(&encoder, privilege->noPrivilege));
  }

  if (tEncodeI32(&encoder, pRsp->debugFlag) < 0) return -1;
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMqHbRsp(void *buf, int32_t bufLen, SMqHbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, (char *)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t sz = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &sz));
  if (sz > 0) {
    pRsp->topicPrivileges = taosArrayInit(sz, sizeof(STopicPrivilege));
    if (NULL == pRsp->topicPrivileges) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < sz; ++i) {
      STopicPrivilege *data = taosArrayReserve(pRsp->topicPrivileges, 1);
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, data->topic));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &data->noPrivilege));
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32(&decoder, &pRsp->debugFlag) < 0) return -1;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroySMqHbReq(SMqHbReq *pReq) {
  for (int i = 0; i < taosArrayGetSize(pReq->topics); i++) {
    TopicOffsetRows *vgs = taosArrayGet(pReq->topics, i);
    if (vgs) taosArrayDestroy(vgs->offsetRows);
  }
  taosArrayDestroy(pReq->topics);
}

int32_t tSerializeSMqHbReq(void *buf, int32_t bufLen, SMqHbReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->consumerId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->epoch));

  int32_t sz = taosArrayGetSize(pReq->topics);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, sz));
  for (int32_t i = 0; i < sz; ++i) {
    TopicOffsetRows *vgs = (TopicOffsetRows *)taosArrayGet(pReq->topics, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, vgs->topicName));
    int32_t szVgs = taosArrayGetSize(vgs->offsetRows);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, szVgs));
    for (int32_t j = 0; j < szVgs; ++j) {
      OffsetRows *offRows = taosArrayGet(vgs->offsetRows, j);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, offRows->vgId));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, offRows->rows));
      TAOS_CHECK_EXIT(tEncodeSTqOffsetVal(&encoder, &offRows->offset));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, offRows->ever));
    }
  }

  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->pollFlag));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMqHbReq(void *buf, int32_t bufLen, SMqHbReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->consumerId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->epoch));
  int32_t sz = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &sz));
  if (sz > 0) {
    pReq->topics = taosArrayInit(sz, sizeof(TopicOffsetRows));
    if (NULL == pReq->topics) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < sz; ++i) {
      TopicOffsetRows *data = taosArrayReserve(pReq->topics, 1);
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, data->topicName));
      int32_t szVgs = 0;
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &szVgs));
      if (szVgs > 0) {
        data->offsetRows = taosArrayInit(szVgs, sizeof(OffsetRows));
        if (NULL == data->offsetRows) {
          TAOS_CHECK_EXIT(terrno);
        }
        for (int32_t j = 0; j < szVgs; ++j) {
          OffsetRows *offRows = taosArrayReserve(data->offsetRows, 1);
          TAOS_CHECK_EXIT(tDecodeI32(&decoder, &offRows->vgId));
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &offRows->rows));
          TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(&decoder, &offRows->offset));
          TAOS_CHECK_EXIT(tDecodeI64(&decoder, &offRows->ever));
        }
      }
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->pollFlag));
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMqSeekReq(void *buf, int32_t bufLen, SMqSeekReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->consumerId));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->subKey));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->head.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSMqSeekReq(void *buf, int32_t bufLen, SMqSeekReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  int32_t  headLen = sizeof(SMsgHead);
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->consumerId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->subKey));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSSubQueryMsg(void *buf, int32_t bufLen, SSubQueryMsg *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->sId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->queryId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->refId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->execId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->msgMask));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->taskType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->explain));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->needFetch));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->compress));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->sqlLen));
  TAOS_CHECK_EXIT(tEncodeCStrWithLen(&encoder, pReq->sql, pReq->sqlLen));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->msgLen));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (uint8_t *)pReq->msg, pReq->msgLen));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->clientId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSSubQueryMsg(void *buf, int32_t bufLen, SSubQueryMsg *pReq) {
  int32_t   code = 0;
  int32_t   lino;
  int32_t   headLen = sizeof(SMsgHead);
  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->sId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->queryId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->refId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->execId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->msgMask));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->taskType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->explain));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->needFetch));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->compress));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->sqlLen));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pReq->sql));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->msgLen));
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->msg, NULL));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->clientId));
  } else {
    pReq->clientId = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSSubQueryMsg(SSubQueryMsg *pReq) {
  if (NULL == pReq) {
    return;
  }

  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->msg);
}

int32_t tSerializeSOperatorParam(SEncoder *pEncoder, SOperatorParam *pOpParam) {
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pOpParam->opType));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pOpParam->downstreamIdx));
  switch (pOpParam->opType) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      STagScanOperatorParam *pTagScan = (STagScanOperatorParam *)pOpParam->value;
      TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pTagScan->vcUid));
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN: {
      STableScanOperatorParam *pScan = (STableScanOperatorParam *)pOpParam->value;
      TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pScan->tableSeq));
      int32_t uidNum = taosArrayGetSize(pScan->pUidList);
      TAOS_CHECK_RETURN(tEncodeI32(pEncoder, uidNum));
      for (int32_t m = 0; m < uidNum; ++m) {
        int64_t *pUid = taosArrayGet(pScan->pUidList, m);
        TAOS_CHECK_RETURN(tEncodeI64(pEncoder, *pUid));
      }
      if (pScan->pOrgTbInfo) {
        TAOS_CHECK_RETURN(tEncodeBool(pEncoder, true));
        TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pScan->pOrgTbInfo->vgId));
        TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pScan->pOrgTbInfo->tbName));
        int32_t num = taosArrayGetSize(pScan->pOrgTbInfo->colMap);
        TAOS_CHECK_RETURN(tEncodeI32(pEncoder, num));
        for (int32_t i = 0; i < num; ++i) {
          SColIdNameKV *pColKV = taosArrayGet(pScan->pOrgTbInfo->colMap, i);
          TAOS_CHECK_RETURN(tEncodeI16(pEncoder, pColKV->colId));
          TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pColKV->colName));
        }
      } else {
        TAOS_CHECK_RETURN(tEncodeBool(pEncoder, false));
      }
      TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pScan->window.skey));
      TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pScan->window.ekey));
      break;
    }
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  int32_t n = taosArrayGetSize(pOpParam->pChildren);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, n));
  for (int32_t i = 0; i < n; ++i) {
    SOperatorParam *pChild = *(SOperatorParam **)taosArrayGet(pOpParam->pChildren, i);
    TAOS_CHECK_RETURN(tSerializeSOperatorParam(pEncoder, pChild));
  }

  TAOS_CHECK_RETURN(tEncodeBool(pEncoder, pOpParam->reUse));
  return 0;
}

int32_t tDeserializeSOperatorParam(SDecoder *pDecoder, SOperatorParam *pOpParam) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pOpParam->opType));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pOpParam->downstreamIdx));
  switch (pOpParam->opType) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN: {
      pOpParam->value = taosMemoryMalloc(sizeof(STagScanOperatorParam));
      if (NULL == pOpParam->value) {
        TAOS_CHECK_RETURN(terrno);
      }
      STagScanOperatorParam *pTagScan = pOpParam->value;
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pTagScan->vcUid));
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN: {
      pOpParam->value = taosMemoryMalloc(sizeof(STableScanOperatorParam));
      if (NULL == pOpParam->value) {
        TAOS_CHECK_RETURN(terrno);
      }
      STableScanOperatorParam *pScan = pOpParam->value;
      TAOS_CHECK_RETURN(tDecodeI8(pDecoder, (int8_t *)&pScan->tableSeq));
      int32_t uidNum = 0;
      int64_t uid = 0;
      TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &uidNum));
      if (uidNum > 0) {
        pScan->pUidList = taosArrayInit(uidNum, sizeof(int64_t));
        if (NULL == pScan->pUidList) {
          TAOS_CHECK_RETURN(terrno);
        }

        for (int32_t m = 0; m < uidNum; ++m) {
          TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &uid));
          if (taosArrayPush(pScan->pUidList, &uid) == NULL) {
            TAOS_CHECK_RETURN(terrno);
          }
        }
      } else {
        pScan->pUidList = NULL;
      }

      bool hasTbInfo = false;
      TAOS_CHECK_RETURN(tDecodeBool(pDecoder, &hasTbInfo));
      if (hasTbInfo) {
        pScan->pOrgTbInfo = taosMemoryMalloc(sizeof(SOrgTbInfo));
        if (NULL == pScan->pOrgTbInfo) {
          TAOS_CHECK_RETURN(terrno);
        }
        TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pScan->pOrgTbInfo->vgId));
        TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pScan->pOrgTbInfo->tbName));
        int32_t num = 0;
        TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &num));
        pScan->pOrgTbInfo->colMap = taosArrayInit(num, sizeof(SColIdNameKV));
        for (int32_t i = 0; i < num; ++i) {
          SColIdNameKV pColKV;
          TAOS_CHECK_RETURN(tDecodeI16(pDecoder, (int16_t *)&(pColKV.colId)));
          TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pColKV.colName));
          if (taosArrayPush(pScan->pOrgTbInfo->colMap, &pColKV) == NULL) {
            TAOS_CHECK_RETURN(terrno);
          }
        }
      } else {
        pScan->pOrgTbInfo = NULL;
      }
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pScan->window.skey));
      TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pScan->window.ekey));
      break;
    }
    default:
      return TSDB_CODE_INVALID_PARA;
  }

  int32_t childrenNum = 0;
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &childrenNum));

  if (childrenNum > 0) {
    pOpParam->pChildren = taosArrayInit(childrenNum, POINTER_BYTES);
    if (NULL == pOpParam->pChildren) {
      TAOS_CHECK_RETURN(terrno);
    }
    for (int32_t i = 0; i < childrenNum; ++i) {
      SOperatorParam *pChild = taosMemoryCalloc(1, sizeof(SOperatorParam));
      if (NULL == pChild) {
        TAOS_CHECK_RETURN(terrno);
      }
      TAOS_CHECK_RETURN(tDeserializeSOperatorParam(pDecoder, pChild));
      if (taosArrayPush(pOpParam->pChildren, &pChild) == NULL) {
        TAOS_CHECK_RETURN(terrno);
      }
    }
  } else {
    pOpParam->pChildren = NULL;
  }

  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeBool(pDecoder, &pOpParam->reUse));
  } else {
    pOpParam->reUse = false;
  }

  return 0;
}

int32_t tSerializeSResFetchReq(void *buf, int32_t bufLen, SResFetchReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->sId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->queryId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->execId));
  if (pReq->pOpParam) {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, 1));
    TAOS_CHECK_EXIT(tSerializeSOperatorParam(&encoder, pReq->pOpParam));
  } else {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, 0));
  }
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->clientId));
  if (pReq->pStRtFuncInfo) {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, 1));
    TAOS_CHECK_EXIT(tSerializeStRtFuncInfo(&encoder, pReq->pStRtFuncInfo));
  } else {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, 0));
  }
  TAOS_CHECK_EXIT(tEncodeBool(&encoder, pReq->reset));
  TAOS_CHECK_EXIT(tEncodeBool(&encoder, pReq->dynTbname));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSResFetchReq(void *buf, int32_t bufLen, SResFetchReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->sId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->queryId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->execId));

  int32_t paramNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &paramNum));
  if (paramNum > 0) {
    pReq->pOpParam = taosMemoryMalloc(sizeof(*pReq->pOpParam));
    if (NULL == pReq->pOpParam) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDeserializeSOperatorParam(&decoder, pReq->pOpParam));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->clientId));
  } else {
    pReq->clientId = 0;
  }
  if (!tDecodeIsEnd(&decoder)) {
    int32_t hasStRtFuncInfo = 0;
    TAOS_CHECK_ERRNO(tDecodeI32(&decoder, &hasStRtFuncInfo));
    if (hasStRtFuncInfo > 0) {
      pReq->pStRtFuncInfo = taosMemoryCalloc(1, sizeof(SStreamRuntimeFuncInfo));;
      if (NULL == pReq->pStRtFuncInfo) {
        TAOS_CHECK_EXIT(terrno);
      }
      TAOS_CHECK_EXIT(tDeserializeStRtFuncInfo(&decoder, pReq->pStRtFuncInfo));
    }
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pReq->reset));
    TAOS_CHECK_EXIT(tDecodeBool(&decoder, &pReq->dynTbname));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void    tDestroySResFetchReq(SResFetchReq* pReq){
  if (pReq != NULL) {
    tDestroyStRtFuncInfo(pReq->pStRtFuncInfo);
    taosMemoryFree(pReq->pStRtFuncInfo);
  }
}

int32_t tSerializeSMqPollReq(void *buf, int32_t bufLen, SMqPollReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  int32_t code = 0;
  int32_t lino;
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->subKey));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->withTbName));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->useSnapshot));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->epoch));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->reqId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->consumerId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->timeout));
  TAOS_CHECK_EXIT(tEncodeSTqOffsetVal(&encoder, &pReq->reqOffset));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->enableReplay));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->sourceExcluded));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->enableBatchMeta));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->rawData));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->minPollRows));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->head.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSMqPollReq(void *buf, int32_t bufLen, SMqPollReq *pReq) {
  int32_t  code = 0;
  int32_t  lino;
  int32_t  headLen = sizeof(SMsgHead);
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->subKey));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->withTbName));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->useSnapshot));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->epoch));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->reqId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->consumerId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->timeout));
  TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(&decoder, &pReq->reqOffset));

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->enableReplay));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->sourceExcluded));
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->enableBatchMeta));
  } else {
    pReq->enableBatchMeta = false;
  }

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->rawData));
    TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->minPollRows));
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tDestroySMqPollReq(SMqPollReq *pReq) {
  tOffsetDestroy(&pReq->reqOffset);
  if (pReq->uidHash != NULL) {
    taosHashCleanup(pReq->uidHash);
    pReq->uidHash = NULL;
  }
}
int32_t tSerializeSTaskDropReq(void *buf, int32_t bufLen, STaskDropReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t tlen;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->sId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->queryId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->refId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->execId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->clientId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSTaskDropReq(void *buf, int32_t bufLen, STaskDropReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  int32_t code = 0;
  int32_t lino;

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->sId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->queryId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->refId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->execId));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->clientId));
  } else {
    pReq->clientId = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSTaskNotifyReq(void *buf, int32_t bufLen, STaskNotifyReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t tlen;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->sId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->queryId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pReq->refId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->execId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->clientId));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSTaskNotifyReq(void *buf, int32_t bufLen, STaskNotifyReq *pReq) {
  int32_t headLen = sizeof(SMsgHead);
  int32_t code = 0;
  int32_t lino;

  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->sId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->queryId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pReq->refId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->execId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, (int32_t *)&pReq->type));
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->clientId));
  } else {
    pReq->clientId = 0;
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->code));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->affectedRows));
  int32_t tbNum = taosArrayGetSize(pRsp->tbVerInfo);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, tbNum));
  if (tbNum > 0) {
    for (int32_t i = 0; i < tbNum; ++i) {
      STbVerInfo *pVer = taosArrayGet(pRsp->tbVerInfo, i);
      TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pVer->tbFName));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pVer->sversion));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pVer->tversion));
    }
  }

  if (tbNum > 0) {
    for (int32_t i = 0; i < tbNum; ++i) {
      STbVerInfo *pVer = taosArrayGet(pRsp->tbVerInfo, i);
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, pVer->rversion));
    }
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQueryTableRsp(void *buf, int32_t bufLen, SQueryTableRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, (char *)buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->code));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->affectedRows));
  int32_t tbNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &tbNum));
  if (tbNum > 0) {
    pRsp->tbVerInfo = taosArrayInit(tbNum, sizeof(STbVerInfo));
    if (NULL == pRsp->tbVerInfo) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < tbNum; i++) {
      STbVerInfo tbVer;
      TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, tbVer.tbFName));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &tbVer.sversion));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &tbVer.tversion));
      tbVer.rversion = 1;
      if (NULL == taosArrayPush(pRsp->tbVerInfo, &tbVer)) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  if (!tDecodeIsEnd(&decoder)) {
    if (tbNum > 0) {
      for (int32_t i = 0; i < tbNum; i++) {
        STbVerInfo *pVer = taosArrayGet(pRsp->tbVerInfo, i);
        if (NULL == pVer) {
          TAOS_CHECK_EXIT(terrno);
        }
        TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pVer->rversion));
      }
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSSchedulerHbReq(void *buf, int32_t bufLen, SSchedulerHbReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t tlen;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->clientId));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->epId.nodeId));
  TAOS_CHECK_EXIT(tEncodeU16(&encoder, pReq->epId.ep.port));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->epId.ep.fqdn));
  if (pReq->taskAction) {
    int32_t num = taosArrayGetSize(pReq->taskAction);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
    for (int32_t i = 0; i < num; ++i) {
      STaskAction *action = taosArrayGet(pReq->taskAction, i);
      TAOS_CHECK_EXIT(tEncodeU64(&encoder, action->queryId));
      TAOS_CHECK_EXIT(tEncodeU64(&encoder, action->taskId));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, action->action));
    }
  } else {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, 0));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }
    return tlen + headLen;
  }
}

int32_t tDeserializeSSchedulerHbReq(void *buf, int32_t bufLen, SSchedulerHbReq *pReq) {
  int32_t   headLen = sizeof(SMsgHead);
  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;
  int32_t code = 0;
  int32_t lino;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->clientId));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->epId.nodeId));
  TAOS_CHECK_EXIT(tDecodeU16(&decoder, &pReq->epId.ep.port));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->epId.ep.fqdn));
  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (num > 0) {
    pReq->taskAction = taosArrayInit(num, sizeof(STaskStatus));
    if (NULL == pReq->taskAction) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < num; ++i) {
      STaskAction action = {0};
      TAOS_CHECK_EXIT(tDecodeU64(&decoder, &action.queryId));
      TAOS_CHECK_EXIT(tDecodeU64(&decoder, &action.taskId));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &action.action));
      if (taosArrayPush(pReq->taskAction, &action) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  } else {
    pReq->taskAction = NULL;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSSchedulerHbReq(SSchedulerHbReq *pReq) { taosArrayDestroy(pReq->taskAction); }

int32_t tSerializeSSchedulerHbRsp(void *buf, int32_t bufLen, SSchedulerHbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pRsp->epId.nodeId));
  TAOS_CHECK_EXIT(tEncodeU16(&encoder, pRsp->epId.ep.port));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->epId.ep.fqdn));
  if (pRsp->taskStatus) {
    int32_t num = taosArrayGetSize(pRsp->taskStatus);
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, num));
    for (int32_t i = 0; i < num; ++i) {
      STaskStatus *status = taosArrayGet(pRsp->taskStatus, i);
      TAOS_CHECK_EXIT(tEncodeU64(&encoder, status->queryId));
      TAOS_CHECK_EXIT(tEncodeU64(&encoder, status->taskId));
      TAOS_CHECK_EXIT(tEncodeI64(&encoder, status->refId));
      TAOS_CHECK_EXIT(tEncodeI32(&encoder, status->execId));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, status->status));
    }
    for (int32_t i = 0; i < num; ++i) {
      STaskStatus *status = taosArrayGet(pRsp->taskStatus, i);
      TAOS_CHECK_EXIT(tEncodeU64(&encoder, status->clientId));
    }
  } else {
    TAOS_CHECK_EXIT(tEncodeI32(&encoder, 0));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSSchedulerHbRsp(void *buf, int32_t bufLen, SSchedulerHbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pRsp->epId.nodeId));
  TAOS_CHECK_EXIT(tDecodeU16(&decoder, &pRsp->epId.ep.port));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->epId.ep.fqdn));
  int32_t num = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &num));
  if (num > 0) {
    pRsp->taskStatus = taosArrayInit(num, sizeof(STaskStatus));
    if (NULL == pRsp->taskStatus) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < num; ++i) {
      STaskStatus status = {0};
      TAOS_CHECK_EXIT(tDecodeU64(&decoder, &status.queryId));
      TAOS_CHECK_EXIT(tDecodeU64(&decoder, &status.taskId));
      TAOS_CHECK_EXIT(tDecodeI64(&decoder, &status.refId));
      TAOS_CHECK_EXIT(tDecodeI32(&decoder, &status.execId));
      TAOS_CHECK_EXIT(tDecodeI8(&decoder, &status.status));
      if (taosArrayPush(pRsp->taskStatus, &status) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
    if (!tDecodeIsEnd(&decoder)) {
      for (int32_t i = 0; i < num; ++i) {
        STaskStatus *status = taosArrayGet(pRsp->taskStatus, i);
        TAOS_CHECK_EXIT(tDecodeU64(&decoder, &status->clientId));
      }
    }
  } else {
    pRsp->taskStatus = NULL;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSSchedulerHbRsp(SSchedulerHbRsp *pRsp) { taosArrayDestroy(pRsp->taskStatus); }

int tEncodeSVCreateTbBatchRsp(SEncoder *pCoder, const SVCreateTbBatchRsp *pRsp) {
  int32_t        nRsps = taosArrayGetSize(pRsp->pArray);
  SVCreateTbRsp *pCreateRsp;

  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, nRsps));
  for (int32_t i = 0; i < nRsps; i++) {
    pCreateRsp = taosArrayGet(pRsp->pArray, i);
    TAOS_CHECK_RETURN(tEncodeSVCreateTbRsp(pCoder, pCreateRsp));
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbBatchRsp(SDecoder *pCoder, SVCreateTbBatchRsp *pRsp) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pRsp->nRsps));
  pRsp->pRsps = (SVCreateTbRsp *)tDecoderMalloc(pCoder, sizeof(*pRsp->pRsps) * pRsp->nRsps);
  if (pRsp->pRsps == NULL) {
    TAOS_CHECK_RETURN(terrno);
  }
  for (int32_t i = 0; i < pRsp->nRsps; i++) {
    TAOS_CHECK_RETURN(tDecodeSVCreateTbRsp(pCoder, pRsp->pRsps + i));
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeTSma(SEncoder *pCoder, const STSma *pSma) {
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pSma->version));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pSma->intervalUnit));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pSma->slidingUnit));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pSma->timezoneInt));
  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pSma->dstVgId));
  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pSma->indexName));
  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pSma->exprLen));
  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pSma->tagsFilterLen));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pSma->indexUid));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pSma->tableUid));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pSma->dstTbUid));
  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pSma->dstTbName));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pSma->interval));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pSma->offset));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pSma->sliding));
  if (pSma->exprLen > 0) {
    TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pSma->expr));
  }
  if (pSma->tagsFilterLen > 0) {
    TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pSma->tagsFilter));
  }

  TAOS_CHECK_RETURN(tEncodeSSchemaWrapper(pCoder, &pSma->schemaRow));
  TAOS_CHECK_RETURN(tEncodeSSchemaWrapper(pCoder, &pSma->schemaTag));

  return 0;
}

int32_t tDecodeTSma(SDecoder *pCoder, STSma *pSma, bool deepCopy) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pSma->version));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pSma->intervalUnit));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pSma->slidingUnit));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pSma->timezoneInt));
  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pSma->dstVgId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pCoder, pSma->indexName));
  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pSma->exprLen));
  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pSma->tagsFilterLen));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSma->indexUid));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSma->tableUid));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSma->dstTbUid));
  if (deepCopy) {
    TAOS_CHECK_EXIT(tDecodeCStrAlloc(pCoder, &pSma->dstTbName));
  } else {
    TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pSma->dstTbName));
  }

  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSma->interval));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSma->offset));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSma->sliding));
  if (pSma->exprLen > 0) {
    if (deepCopy) {
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(pCoder, &pSma->expr));
    } else {
      TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pSma->expr));
    }
  } else {
    pSma->expr = NULL;
  }
  if (pSma->tagsFilterLen > 0) {
    if (deepCopy) {
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(pCoder, &pSma->tagsFilter));
    } else {
      TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pSma->tagsFilter));
    }
  } else {
    pSma->tagsFilter = NULL;
  }
  // only needed in dstVgroup
  TAOS_CHECK_EXIT(tDecodeSSchemaWrapperEx(pCoder, &pSma->schemaRow));
  TAOS_CHECK_EXIT(tDecodeSSchemaWrapperEx(pCoder, &pSma->schemaTag));

_exit:
  return code;
}

int32_t tEncodeSVCreateTSmaReq(SEncoder *pCoder, const SVCreateTSmaReq *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeTSma(pCoder, pReq));
  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVCreateTSmaReq(SDecoder *pCoder, SVCreateTSmaReq *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeTSma(pCoder, pReq, false));
  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropTSmaReq(SEncoder *pCoder, const SVDropTSmaReq *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->indexUid));
  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pReq->indexName));

  tEndEncode(pCoder);
  return 0;
}

int32_t tSerializeSVDeleteReq(void *buf, int32_t bufLen, SVDeleteReq *pReq) {
  int32_t code = 0;
  int32_t lino;
  int32_t headLen = sizeof(SMsgHead);
  if (buf != NULL) {
    buf = (char *)buf + headLen;
    bufLen -= headLen;
  }

  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->sId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->queryId));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->taskId));
  TAOS_CHECK_EXIT(tEncodeU32(&encoder, pReq->sqlLen));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->sql));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, pReq->msg, pReq->phyLen));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->source));
  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pReq->clientId));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tEncoderClear(&encoder);
    return code;
  } else {
    int32_t tlen = encoder.pos;
    tEncoderClear(&encoder);

    if (buf != NULL) {
      SMsgHead *pHead = (SMsgHead *)((char *)buf - headLen);
      pHead->vgId = htonl(pReq->header.vgId);
      pHead->contLen = htonl(tlen + headLen);
    }

    return tlen + headLen;
  }
}

int32_t tDeserializeSVDeleteReq(void *buf, int32_t bufLen, SVDeleteReq *pReq) {
  int32_t   code = 0;
  int32_t   lino;
  int32_t   headLen = sizeof(SMsgHead);
  SMsgHead *pHead = buf;
  pHead->vgId = pReq->header.vgId;
  pHead->contLen = pReq->header.contLen;

  SDecoder decoder = {0};
  tDecoderInit(&decoder, (char *)buf + headLen, bufLen - headLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->sId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->queryId));
  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->taskId));
  TAOS_CHECK_EXIT(tDecodeU32(&decoder, &pReq->sqlLen));
  pReq->sql = taosMemoryCalloc(1, pReq->sqlLen + 1);
  if (NULL == pReq->sql) {
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->sql));
  uint64_t msgLen = 0;
  TAOS_CHECK_EXIT(tDecodeBinaryAlloc(&decoder, (void **)&pReq->msg, &msgLen));
  pReq->phyLen = msgLen;

  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->source));
  }
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pReq->clientId));
  } else {
    pReq->clientId = 0;
  }
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return 0;
}

int32_t tEncodeSVDeleteRsp(SEncoder *pCoder, const SVDeleteRsp *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->affectedRows));
  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDeleteRsp(SDecoder *pCoder, SVDeleteRsp *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pReq->affectedRows));
  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSRSmaParam(SEncoder *pCoder, const SRSmaParam *pRSmaParam) {
  int32_t code = 0;
  int32_t lino;
  for (int32_t i = 0; i < 2; ++i) {
    TAOS_CHECK_EXIT(tEncodeI64v(pCoder, pRSmaParam->maxdelay[i]));
    TAOS_CHECK_EXIT(tEncodeI64v(pCoder, pRSmaParam->watermark[i]));
    TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pRSmaParam->qmsgLen[i]));
    if (pRSmaParam->qmsgLen[i] > 0) {
      TAOS_CHECK_EXIT(tEncodeBinary(pCoder, pRSmaParam->qmsg[i], (uint64_t)pRSmaParam->qmsgLen[i]));
    }
  }

_exit:
  return code;
}

int32_t tDecodeSRSmaParam(SDecoder *pCoder, SRSmaParam *pRSmaParam) {
  int32_t code = 0;
  int32_t lino;
  for (int32_t i = 0; i < 2; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64v(pCoder, &pRSmaParam->maxdelay[i]));
    TAOS_CHECK_EXIT(tDecodeI64v(pCoder, &pRSmaParam->watermark[i]));
    TAOS_CHECK_EXIT(tDecodeI32v(pCoder, &pRSmaParam->qmsgLen[i]));
    if (pRSmaParam->qmsgLen[i] > 0) {
      TAOS_CHECK_EXIT(tDecodeBinary(pCoder, (uint8_t **)&pRSmaParam->qmsg[i], NULL));  // qmsgLen contains len of '\0'
    } else {
      pRSmaParam->qmsg[i] = NULL;
    }
  }

_exit:
  return code;
}

int32_t tEncodeSColRefWrapper(SEncoder *pCoder, const SColRefWrapper *pWrapper) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pWrapper->nCols));
  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pWrapper->version));
  for (int32_t i = 0; i < pWrapper->nCols; i++) {
    SColRef *p = &pWrapper->pColRef[i];
    TAOS_CHECK_EXIT(tEncodeI8(pCoder, p->hasRef));
    TAOS_CHECK_EXIT(tEncodeI16v(pCoder, p->id));
    if (p->hasRef) {
      TAOS_CHECK_EXIT(tEncodeCStr(pCoder, p->refDbName));
      TAOS_CHECK_EXIT(tEncodeCStr(pCoder, p->refTableName));
      TAOS_CHECK_EXIT(tEncodeCStr(pCoder, p->refColName));
    }
  }

_exit:
  return code;
}

int32_t tDecodeSColRefWrapperEx(SDecoder *pDecoder, SColRefWrapper *pWrapper, bool decoderMalloc) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pWrapper->nCols));
  TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pWrapper->version));

  pWrapper->pColRef = decoderMalloc ? (SColRef *)tDecoderMalloc(pDecoder, pWrapper->nCols * sizeof(SColRef)) : (SColRef *)taosMemoryCalloc(pWrapper->nCols, sizeof(SColRef));
  if (pWrapper->pColRef == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int i = 0; i < pWrapper->nCols; i++) {
    SColRef *p = &pWrapper->pColRef[i];
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, (int8_t *)&p->hasRef));
    TAOS_CHECK_EXIT(tDecodeI16v(pDecoder, &p->id));
    if (p->hasRef) {
      TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, p->refDbName));
      TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, p->refTableName));
      TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, p->refColName));
    }
  }

_exit:
  if (code) {
    taosMemoryFree(pWrapper->pColRef);
  }
  return code;
}

int32_t tEncodeSColCmprWrapper(SEncoder *pCoder, const SColCmprWrapper *pWrapper) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pWrapper->nCols));
  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pWrapper->version));
  for (int32_t i = 0; i < pWrapper->nCols; i++) {
    SColCmpr *p = &pWrapper->pColCmpr[i];
    TAOS_CHECK_EXIT(tEncodeI16v(pCoder, p->id));
    TAOS_CHECK_EXIT(tEncodeU32(pCoder, p->alg));
  }

_exit:
  return code;
}

int32_t tDecodeSColCmprWrapperEx(SDecoder *pDecoder, SColCmprWrapper *pWrapper) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pWrapper->nCols));
  TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pWrapper->version));

  pWrapper->pColCmpr = (SColCmpr *)tDecoderMalloc(pDecoder, pWrapper->nCols * sizeof(SColCmpr));
  if (pWrapper->pColCmpr == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int i = 0; i < pWrapper->nCols; i++) {
    SColCmpr *p = &pWrapper->pColCmpr[i];
    TAOS_CHECK_EXIT(tDecodeI16v(pDecoder, &p->id));
    TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &p->alg));
  }

_exit:
  if (code) {
    taosMemoryFree(pWrapper->pColCmpr);
  }
  return code;
}

static int32_t tEncodeSExtSchema(SEncoder *pCoder, const SExtSchema *pExtSchema) {
  int32_t code = 0, lino;
  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pExtSchema->typeMod));

_exit:
  return code;
}

int32_t tDecodeSExtSchema(SDecoder *pCoder, SExtSchema *pExtSchema) {
  int32_t code = 0, lino;
  TAOS_CHECK_EXIT(tDecodeI32v(pCoder, &pExtSchema->typeMod));

_exit:
  return code;
}

static int32_t tEncodeSExtSchemas(SEncoder *pCoder, const SExtSchema *pExtSchemas, int32_t nCol) {
  int32_t code = 0, lino;
  for (int32_t i = 0; i < nCol; ++i) {
    TAOS_CHECK_EXIT(tEncodeSExtSchema(pCoder, pExtSchemas + i));
  }

_exit:
  return code;
}

static int32_t tDecodeSExtSchemas(SDecoder *pCoder, SExtSchema **ppExtSchema, int32_t nCol) {
  int32_t code = 0, lino;
  *ppExtSchema = tDecoderMalloc(pCoder, sizeof(SExtSchema) * nCol);
  if (!*ppExtSchema) TAOS_CHECK_EXIT(terrno);
  for (int32_t i = 0; i < nCol; ++i) {
    TAOS_CHECK_EXIT(tDecodeSExtSchema(pCoder, (*ppExtSchema) + i));
  }

_exit:
  return code;
}

int tEncodeSVCreateStbReq(SEncoder *pCoder, const SVCreateStbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pCoder));

  TAOS_CHECK_EXIT(tEncodeCStr(pCoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pReq->suid));
  TAOS_CHECK_EXIT(tEncodeI8(pCoder, pReq->rollup));
  TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pCoder, &pReq->schemaRow));
  TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pCoder, &pReq->schemaTag));
  if (pReq->rollup) {
    TAOS_CHECK_EXIT(tEncodeSRSmaParam(pCoder, &pReq->rsmaParam));
  }

  TAOS_CHECK_EXIT(tEncodeI32(pCoder, pReq->alterOriDataLen));
  if (pReq->alterOriDataLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(pCoder, pReq->alterOriData, pReq->alterOriDataLen));
  }
  TAOS_CHECK_EXIT(tEncodeI8(pCoder, pReq->source));

  TAOS_CHECK_EXIT(tEncodeI8(pCoder, pReq->colCmpred));
  TAOS_CHECK_EXIT(tEncodeSColCmprWrapper(pCoder, &pReq->colCmpr));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pReq->keep));
  if (pReq->pExtSchemas) {
    TAOS_CHECK_EXIT(tEncodeI8(pCoder, 1));
    TAOS_CHECK_EXIT(tEncodeSExtSchemas(pCoder, pReq->pExtSchemas, pReq->schemaRow.nCols));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(pCoder, 0));
  }
  TAOS_CHECK_EXIT(tEncodeI8(pCoder, pReq->virtualStb));
  tEndEncode(pCoder);

_exit:
  return code;
}

int tDecodeSVCreateStbReq(SDecoder *pCoder, SVCreateStbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pCoder));

  TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pReq->name));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->suid));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->rollup));
  TAOS_CHECK_EXIT(tDecodeSSchemaWrapperEx(pCoder, &pReq->schemaRow));
  TAOS_CHECK_EXIT(tDecodeSSchemaWrapperEx(pCoder, &pReq->schemaTag));
  if (pReq->rollup) {
    TAOS_CHECK_EXIT(tDecodeSRSmaParam(pCoder, &pReq->rsmaParam));
  }

  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pReq->alterOriDataLen));
  if (pReq->alterOriDataLen > 0) {
    TAOS_CHECK_EXIT(tDecodeBinary(pCoder, (uint8_t **)&pReq->alterOriData, NULL));
  }
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->source));

    if (!tDecodeIsEnd(pCoder)) {
      TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->colCmpred));
    }
    if (!tDecodeIsEnd(pCoder)) {
      TAOS_CHECK_EXIT(tDecodeSColCmprWrapperEx(pCoder, &pReq->colCmpr));
    }
    if (!tDecodeIsEnd(pCoder)) {
      TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->keep));
    }
    if (!tDecodeIsEnd(pCoder)) {
      int8_t hasExtSchema = 0;
      TAOS_CHECK_EXIT(tDecodeI8(pCoder, &hasExtSchema));
      if (hasExtSchema) {
        TAOS_CHECK_EXIT(tDecodeSExtSchemas(pCoder, &pReq->pExtSchemas, pReq->schemaRow.nCols));
      }
    }
  }
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->virtualStb));
  }
  tEndDecode(pCoder);

_exit:
  return code;
}

int tEncodeSVCreateTbReq(SEncoder *pCoder, const SVCreateTbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pCoder));

  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pReq->flags));
  TAOS_CHECK_EXIT(tEncodeCStr(pCoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pReq->uid));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pReq->btime));
  TAOS_CHECK_EXIT(tEncodeI32(pCoder, pReq->ttl));
  TAOS_CHECK_EXIT(tEncodeI8(pCoder, pReq->type));
  TAOS_CHECK_EXIT(tEncodeI32(pCoder, pReq->commentLen));
  if (pReq->commentLen > 0) {
    TAOS_CHECK_EXIT(tEncodeCStr(pCoder, pReq->comment));
  }

  if (pReq->type == TSDB_CHILD_TABLE || pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
    TAOS_CHECK_EXIT(tEncodeCStr(pCoder, pReq->ctb.stbName));
    TAOS_CHECK_EXIT(tEncodeU8(pCoder, pReq->ctb.tagNum));
    TAOS_CHECK_EXIT(tEncodeI64(pCoder, pReq->ctb.suid));
    TAOS_CHECK_EXIT(tEncodeTag(pCoder, (const STag *)pReq->ctb.pTag));
    int32_t len = taosArrayGetSize(pReq->ctb.tagName);
    TAOS_CHECK_EXIT(tEncodeI32(pCoder, len));
    for (int32_t i = 0; i < len; i++) {
      char *name = taosArrayGet(pReq->ctb.tagName, i);
      TAOS_CHECK_EXIT(tEncodeCStr(pCoder, name));
    }
  } else if (pReq->type == TSDB_NORMAL_TABLE || pReq->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pCoder, &pReq->ntb.schemaRow));
  } else {
    return TSDB_CODE_INVALID_MSG;
  }
  // ENCODESQL

  TAOS_CHECK_EXIT(tEncodeI32(pCoder, pReq->sqlLen));
  if (pReq->sqlLen > 0) {
    TAOS_CHECK_EXIT(tEncodeBinary(pCoder, pReq->sql, pReq->sqlLen));
  }
  // Encode Column Options: encode compress level
  if (pReq->type == TSDB_SUPER_TABLE || pReq->type == TSDB_NORMAL_TABLE) {
    TAOS_CHECK_EXIT(tEncodeSColCmprWrapper(pCoder, &pReq->colCmpr));
  }
  if (pReq->type == TSDB_VIRTUAL_NORMAL_TABLE || pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
    TAOS_CHECK_EXIT(tEncodeSColRefWrapper(pCoder, &pReq->colRef));
  }
  if (pReq->pExtSchemas) {
    TAOS_CHECK_EXIT(tEncodeI8(pCoder, 1));
    TAOS_CHECK_EXIT(tEncodeSExtSchemas(pCoder, pReq->pExtSchemas, pReq->ntb.schemaRow.nCols));
  } else {
    TAOS_CHECK_EXIT(tEncodeI8(pCoder, 0));
  }

  tEndEncode(pCoder);
_exit:
  return code;
}

int tDecodeSVCreateTbReq(SDecoder *pCoder, SVCreateTbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pCoder));

  TAOS_CHECK_EXIT(tDecodeI32v(pCoder, &pReq->flags));
  TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pReq->name));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->uid));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->btime));
  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pReq->ttl));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->type));
  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pReq->commentLen));
  if (pReq->commentLen > 0) {
    pReq->comment = taosMemoryMalloc(pReq->commentLen + 1);
    if (pReq->comment == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeCStrTo(pCoder, pReq->comment));
  }

  if (pReq->type == TSDB_CHILD_TABLE || pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
    TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pReq->ctb.stbName));
    TAOS_CHECK_EXIT(tDecodeU8(pCoder, &pReq->ctb.tagNum));
    TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->ctb.suid));
    TAOS_CHECK_EXIT(tDecodeTag(pCoder, (STag **)&pReq->ctb.pTag));
    int32_t len = 0;
    TAOS_CHECK_EXIT(tDecodeI32(pCoder, &len));
    pReq->ctb.tagName = taosArrayInit(len, TSDB_COL_NAME_LEN);
    if (pReq->ctb.tagName == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < len; i++) {
      char  name[TSDB_COL_NAME_LEN] = {0};
      char *tmp = NULL;
      TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &tmp));
      tstrncpy(name, tmp, TSDB_COL_NAME_LEN);
      if (taosArrayPush(pReq->ctb.tagName, name) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  } else if (pReq->type == TSDB_NORMAL_TABLE || pReq->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    TAOS_CHECK_EXIT(tDecodeSSchemaWrapperEx(pCoder, &pReq->ntb.schemaRow));
  } else {
    return TSDB_CODE_INVALID_MSG;
  }

  // DECODESQL
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pReq->sqlLen));
    if (pReq->sqlLen > 0) {
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pCoder, (void **)&pReq->sql, NULL));
    }
    if (pReq->type == TSDB_NORMAL_TABLE || pReq->type == TSDB_SUPER_TABLE) {
      if (!tDecodeIsEnd(pCoder)) {
        TAOS_CHECK_EXIT(tDecodeSColCmprWrapperEx(pCoder, &pReq->colCmpr));
      }
    } else if (pReq->type == TSDB_VIRTUAL_NORMAL_TABLE || pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
      if (!tDecodeIsEnd(pCoder)) {
        TAOS_CHECK_EXIT(tDecodeSColRefWrapperEx(pCoder, &pReq->colRef, true));
      }
    }

    if (!tDecodeIsEnd(pCoder)) {
      int8_t hasExtSchema = 0;
      TAOS_CHECK_EXIT(tDecodeI8(pCoder, &hasExtSchema));
      if (hasExtSchema) {
        TAOS_CHECK_EXIT(tDecodeSExtSchemas(pCoder, &pReq->pExtSchemas, pReq->ntb.schemaRow.nCols));
      }
    }
  }

  tEndDecode(pCoder);
_exit:
  return code;
}

void tDestroySVCreateTbReq(SVCreateTbReq *pReq, int32_t flags) {
  if (pReq == NULL) return;

  if (flags & TSDB_MSG_FLG_ENCODE) {
    // TODO
  } else if (flags & TSDB_MSG_FLG_DECODE) {
    taosMemoryFreeClear(pReq->comment);

    if (pReq->type == TSDB_CHILD_TABLE || pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
      taosArrayDestroy(pReq->ctb.tagName);
      pReq->ctb.tagName = NULL;
    } else if (pReq->type == TSDB_NORMAL_TABLE || pReq->type == TSDB_VIRTUAL_NORMAL_TABLE) {
      taosMemoryFreeClear(pReq->ntb.schemaRow.pSchema);
    }
  }

  taosMemoryFreeClear(pReq->colCmpr.pColCmpr);
  taosMemoryFreeClear(pReq->colRef.pColRef);
  taosMemoryFreeClear(pReq->sql);
}

void tDestroySVSubmitCreateTbReq(SVCreateTbReq *pReq, int32_t flags) {
  if (pReq == NULL) return;

  if (flags & TSDB_MSG_FLG_ENCODE) {
    // TODO
  } else if (flags & TSDB_MSG_FLG_DECODE) {
    taosMemoryFreeClear(pReq->comment);

    if (pReq->type == TSDB_CHILD_TABLE || pReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
      taosArrayDestroy(pReq->ctb.tagName);
      pReq->ctb.tagName = NULL;
    }
  }

  taosMemoryFreeClear(pReq->colRef.pColRef);
  taosMemoryFreeClear(pReq->sql);
}

int tEncodeSVCreateTbBatchReq(SEncoder *pCoder, const SVCreateTbBatchReq *pReq) {
  int32_t nReq = taosArrayGetSize(pReq->pArray);

  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, nReq));
  for (int iReq = 0; iReq < nReq; iReq++) {
    TAOS_CHECK_RETURN(tEncodeSVCreateTbReq(pCoder, (SVCreateTbReq *)taosArrayGet(pReq->pArray, iReq)));
  }

  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pReq->source));

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbBatchReq(SDecoder *pCoder, SVCreateTbBatchReq *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));

  TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pReq->nReqs));
  pReq->pReqs = (SVCreateTbReq *)tDecoderMalloc(pCoder, sizeof(SVCreateTbReq) * pReq->nReqs);
  if (pReq->pReqs == NULL) {
    TAOS_CHECK_RETURN(terrno);
  }
  for (int iReq = 0; iReq < pReq->nReqs; iReq++) {
    TAOS_CHECK_RETURN(tDecodeSVCreateTbReq(pCoder, pReq->pReqs + iReq));
  }

  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pReq->source));
  }

  tEndDecode(pCoder);
  return 0;
}

void tDeleteSVCreateTbBatchReq(SVCreateTbBatchReq *pReq) {
  for (int32_t iReq = 0; iReq < pReq->nReqs; iReq++) {
    SVCreateTbReq *pCreateReq = pReq->pReqs + iReq;
    taosMemoryFreeClear(pCreateReq->sql);
    taosMemoryFreeClear(pCreateReq->comment);
    if (pCreateReq->type == TSDB_CHILD_TABLE || pCreateReq->type == TSDB_VIRTUAL_CHILD_TABLE) {
      taosArrayDestroy(pCreateReq->ctb.tagName);
      pCreateReq->ctb.tagName = NULL;
    }
  }
}

int tEncodeSVCreateTbRsp(SEncoder *pCoder, const SVCreateTbRsp *pRsp) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));

  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pRsp->code));
  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pRsp->pMeta ? 1 : 0));
  if (pRsp->pMeta) {
    TAOS_CHECK_RETURN(tEncodeSTableMetaRsp(pCoder, pRsp->pMeta));
  }

  tEndEncode(pCoder);
  return 0;
}

int tDecodeSVCreateTbRsp(SDecoder *pCoder, SVCreateTbRsp *pRsp) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));

  TAOS_CHECK_RETURN(tDecodeI32(pCoder, &pRsp->code));

  int32_t meta = 0;
  TAOS_CHECK_RETURN(tDecodeI32(pCoder, &meta));
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) {
      TAOS_CHECK_RETURN(terrno);
    }
    TAOS_CHECK_RETURN(tDecodeSTableMetaRsp(pCoder, pRsp->pMeta));
  } else {
    pRsp->pMeta = NULL;
  }

  tEndDecode(pCoder);
  return 0;
}

void tFreeSVCreateTbRsp(void *param) {
  if (NULL == param) {
    return;
  }

  SVCreateTbRsp *pRsp = (SVCreateTbRsp *)param;
  if (pRsp->pMeta) {
    taosMemoryFree(pRsp->pMeta->pSchemas);
    taosMemoryFree(pRsp->pMeta->pSchemaExt);
    taosMemoryFree(pRsp->pMeta->pColRefs);
    taosMemoryFree(pRsp->pMeta);
  }
}

// TDMT_VND_DROP_TABLE =================
static int32_t tEncodeSVDropTbReq(SEncoder *pCoder, const SVDropTbReq *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pReq->name));
  TAOS_CHECK_RETURN(tEncodeU64(pCoder, pReq->suid));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->uid));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pReq->igNotExists));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pReq->isVirtual));

  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSVDropTbReq(SDecoder *pCoder, SVDropTbReq *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pReq->name));
  TAOS_CHECK_RETURN(tDecodeU64(pCoder, &pReq->suid));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pReq->uid));
  TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pReq->igNotExists));
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_RETURN(tDecodeI8(pCoder, &pReq->isVirtual));
  }

  tEndDecode(pCoder);
  return 0;
}

static int32_t tEncodeSVDropTbRsp(SEncoder *pCoder, const SVDropTbRsp *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pReq->code));
  tEndEncode(pCoder);
  return 0;
}

static int32_t tDecodeSVDropTbRsp(SDecoder *pCoder, SVDropTbRsp *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI32(pCoder, &pReq->code));
  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropTbBatchReq(SEncoder *pCoder, const SVDropTbBatchReq *pReq) {
  int32_t      nReqs = taosArrayGetSize(pReq->pArray);
  SVDropTbReq *pDropTbReq;

  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, nReqs));
  for (int iReq = 0; iReq < nReqs; iReq++) {
    pDropTbReq = (SVDropTbReq *)taosArrayGet(pReq->pArray, iReq);
    TAOS_CHECK_RETURN(tEncodeSVDropTbReq(pCoder, pDropTbReq));
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropTbBatchReq(SDecoder *pCoder, SVDropTbBatchReq *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pReq->nReqs));
  pReq->pReqs = (SVDropTbReq *)tDecoderMalloc(pCoder, sizeof(SVDropTbReq) * pReq->nReqs);
  if (pReq->pReqs == NULL) {
    TAOS_CHECK_RETURN(terrno);
  }
  for (int iReq = 0; iReq < pReq->nReqs; iReq++) {
    TAOS_CHECK_RETURN(tDecodeSVDropTbReq(pCoder, pReq->pReqs + iReq));
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropTbBatchRsp(SEncoder *pCoder, const SVDropTbBatchRsp *pRsp) {
  int32_t nRsps = taosArrayGetSize(pRsp->pArray);
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI32v(pCoder, nRsps));
  for (int iRsp = 0; iRsp < nRsps; iRsp++) {
    TAOS_CHECK_RETURN(tEncodeSVDropTbRsp(pCoder, (SVDropTbRsp *)taosArrayGet(pRsp->pArray, iRsp)));
  }

  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropTbBatchRsp(SDecoder *pCoder, SVDropTbBatchRsp *pRsp) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeI32v(pCoder, &pRsp->nRsps));
  pRsp->pRsps = (SVDropTbRsp *)tDecoderMalloc(pCoder, sizeof(SVDropTbRsp) * pRsp->nRsps);
  if (pRsp->pRsps == NULL) {
    TAOS_CHECK_RETURN(terrno);
  }
  for (int iRsp = 0; iRsp < pRsp->nRsps; iRsp++) {
    TAOS_CHECK_RETURN(tDecodeSVDropTbRsp(pCoder, pRsp->pRsps + iRsp));
  }

  tEndDecode(pCoder);
  return 0;
}

int32_t tEncodeSVDropStbReq(SEncoder *pCoder, const SVDropStbReq *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pReq->name));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->suid));
  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSVDropStbReq(SDecoder *pCoder, SVDropStbReq *pReq) {
  TAOS_CHECK_RETURN(tStartDecode(pCoder));
  TAOS_CHECK_RETURN(tDecodeCStr(pCoder, &pReq->name));
  TAOS_CHECK_RETURN(tDecodeI64(pCoder, &pReq->suid));
  tEndDecode(pCoder);
  return 0;
}

static int32_t tEncodeSSubmitBlkRsp(SEncoder *pEncoder, const SSubmitBlkRsp *pBlock) {
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tStartEncode(pEncoder));

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pBlock->code));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pBlock->uid));
  if (pBlock->tblFName) {
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pBlock->tblFName));
  } else {
    TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, ""));
  }
  TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pBlock->numOfRows));
  TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pBlock->affectedRows));
  TAOS_CHECK_EXIT(tEncodeI64v(pEncoder, pBlock->sver));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pBlock->pMeta ? 1 : 0));
  if (pBlock->pMeta) {
    TAOS_CHECK_EXIT(tEncodeSTableMetaRsp(pEncoder, pBlock->pMeta));
  }

  tEndEncode(pEncoder);
_exit:
  return code;
}

void tFreeSSubmitRsp(SSubmitRsp *pRsp) {
  if (NULL == pRsp) return;

  if (pRsp->pBlocks) {
    for (int32_t i = 0; i < pRsp->nBlocks; ++i) {
      SSubmitBlkRsp *sRsp = pRsp->pBlocks + i;
      taosMemoryFree(sRsp->tblFName);
      tFreeSTableMetaRsp(sRsp->pMeta);
      taosMemoryFree(sRsp->pMeta);
    }

    taosMemoryFree(pRsp->pBlocks);
  }

  taosMemoryFree(pRsp);
}

int32_t tEncodeSVAlterTbReq(SEncoder *pEncoder, const SVAlterTbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));

  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->tbName));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->action));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->colId));
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->type));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->flags));
      TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pReq->bytes));
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->colModType));
      TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pReq->colModBytes));
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colNewName));
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->tagName));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->isNull));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->tagType));
      if (!pReq->isNull) {
        TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pReq->pTagVal, pReq->nTagVal));
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL: {
      int32_t nTags = taosArrayGetSize(pReq->pMultiTag);
      TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, nTags));
      for (int32_t i = 0; i < nTags; i++) {
        SMultiTagUpateVal *pTag = taosArrayGet(pReq->pMultiTag, i);
        TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pTag->colId));
        TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTag->tagName));
        TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTag->isNull));
        TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTag->tagType));
        if (!pTag->isNull) {
          TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pTag->pTagVal, pTag->nTagVal));
        }
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->updateTTL));
      if (pReq->updateTTL) {
        TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pReq->newTTL));
      }
      TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pReq->newCommentLen));
      if (pReq->newCommentLen > 0) {
        TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->newComment));
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeU32(pEncoder, pReq->compress));
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->type));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->flags));
      TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pReq->bytes));
      TAOS_CHECK_EXIT(tEncodeU32(pEncoder, pReq->compress));
      break;
    case TSDB_ALTER_TABLE_ALTER_COLUMN_REF:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->refDbName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->refTbName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->refColName));
      break;
    case TSDB_ALTER_TABLE_REMOVE_COLUMN_REF:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF:
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->colName));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->type));
      TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->flags));
      TAOS_CHECK_EXIT(tEncodeI32v(pEncoder, pReq->bytes));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->refDbName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->refTbName));
      TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pReq->refColName));
      break;
    default:
      break;
  }
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->ctimeMs));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->source));
  if (pReq->action == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION || pReq->action == TSDB_ALTER_TABLE_ADD_COLUMN) {
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pReq->typeMod));
  }

  tEndEncode(pEncoder);
_exit:
  return code;
}

static int32_t tDecodeSVAlterTbReqCommon(SDecoder *pDecoder, SVAlterTbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->tbName));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->action));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->colId));
  switch (pReq->action) {
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->type));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->flags));
      TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pReq->bytes));
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->colModType));
      TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pReq->colModBytes));
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colNewName));
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_VAL:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->tagName));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->isNull));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->tagType));
      if (!pReq->isNull) {
        TAOS_CHECK_EXIT(tDecodeBinary(pDecoder, &pReq->pTagVal, &pReq->nTagVal));
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL: {
      int32_t nTags;
      TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &nTags));
      pReq->pMultiTag = taosArrayInit(nTags, sizeof(SMultiTagUpateVal));
      if (pReq->pMultiTag == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      for (int32_t i = 0; i < nTags; i++) {
        SMultiTagUpateVal tag;
        TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &tag.colId));
        TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &tag.tagName));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &tag.isNull));
        TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &tag.tagType));
        if (!tag.isNull) {
          TAOS_CHECK_EXIT(tDecodeBinary(pDecoder, &tag.pTagVal, &tag.nTagVal));
        }
        if (taosArrayPush(pReq->pMultiTag, &tag) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
      break;
    }
    case TSDB_ALTER_TABLE_UPDATE_OPTIONS:
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->updateTTL));
      if (pReq->updateTTL) {
        TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pReq->newTTL));
      }
      TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pReq->newCommentLen));
      if (pReq->newCommentLen > 0) {
        TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->newComment));
      }
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &pReq->compress));
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->type));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->flags));
      TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pReq->bytes));
      TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &pReq->compress));
      break;
    case TSDB_ALTER_TABLE_ALTER_COLUMN_REF:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->refDbName));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->refTbName));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->refColName));
      break;
    case TSDB_ALTER_TABLE_REMOVE_COLUMN_REF:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF:
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->colName));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->type));
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->flags));
      TAOS_CHECK_EXIT(tDecodeI32v(pDecoder, &pReq->bytes));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->refDbName));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->refTbName));
      TAOS_CHECK_EXIT(tDecodeCStr(pDecoder, &pReq->refColName));
      break;
    default:
      break;
  }
_exit:
  return code;
}

int32_t tDecodeSVAlterTbReq(SDecoder *pDecoder, SVAlterTbReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeSVAlterTbReqCommon(pDecoder, pReq));

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->ctimeMs));
  }
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->source));
  }
  if (pReq->action == TSDB_ALTER_TABLE_ADD_COLUMN || pReq->action == TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION) {
    if (!tDecodeIsEnd(pDecoder)) {
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->typeMod));
    }
  }

  tEndDecode(pDecoder);
_exit:
  return code;
}

int32_t tDecodeSVAlterTbReqSetCtime(SDecoder *pDecoder, SVAlterTbReq *pReq, int64_t ctimeMs) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeSVAlterTbReqCommon(pDecoder, pReq));

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    *(int64_t *)(pDecoder->data + pDecoder->pos) = ctimeMs;
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->ctimeMs));
  }

  tEndDecode(pDecoder);
_exit:
  return code;
}

void tfreeMultiTagUpateVal(void *val) {
  SMultiTagUpateVal *pTag = val;
  taosMemoryFree(pTag->tagName);
  for (int i = 0; i < taosArrayGetSize(pTag->pTagArray); ++i) {
    STagVal *p = (STagVal *)taosArrayGet(pTag->pTagArray, i);
    if (IS_VAR_DATA_TYPE(p->type)) {
      taosMemoryFreeClear(p->pData);
    }
  }

  taosArrayDestroy(pTag->pTagArray);
}
int32_t tEncodeSVAlterTbRsp(SEncoder *pEncoder, const SVAlterTbRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->code));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->pMeta ? 1 : 0));
  if (pRsp->pMeta) {
    TAOS_CHECK_EXIT(tEncodeSTableMetaRsp(pEncoder, pRsp->pMeta));
  }
  tEndEncode(pEncoder);
_exit:
  return code;
}

int32_t tDecodeSVAlterTbRsp(SDecoder *pDecoder, SVAlterTbRsp *pRsp) {
  int32_t meta = 0;
  int32_t code = 0;
  int32_t lino;
  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->code));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &meta));
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSTableMetaRsp(pDecoder, pRsp->pMeta));
  }
  tEndDecode(pDecoder);
_exit:
  return code;
}

int32_t tEncodeSMAlterStbRsp(SEncoder *pEncoder, const SMAlterStbRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->pMeta->pSchemas ? 1 : 0));
  if (pRsp->pMeta->pSchemas) {
    TAOS_CHECK_EXIT(tEncodeSTableMetaRsp(pEncoder, pRsp->pMeta));
  }
  tEndEncode(pEncoder);
_exit:
  return code;
}

int32_t tDecodeSMAlterStbRsp(SDecoder *pDecoder, SMAlterStbRsp *pRsp) {
  int32_t meta = 0;
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &meta));
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSTableMetaRsp(pDecoder, pRsp->pMeta));
  }
  tEndDecode(pDecoder);
_exit:
  return code;
}

void tFreeSMAlterStbRsp(SMAlterStbRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  if (pRsp->pMeta) {
    taosMemoryFree(pRsp->pMeta->pSchemas);
    taosMemoryFree(pRsp->pMeta->pSchemaExt);
    taosMemoryFree(pRsp->pMeta->pColRefs);
    taosMemoryFree(pRsp->pMeta);
  }
}

int32_t tEncodeSMCreateStbRsp(SEncoder *pEncoder, const SMCreateStbRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pEncoder));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->pMeta->pSchemas ? 1 : 0));
  if (pRsp->pMeta->pSchemas) {
    TAOS_CHECK_EXIT(tEncodeSTableMetaRsp(pEncoder, pRsp->pMeta));
  }
  tEndEncode(pEncoder);

_exit:
  return code;
}

int32_t tDecodeSMCreateStbRsp(SDecoder *pDecoder, SMCreateStbRsp *pRsp) {
  int32_t meta = 0;
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pDecoder));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &meta));
  if (meta) {
    pRsp->pMeta = taosMemoryCalloc(1, sizeof(STableMetaRsp));
    if (NULL == pRsp->pMeta) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSTableMetaRsp(pDecoder, pRsp->pMeta));
  }
  tEndDecode(pDecoder);

  return code;

_exit:
  tFreeSTableMetaRsp(pRsp->pMeta);
  taosMemoryFreeClear(pRsp->pMeta);
  return code;
}

void tFreeSMCreateStbRsp(SMCreateStbRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  if (pRsp->pMeta) {
    taosMemoryFree(pRsp->pMeta->pSchemas);
    taosMemoryFree(pRsp->pMeta->pSchemaExt);
    taosMemoryFree(pRsp->pMeta->pColRefs);
    taosMemoryFree(pRsp->pMeta);
  }
}

int32_t tEncodeSTqOffsetVal(SEncoder *pEncoder, const STqOffsetVal *pOffsetVal) {
  int32_t code = 0;
  int32_t lino;

  int8_t type = pOffsetVal->type < 0 ? pOffsetVal->type : (TQ_OFFSET_VERSION << 4) | pOffsetVal->type;
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, type));
  if (pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_DATA || pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_META) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pOffsetVal->uid));
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pOffsetVal->ts));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pOffsetVal->primaryKey.type));
    if (IS_VAR_DATA_TYPE(pOffsetVal->primaryKey.type)) {
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pOffsetVal->primaryKey.pData, pOffsetVal->primaryKey.nData));
    } else {
      TAOS_CHECK_EXIT(tEncodeI64(pEncoder, VALUE_GET_TRIVIAL_DATUM(&pOffsetVal->primaryKey)));
    }

  } else if (pOffsetVal->type == TMQ_OFFSET__LOG) {
    TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pOffsetVal->version));
  } else {
    // do nothing
  }
_exit:
  return code;
}

int32_t tDecodeSTqOffsetVal(SDecoder *pDecoder, STqOffsetVal *pOffsetVal) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pOffsetVal->type));
  int8_t offsetVersion = 0;
  if (pOffsetVal->type > 0) {
    offsetVersion = (pOffsetVal->type >> 4);
    pOffsetVal->type = pOffsetVal->type & 0x0F;
  }
  if (pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_DATA || pOffsetVal->type == TMQ_OFFSET__SNAPSHOT_META) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pOffsetVal->uid));
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pOffsetVal->ts));
    if (offsetVersion >= TQ_OFFSET_VERSION) {
      TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pOffsetVal->primaryKey.type));
      if (IS_VAR_DATA_TYPE(pOffsetVal->primaryKey.type)) {
        TAOS_CHECK_EXIT(
            tDecodeBinaryAlloc32(pDecoder, (void **)&pOffsetVal->primaryKey.pData, &pOffsetVal->primaryKey.nData));
      } else {
        TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &VALUE_GET_TRIVIAL_DATUM(&pOffsetVal->primaryKey)));
      }
    }
  } else if (pOffsetVal->type == TMQ_OFFSET__LOG) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pOffsetVal->version));
  } else {
    // do nothing
  }
_exit:
  return code;
}

void tFormatOffset(char *buf, int32_t maxLen, const STqOffsetVal *pVal) {
  if (pVal->type == TMQ_OFFSET__RESET_NONE) {
    (void)snprintf(buf, maxLen, "none");
  } else if (pVal->type == TMQ_OFFSET__RESET_EARLIEST) {
    (void)snprintf(buf, maxLen, "earliest");
  } else if (pVal->type == TMQ_OFFSET__RESET_LATEST) {
    (void)snprintf(buf, maxLen, "latest");
  } else if (pVal->type == TMQ_OFFSET__LOG) {
    (void)snprintf(buf, maxLen, "wal:%" PRId64, pVal->version);
  } else if (pVal->type == TMQ_OFFSET__SNAPSHOT_DATA || pVal->type == TMQ_OFFSET__SNAPSHOT_META) {
    if (IS_VAR_DATA_TYPE(pVal->primaryKey.type)) {
      char *tmp = taosMemoryCalloc(1, pVal->primaryKey.nData + 1);
      if (tmp == NULL) return;
      (void)memcpy(tmp, pVal->primaryKey.pData, pVal->primaryKey.nData);
      (void)snprintf(buf, maxLen, "tsdb:%" PRId64 "|%" PRId64 ",pk type:%d,val:%s", pVal->uid, pVal->ts,
                     pVal->primaryKey.type, tmp);
      taosMemoryFree(tmp);
    } else {
      (void)snprintf(buf, maxLen, "tsdb:%" PRId64 "|%" PRId64 ",pk type:%d,val:%" PRId64, pVal->uid, pVal->ts,
                     pVal->primaryKey.type, VALUE_GET_TRIVIAL_DATUM(&pVal->primaryKey));
    }
  }
}

bool tOffsetEqual(const STqOffsetVal *pLeft, const STqOffsetVal *pRight) {
  if (pLeft->type == pRight->type) {
    if (pLeft->type == TMQ_OFFSET__LOG) {
      return pLeft->version == pRight->version;
    } else if (pLeft->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      if (pLeft->primaryKey.type != 0) {
        if (pLeft->primaryKey.type != pRight->primaryKey.type) return false;
        if (tValueCompare(&pLeft->primaryKey, &pRight->primaryKey) != 0) return false;
      }
      return pLeft->uid == pRight->uid && pLeft->ts == pRight->ts;
    } else if (pLeft->type == TMQ_OFFSET__SNAPSHOT_META) {
      return pLeft->uid == pRight->uid;
    } else {
      uError("offset type:%d", pLeft->type);
    }
  }
  return false;
}

void tOffsetCopy(STqOffsetVal *pLeft, const STqOffsetVal *pRight) {
  tOffsetDestroy(pLeft);
  *pLeft = *pRight;
  if (IS_VAR_DATA_TYPE(pRight->primaryKey.type)) {
    pLeft->primaryKey.pData = taosMemoryMalloc(pRight->primaryKey.nData);
    if (pLeft->primaryKey.pData == NULL) {
      uError("failed to allocate memory for offset");
      return;
    }
    (void)memcpy(pLeft->primaryKey.pData, pRight->primaryKey.pData, pRight->primaryKey.nData);
  }
}

void tOffsetDestroy(void *param) {
  if (param == NULL) return;
  STqOffsetVal *pVal = (STqOffsetVal *)param;
  if (IS_VAR_DATA_TYPE(pVal->primaryKey.type)) {
    taosMemoryFreeClear(pVal->primaryKey.pData);
  }
}

void tDeleteSTqOffset(void *param) {
  if (param == NULL) return;
  STqOffset *pVal = (STqOffset *)param;
  tOffsetDestroy(&pVal->val);
}

int32_t tEncodeSTqOffset(SEncoder *pEncoder, const STqOffset *pOffset) {
  TAOS_CHECK_RETURN(tEncodeSTqOffsetVal(pEncoder, &pOffset->val));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pOffset->subKey));
  return 0;
}

int32_t tDecodeSTqOffset(SDecoder *pDecoder, STqOffset *pOffset) {
  TAOS_CHECK_RETURN(tDecodeSTqOffsetVal(pDecoder, &pOffset->val));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pOffset->subKey));
  return 0;
}

int32_t tEncodeMqVgOffset(SEncoder *pEncoder, const SMqVgOffset *pOffset) {
  TAOS_CHECK_RETURN(tEncodeSTqOffset(pEncoder, &pOffset->offset));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pOffset->consumerId));
  return 0;
}

int32_t tDecodeMqVgOffset(SDecoder *pDecoder, SMqVgOffset *pOffset) {
  TAOS_CHECK_RETURN(tDecodeSTqOffset(pDecoder, &pOffset->offset));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pOffset->consumerId));
  return 0;
}

int32_t tEncodeSTqCheckInfo(SEncoder *pEncoder, const STqCheckInfo *pInfo) {
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pInfo->topic));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pInfo->ntbUid));
  int32_t sz = taosArrayGetSize(pInfo->colIdList);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, sz));
  for (int32_t i = 0; i < sz; i++) {
    int16_t colId = *(int16_t *)taosArrayGet(pInfo->colIdList, i);
    TAOS_CHECK_RETURN(tEncodeI16(pEncoder, colId));
  }
  return pEncoder->pos;
}

int32_t tDecodeSTqCheckInfo(SDecoder *pDecoder, STqCheckInfo *pInfo) {
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pInfo->topic));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pInfo->ntbUid));
  int32_t sz = 0;
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &sz));
  pInfo->colIdList = taosArrayInit(sz, sizeof(int16_t));
  if (pInfo->colIdList == NULL) {
    TAOS_CHECK_RETURN(terrno);
  }
  for (int32_t i = 0; i < sz; i++) {
    int16_t colId = 0;
    TAOS_CHECK_RETURN(tDecodeI16(pDecoder, &colId));
    if (taosArrayPush(pInfo->colIdList, &colId) == NULL) {
      TAOS_CHECK_RETURN(terrno);
    }
  }
  return 0;
}
void tDeleteSTqCheckInfo(STqCheckInfo *pInfo) { taosArrayDestroy(pInfo->colIdList); }

int32_t tEncodeSMqRebVgReq(SEncoder *pCoder, const SMqRebVgReq *pReq) {
  TAOS_CHECK_RETURN(tStartEncode(pCoder));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->leftForVer));
  TAOS_CHECK_RETURN(tEncodeI32(pCoder, pReq->vgId));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->oldConsumerId));
  TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->newConsumerId));
  TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pReq->subKey));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pReq->subType));
  TAOS_CHECK_RETURN(tEncodeI8(pCoder, pReq->withMeta));

  if (pReq->subType == TOPIC_SUB_TYPE__COLUMN) {
    TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pReq->qmsg));
  } else if (pReq->subType == TOPIC_SUB_TYPE__TABLE) {
    TAOS_CHECK_RETURN(tEncodeI64(pCoder, pReq->suid));
    TAOS_CHECK_RETURN(tEncodeCStr(pCoder, pReq->qmsg));
  }
  tEndEncode(pCoder);
  return 0;
}

int32_t tDecodeSMqRebVgReq(SDecoder *pCoder, SMqRebVgReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartDecode(pCoder));

  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->leftForVer));

  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pReq->vgId));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->oldConsumerId));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->newConsumerId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pCoder, pReq->subKey));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->subType));
  TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pReq->withMeta));

  if (pReq->subType == TOPIC_SUB_TYPE__COLUMN) {
    TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pReq->qmsg));
  } else if (pReq->subType == TOPIC_SUB_TYPE__TABLE) {
    TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pReq->suid));
    if (!tDecodeIsEnd(pCoder)) {
      TAOS_CHECK_EXIT(tDecodeCStr(pCoder, &pReq->qmsg));
    }
  }

  tEndDecode(pCoder);
_exit:
  return code;
}

int32_t tEncodeDeleteRes(SEncoder *pCoder, const SDeleteRes *pRes) {
  int32_t nUid = taosArrayGetSize(pRes->uidList);
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeU64(pCoder, pRes->suid));
  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, nUid));
  for (int32_t iUid = 0; iUid < nUid; iUid++) {
    TAOS_CHECK_EXIT(tEncodeU64(pCoder, *(uint64_t *)taosArrayGet(pRes->uidList, iUid)));
  }
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pRes->skey));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pRes->ekey));
  TAOS_CHECK_EXIT(tEncodeI64v(pCoder, pRes->affectedRows));

  TAOS_CHECK_EXIT(tEncodeCStr(pCoder, pRes->tableFName));
  TAOS_CHECK_EXIT(tEncodeCStr(pCoder, pRes->tsColName));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pRes->ctimeMs));
  TAOS_CHECK_EXIT(tEncodeI8(pCoder, pRes->source));

_exit:
  return code;
}

int32_t tDecodeDeleteRes(SDecoder *pCoder, SDeleteRes *pRes) {
  int32_t  nUid;
  uint64_t uid;
  int32_t  code = 0;
  int32_t  lino;

  TAOS_CHECK_EXIT(tDecodeU64(pCoder, &pRes->suid));
  TAOS_CHECK_EXIT(tDecodeI32v(pCoder, &nUid));
  for (int32_t iUid = 0; iUid < nUid; iUid++) {
    TAOS_CHECK_EXIT(tDecodeU64(pCoder, &uid));
    if (pRes->uidList) {
      if (taosArrayPush(pRes->uidList, &uid) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pRes->skey));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pRes->ekey));
  TAOS_CHECK_EXIT(tDecodeI64v(pCoder, &pRes->affectedRows));

  TAOS_CHECK_EXIT(tDecodeCStrTo(pCoder, pRes->tableFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pCoder, pRes->tsColName));

  pRes->ctimeMs = 0;
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pRes->ctimeMs));
  }
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pCoder, &pRes->source));
  }

_exit:
  return code;
}

int32_t tEncodeMqMetaRsp(SEncoder *pEncoder, const SMqMetaRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeSTqOffsetVal(pEncoder, &pRsp->rspOffset));
  TAOS_CHECK_RETURN(tEncodeI16(pEncoder, pRsp->resMsgType));
  TAOS_CHECK_RETURN(tEncodeBinary(pEncoder, pRsp->metaRsp, pRsp->metaRspLen));
  return 0;
}

int32_t tDecodeMqMetaRsp(SDecoder *pDecoder, SMqMetaRsp *pRsp) {
  TAOS_CHECK_RETURN(tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset));
  TAOS_CHECK_RETURN(tDecodeI16(pDecoder, &pRsp->resMsgType));
  TAOS_CHECK_RETURN(tDecodeBinaryAlloc(pDecoder, &pRsp->metaRsp, (uint64_t *)&pRsp->metaRspLen));
  return 0;
}

void tDeleteMqMetaRsp(SMqMetaRsp *pRsp) { taosMemoryFree(pRsp->metaRsp); }

int32_t tEncodeMqDataRspCommon(SEncoder *pEncoder, const SMqDataRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeSTqOffsetVal(pEncoder, &pRsp->reqOffset));
  TAOS_CHECK_EXIT(tEncodeSTqOffsetVal(pEncoder, &pRsp->rspOffset));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->blockNum));
  if (pRsp->blockNum != 0) {
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->withTbName));
    TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->withSchema));

    for (int32_t i = 0; i < pRsp->blockNum; i++) {
      int32_t bLen = *(int32_t *)taosArrayGet(pRsp->blockDataLen, i);
      void   *data = taosArrayGetP(pRsp->blockData, i);
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, (const uint8_t *)data, bLen));
      if (pRsp->withSchema) {
        SSchemaWrapper *pSW = (SSchemaWrapper *)taosArrayGetP(pRsp->blockSchema, i);
        TAOS_CHECK_EXIT(tEncodeSSchemaWrapper(pEncoder, pSW));
      }
      if (pRsp->withTbName) {
        char *tbName = (char *)taosArrayGetP(pRsp->blockTbName, i);
        TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, tbName));
      }
    }
  }

_exit:
  return code;
}

int32_t tEncodeMqDataRsp(SEncoder *pEncoder, const SMqDataRsp *pRsp) {
  TAOS_CHECK_RETURN(tEncodeMqDataRspCommon(pEncoder, pRsp));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pRsp->sleepTime));

  return 0;
}

int32_t tDecodeMqDataRspCommon(SDecoder *pDecoder, SMqDataRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(pDecoder, &pRsp->reqOffset));
  TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->blockNum));

  if (pRsp->blockNum != 0) {
    if ((pRsp->blockData = taosArrayInit(pRsp->blockNum, sizeof(void *))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if ((pRsp->blockDataLen = taosArrayInit(pRsp->blockNum, sizeof(int32_t))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pRsp->withTbName));
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pRsp->withSchema));
    if (pRsp->withTbName) {
      if ((pRsp->blockTbName = taosArrayInit(pRsp->blockNum, sizeof(void *))) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
    if (pRsp->withSchema) {
      if ((pRsp->blockSchema = taosArrayInit(pRsp->blockNum, sizeof(void *))) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }

    for (int32_t i = 0; i < pRsp->blockNum; i++) {
      void    *data = NULL;
      uint32_t bLen = 0;
      TAOS_CHECK_EXIT(tDecodeBinary(pDecoder, (uint8_t **)&data, &bLen));
      if (taosArrayPush(pRsp->blockData, &data) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      pRsp->blockDataElementFree = false;

      int32_t len = bLen;
      if (taosArrayPush(pRsp->blockDataLen, &len) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }

      if (pRsp->withSchema) {
        SSchemaWrapper *pSW = (SSchemaWrapper *)taosMemoryCalloc(1, sizeof(SSchemaWrapper));
        if (pSW == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }

        if ((code = tDecodeSSchemaWrapper(pDecoder, pSW))) {
          taosMemoryFree(pSW);
          goto _exit;
        }

        if (taosArrayPush(pRsp->blockSchema, &pSW) == NULL) {
          taosMemoryFree(pSW);
          TAOS_CHECK_EXIT(terrno);
        }
      }

      if (pRsp->withTbName) {
        char *tbName;
        TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &tbName));
        if (taosArrayPush(pRsp->blockTbName, &tbName) == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
      }
    }
  }

_exit:
  return code;
}

int32_t tDecodeMqDataRsp(SDecoder *pDecoder, SMqDataRsp *pRsp) {
  TAOS_CHECK_RETURN(tDecodeMqDataRspCommon(pDecoder, pRsp));
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pRsp->sleepTime));
  }

  return 0;
}

int32_t tDecodeMqRawDataRsp(SDecoder *pDecoder, SMqDataRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(pDecoder, &pRsp->reqOffset));
  TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->blockNum));
_exit:
  return code;
}

static void tDeleteMqDataRspCommon(SMqDataRsp *pRsp) {
  taosArrayDestroy(pRsp->blockDataLen);
  pRsp->blockDataLen = NULL;
  if (pRsp->blockDataElementFree) {
    taosArrayDestroyP(pRsp->blockData, NULL);
  } else {
    taosArrayDestroy(pRsp->blockData);
  }
  pRsp->blockData = NULL;
  taosArrayDestroyP(pRsp->blockSchema, (FDelete)tDeleteSchemaWrapper);
  pRsp->blockSchema = NULL;
  taosArrayDestroyP(pRsp->blockTbName, NULL);
  pRsp->blockTbName = NULL;
  tOffsetDestroy(&pRsp->reqOffset);
  tOffsetDestroy(&pRsp->rspOffset);
  taosMemoryFreeClear(pRsp->data);
}

void tDeleteMqDataRsp(SMqDataRsp *rsp) { tDeleteMqDataRspCommon(rsp); }

int32_t tEncodeSTaosxRsp(SEncoder *pEncoder, const SMqDataRsp *pRsp) {
  int32_t code = 0;
  int32_t lino = 0;

  TAOS_CHECK_EXIT(tEncodeMqDataRspCommon(pEncoder, pRsp));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->createTableNum));
  if (pRsp->createTableNum) {
    for (int32_t i = 0; i < pRsp->createTableNum; i++) {
      void   *createTableReq = taosArrayGetP(pRsp->createTableReq, i);
      int32_t createTableLen = *(int32_t *)taosArrayGet(pRsp->createTableLen, i);
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, createTableReq, createTableLen));
    }
  }

_exit:
  return code;
}

int32_t tDecodeSTaosxRsp(SDecoder *pDecoder, SMqDataRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeMqDataRspCommon(pDecoder, pRsp));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->createTableNum));
  if (pRsp->createTableNum) {
    if ((pRsp->createTableLen = taosArrayInit(pRsp->createTableNum, sizeof(int32_t))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    if ((pRsp->createTableReq = taosArrayInit(pRsp->createTableNum, sizeof(void *))) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < pRsp->createTableNum; i++) {
      void    *pCreate = NULL;
      uint64_t len = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, &pCreate, &len));
      int32_t l = (int32_t)len;
      if (taosArrayPush(pRsp->createTableLen, &l) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      if (taosArrayPush(pRsp->createTableReq, &pCreate) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
_exit:
  return code;
}

void tDeleteSTaosxRsp(SMqDataRsp *pRsp) {
  tDeleteMqDataRspCommon(pRsp);

  taosArrayDestroy(pRsp->createTableLen);
  pRsp->createTableLen = NULL;
  taosArrayDestroyP(pRsp->createTableReq, NULL);
  pRsp->createTableReq = NULL;
}

void tDeleteMqRawDataRsp(SMqDataRsp *pRsp) {
  tOffsetDestroy(&pRsp->reqOffset);
  tOffsetDestroy(&pRsp->rspOffset);
  if (pRsp->rawData != NULL) {
    taosMemoryFree(POINTER_SHIFT(pRsp->rawData, -sizeof(SMqRspHead)));
  }
}

int32_t tEncodeSSingleDeleteReq(SEncoder *pEncoder, const SSingleDeleteReq *pReq) {
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pReq->tbname));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->startTs));
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pReq->endTs));
  return 0;
}

int32_t tDecodeSSingleDeleteReq(SDecoder *pDecoder, SSingleDeleteReq *pReq) {
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pReq->tbname));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->startTs));
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pReq->endTs));
  return 0;
}

int32_t tEncodeSBatchDeleteReq(SEncoder *pEncoder, const SBatchDeleteReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->suid));
  int32_t sz = taosArrayGetSize(pReq->deleteReqs);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, sz));
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq *pOneReq = taosArrayGet(pReq->deleteReqs, i);
    TAOS_CHECK_EXIT(tEncodeSSingleDeleteReq(pEncoder, pOneReq));
  }
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pReq->ctimeMs));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pReq->level));
_exit:
  return code;
}

static int32_t tDecodeSBatchDeleteReqCommon(SDecoder *pDecoder, SBatchDeleteReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->suid));
  int32_t sz;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &sz));
  pReq->deleteReqs = taosArrayInit(0, sizeof(SSingleDeleteReq));
  if (pReq->deleteReqs == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < sz; i++) {
    SSingleDeleteReq deleteReq;
    TAOS_CHECK_EXIT(tDecodeSSingleDeleteReq(pDecoder, &deleteReq));
    if (taosArrayPush(pReq->deleteReqs, &deleteReq) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
_exit:
  return code;
}

int32_t tDecodeSBatchDeleteReq(SDecoder *pDecoder, SBatchDeleteReq *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSBatchDeleteReqCommon(pDecoder, pReq));

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->ctimeMs));
  }
  if (!tDecodeIsEnd(pDecoder)) {
    TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pReq->level));
  }

_exit:
  return code;
}

int32_t tDecodeSBatchDeleteReqSetCtime(SDecoder *pDecoder, SBatchDeleteReq *pReq, int64_t ctimeMs) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSBatchDeleteReqCommon(pDecoder, pReq));

  pReq->ctimeMs = 0;
  if (!tDecodeIsEnd(pDecoder)) {
    *(int64_t *)(pDecoder->data + pDecoder->pos) = ctimeMs;
    TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pReq->ctimeMs));
  }

_exit:
  return code;
}
int32_t transformRawSSubmitTbData(void *data, int64_t suid, int64_t uid, int32_t sver) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, (uint8_t *)POINTER_SHIFT(data, INT_BYTES), *(uint32_t *)data);

  int32_t flags = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &flags));
  flags |= TD_REQ_FROM_TAOX;
  flags &= ~SUBMIT_REQ_AUTO_CREATE_TABLE;

  SEncoder encoder = {0};
  tEncoderInit(&encoder, (uint8_t *)POINTER_SHIFT(data, INT_BYTES), *(uint32_t *)data);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, flags));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, suid));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, uid));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, sver));
_exit:
  return code;
}

static int32_t tPreCheckSubmitTbData(const SSubmitTbData *pSubmitData, int8_t *hasBlog) {
  int32_t code = 0;
  int32_t line = 0;
  if (tBlobSetSize(pSubmitData->pBlobSet) > 0) {
    *hasBlog = 1;
    return code;
  }
    return 0;
}
static int32_t tEncodeSSubmitTbData(SEncoder *pCoder, const SSubmitTbData *pSubmitTbData) {
  int32_t code = 0;
  int32_t lino;
  int8_t  hasBlog = 0;

  int32_t count = 0;
  TAOS_CHECK_EXIT(tPreCheckSubmitTbData(pSubmitTbData, &hasBlog));

  TAOS_CHECK_EXIT(tStartEncode(pCoder));

  int32_t flags = pSubmitTbData->flags | ((SUBMIT_REQUEST_VERSION) << 8);

  if (hasBlog) {
    flags |= SUBMIT_REQ_WITH_BLOB;
  }
  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, flags));

  // auto create table
  if (pSubmitTbData->flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    if (!(pSubmitTbData->pCreateTbReq)) {
      uError("auto create table but request is NULL");
      return TSDB_CODE_INVALID_MSG;
    }
    TAOS_CHECK_EXIT(tEncodeSVCreateTbReq(pCoder, pSubmitTbData->pCreateTbReq));
  }

  // submit data
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pSubmitTbData->suid));
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pSubmitTbData->uid));
  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pSubmitTbData->sver));

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t  nColData = TARRAY_SIZE(pSubmitTbData->aCol);
    SColData *aColData = (SColData *)TARRAY_DATA(pSubmitTbData->aCol);

    uError("encode %d row data", (int32_t)(nColData));
    TAOS_CHECK_EXIT(tEncodeU64v(pCoder, nColData));

    for (uint64_t i = 0; i < nColData; i++) {
      if (IS_STR_DATA_BLOB(aColData[i].type)) {
        count = aColData[i].numOfNull + aColData[i].numOfValue + aColData[i].numOfNone; 
      }  
      TAOS_CHECK_EXIT(tEncodeColData(SUBMIT_REQUEST_VERSION, pCoder, &aColData[i]));
    }

  } else {
    uTrace("encode %d row data", (int32_t)(TARRAY_SIZE(pSubmitTbData->aRowP)));
    TAOS_CHECK_EXIT(tEncodeU64v(pCoder, TARRAY_SIZE(pSubmitTbData->aRowP)));

    SRow **rows = (SRow **)TARRAY_DATA(pSubmitTbData->aRowP);
    for (int32_t iRow = 0; iRow < TARRAY_SIZE(pSubmitTbData->aRowP); ++iRow) {
      TAOS_CHECK_EXIT(tEncodeRow(pCoder, rows[iRow]));
    }
    count = TARRAY_SIZE(pSubmitTbData->aRowP);
  }
  TAOS_CHECK_EXIT(tEncodeI64(pCoder, pSubmitTbData->ctimeMs));

  if (hasBlog) {
    tEncodeBlobSet(pCoder, pSubmitTbData->pBlobSet);
    if (tBlobSetSize(pSubmitTbData->pBlobSet) != count) {
      uError("blob set size %d not match row size %d", tBlobSetSize(pSubmitTbData->pBlobSet), count);
      return TSDB_CODE_INVALID_MSG;
    }
  }

  tEndEncode(pCoder);
_exit:
  return code;
}

static int32_t tDecodeSSubmitTbData(SDecoder *pCoder, SSubmitTbData *pSubmitTbData, void *rawData) {
  int32_t code = 0;
  int32_t lino;
  int32_t flags;
  uint8_t version;

  int8_t hasBlob = 0;
  uint8_t*      dataAfterCreate = NULL;
  uint8_t*      dataStart = pCoder->data + pCoder->pos;
  uint32_t      posAfterCreate = 0;

  TAOS_CHECK_EXIT(tStartDecode(pCoder));
  uint32_t pos = pCoder->pos;
  TAOS_CHECK_EXIT(tDecodeI32v(pCoder, &flags));
  uint32_t flagsLen = pCoder->pos - pos;

  if (flags & SUBMIT_REQ_WITH_BLOB) {
    hasBlob = 1;
  }
  pSubmitTbData->flags = flags & 0xff;
  version = (flags >> 8) & 0xff;

  if (pSubmitTbData->flags & SUBMIT_REQ_AUTO_CREATE_TABLE) {
    pSubmitTbData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (pSubmitTbData->pCreateTbReq == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tDecodeSVCreateTbReq(pCoder, pSubmitTbData->pCreateTbReq));
    dataAfterCreate = pCoder->data + pCoder->pos;
    posAfterCreate = pCoder->pos;
  }

  // submit data
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSubmitTbData->suid));
  TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSubmitTbData->uid));
  TAOS_CHECK_EXIT(tDecodeI32v(pCoder, &pSubmitTbData->sver));

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    uint64_t nColData = 0;

    TAOS_CHECK_EXIT(tDecodeU64v(pCoder, &nColData));

    pSubmitTbData->aCol = taosArrayInit(nColData, sizeof(SColData));
    if (pSubmitTbData->aCol == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < nColData; ++i) {
      TAOS_CHECK_EXIT(tDecodeColData(version, pCoder, taosArrayReserve(pSubmitTbData->aCol, 1)));
    }
  } else {
    uint64_t nRow = 0;
    TAOS_CHECK_EXIT(tDecodeU64v(pCoder, &nRow));

    uTrace("decode %d row data", (int32_t)nRow);
    pSubmitTbData->aRowP = taosArrayInit(nRow, sizeof(SRow *));
    if (pSubmitTbData->aRowP == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t iRow = 0; iRow < nRow; ++iRow) {
      SRow **ppRow = taosArrayReserve(pSubmitTbData->aRowP, 1);
      if (ppRow == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }

      TAOS_CHECK_EXIT(tDecodeRow(pCoder, ppRow));
    }
    uTrace("decode row data size %d", (int32_t)(TARRAY_SIZE(pSubmitTbData->aRowP)));
  }

  pSubmitTbData->ctimeMs = 0;
  if (!tDecodeIsEnd(pCoder)) {
    TAOS_CHECK_EXIT(tDecodeI64(pCoder, &pSubmitTbData->ctimeMs));
  }

  if (!tDecodeIsEnd(pCoder) && hasBlob) {
    TAOS_CHECK_EXIT(tDecodeBlobSet(pCoder, &pSubmitTbData->pBlobSet));
  }

  if (rawData != NULL) {
    if (dataAfterCreate != NULL) {
      TAOS_MEMCPY(dataAfterCreate - INT_BYTES - flagsLen, dataStart, INT_BYTES + flagsLen);
      *(int32_t *)(dataAfterCreate - INT_BYTES - flagsLen) = pCoder->pos - posAfterCreate + flagsLen;
      *(void **)rawData = dataAfterCreate - INT_BYTES - flagsLen;
    } else {
      *(void **)rawData = dataStart;
    }
  }
  tEndDecode(pCoder);

_exit:
  return code;
}

int32_t tEncodeSubmitReq(SEncoder *pCoder, const SSubmitReq2 *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pCoder));
  TAOS_CHECK_EXIT(tEncodeU64v(pCoder, taosArrayGetSize(pReq->aSubmitTbData)));
  if (pReq->raw) {
    for (uint64_t i = 0; i < taosArrayGetSize(pReq->aSubmitTbData); i++) {
      void *data = taosArrayGetP(pReq->aSubmitTbData, i);
      if (pCoder->data != NULL) {
        TAOS_MEMCPY(pCoder->data + pCoder->pos, data, *(uint32_t *)data + INT_BYTES);
      }
      pCoder->pos += *(uint32_t *)data + INT_BYTES;
    }
  } else {
    for (uint64_t i = 0; i < taosArrayGetSize(pReq->aSubmitTbData); i++) {
      SSubmitTbData *pSubmitTbData = taosArrayGet(pReq->aSubmitTbData, i);
      if ((pSubmitTbData->flags & SUBMIT_REQ_AUTO_CREATE_TABLE) && pSubmitTbData->pCreateTbReq == NULL) {
        pSubmitTbData->flags &= ~SUBMIT_REQ_AUTO_CREATE_TABLE;
      }
      TAOS_CHECK_EXIT(tEncodeSSubmitTbData(pCoder, pSubmitTbData));
    }
  }

  tEndEncode(pCoder);
_exit:
  return code;
}

int32_t tDecodeSubmitReq(SDecoder *pCoder, SSubmitReq2 *pReq, SArray *rawList) {
  int32_t code = 0;

  memset(pReq, 0, sizeof(*pReq));

  // decode
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  uint64_t nSubmitTbData;
  if (tDecodeU64v(pCoder, &nSubmitTbData) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  pReq->aSubmitTbData = taosArrayInit(nSubmitTbData, sizeof(SSubmitTbData));
  if (pReq->aSubmitTbData == NULL) {
    code = terrno;
    goto _exit;
  }

  for (uint64_t i = 0; i < nSubmitTbData; i++) {
    SSubmitTbData *data = taosArrayReserve(pReq->aSubmitTbData, 1);
    if (tDecodeSSubmitTbData(pCoder, data, rawList != NULL ? taosArrayReserve(rawList, 1) : NULL) < 0) {
      code = TSDB_CODE_INVALID_MSG;
      goto _exit;
    }
  }

  tEndDecode(pCoder);

_exit:
  return code;
}

void tDestroySubmitTbData(SSubmitTbData *pTbData, int32_t flag) {
  if (NULL == pTbData) {
    return;
  }

  if (flag == TSDB_MSG_FLG_ENCODE || flag == TSDB_MSG_FLG_CMPT) {
    if (pTbData->pCreateTbReq) {
      if (flag == TSDB_MSG_FLG_ENCODE) {
        tdDestroySVCreateTbReq(pTbData->pCreateTbReq);
      } else {
        tDestroySVCreateTbReq(pTbData->pCreateTbReq, TSDB_MSG_FLG_DECODE);
      }
      taosMemoryFreeClear(pTbData->pCreateTbReq);
    }

    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      if (pTbData->aCol) {
        int32_t   nColData = TARRAY_SIZE(pTbData->aCol);
        SColData *aColData = (SColData *)TARRAY_DATA(pTbData->aCol);

        for (int32_t i = 0; i < nColData; ++i) {
          tColDataDestroy(&aColData[i]);
        }
        taosArrayDestroy(pTbData->aCol);
        pTbData->aCol = NULL;
      }
    } else if (pTbData->aRowP) {
      int32_t nRow = TARRAY_SIZE(pTbData->aRowP);
      SRow  **rows = (SRow **)TARRAY_DATA(pTbData->aRowP);

      for (int32_t i = 0; i < nRow; ++i) {
        tRowDestroy(rows[i]);
        rows[i] = NULL;
      }
      taosArrayDestroy(pTbData->aRowP);
    }
  } else if (flag == TSDB_MSG_FLG_DECODE) {
    if (pTbData->pCreateTbReq) {
      tDestroySVSubmitCreateTbReq(pTbData->pCreateTbReq, TSDB_MSG_FLG_DECODE);
      taosMemoryFreeClear(pTbData->pCreateTbReq);
    }

    if (pTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      taosArrayDestroy(pTbData->aCol);
      pTbData->aCol = NULL;
    } else {
      taosArrayDestroy(pTbData->aRowP);
      pTbData->aRowP = NULL;
    }
  }

  if (pTbData->pBlobSet) {
    tBlobSetDestroy(pTbData->pBlobSet);
    pTbData->pBlobSet = NULL;
  }
  pTbData->aRowP = NULL;
}

void tDestroySubmitReq(SSubmitReq2 *pReq, int32_t flag) {
  if (pReq->aSubmitTbData == NULL) return;

  if (!pReq->raw) {
    int32_t        nSubmitTbData = TARRAY_SIZE(pReq->aSubmitTbData);
    SSubmitTbData *aSubmitTbData = (SSubmitTbData *)TARRAY_DATA(pReq->aSubmitTbData);

    for (int32_t i = 0; i < nSubmitTbData; i++) {
      tDestroySubmitTbData(&aSubmitTbData[i], flag);
    }
  }

  taosArrayDestroy(pReq->aSubmitTbData);
  pReq->aSubmitTbData = NULL;

  if (pReq->aSubmitBlobData != NULL) {
    int32_t nSubmitBlobData = TARRAY_SIZE(pReq->aSubmitBlobData);
    for (int32_t i = 0; i < nSubmitBlobData; i++) {
      SBlobSet *pBlobData = taosArrayGetP(pReq->aSubmitBlobData, i);
      if (pBlobData) {
        tBlobSetDestroy(pBlobData);
      }
    }
    taosArrayDestroy(pReq->aSubmitBlobData);
    pReq->aSubmitBlobData = NULL;
  }
}

int32_t tEncodeSSubmitRsp2(SEncoder *pCoder, const SSubmitRsp2 *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tStartEncode(pCoder));

  TAOS_CHECK_EXIT(tEncodeI32v(pCoder, pRsp->affectedRows));

  TAOS_CHECK_EXIT(tEncodeU64v(pCoder, taosArrayGetSize(pRsp->aCreateTbRsp)));
  for (int32_t i = 0; i < taosArrayGetSize(pRsp->aCreateTbRsp); ++i) {
    TAOS_CHECK_EXIT(tEncodeSVCreateTbRsp(pCoder, taosArrayGet(pRsp->aCreateTbRsp, i)));
  }

  tEndEncode(pCoder);
_exit:
  return code;
}

int32_t tDecodeSSubmitRsp2(SDecoder *pCoder, SSubmitRsp2 *pRsp) {
  int32_t code = 0;

  memset(pRsp, 0, sizeof(SSubmitRsp2));

  // decode
  if (tStartDecode(pCoder) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (tDecodeI32v(pCoder, &pRsp->affectedRows) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  uint64_t nCreateTbRsp;
  if (tDecodeU64v(pCoder, &nCreateTbRsp) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  if (nCreateTbRsp) {
    pRsp->aCreateTbRsp = taosArrayInit(nCreateTbRsp, sizeof(SVCreateTbRsp));
    if (pRsp->aCreateTbRsp == NULL) {
      code = terrno;
      goto _exit;
    }

    for (int32_t i = 0; i < nCreateTbRsp; ++i) {
      SVCreateTbRsp *pCreateTbRsp = taosArrayReserve(pRsp->aCreateTbRsp, 1);
      if (tDecodeSVCreateTbRsp(pCoder, pCreateTbRsp) < 0) {
        code = TSDB_CODE_INVALID_MSG;
        goto _exit;
      }
    }
  }

  tEndDecode(pCoder);

_exit:
  if (code) {
    if (pRsp->aCreateTbRsp) {
      taosArrayDestroyEx(pRsp->aCreateTbRsp, NULL /* todo */);
    }
  }
  return code;
}

void tDestroySSubmitRsp2(SSubmitRsp2 *pRsp, int32_t flag) {
  if (NULL == pRsp) {
    return;
  }

  if (flag & TSDB_MSG_FLG_ENCODE) {
    if (pRsp->aCreateTbRsp) {
      int32_t        nCreateTbRsp = TARRAY_SIZE(pRsp->aCreateTbRsp);
      SVCreateTbRsp *aCreateTbRsp = TARRAY_DATA(pRsp->aCreateTbRsp);
      for (int32_t i = 0; i < nCreateTbRsp; ++i) {
        if (aCreateTbRsp[i].pMeta) {
          taosMemoryFree(aCreateTbRsp[i].pMeta->pSchemas);
          taosMemoryFree(aCreateTbRsp[i].pMeta->pSchemaExt);
          taosMemoryFree(aCreateTbRsp[i].pMeta->pColRefs);
          taosMemoryFree(aCreateTbRsp[i].pMeta);
        }
      }
      taosArrayDestroy(pRsp->aCreateTbRsp);
    }
  } else if (flag & TSDB_MSG_FLG_DECODE) {
    if (pRsp->aCreateTbRsp) {
      int32_t        nCreateTbRsp = TARRAY_SIZE(pRsp->aCreateTbRsp);
      SVCreateTbRsp *aCreateTbRsp = TARRAY_DATA(pRsp->aCreateTbRsp);
      for (int32_t i = 0; i < nCreateTbRsp; ++i) {
        if (aCreateTbRsp[i].pMeta) {
          taosMemoryFreeClear(aCreateTbRsp[i].pMeta->pSchemas);
          taosMemoryFreeClear(aCreateTbRsp[i].pMeta->pSchemaExt);
          taosMemoryFreeClear(aCreateTbRsp[i].pMeta->pColRefs);
          taosMemoryFreeClear(aCreateTbRsp[i].pMeta);
        }
      }
      taosArrayDestroy(pRsp->aCreateTbRsp);
    }
  }
}

int32_t tEncodeMqSubTopicEp(void **buf, const SMqSubTopicEp *pTopicEp) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pTopicEp->topic);
  tlen += taosEncodeString(buf, pTopicEp->db);
  int32_t sz = taosArrayGetSize(pTopicEp->vgs);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubVgEp *pVgEp = (SMqSubVgEp *)taosArrayGet(pTopicEp->vgs, i);
    tlen += tEncodeSMqSubVgEp(buf, pVgEp);
  }
  tlen += taosEncodeSSchemaWrapper(buf, &pTopicEp->schema);
  return tlen;
}

void *tDecodeMqSubTopicEp(void *buf, SMqSubTopicEp *pTopicEp) {
  buf = taosDecodeStringTo(buf, pTopicEp->topic);
  buf = taosDecodeStringTo(buf, pTopicEp->db);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pTopicEp->vgs = taosArrayInit(sz, sizeof(SMqSubVgEp));
  if (pTopicEp->vgs == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubVgEp vgEp;
    buf = tDecodeSMqSubVgEp(buf, &vgEp);
    if (taosArrayPush(pTopicEp->vgs, &vgEp) == NULL) {
      taosArrayDestroy(pTopicEp->vgs);
      pTopicEp->vgs = NULL;
      return NULL;
    }
  }
  buf = taosDecodeSSchemaWrapper(buf, &pTopicEp->schema);
  return buf;
}

void tDeleteMqSubTopicEp(SMqSubTopicEp *pSubTopicEp) {
  taosMemoryFreeClear(pSubTopicEp->schema.pSchema);
  pSubTopicEp->schema.nCols = 0;
  taosArrayDestroy(pSubTopicEp->vgs);
}

int32_t tSerializeSCMCreateViewReq(void *buf, int32_t bufLen, const SCMCreateViewReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->fullname));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->querySql));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->sql));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->orReplace));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->precision));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, pReq->numOfCols));
  for (int32_t i = 0; i < pReq->numOfCols; ++i) {
    SSchema *pSchema = &pReq->pSchema[i];
    TAOS_CHECK_EXIT(tEncodeSSchema(&encoder, pSchema));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMCreateViewReq(void *buf, int32_t bufLen, SCMCreateViewReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->fullname));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pReq->querySql));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pReq->sql));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->orReplace));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->precision));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &pReq->numOfCols));

  if (pReq->numOfCols > 0) {
    pReq->pSchema = taosMemoryCalloc(pReq->numOfCols, sizeof(SSchema));
    if (pReq->pSchema == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < pReq->numOfCols; ++i) {
      SSchema *pSchema = pReq->pSchema + i;
      TAOS_CHECK_EXIT(tDecodeSSchema(&decoder, pSchema));
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCMCreateViewReq(SCMCreateViewReq *pReq) {
  if (NULL == pReq) {
    return;
  }

  taosMemoryFreeClear(pReq->querySql);
  taosMemoryFreeClear(pReq->sql);
  taosMemoryFreeClear(pReq->pSchema);
}

int32_t tSerializeSCMDropViewReq(void *buf, int32_t bufLen, const SCMDropViewReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->fullname));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->sql));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->igNotExists));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCMDropViewReq(void *buf, int32_t bufLen, SCMDropViewReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->fullname));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pReq->sql));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->igNotExists));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}
void tFreeSCMDropViewReq(SCMDropViewReq *pReq) {
  if (NULL == pReq) {
    return;
  }

  taosMemoryFree(pReq->sql);
}

int32_t tSerializeSViewMetaReq(void *buf, int32_t bufLen, const SViewMetaReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->fullname));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSViewMetaReq(void *buf, int32_t bufLen, SViewMetaReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->fullname));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t tEncodeSViewMetaRsp(SEncoder *pEncoder, const SViewMetaRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pRsp->name));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pRsp->user));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pRsp->dbId));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pRsp->viewId));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pRsp->querySql));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->precision));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pRsp->type));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->version));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pRsp->numOfCols));
  for (int32_t i = 0; i < pRsp->numOfCols; ++i) {
    SSchema *pSchema = &pRsp->pSchema[i];
    TAOS_CHECK_EXIT(tEncodeSSchema(pEncoder, pSchema));
  }

_exit:
  return code;
}

int32_t tSerializeSViewMetaRsp(void *buf, int32_t bufLen, const SViewMetaRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeSViewMetaRsp(&encoder, pRsp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

static int32_t tDecodeSViewMetaRsp(SDecoder *pDecoder, SViewMetaRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pRsp->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pRsp->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pRsp->user));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pRsp->dbId));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pRsp->viewId));
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pRsp->querySql));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pRsp->precision));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pRsp->type));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->version));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pRsp->numOfCols));
  if (pRsp->numOfCols > 0) {
    pRsp->pSchema = taosMemoryCalloc(pRsp->numOfCols, sizeof(SSchema));
    if (pRsp->pSchema == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int32_t i = 0; i < pRsp->numOfCols; ++i) {
      SSchema *pSchema = pRsp->pSchema + i;
      TAOS_CHECK_EXIT(tDecodeSSchema(pDecoder, pSchema));
    }
  }

_exit:
  return code;
}

int32_t tDeserializeSViewMetaRsp(void *buf, int32_t bufLen, SViewMetaRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeSViewMetaRsp(&decoder, pRsp));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSViewMetaRsp(SViewMetaRsp *pRsp) {
  if (NULL == pRsp) {
    return;
  }

  taosMemoryFree(pRsp->user);
  taosMemoryFree(pRsp->querySql);
  taosMemoryFree(pRsp->pSchema);
}

int32_t tSerializeSViewHbRsp(void *buf, int32_t bufLen, SViewHbRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  int32_t numOfMeta = taosArrayGetSize(pRsp->pViewRsp);
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, numOfMeta));
  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *pMetaRsp = taosArrayGetP(pRsp->pViewRsp, i);
    TAOS_CHECK_EXIT(tEncodeSViewMetaRsp(&encoder, pMetaRsp));
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSViewHbRsp(void *buf, int32_t bufLen, SViewHbRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  int32_t numOfMeta = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &numOfMeta));
  pRsp->pViewRsp = taosArrayInit(numOfMeta, POINTER_BYTES);
  if (pRsp->pViewRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *metaRsp = taosMemoryCalloc(1, sizeof(SViewMetaRsp));
    if (NULL == metaRsp) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeSViewMetaRsp(&decoder, metaRsp));
    if (taosArrayPush(pRsp->pViewRsp, &metaRsp) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSViewHbRsp(SViewHbRsp *pRsp) {
  int32_t numOfMeta = taosArrayGetSize(pRsp->pViewRsp);
  for (int32_t i = 0; i < numOfMeta; ++i) {
    SViewMetaRsp *pMetaRsp = taosArrayGetP(pRsp->pViewRsp, i);
    tFreeSViewMetaRsp(pMetaRsp);
    taosMemoryFree(pMetaRsp);
  }

  taosArrayDestroy(pRsp->pViewRsp);
}

void setDefaultOptionsForField(SFieldWithOptions *field) {
  setColEncode(&field->compress, getDefaultEncode(field->type));
  setColCompress(&field->compress, getDefaultCompress(field->type));
  setColLevel(&field->compress, getDefaultLevel(field->type));
}

void setFieldWithOptions(SFieldWithOptions *fieldWithOptions, SField *field) {
  fieldWithOptions->bytes = field->bytes;
  fieldWithOptions->flags = field->flags;
  fieldWithOptions->type = field->type;
  tstrncpy(fieldWithOptions->name, field->name, TSDB_COL_NAME_LEN);
}
int32_t tSerializeTableTSMAInfoReq(void *buf, int32_t bufLen, const STableTSMAInfoReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->name));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->fetchingWithTsmaName));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeTableTSMAInfoReq(void *buf, int32_t bufLen, STableTSMAInfoReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->name));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, (uint8_t *)&pReq->fetchingWithTsmaName));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

static int32_t tEncodeTableTSMAInfo(SEncoder *pEncoder, const STableTSMAInfo *pTsmaInfo) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTsmaInfo->name));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTsmaInfo->tsmaId));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTsmaInfo->tb));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTsmaInfo->dbFName));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTsmaInfo->suid));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTsmaInfo->destTbUid));
  TAOS_CHECK_EXIT(tEncodeU64(pEncoder, pTsmaInfo->dbId));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pTsmaInfo->version));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTsmaInfo->targetTb));
  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTsmaInfo->targetDbFName));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTsmaInfo->interval));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTsmaInfo->unit));

  int32_t size = pTsmaInfo->pFuncs ? pTsmaInfo->pFuncs->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    STableTSMAFuncInfo *pFuncInfo = taosArrayGet(pTsmaInfo->pFuncs, i);
    TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pFuncInfo->funcId));
    TAOS_CHECK_EXIT(tEncodeI16(pEncoder, pFuncInfo->colId));
  }

  size = pTsmaInfo->pTags ? pTsmaInfo->pTags->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    const SSchema *pSchema = taosArrayGet(pTsmaInfo->pTags, i);
    TAOS_CHECK_EXIT(tEncodeSSchema(pEncoder, pSchema));
  }
  size = pTsmaInfo->pUsedCols ? pTsmaInfo->pUsedCols->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    const SSchema *pSchema = taosArrayGet(pTsmaInfo->pUsedCols, i);
    TAOS_CHECK_EXIT(tEncodeSSchema(pEncoder, pSchema));
  }

  TAOS_CHECK_EXIT(tEncodeCStr(pEncoder, pTsmaInfo->ast));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTsmaInfo->streamUid));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTsmaInfo->reqTs));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTsmaInfo->rspTs));
  TAOS_CHECK_EXIT(tEncodeI64(pEncoder, pTsmaInfo->delayDuration));
  TAOS_CHECK_EXIT(tEncodeI8(pEncoder, pTsmaInfo->fillHistoryFinished));
  size = pTsmaInfo->streamAddr ? 1 : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  if (pTsmaInfo->streamAddr) {
    TAOS_CHECK_EXIT(tEncodeSStreamTaskAddr(pEncoder, pTsmaInfo->streamAddr));
  }

_exit:
  return code;
}

static int32_t tDecodeTableTSMAInfo(SDecoder *pDecoder, STableTSMAInfo *pTsmaInfo) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTsmaInfo->name));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pTsmaInfo->tsmaId));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTsmaInfo->tb));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTsmaInfo->dbFName));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pTsmaInfo->suid));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pTsmaInfo->destTbUid));
  TAOS_CHECK_EXIT(tDecodeU64(pDecoder, &pTsmaInfo->dbId));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pTsmaInfo->version));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTsmaInfo->targetTb));
  TAOS_CHECK_EXIT(tDecodeCStrTo(pDecoder, pTsmaInfo->targetDbFName));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTsmaInfo->interval));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, &pTsmaInfo->unit));
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size > 0) {
    pTsmaInfo->pFuncs = taosArrayInit(size, sizeof(STableTSMAFuncInfo));
    if (!pTsmaInfo->pFuncs) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < size; ++i) {
      STableTSMAFuncInfo funcInfo = {0};
      TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &funcInfo.funcId));
      TAOS_CHECK_EXIT(tDecodeI16(pDecoder, &funcInfo.colId));
      if (!taosArrayPush(pTsmaInfo->pFuncs, &funcInfo)) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size > 0) {
    pTsmaInfo->pTags = taosArrayInit(size, sizeof(SSchema));
    if (!pTsmaInfo->pTags) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < size; ++i) {
      SSchema schema = {0};
      TAOS_CHECK_EXIT(tDecodeSSchema(pDecoder, &schema));
      if (taosArrayPush(pTsmaInfo->pTags, &schema) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size > 0) {
    pTsmaInfo->pUsedCols = taosArrayInit(size, sizeof(SSchema));
    if (!pTsmaInfo->pUsedCols) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < size; ++i) {
      SSchema schema = {0};
      TAOS_CHECK_EXIT(tDecodeSSchema(pDecoder, &schema));
      if (taosArrayPush(pTsmaInfo->pUsedCols, &schema) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
  TAOS_CHECK_EXIT(tDecodeCStrAlloc(pDecoder, &pTsmaInfo->ast));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTsmaInfo->streamUid));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTsmaInfo->reqTs));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTsmaInfo->rspTs));
  TAOS_CHECK_EXIT(tDecodeI64(pDecoder, &pTsmaInfo->delayDuration));
  TAOS_CHECK_EXIT(tDecodeI8(pDecoder, (int8_t *)&pTsmaInfo->fillHistoryFinished));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size > 0) {
    pTsmaInfo->streamAddr = taosMemoryCalloc(1, sizeof(SStreamTaskAddr));
    if (!pTsmaInfo->streamAddr) {
      TAOS_CHECK_EXIT(terrno);
    }

    TAOS_CHECK_EXIT(tDecodeSStreamTaskAddr(pDecoder, pTsmaInfo->streamAddr));
  } else {
    pTsmaInfo->streamAddr = NULL;
  }


_exit:
  return code;
}

static int32_t tEncodeTableTSMAInfoRsp(SEncoder *pEncoder, const STableTSMAInfoRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  int32_t size = pRsp->pTsmas ? pRsp->pTsmas->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    STableTSMAInfo *pInfo = taosArrayGetP(pRsp->pTsmas, i);
    TAOS_CHECK_EXIT(tEncodeTableTSMAInfo(pEncoder, pInfo));
  }
_exit:
  return code;
}

static int32_t tDecodeTableTSMAInfoRsp(SDecoder *pDecoder, STableTSMAInfoRsp *pRsp) {
  int32_t size = 0;
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size <= 0) return 0;
  pRsp->pTsmas = taosArrayInit(size, POINTER_BYTES);
  if (!pRsp->pTsmas) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < size; ++i) {
    STableTSMAInfo *pTsma = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
    if (!pTsma) {
      TAOS_CHECK_EXIT(terrno);
    }
    if (taosArrayPush(pRsp->pTsmas, &pTsma) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
    TAOS_CHECK_EXIT(tDecodeTableTSMAInfo(pDecoder, pTsma));
  }
_exit:
  return code;
}

int32_t tSerializeTableTSMAInfoRsp(void *buf, int32_t bufLen, const STableTSMAInfoRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeTableTSMAInfoRsp(&encoder, pRsp));

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeTableTSMAInfoRsp(void *buf, int32_t bufLen, STableTSMAInfoRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeTableTSMAInfoRsp(&decoder, pRsp));

  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeTableTSMAInfo(void *p) {
  STableTSMAInfo *pTsmaInfo = p;
  if (pTsmaInfo) {
    taosArrayDestroy(pTsmaInfo->pFuncs);
    taosArrayDestroy(pTsmaInfo->pTags);
    taosArrayDestroy(pTsmaInfo->pUsedCols);
    taosMemoryFree(pTsmaInfo->ast);
  }
}

void tFreeAndClearTableTSMAInfo(void *p) {
  STableTSMAInfo *pTsmaInfo = (STableTSMAInfo *)p;
  if (pTsmaInfo) {
    tFreeTableTSMAInfo(pTsmaInfo);
    taosMemoryFree(pTsmaInfo);
  }
}

void tFreeAndClearRefDbName(void *p) {
  char *dbName = (char *)p;
  if (dbName) {
    taosMemoryFree(dbName);
  }
}

int32_t tCloneTbTSMAInfo(STableTSMAInfo *pInfo, STableTSMAInfo **pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (NULL == pInfo) {
    return TSDB_CODE_SUCCESS;
  }
  STableTSMAInfo *pRet = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
  if (!pRet) return terrno;

  *pRet = *pInfo;
  if (pInfo->pFuncs) {
    pRet->pFuncs = taosArrayDup(pInfo->pFuncs, NULL);
    if (!pRet->pFuncs) code = terrno;
  }
  if (pInfo->pTags && code == TSDB_CODE_SUCCESS) {
    pRet->pTags = taosArrayDup(pInfo->pTags, NULL);
    if (!pRet->pTags) code = terrno;
  }
  if (pInfo->pUsedCols && code == TSDB_CODE_SUCCESS) {
    pRet->pUsedCols = taosArrayDup(pInfo->pUsedCols, NULL);
    if (!pRet->pUsedCols) code = terrno;
  }
  if (pInfo->ast && code == TSDB_CODE_SUCCESS) {
    pRet->ast = taosStrdup(pInfo->ast);
    if (!pRet->ast) code = terrno;
  }
  if (code) {
    tFreeAndClearTableTSMAInfo(pRet);
    pRet = NULL;
  }
  *pRes = pRet;
  return code;
}

void tFreeTableTSMAInfoRsp(STableTSMAInfoRsp *pRsp) {
  if (pRsp && pRsp->pTsmas) {
    taosArrayDestroyP(pRsp->pTsmas, tFreeAndClearTableTSMAInfo);
  }
}

int32_t tEncodeSMDropTbReqOnSingleVg(SEncoder *pEncoder, const SMDropTbReqsOnSingleVg *pReq) {
  const SVgroupInfo *pVgInfo = &pReq->vgInfo;
  int32_t            code = 0;
  int32_t            lino;

  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pVgInfo->vgId));
  TAOS_CHECK_EXIT(tEncodeU32(pEncoder, pVgInfo->hashBegin));
  TAOS_CHECK_EXIT(tEncodeU32(pEncoder, pVgInfo->hashEnd));
  TAOS_CHECK_EXIT(tEncodeSEpSet(pEncoder, &pVgInfo->epSet));
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, pVgInfo->numOfTable));
  int32_t size = pReq->pTbs ? pReq->pTbs->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; ++i) {
    const SVDropTbReq *pInfo = taosArrayGet(pReq->pTbs, i);
    TAOS_CHECK_EXIT(tEncodeSVDropTbReq(pEncoder, pInfo));
  }
_exit:
  return code;
}

int32_t tDecodeSMDropTbReqOnSingleVg(SDecoder *pDecoder, SMDropTbReqsOnSingleVg *pReq) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->vgInfo.vgId));
  TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &pReq->vgInfo.hashBegin));
  TAOS_CHECK_EXIT(tDecodeU32(pDecoder, &pReq->vgInfo.hashEnd));
  TAOS_CHECK_EXIT(tDecodeSEpSet(pDecoder, &pReq->vgInfo.epSet));
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &pReq->vgInfo.numOfTable));
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  pReq->pTbs = taosArrayInit(size, sizeof(SVDropTbReq));
  if (!pReq->pTbs) {
    TAOS_CHECK_EXIT(terrno);
  }
  SVDropTbReq pTbReq = {0};
  for (int32_t i = 0; i < size; ++i) {
    TAOS_CHECK_EXIT(tDecodeSVDropTbReq(pDecoder, &pTbReq));
    if (taosArrayPush(pReq->pTbs, &pTbReq) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

_exit:
  return code;
}

void tFreeSMDropTbReqOnSingleVg(void *p) {
  SMDropTbReqsOnSingleVg *pReq = p;
  taosArrayDestroy(pReq->pTbs);
}

int32_t tSerializeSMDropTbsReq(void *buf, int32_t bufLen, const SMDropTbsReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0;
  int32_t  lino;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  int32_t size = pReq->pVgReqs ? pReq->pVgReqs->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, size));
  for (int32_t i = 0; i < size; ++i) {
    SMDropTbReqsOnSingleVg *pVgReq = taosArrayGet(pReq->pVgReqs, i);
    TAOS_CHECK_EXIT(tEncodeSMDropTbReqOnSingleVg(&encoder, pVgReq));
  }
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMDropTbsReq(void *buf, int32_t bufLen, SMDropTbsReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0;
  int32_t  lino;

  tDecoderInit(&decoder, buf, bufLen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &size));
  pReq->pVgReqs = taosArrayInit(size, sizeof(SMDropTbReqsOnSingleVg));
  if (!pReq->pVgReqs) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < size; ++i) {
    SMDropTbReqsOnSingleVg vgReq = {0};
    TAOS_CHECK_EXIT(tDecodeSMDropTbReqOnSingleVg(&decoder, &vgReq));
    if (taosArrayPush(pReq->pVgReqs, &vgReq) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSMDropTbsReq(void *p) {
  SMDropTbsReq *pReq = p;
  taosArrayDestroyEx(pReq->pVgReqs, tFreeSMDropTbReqOnSingleVg);
}

int32_t tEncodeVFetchTtlExpiredTbsRsp(SEncoder *pCoder, const SVFetchTtlExpiredTbsRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeI32(pCoder, pRsp->vgId));
  int32_t size = pRsp->pExpiredTbs ? pRsp->pExpiredTbs->size : 0;
  TAOS_CHECK_EXIT(tEncodeI32(pCoder, size));
  for (int32_t i = 0; i < size; ++i) {
    TAOS_CHECK_EXIT(tEncodeSVDropTbReq(pCoder, taosArrayGet(pRsp->pExpiredTbs, i)));
  }

_exit:
  return code;
}

int32_t tDecodeVFetchTtlExpiredTbsRsp(SDecoder *pCoder, SVFetchTtlExpiredTbsRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &pRsp->vgId));
  int32_t size = 0;
  TAOS_CHECK_EXIT(tDecodeI32(pCoder, &size));
  if (size > 0) {
    pRsp->pExpiredTbs = taosArrayInit(size, sizeof(SVDropTbReq));
    if (!pRsp->pExpiredTbs) {
      TAOS_CHECK_EXIT(terrno);
    }
    SVDropTbReq tb = {0};
    for (int32_t i = 0; i < size; ++i) {
      TAOS_CHECK_EXIT(tDecodeSVDropTbReq(pCoder, &tb));
      if (taosArrayPush(pRsp->pExpiredTbs, &tb) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
_exit:
  return code;
}

void tFreeFetchTtlExpiredTbsRsp(void *p) {
  SVFetchTtlExpiredTbsRsp *pRsp = p;
  taosArrayDestroy(pRsp->pExpiredTbs);
}

int32_t tEncodeMqBatchMetaRsp(SEncoder *pEncoder, const SMqBatchMetaRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tEncodeSTqOffsetVal(pEncoder, &pRsp->rspOffset));

  int32_t size = taosArrayGetSize(pRsp->batchMetaReq);
  TAOS_CHECK_EXIT(tEncodeI32(pEncoder, size));
  if (size > 0) {
    for (int32_t i = 0; i < size; i++) {
      void   *pMetaReq = taosArrayGetP(pRsp->batchMetaReq, i);
      int32_t metaLen = *(int32_t *)taosArrayGet(pRsp->batchMetaLen, i);
      TAOS_CHECK_EXIT(tEncodeBinary(pEncoder, pMetaReq, metaLen));
    }
  }
_exit:
  return code;
}

int32_t tDecodeMqBatchMetaRsp(SDecoder *pDecoder, SMqBatchMetaRsp *pRsp) {
  int32_t size = 0;
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeI32(pDecoder, &size));
  if (size > 0) {
    pRsp->batchMetaReq = taosArrayInit(size, POINTER_BYTES);
    if (!pRsp->batchMetaReq) {
      TAOS_CHECK_EXIT(terrno);
    }
    pRsp->batchMetaLen = taosArrayInit(size, sizeof(int32_t));
    if (!pRsp->batchMetaLen) {
      TAOS_CHECK_EXIT(terrno);
    }
    for (int32_t i = 0; i < size; i++) {
      void    *pCreate = NULL;
      uint64_t len = 0;
      TAOS_CHECK_EXIT(tDecodeBinaryAlloc(pDecoder, &pCreate, &len));
      int32_t l = (int32_t)len;
      if (taosArrayPush(pRsp->batchMetaReq, &pCreate) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
      if (taosArrayPush(pRsp->batchMetaLen, &l) == NULL) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
  }
_exit:
  return code;
}

int32_t tSemiDecodeMqBatchMetaRsp(SDecoder *pDecoder, SMqBatchMetaRsp *pRsp) {
  int32_t code = 0;
  int32_t lino;

  TAOS_CHECK_EXIT(tDecodeSTqOffsetVal(pDecoder, &pRsp->rspOffset));
  if (pDecoder->size < pDecoder->pos) {
    return TSDB_CODE_INVALID_PARA;
  }
  pRsp->metaBuffLen = TD_CODER_REMAIN_CAPACITY(pDecoder);
  pRsp->pMetaBuff = taosMemoryCalloc(1, pRsp->metaBuffLen);
  if (pRsp->pMetaBuff == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  memcpy(pRsp->pMetaBuff, TD_CODER_CURRENT(pDecoder), pRsp->metaBuffLen);

_exit:
  return code;
}

void tDeleteMqBatchMetaRsp(SMqBatchMetaRsp *pRsp) {
  taosMemoryFreeClear(pRsp->pMetaBuff);
  taosArrayDestroyP(pRsp->batchMetaReq, NULL);
  taosArrayDestroy(pRsp->batchMetaLen);
  pRsp->batchMetaReq = NULL;
  pRsp->batchMetaLen = NULL;
}

bool hasExtSchema(const SExtSchema *pExtSchema) { return pExtSchema->typeMod != 0; }

#ifdef USE_MOUNT
int32_t tSerializeSMountStbInfo(void *buf, int32_t bufLen, int32_t *pFLen, SMountStbInfo *pInfo) {
  SEncoder        encoder = {0};
  int32_t         code = 0, lino = 0;
  int32_t         flen = 0, qlen = 0;
  SMCreateStbReq *pReq = &pInfo->req;
  void           *qBuf = NULL;

  flen = tSerializeSMCreateStbReq(buf, bufLen, pReq);
  if (flen <= 0) {
    TAOS_RETURN(flen < 0 ? flen : TSDB_CODE_INTERNAL_ERROR);
  }
  if (pFLen) *pFLen = flen;

  if (buf) qBuf = POINTER_SHIFT(buf, flen);
  tEncoderInit(&encoder, qBuf, qBuf ? bufLen - flen : 0);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  for (int32_t i = 0; i < pReq->numOfColumns; ++i) {
    void *pColExt = taosArrayGet(pInfo->pColExts, i);
    TAOS_CHECK_EXIT(tEncodeI16v(&encoder, *(col_id_t *)pColExt));
  }
  for (int32_t i = 0; i < pReq->numOfTags; ++i) {
    void *pTagExt = taosArrayGet(pInfo->pTagExts, i);
    TAOS_CHECK_EXIT(tEncodeI16v(&encoder, *(col_id_t *)pTagExt));
  }
  tEndEncode(&encoder);
_exit:
  qlen = code ? code : encoder.pos;
  tEncoderClear(&encoder);
  return code ? code : (flen + qlen);
}

int32_t tDeserializeSMountStbInfo(void *buf, int32_t bufLen, int32_t flen, SMountStbInfo *pInfo) {
  SDecoder decoder = {0};
  int32_t  code = 0, lino = 0;
  void    *qBuf = POINTER_SHIFT(buf, flen);

  TAOS_CHECK_EXIT(tDeserializeSMCreateStbReq(buf, flen, &pInfo->req));

  tDecoderInit(&decoder, qBuf, bufLen - flen);
  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  if ((pInfo->pColExts = taosArrayInit(pInfo->req.numOfColumns, sizeof(col_id_t))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  if ((pInfo->pTagExts = taosArrayInit(pInfo->req.numOfTags, sizeof(col_id_t))) == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  for (int32_t i = 0; i < pInfo->req.numOfColumns; ++i) {
    col_id_t colId = 0;
    TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &colId));
    if (taosArrayPush(pInfo->pColExts, &colId) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  for (int32_t i = 0; i < pInfo->req.numOfTags; ++i) {
    col_id_t colId = 0;
    TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &colId));
    if (taosArrayPush(pInfo->pTagExts, &colId) == NULL) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSCreateMountReq(void *buf, int32_t bufLen, SCreateMountReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0, lino = 0;
  int32_t  tlen;

  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountName));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ignoreExist));
  TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pReq->nMounts));
  for (int32_t i = 0; i < pReq->nMounts; ++i) {
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->dnodeIds[i]));
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountPaths[i]));
  }
  ENCODESQL();

  tEndEncode(&encoder);
_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSCreateMountReq(void *buf, int32_t bufLen, SCreateMountReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0, lino = 0;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->mountName));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ignoreExist));
  TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &pReq->nMounts));
  if(pReq->nMounts > 0) {
    TSDB_CHECK_NULL((pReq->dnodeIds = taosMemoryMalloc(pReq->nMounts * sizeof(int32_t))), code, lino, _exit, terrno);
    TSDB_CHECK_NULL((pReq->mountPaths = taosMemoryMalloc(pReq->nMounts * sizeof(char*))), code, lino, _exit, terrno);
    for (int32_t i = 0; i < pReq->nMounts; ++i) {
      TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->dnodeIds[i]));
      TAOS_CHECK_EXIT(tDecodeCStrAlloc(&decoder, &pReq->mountPaths[i]));
    }
  }
  DECODESQL();

  tEndDecode(&decoder);
_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSCreateMountReq(SCreateMountReq *pReq) {
  if (pReq) {
    taosMemoryFreeClear(pReq->dnodeIds);
    if (pReq->mountPaths) {
      for (int32_t i = 0; i < pReq->nMounts; ++i) {
        taosMemoryFreeClear(pReq->mountPaths[i]);
      }
      taosMemoryFreeClear(pReq->mountPaths);
    }
    FREESQL();
  }
}

int32_t tSerializeSDropMountReq(void *buf, int32_t bufLen, SDropMountReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0, lino = 0;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountName));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ignoreNotExists));
  ENCODESQL();
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropMountReq(void *buf, int32_t bufLen, SDropMountReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0, lino = 0;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->mountName));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ignoreNotExists));
  DECODESQL();
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

void tFreeSDropMountReq(SDropMountReq *pReq) { FREESQL(); }

int32_t tSerializeSDropMountRsp(void *buf, int32_t bufLen, SDropMountRsp *pRsp) {
  SEncoder encoder = {0};
  int32_t  code = 0, lino = 0;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pRsp->name));
  TAOS_CHECK_EXIT(tEncodeI64(&encoder, pRsp->uid));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSDropMountRsp(void *buf, int32_t bufLen, SDropMountRsp *pRsp) {
  SDecoder decoder = {0};
  int32_t  code = 0, lino = 0;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pRsp->name));
  TAOS_CHECK_EXIT(tDecodeI64(&decoder, &pRsp->uid));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSRetrieveMountPathReq(void *buf, int32_t bufLen, SRetrieveMountPathReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0, lino = 0;
  int32_t  tlen;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountPath));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->mountUid));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->dnodeId));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pReq->ignoreExist));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t*)pReq->pVal, pReq->valLen));
  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSRetrieveMountPathReq(void *buf, int32_t bufLen, SRetrieveMountPathReq *pReq) {
  SDecoder decoder = {0};
  int32_t  code = 0, lino = 0;
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->mountName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->mountPath));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->mountUid));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->dnodeId));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pReq->ignoreExist));
  TAOS_CHECK_EXIT(tDecodeBinary(&decoder, (uint8_t**)&pReq->pVal, &pReq->valLen));
  tEndDecode(&decoder);

_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeSMountInfo(void *buf, int32_t bufLen, SMountInfo *pInfo) {
  SEncoder encoder = {0};
  int32_t  code = 0, lino = 0;
  int32_t  tlen, nDb = 0, nVg = 0, nStb = 0;
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pInfo->mountName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pInfo->mountPath));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pInfo->ignoreExist));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pInfo->mountUid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pInfo->clusterId));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pInfo->dnodeId));
  TAOS_CHECK_EXIT(tEncodeBinary(&encoder, (const uint8_t *)pInfo->pVal, pInfo->valLen));
  nDb = taosArrayGetSize(pInfo->pDbs);
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nDb));
  for (int32_t i = 0; i < nDb; ++i) {
    SMountDbInfo *pDbInfo = TARRAY_GET_ELEM(pInfo->pDbs, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pDbInfo->dbName));
    TAOS_CHECK_EXIT(tEncodeU64v(&encoder, pDbInfo->dbId));
    nVg = taosArrayGetSize(pDbInfo->pVgs);
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nVg));
    for (int32_t j = 0; j < nVg; ++j) {
      SMountVgInfo *pVgInfo = TARRAY_GET_ELEM(pDbInfo->pVgs, j);
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->diskPrimary));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->vgId));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->cacheLastSize));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->szPage));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->szCache));
      TAOS_CHECK_EXIT(tEncodeU64v(&encoder, pVgInfo->szBuf));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->cacheLast));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->standby));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->hashMethod));
      TAOS_CHECK_EXIT(tEncodeU32v(&encoder, pVgInfo->hashBegin));
      TAOS_CHECK_EXIT(tEncodeU32v(&encoder, pVgInfo->hashEnd));
      TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pVgInfo->hashPrefix));
      TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pVgInfo->hashSuffix));
      TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pVgInfo->sttTrigger));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->replications));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->precision));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->compression));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->slLevel));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->daysPerFile));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->keep0));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->keep1));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->keep2));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->keepTimeOffset));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->minRows));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->maxRows));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->tsdbPageSize));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->ssChunkSize));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->ssKeepLocal));
      TAOS_CHECK_EXIT(tEncodeI8(&encoder, pVgInfo->ssCompact));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->walFsyncPeriod));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->walRetentionPeriod));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->walRollPeriod));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->walRetentionSize));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->walSegSize));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->walLevel));
      TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pVgInfo->encryptAlgorithm));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->committed));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->commitID));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->commitTerm));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->numOfSTables));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->numOfCTables));
      TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pVgInfo->numOfNTables));
      TAOS_CHECK_EXIT(tEncodeU64v(&encoder, pVgInfo->dbId));
    }
    nStb = taosArrayGetSize(pDbInfo->pStbs);
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, nStb));
    for (int32_t k = 0; k < nStb; ++k) {
      void **pVal = TARRAY_GET_ELEM(pDbInfo->pStbs, k);
      TAOS_CHECK_EXIT(tEncodeBinary(&encoder, *(const uint8_t **)(pVal), **(int32_t **)pVal));
    }
  }

  tEndEncode(&encoder);

_exit:
  if (code) {
    tlen = code;
  } else {
    tlen = encoder.pos;
  }
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMountInfo(SDecoder *decoder, SMountInfo *pInfo, bool extractStb) {
  int32_t code = 0, lino = 0;
  int32_t nDb = 0, nVg = 0, nStb = 0;

  TAOS_CHECK_EXIT(tStartDecode(decoder));
  TAOS_CHECK_EXIT(tDecodeCStrTo(decoder, pInfo->mountName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(decoder, pInfo->mountPath));
  TAOS_CHECK_EXIT(tDecodeI8(decoder, &pInfo->ignoreExist));
  TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pInfo->mountUid));
  TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pInfo->clusterId));
  TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pInfo->dnodeId));
  TAOS_CHECK_EXIT(tDecodeBinary(decoder, (uint8_t **)&pInfo->pVal, &pInfo->valLen));
  TAOS_CHECK_EXIT(tDecodeI32v(decoder, &nDb));
  if (nDb > 0) {
    TSDB_CHECK_NULL((pInfo->pDbs = taosArrayInit_s(sizeof(SMountDbInfo), nDb)), code, lino, _exit, terrno);
    for (int32_t i = 0; i < nDb; ++i) {
      SMountDbInfo *pDbInfo = TARRAY_GET_ELEM(pInfo->pDbs, i);
      TAOS_CHECK_EXIT(tDecodeCStrTo(decoder, pDbInfo->dbName));
      TAOS_CHECK_EXIT(tDecodeU64v(decoder, &pDbInfo->dbId));
      TAOS_CHECK_EXIT(tDecodeI32v(decoder, &nVg));
      if (nVg > 0) {
        TSDB_CHECK_NULL((pDbInfo->pVgs = taosArrayInit_s(sizeof(SMountVgInfo), nVg)), code, lino, _exit, terrno);
        for (int32_t j = 0; j < nVg; ++j) {
          SMountVgInfo *pVgInfo = TARRAY_GET_ELEM(pDbInfo->pVgs, j);
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->diskPrimary));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->vgId));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->cacheLastSize));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->szPage));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->szCache));
          TAOS_CHECK_EXIT(tDecodeU64v(decoder, &pVgInfo->szBuf));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->cacheLast));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->standby));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->hashMethod));
          TAOS_CHECK_EXIT(tDecodeU32v(decoder, &pVgInfo->hashBegin));
          TAOS_CHECK_EXIT(tDecodeU32v(decoder, &pVgInfo->hashEnd));
          TAOS_CHECK_EXIT(tDecodeI16v(decoder, &pVgInfo->hashPrefix));
          TAOS_CHECK_EXIT(tDecodeI16v(decoder, &pVgInfo->hashSuffix));
          TAOS_CHECK_EXIT(tDecodeI16v(decoder, &pVgInfo->sttTrigger));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->replications));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->precision));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->compression));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->slLevel));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->daysPerFile));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->keep0));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->keep1));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->keep2));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->keepTimeOffset));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->minRows));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->maxRows));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->tsdbPageSize));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->ssChunkSize));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->ssKeepLocal));
          TAOS_CHECK_EXIT(tDecodeI8(decoder, &pVgInfo->ssCompact));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->walFsyncPeriod));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->walRetentionPeriod));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->walRollPeriod));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->walRetentionSize));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->walSegSize));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->walLevel));
          TAOS_CHECK_EXIT(tDecodeI32v(decoder, &pVgInfo->encryptAlgorithm));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->committed));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->commitID));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->commitTerm));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->numOfSTables));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->numOfCTables));
          TAOS_CHECK_EXIT(tDecodeI64v(decoder, &pVgInfo->numOfNTables));
          TAOS_CHECK_EXIT(tDecodeU64v(decoder, &pVgInfo->dbId));
        }
      }
      TAOS_CHECK_EXIT(tDecodeI32v(decoder, &nStb));
      if (nStb > 0) {
        if (extractStb) {
          TSDB_CHECK_NULL((pDbInfo->pStbs = taosArrayInit_s(sizeof(SMountStbInfo), nStb)), code, lino, _exit, terrno);
          for (int32_t k = 0; k < nStb; ++k) {
            int32_t vlen = 0;
            void   *pVal = NULL;
            TAOS_CHECK_EXIT(tDecodeBinary(decoder, (uint8_t **)&pVal, &vlen));
            if (vlen < 8) {
              TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);  // totalLen(4) + 1stPartLen(4) + 1stPart(SMCreateStbReq) + 2ndPart(colIds and tagIds)
            }
            int32_t flen = *(int32_t *)POINTER_SHIFT(pVal,4);
            SMountStbInfo *pStbInfo = TARRAY_GET_ELEM(pDbInfo->pStbs, k);
            TAOS_CHECK_EXIT(tDeserializeSMountStbInfo(POINTER_SHIFT(pVal, 8), vlen - 8, flen, pStbInfo));
          }
        } else {
          TSDB_CHECK_NULL((pDbInfo->pStbs = taosArrayInit_s(sizeof(void *), nStb)), code, lino, _exit, terrno);
          for (int32_t k = 0; k < nStb; ++k) {
            int32_t vlen = 0;
            void   *pVal = NULL;
            TAOS_CHECK_EXIT(tDecodeBinary(decoder, (uint8_t **)&pVal, &vlen));
            if (vlen < 8) {
              TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);  // totalLen(4) + 1stPartLen(4) + 1stPart + 2ndPart
            }
            void *pStb = TARRAY_GET_ELEM(pDbInfo->pStbs, k);
            void *pNewVal = taosMemoryMalloc(vlen);
            if (pNewVal == NULL) {
              TAOS_CHECK_EXIT(terrno);
            }
            memcpy(pNewVal, pVal, vlen);
            *(void **)pStb = pNewVal;
          }
        }
      }
    }
  }

  tEndDecode(decoder);
_exit:
  return code;
}

void tFreeMountInfo(SMountInfo *pInfo, bool stbExtracted) {
  if (pInfo) {
    if (pInfo->pDbs) {
      for (int32_t i = 0; i < TARRAY_SIZE(pInfo->pDbs); ++i) {
        SMountDbInfo *pDbInfo = TARRAY_GET_ELEM(pInfo->pDbs, i);
        taosArrayDestroy(pDbInfo->pVgs);
        if (stbExtracted) {
          for (int32_t j = 0; j < taosArrayGetSize(pDbInfo->pStbs); ++j) {
            SMountStbInfo *pStbInfo = TARRAY_GET_ELEM(pDbInfo->pStbs, j);
            tFreeSMCreateStbReq(&pStbInfo->req);
            taosArrayDestroy(pStbInfo->pColExts);
            taosArrayDestroy(pStbInfo->pTagExts);
          }
          taosArrayDestroy(pDbInfo->pStbs);
        } else {
          taosArrayDestroyP(pDbInfo->pStbs, NULL);
        }
      }
      taosArrayDestroy(pInfo->pDbs);
    }
    for (int32_t i = 0; i < TFS_MAX_TIERS; ++i) {
      taosArrayDestroyP(pInfo->pDisks[i], NULL);
    }
    if (pInfo->pFile) {
      (void)taosUnLockFile(pInfo->pFile);
      (void)taosCloseFile(&pInfo->pFile);
    }
  }
}

int32_t tSerializeSMountVnodeReq(void *buf, int32_t *cBufLen, int32_t *tBufLen, SMountVnodeReq *pReq) {
  SEncoder encoder = {0};
  int32_t  code = 0, lino = 0;
  int32_t  clen = 0, tlen = 0;

  tEncoderInit(&encoder, buf, *tBufLen);
  TAOS_CHECK_EXIT(tStartEncode(&encoder));
  TAOS_CHECK_EXIT(tEncodeI32(&encoder, *cBufLen));  // createReq
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pReq->mountPath));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->mountId));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->diskPrimary));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pReq->mountVgId));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->committed));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->commitID));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->commitTerm));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->numOfSTables));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->numOfCTables));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->numOfNTables));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, 10));  // nReserved
  for (int32_t i = 0; i < 10; ++i) {           // reserved fields
    TAOS_CHECK_EXIT(tEncodeI64v(&encoder, 0));
  }
  tEndEncode(&encoder);

  if (buf == NULL) {
    clen = tSerializeSCreateVnodeReq(NULL, 0, &pReq->createReq);
  } else {
    clen = tSerializeSCreateVnodeReq(POINTER_SHIFT(buf, encoder.pos), *cBufLen, &pReq->createReq);
  }

_exit:
  if (code || clen < 0) {
    if (clen < 0) code = clen;
  } else {
    *cBufLen = clen;
    *tBufLen = encoder.pos + clen;
  }
  tEncoderClear(&encoder);
  return code;
}

int32_t tDeserializeSMountVnodeReq(void *buf, int32_t bufLen, SMountVnodeReq *pReq) {
  SDecoder         decoder = {0};
  int32_t          code = 0, lino = 0;
  int32_t          cBufLen = 0, tBufLen = 0, nReserved = 0;
  int64_t          padding;
  SCreateVnodeReq *pCreateReq = &pReq->createReq;

  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));
  TAOS_CHECK_EXIT(tDecodeI32(&decoder, &cBufLen));  // createReq
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->mountName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pReq->mountPath));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->mountId));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->diskPrimary));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pReq->mountVgId));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->committed));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->commitID));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->commitTerm));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->numOfSTables));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->numOfCTables));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pReq->numOfNTables));
  // reserved fields
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &nReserved));
  for(int32_t i = 0; i < nReserved; ++i) {
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &padding));
  }
  tEndDecode(&decoder);
  TAOS_CHECK_EXIT(tDeserializeSCreateVnodeReq(POINTER_SHIFT(buf, decoder.pos), cBufLen, pCreateReq));
_exit:
  tDecoderClear(&decoder);
  return code;
}

int32_t tFreeSMountVnodeReq(SMountVnodeReq *pReq) {
  (void)tFreeSCreateVnodeReq(&pReq->createReq);
  return 0;
}
#endif // USE_MOUNT