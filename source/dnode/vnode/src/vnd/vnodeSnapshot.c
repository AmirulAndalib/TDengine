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

#include "bse.h"
#include "tsdb.h"
#include "vnd.h"

static int32_t vnodeExtractSnapInfoDiff(void *buf, int32_t bufLen, TFileSetRangeArray **ppRanges) {
  int32_t            code = 0;
  STsdbFSetPartList *pList = tsdbFSetPartListCreate();
  if (pList == NULL) {
    code = terrno;
    goto _out;
  }

  code = tDeserializeTsdbFSetPartList(buf, bufLen, pList);
  if (code) goto _out;

  code = tsdbFSetPartListToRangeDiff(pList, ppRanges);
  if (code) goto _out;

_out:
  tsdbFSetPartListDestroy(&pList);
  return code;
}

// SVSnapReader ========================================================
struct SVSnapReader {
  SVnode *pVnode;
  int64_t sver;
  int64_t ever;
  int64_t index;
  // config
  int8_t cfgDone;
  // meta
  int8_t           metaDone;
  SMetaSnapReader *pMetaReader;
  // tsdb
  int8_t              tsdbDone;
  TFileSetRangeArray *pRanges;
  STsdbSnapReader    *pTsdbReader;
  // tsdb raw
  int8_t              tsdbRAWDone;
  STsdbSnapRAWReader *pTsdbRAWReader;

  // tq
  int8_t         tqHandleDone;
  STqSnapReader *pTqSnapReader;
  int8_t         tqOffsetDone;
  STqSnapReader *pTqOffsetReader;
  int8_t         tqCheckInfoDone;
  STqSnapReader *pTqCheckInfoReader;
  // stream
  int8_t              streamTaskDone;
  SStreamTaskReader  *pStreamTaskReader;
  int8_t              streamStateDone;
  SStreamStateReader *pStreamStateReader;
  // rsma
  int8_t              rsmaDone;
  TFileSetRangeArray *pRsmaRanges[TSDB_RETENTION_L2];
  SRSmaSnapReader    *pRsmaReader;

  // bse
  int8_t          bseDone;
  SBseSnapReader *pBseReader;
};

static TFileSetRangeArray **vnodeSnapReaderGetTsdbRanges(SVSnapReader *pReader, int32_t tsdbTyp) {
  if (!(sizeof(pReader->pRsmaRanges) / sizeof(pReader->pRsmaRanges[0]) == 2)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  switch (tsdbTyp) {
    case SNAP_DATA_TSDB:
      return &pReader->pRanges;
    case SNAP_DATA_RSMA1:
      return &pReader->pRsmaRanges[0];
    case SNAP_DATA_RSMA2:
      return &pReader->pRsmaRanges[1];
    default:
      return NULL;
  }
}

static int32_t vnodeSnapReaderDealWithSnapInfo(SVSnapReader *pReader, SSnapshotParam *pParam) {
  int32_t code = 0;
  SVnode *pVnode = pReader->pVnode;

  if (pParam->data) {
    // decode
    SSyncTLV *datHead = (void *)pParam->data;
    if (datHead->typ != TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      terrno = code;
      goto _out;
    }

    STsdbRepOpts         tsdbOpts = {0};
    TFileSetRangeArray **ppRanges = NULL;
    int32_t              offset = 0;

    while (offset + sizeof(SSyncTLV) < datHead->len) {
      SSyncTLV *subField = (void *)(datHead->val + offset);
      offset += sizeof(SSyncTLV) + subField->len;
      void   *buf = subField->val;
      int32_t bufLen = subField->len;

      switch (subField->typ) {
        case SNAP_DATA_TSDB:
        case SNAP_DATA_RSMA1:
        case SNAP_DATA_RSMA2: {
          ppRanges = vnodeSnapReaderGetTsdbRanges(pReader, subField->typ);
          if (ppRanges == NULL) {
            vError("vgId:%d, unexpected subfield type in snapshot param. subtyp:%d", TD_VID(pVnode), subField->typ);
            code = TSDB_CODE_INVALID_DATA_FMT;
            goto _out;
          }
          code = vnodeExtractSnapInfoDiff(buf, bufLen, ppRanges);
          if (code) {
            vError("vgId:%d, failed to get range diff since %s", TD_VID(pVnode), terrstr());
            goto _out;
          }
        } break;
        case SNAP_DATA_RAW: {
          code = tDeserializeTsdbRepOpts(buf, bufLen, &tsdbOpts);
          if (code) {
            vError("vgId:%d, failed to deserialize tsdb rep opts since %s", TD_VID(pVnode), terrstr());
            goto _out;
          }
        } break;
        default:
          vError("vgId:%d, unexpected subfield type of snap info. typ:%d", TD_VID(pVnode), subField->typ);
          code = TSDB_CODE_INVALID_DATA_FMT;
          goto _out;
      }
    }

    // toggle snap replication mode
    vInfo("vgId:%d, vnode snap reader supported tsdb rep of format:%d", TD_VID(pVnode), tsdbOpts.format);
    if (pReader->sver == 0 && tsdbOpts.format == TSDB_SNAP_REP_FMT_RAW) {
      pReader->tsdbDone = true;
    } else {
      pReader->tsdbRAWDone = true;
    }

    vInfo("vgId:%d, vnode snap writer enabled replication mode: %s", TD_VID(pVnode),
          (pReader->tsdbDone ? "raw" : "normal"));
  }

_out:
  return code;
}

int32_t vnodeSnapReaderOpen(SVnode *pVnode, SSnapshotParam *pParam, SVSnapReader **ppReader) {
  int32_t       code = 0;
  int64_t       sver = pParam->start;
  int64_t       ever = pParam->end;
  SVSnapReader *pReader = NULL;

  pReader = (SVSnapReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = terrno;
    goto _exit;
  }
  pReader->pVnode = pVnode;
  pReader->sver = sver;
  pReader->ever = ever;

  // snapshot info
  code = vnodeSnapReaderDealWithSnapInfo(pReader, pParam);
  if (code) goto _exit;

  // open tsdb snapshot raw reader
  if (!pReader->tsdbRAWDone) {
    code = tsdbSnapRAWReaderOpen(pVnode->pTsdb, ever, SNAP_DATA_RAW, &pReader->pTsdbRAWReader);
    if (code) goto _exit;
  }

  // check snapshot ever
  SSnapshot snapshot = {0};
  code = vnodeGetSnapshot(pVnode, &snapshot);
  if (code) goto _exit;
  if (ever != snapshot.lastApplyIndex) {
    vError("vgId:%d, abort reader open due to vnode snapshot changed. ever:%" PRId64 ", commit ver:%" PRId64,
           TD_VID(pVnode), ever, snapshot.lastApplyIndex);
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _exit;
  }

_exit:
  if (code) {
    vError("vgId:%d, vnode snapshot reader open failed since %s", TD_VID(pVnode), tstrerror(code));
    *ppReader = NULL;
  } else {
    vInfo("vgId:%d, vnode snapshot reader opened, sver:%" PRId64 " ever:%" PRId64, TD_VID(pVnode), sver, ever);
    *ppReader = pReader;
  }
  return code;
}

static void vnodeSnapReaderDestroyTsdbRanges(SVSnapReader *pReader) {
  int32_t tsdbTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  for (int32_t j = 0; j < TSDB_RETENTION_MAX; ++j) {
    TFileSetRangeArray **ppRanges = vnodeSnapReaderGetTsdbRanges(pReader, tsdbTyps[j]);
    if (ppRanges == NULL) continue;
    tsdbTFileSetRangeArrayDestroy(ppRanges);
  }
}

void vnodeSnapReaderClose(SVSnapReader *pReader) {
  vInfo("vgId:%d, close vnode snapshot reader", TD_VID(pReader->pVnode));
  vnodeSnapReaderDestroyTsdbRanges(pReader);
#ifdef USE_RSMA
  if (pReader->pRsmaReader) {
    rsmaSnapReaderClose(&pReader->pRsmaReader);
  }
#endif

  if (pReader->pTsdbReader) {
    tsdbSnapReaderClose(&pReader->pTsdbReader);
  }

  if (pReader->pTsdbRAWReader) {
    tsdbSnapRAWReaderClose(&pReader->pTsdbRAWReader);
  }

  if (pReader->pMetaReader) {
    metaSnapReaderClose(&pReader->pMetaReader);
  }
#ifdef USE_TQ
  if (pReader->pTqSnapReader) {
    tqSnapReaderClose(&pReader->pTqSnapReader);
  }

  if (pReader->pTqOffsetReader) {
    tqSnapReaderClose(&pReader->pTqOffsetReader);
  }

  if (pReader->pTqCheckInfoReader) {
    tqSnapReaderClose(&pReader->pTqCheckInfoReader);
  }

#endif

  if (pReader->pBseReader) {
    bseSnapReaderClose(&pReader->pBseReader);
  }
  taosMemoryFree(pReader);
}

int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData) {
  int32_t code = 0;
  int32_t lino;
  SVnode *pVnode = pReader->pVnode;
  int32_t vgId = TD_VID(pReader->pVnode);

  // CONFIG ==============
  // FIXME: if commit multiple times and the config changed?
  if (!pReader->cfgDone) {
    char    fName[TSDB_FILENAME_LEN];
    int32_t offset = 0;

    vnodeGetPrimaryPath(pVnode, false, fName, TSDB_FILENAME_LEN);
    offset = strlen(fName);
    snprintf(fName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VND_INFO_FNAME);

    TdFilePtr pFile = taosOpenFile(fName, TD_FILE_READ);
    if (NULL == pFile) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    int64_t size;
    code = taosFStatFile(pFile, &size, NULL);
    if (code != 0) {
      if (taosCloseFile(&pFile) != 0) {
        vError("vgId:%d, failed to close file", vgId);
      }
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + size + 1);
    if (*ppData == NULL) {
      if (taosCloseFile(&pFile) != 0) {
        vError("vgId:%d, failed to close file", vgId);
      }
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
    ((SSnapDataHdr *)(*ppData))->type = SNAP_DATA_CFG;
    ((SSnapDataHdr *)(*ppData))->size = size + 1;
    ((SSnapDataHdr *)(*ppData))->data[size] = '\0';

    if (taosReadFile(pFile, ((SSnapDataHdr *)(*ppData))->data, size) < 0) {
      taosMemoryFree(*ppData);
      if (taosCloseFile(&pFile) != 0) {
        vError("vgId:%d, failed to close file", vgId);
      }
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }

    if (taosCloseFile(&pFile) != 0) {
      vError("vgId:%d, failed to close file", vgId);
    }

    pReader->cfgDone = 1;
    goto _exit;
  }

  // META ==============
  if (!pReader->metaDone) {
    // open reader if not
    if (pReader->pMetaReader == NULL) {
      code = metaSnapReaderOpen(pReader->pVnode->pMeta, pReader->sver, pReader->ever, &pReader->pMetaReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = metaSnapRead(pReader->pMetaReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (*ppData) {
      goto _exit;
    } else {
      pReader->metaDone = 1;
      metaSnapReaderClose(&pReader->pMetaReader);
    }
  }

  // TSDB ==============
  if (!pReader->tsdbDone) {
    // open if not
    if (pReader->pTsdbReader == NULL) {
      code = tsdbSnapReaderOpen(pReader->pVnode->pTsdb, pReader->sver, pReader->ever, SNAP_DATA_TSDB, pReader->pRanges,
                                &pReader->pTsdbReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapRead(pReader->pTsdbReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->tsdbDone = 1;
      tsdbSnapReaderClose(&pReader->pTsdbReader);
    }
  }

  if (!pReader->tsdbRAWDone) {
    // open if not
    if (pReader->pTsdbRAWReader == NULL) {
      code = tsdbSnapRAWReaderOpen(pReader->pVnode->pTsdb, pReader->ever, SNAP_DATA_RAW, &pReader->pTsdbRAWReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapRAWRead(pReader->pTsdbRAWReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->tsdbRAWDone = 1;
      tsdbSnapRAWReaderClose(&pReader->pTsdbRAWReader);
    }
  }

  // TQ ================
#ifdef USE_TQ
  vInfo("vgId:%d tq transform start", vgId);
  if (!pReader->tqHandleDone) {
    if (pReader->pTqSnapReader == NULL) {
      code = tqSnapReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->ever, SNAP_DATA_TQ_HANDLE,
                              &pReader->pTqSnapReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tqSnapRead(pReader->pTqSnapReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->tqHandleDone = 1;
      tqSnapReaderClose(&pReader->pTqSnapReader);
    }
  }
  if (!pReader->tqCheckInfoDone) {
    if (pReader->pTqCheckInfoReader == NULL) {
      code = tqSnapReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->ever, SNAP_DATA_TQ_CHECKINFO,
                              &pReader->pTqCheckInfoReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tqSnapRead(pReader->pTqCheckInfoReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->tqCheckInfoDone = 1;
      tqSnapReaderClose(&pReader->pTqCheckInfoReader);
    }
  }
  if (!pReader->tqOffsetDone) {
    if (pReader->pTqOffsetReader == NULL) {
      code = tqSnapReaderOpen(pReader->pVnode->pTq, pReader->sver, pReader->ever, SNAP_DATA_TQ_OFFSET,
                              &pReader->pTqOffsetReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tqSnapRead(pReader->pTqOffsetReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->tqOffsetDone = 1;
      tqSnapReaderClose(&pReader->pTqOffsetReader);
    }
  }
#endif
  // RSMA ==============
#ifdef USE_RSMA
  if (VND_IS_RSMA(pReader->pVnode) && !pReader->rsmaDone) {
    // open if not
    if (pReader->pRsmaReader == NULL) {
      code = rsmaSnapReaderOpen(pReader->pVnode->pSma, pReader->sver, pReader->ever, &pReader->pRsmaReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = rsmaSnapRead(pReader->pRsmaReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->rsmaDone = 1;
      rsmaSnapReaderClose(&pReader->pRsmaReader);
    }
  }

  if (!pReader->bseDone) {
    if (pReader->pBseReader == NULL) {
      code = bseSnapReaderOpen(pReader->pVnode->pBse, pReader->sver, pReader->ever, &pReader->pBseReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    int32_t len = 0;
    code = bseSnapReaderRead(pReader->pBseReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->bseDone = 1;
      bseSnapReaderClose(&pReader->pBseReader);
    }
  }

#endif
  *ppData = NULL;
  *nData = 0;

_exit:
  if (code) {
    vError("vgId:%d, vnode snapshot read failed at %s:%d since %s", vgId, __FILE__, lino, tstrerror(code));
  } else {
    if (*ppData) {
      SSnapDataHdr *pHdr = (SSnapDataHdr *)(*ppData);

      pReader->index++;
      *nData = sizeof(SSnapDataHdr) + pHdr->size;
      pHdr->index = pReader->index;
      vDebug("vgId:%d, vnode snapshot read data, index:%" PRId64 " type:%d blockLen:%d ", vgId, pReader->index,
             pHdr->type, *nData);
    } else {
      vInfo("vgId:%d, vnode snapshot read data end, index:%" PRId64, vgId, pReader->index);
    }
  }
  return code;
}

// SVSnapWriter ========================================================
struct SVSnapWriter {
  SVnode *pVnode;
  int64_t sver;
  int64_t ever;
  int64_t commitID;
  int64_t index;
  // config
  SVnodeInfo info;
  // meta
  SMetaSnapWriter *pMetaSnapWriter;
  // tsdb
  TFileSetRangeArray *pRanges;
  STsdbSnapWriter    *pTsdbSnapWriter;
  // tsdb raw
  STsdbSnapRAWWriter *pTsdbSnapRAWWriter;
  // tq
  STqSnapWriter *pTqSnapHandleWriter;
  STqSnapWriter *pTqSnapOffsetWriter;
  STqSnapWriter *pTqSnapCheckInfoWriter;
  // rsma
  TFileSetRangeArray *pRsmaRanges[TSDB_RETENTION_L2];
  SRSmaSnapWriter    *pRsmaSnapWriter;

  // bse
  SBseSnapWriter *pBseSnapWriter;
};

TFileSetRangeArray **vnodeSnapWriterGetTsdbRanges(SVSnapWriter *pWriter, int32_t tsdbTyp) {
  switch (tsdbTyp) {
    case SNAP_DATA_TSDB:
      return &pWriter->pRanges;
    case SNAP_DATA_RSMA1:
      return &pWriter->pRsmaRanges[0];
    case SNAP_DATA_RSMA2:
      return &pWriter->pRsmaRanges[1];
    default:
      return NULL;
  }
}

static int32_t vnodeSnapWriterDealWithSnapInfo(SVSnapWriter *pWriter, SSnapshotParam *pParam) {
  SVnode *pVnode = pWriter->pVnode;
  int32_t code = 0;
  int32_t lino;

  if (pParam->data) {
    SSyncTLV *datHead = (void *)pParam->data;
    if (datHead->typ != TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
      TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_DATA_FMT, lino, _exit);
    }

    STsdbRepOpts         tsdbOpts = {0};
    TFileSetRangeArray **ppRanges = NULL;
    int32_t              offset = 0;

    while (offset + sizeof(SSyncTLV) < datHead->len) {
      SSyncTLV *subField = (void *)(datHead->val + offset);
      offset += sizeof(SSyncTLV) + subField->len;
      void   *buf = subField->val;
      int32_t bufLen = subField->len;

      switch (subField->typ) {
        case SNAP_DATA_TSDB:
        case SNAP_DATA_RSMA1:
        case SNAP_DATA_RSMA2: {
          ppRanges = vnodeSnapWriterGetTsdbRanges(pWriter, subField->typ);
          if (ppRanges == NULL) {
            vError("vgId:%d, unexpected subfield type in snapshot param. subtyp:%d", TD_VID(pVnode), subField->typ);
            TSDB_CHECK_CODE(code = terrno, lino, _exit);
          }

          code = vnodeExtractSnapInfoDiff(buf, bufLen, ppRanges);
          TSDB_CHECK_CODE(code, lino, _exit);
        } break;
        case SNAP_DATA_RAW: {
          code = tDeserializeTsdbRepOpts(buf, bufLen, &tsdbOpts);
          TSDB_CHECK_CODE(code, lino, _exit);
        } break;
        default:
          vError("vgId:%d, unexpected subfield type of snap info. typ:%d", TD_VID(pVnode), subField->typ);
          TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_DATA_FMT, lino, _exit);
          goto _exit;
      }
    }

    vInfo("vgId:%d, vnode snap writer supported tsdb rep of format:%d", TD_VID(pVnode), tsdbOpts.format);
  }

_exit:
  if (code) {
    vError("vgId:%d %s failed at %s:%d since %s", TD_VID(pVnode), __func__, __FILE__, __LINE__, tstrerror(code));
  }
  return code;
}

extern int32_t tsdbDisableAndCancelAllBgTask(STsdb *pTsdb);
extern void    tsdbEnableBgTask(STsdb *pTsdb);

static int32_t vnodeCancelAndDisableAllBgTask(SVnode *pVnode) {
  TAOS_CHECK_RETURN(tsdbDisableAndCancelAllBgTask(pVnode->pTsdb));
  TAOS_CHECK_RETURN(vnodeSyncCommit(pVnode));
  return 0;
}

static int32_t vnodeEnableBgTask(SVnode *pVnode) {
  tsdbEnableBgTask(pVnode->pTsdb);
  return 0;
}

int32_t vnodeSnapWriterOpen(SVnode *pVnode, SSnapshotParam *pParam, SVSnapWriter **ppWriter) {
  int32_t       code = 0;
  int32_t       lino;
  SVSnapWriter *pWriter = NULL;
  int64_t       sver = pParam->start;
  int64_t       ever = pParam->end;

  // disable write, cancel and disable all bg tasks
  (void)taosThreadMutexLock(&pVnode->mutex);
  pVnode->disableWrite = true;
  (void)taosThreadMutexUnlock(&pVnode->mutex);

  code = vnodeCancelAndDisableAllBgTask(pVnode);
  TSDB_CHECK_CODE(code, lino, _exit);

  // alloc
  pWriter = (SVSnapWriter *)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  pWriter->pVnode = pVnode;
  pWriter->sver = sver;
  pWriter->ever = ever;

  // inc commit ID
  pWriter->commitID = ++pVnode->state.commitID;

  // snapshot info
  code = vnodeSnapWriterDealWithSnapInfo(pWriter, pParam);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    vError("vgId:%d, vnode snapshot writer open failed since %s", TD_VID(pVnode), tstrerror(code));
    if (pWriter) taosMemoryFreeClear(pWriter);
    *ppWriter = NULL;
  } else {
    vInfo("vgId:%d, vnode snapshot writer opened, sver:%" PRId64 " ever:%" PRId64 " commit id:%" PRId64, TD_VID(pVnode),
          sver, ever, pWriter->commitID);
    *ppWriter = pWriter;
  }
  return code;
}

static void vnodeSnapWriterDestroyTsdbRanges(SVSnapWriter *pWriter) {
  int32_t tsdbTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  for (int32_t j = 0; j < TSDB_RETENTION_MAX; ++j) {
    TFileSetRangeArray **ppRanges = vnodeSnapWriterGetTsdbRanges(pWriter, tsdbTyps[j]);
    if (ppRanges == NULL) continue;
    tsdbTFileSetRangeArrayDestroy(ppRanges);
  }
}

int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback, SSnapshot *pSnapshot) {
  int32_t code = 0;
  SVnode *pVnode = pWriter->pVnode;

  vnodeSnapWriterDestroyTsdbRanges(pWriter);

  // prepare
  if (pWriter->pTsdbSnapWriter) {
    code = tsdbSnapWriterPrepareClose(pWriter->pTsdbSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTsdbSnapRAWWriter) {
    code = tsdbSnapRAWWriterPrepareClose(pWriter->pTsdbSnapRAWWriter);
    if (code) goto _exit;
  }
#ifdef USE_RSMA
  if (pWriter->pRsmaSnapWriter) {
    code = rsmaSnapWriterPrepareClose(pWriter->pRsmaSnapWriter, rollback);
    if (code) goto _exit;
  }
#endif
  // commit json
  if (!rollback) {
    pWriter->info.state.committed = pWriter->ever;
    pVnode->config = pWriter->info.config;
    pVnode->state = (SVState){.committed = pWriter->info.state.committed,
                              .applied = pWriter->info.state.committed,
                              .commitID = pWriter->commitID,
                              .commitTerm = pWriter->info.state.commitTerm,
                              .applyTerm = pWriter->info.state.commitTerm};
    pVnode->statis = pWriter->info.statis;
    char dir[TSDB_FILENAME_LEN] = {0};
    vnodeGetPrimaryPath(pVnode, false, dir, TSDB_FILENAME_LEN);

    code = vnodeCommitInfo(dir);
    if (code) goto _exit;

  } else {
    vnodeRollback(pWriter->pVnode);
  }

  // commit/rollback sub-system
  if (pWriter->pMetaSnapWriter) {
    code = metaSnapWriterClose(&pWriter->pMetaSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTsdbSnapWriter) {
    code = tsdbSnapWriterClose(&pWriter->pTsdbSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTsdbSnapRAWWriter) {
    code = tsdbSnapRAWWriterClose(&pWriter->pTsdbSnapRAWWriter, rollback);
    if (code) goto _exit;
  }
#ifdef USE_TQ
  if (pWriter->pTqSnapHandleWriter) {
    code = tqSnapWriterClose(&pWriter->pTqSnapHandleWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTqSnapCheckInfoWriter) {
    code = tqSnapWriterClose(&pWriter->pTqSnapCheckInfoWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pTqSnapOffsetWriter) {
    code = tqSnapWriterClose(&pWriter->pTqSnapOffsetWriter, rollback);
    if (code) goto _exit;
  }
#endif
#ifdef USE_RSMA
  if (pWriter->pRsmaSnapWriter) {
    code = rsmaSnapWriterClose(&pWriter->pRsmaSnapWriter, rollback);
    if (code) goto _exit;
  }

  if (pWriter->pBseSnapWriter) {
    bseSnapWriterClose(&pWriter->pBseSnapWriter, rollback);
  }

#endif
  code = vnodeBegin(pVnode);
  if (code) goto _exit;

  (void)taosThreadMutexLock(&pVnode->mutex);
  pVnode->disableWrite = false;
  (void)taosThreadMutexUnlock(&pVnode->mutex);

_exit:
  if (code) {
    vError("vgId:%d, vnode snapshot writer close failed since %s", TD_VID(pWriter->pVnode), tstrerror(code));
  } else {
    vInfo("vgId:%d, vnode snapshot writer closed, rollback:%d", TD_VID(pVnode), rollback);
    taosMemoryFree(pWriter);
  }
  if (vnodeEnableBgTask(pVnode) != 0) {
    tsdbError("vgId:%d, failed to enable bg task", TD_VID(pVnode));
  }
  return code;
}

static int32_t vnodeSnapWriteInfo(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t       code = 0;
  int32_t       lino;
  SVnode       *pVnode = pWriter->pVnode;
  SSnapDataHdr *pHdr = (SSnapDataHdr *)pData;

  // decode info
  code = vnodeDecodeInfo(pHdr->data, &pWriter->info);
  TSDB_CHECK_CODE(code, lino, _exit);

  // change some value
  pWriter->info.state.commitID = pWriter->commitID;

  // modify info as needed
  char dir[TSDB_FILENAME_LEN] = {0};
  vnodeGetPrimaryPath(pVnode, false, dir, TSDB_FILENAME_LEN);

  SVnodeStats vndStats = pWriter->info.config.vndStats;
  pWriter->info.config = pVnode->config;
  pWriter->info.config.vndStats = vndStats;
  vDebug("vgId:%d, save config while write snapshot", pWriter->pVnode->config.vgId);
  code = vnodeSaveInfo(dir, &pWriter->info);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  return code;
}

int32_t vnodeSnapWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData) {
  int32_t       code = 0;
  int32_t       lino;
  SSnapDataHdr *pHdr = (SSnapDataHdr *)pData;
  SVnode       *pVnode = pWriter->pVnode;

  if (!(pHdr->size + sizeof(SSnapDataHdr) == nData)) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pHdr->index != pWriter->index + 1) {
    vError("vgId:%d, unexpected vnode snapshot msg. index:%" PRId64 ", expected index:%" PRId64, TD_VID(pVnode),
           pHdr->index, pWriter->index + 1);
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_MSG, lino, _exit);
  }

  pWriter->index = pHdr->index;

  vDebug("vgId:%d, vnode snapshot write data, index:%" PRId64 " type:%d blockLen:%d", TD_VID(pVnode), pHdr->index,
         pHdr->type, nData);

  switch (pHdr->type) {
    case SNAP_DATA_CFG: {
      code = vnodeSnapWriteInfo(pWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
    case SNAP_DATA_META: {
      // meta
      if (pWriter->pMetaSnapWriter == NULL) {
        code = metaSnapWriterOpen(pVnode->pMeta, pWriter->sver, pWriter->ever, &pWriter->pMetaSnapWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = metaSnapWrite(pWriter->pMetaSnapWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
    case SNAP_DATA_TSDB:
    case SNAP_DATA_DEL: {
      // tsdb
      if (pWriter->pTsdbSnapWriter == NULL) {
        code = tsdbSnapWriterOpen(pVnode->pTsdb, pWriter->sver, pWriter->ever, pWriter->pRanges,
                                  &pWriter->pTsdbSnapWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tsdbSnapWrite(pWriter->pTsdbSnapWriter, pHdr);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
    case SNAP_DATA_RAW: {
      // tsdb
      if (pWriter->pTsdbSnapRAWWriter == NULL) {
        code = tsdbSnapRAWWriterOpen(pVnode->pTsdb, pWriter->ever, &pWriter->pTsdbSnapRAWWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tsdbSnapRAWWrite(pWriter->pTsdbSnapRAWWriter, pHdr);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
#ifdef USE_TQ
    case SNAP_DATA_TQ_HANDLE: {
      // tq handle
      if (pWriter->pTqSnapHandleWriter == NULL) {
        code = tqSnapWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pTqSnapHandleWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tqSnapHandleWrite(pWriter->pTqSnapHandleWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
    case SNAP_DATA_TQ_CHECKINFO: {
      // tq checkinfo
      if (pWriter->pTqSnapCheckInfoWriter == NULL) {
        code = tqSnapWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pTqSnapCheckInfoWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tqSnapCheckInfoWrite(pWriter->pTqSnapCheckInfoWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
    case SNAP_DATA_TQ_OFFSET: {
      // tq offset
      if (pWriter->pTqSnapOffsetWriter == NULL) {
        code = tqSnapWriterOpen(pVnode->pTq, pWriter->sver, pWriter->ever, &pWriter->pTqSnapOffsetWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tqSnapOffsetWrite(pWriter->pTqSnapOffsetWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
#endif
#ifdef USE_RSMA
    case SNAP_DATA_RSMA1:
    case SNAP_DATA_RSMA2:
    case SNAP_DATA_QTASK: {
      // rsma1/rsma2/qtask for rsma
      if (pWriter->pRsmaSnapWriter == NULL) {
        code = rsmaSnapWriterOpen(pVnode->pSma, pWriter->sver, pWriter->ever, (void **)pWriter->pRsmaRanges,
                                  &pWriter->pRsmaSnapWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = rsmaSnapWrite(pWriter->pRsmaSnapWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
    case SNAP_DATA_BSE: {
      if (pWriter->pBseSnapWriter == NULL) {
        code = bseSnapWriterOpen(pVnode->pBse, pWriter->sver, pWriter->ever, &pWriter->pBseSnapWriter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      code = bseSnapWriterWrite(pWriter->pBseSnapWriter, pData, nData);
      TSDB_CHECK_CODE(code, lino, _exit);
    } break;
#endif
    default:
      break;
  }
_exit:
  if (code) {
    vError("vgId:%d, vnode snapshot write failed since %s, index:%" PRId64 " type:%d nData:%d", TD_VID(pVnode),
           tstrerror(code), pHdr->index, pHdr->type, nData);
  }
  return code;
}
