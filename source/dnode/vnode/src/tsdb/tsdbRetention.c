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

#include "tcs.h"
#include "tsdb.h"
#include "tsdbFS2.h"
#include "vnd.h"

extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool s3Migrate);
extern bool    tsdbShouldCompact(const STFileSet *fset, int32_t vgId);

typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int64_t now;
  int64_t cid;

  STFileSet   *fset;
  TFileOpArray fopArr;
} SRTNer;

static int32_t tsdbDoRemoveFileObject(SRTNer *rtner, const STFileObj *fobj) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  return TARRAY2_APPEND(&rtner->fopArr, op);
}

static int32_t tsdbCopyFileWithLimitedSpeed(TdFilePtr from, TdFilePtr to, int64_t size, uint32_t limitMB) {
  int64_t interval = 1000;  // 1s
  int64_t limit = limitMB ? limitMB * 1024 * 1024 : INT64_MAX;
  int64_t offset = 0;
  int64_t remain = size;

  while (remain > 0) {
    int64_t n;
    int64_t last = taosGetTimestampMs();
    if ((n = taosFSendFile(to, from, &offset, TMIN(limit, remain))) < 0) {
      TAOS_CHECK_RETURN(terrno);
    }

    remain -= n;

    if (remain > 0) {
      int64_t elapsed = taosGetTimestampMs() - last;
      if (elapsed < interval) {
        taosMsleep(interval - elapsed);
      }
    }
  }

  return 0;
}

static int32_t tsdbDoCopyFileLC(SRTNer *rtner, const STFileObj *from, const STFile *to) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr fdFrom = NULL, fdTo = NULL;
  char      fname_from[TSDB_FILENAME_LEN];
  char      fname_to[TSDB_FILENAME_LEN];

  tsdbTFileLastChunkName(rtner->tsdb, from->f, fname_from);
  tsdbTFileLastChunkName(rtner->tsdb, to, fname_to);

  fdFrom = taosOpenFile(fname_from, TD_FILE_READ);
  if (fdFrom == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  tsdbInfo("vgId: %d, open tofile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname_to, from->f->size);

  fdTo = taosOpenFile(fname_to, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;
  int64_t    lc_size = tsdbLogicToFileSize(to->size, rtner->szPage) - chunksize * (to->lcn - 1);

  if (taosFSendFile(fdTo, fdFrom, 0, lc_size) < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  if (taosCloseFile(&fdFrom) != 0) {
    tsdbError("vgId:%d, failed to close file %s", TD_VID(rtner->tsdb->pVnode), fname_from);
  }
  if (taosCloseFile(&fdTo) != 0) {
    tsdbError("vgId:%d, failed to close file %s", TD_VID(rtner->tsdb->pVnode), fname_to);
  }
  return code;
}

static int32_t tsdbDoCopyFile(SRTNer *rtner, const STFileObj *from, const STFile *to) {
  int32_t code = 0;
  int32_t lino = 0;

  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr fdFrom = NULL;
  TdFilePtr fdTo = NULL;

  tsdbTFileName(rtner->tsdb, to, fname);

  fdFrom = taosOpenFile(from->fname, TD_FILE_READ);
  if (fdFrom == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  tsdbInfo("vgId: %d, open tofile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, from->f->size);

  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  TSDB_CHECK_CODE(code, lino, _exit);

  TAOS_CHECK_GOTO(tsdbCopyFileWithLimitedSpeed(fdFrom, fdTo, tsdbLogicToFileSize(from->f->size, rtner->szPage),
                                               tsRetentionSpeedLimitMB),
                  &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  if (taosCloseFile(&fdFrom) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  if (taosCloseFile(&fdTo) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  return code;
}

static int32_t tsdbDoMigrateFileObj(SRTNer *rtner, const STFileObj *fobj, const SDiskID *did) {
  int32_t  code = 0;
  int32_t  lino = 0;
  STFileOp op = {0};
  int32_t  lcn = fobj->f->lcn;

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = did[0],
              .fid = fobj->f->fid,
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  // do copy the file

  if (lcn < 1) {
    TAOS_CHECK_GOTO(tsdbDoCopyFile(rtner, fobj, &op.nf), &lino, _exit);
  } else {
    TAOS_CHECK_GOTO(tsdbDoCopyFileLC(rtner, fobj, &op.nf), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

typedef struct {
  STsdb  *tsdb;
  int64_t now;
  int32_t fid;
  bool    s3Migrate;
} SRtnArg;

static int32_t tsdbDoRetentionEnd(SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  if (TARRAY2_SIZE(&rtner->fopArr) > 0) {
    TAOS_CHECK_GOTO(tsdbFSEditBegin(rtner->tsdb->pFS, &rtner->fopArr, TSDB_FEDIT_RETENTION), &lino, _exit);

    (void)taosThreadMutexLock(&rtner->tsdb->mutex);

    code = tsdbFSEditCommit(rtner->tsdb->pFS);
    if (code) {
      (void)taosThreadMutexUnlock(&rtner->tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    (void)taosThreadMutexUnlock(&rtner->tsdb->mutex);

    TARRAY2_DESTROY(&rtner->fopArr, NULL);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vid:%d, cid:%" PRId64 ", %s done", TD_VID(rtner->tsdb->pVnode), rtner->cid, __func__);
  }
  return code;
}

static int32_t tsdbRemoveOrMoveFileObject(SRTNer *rtner, int32_t expLevel, STFileObj *fobj) {
  int32_t code = 0;
  int32_t lino = 0;

  if (fobj == NULL) {
    return code;
  }

  if (expLevel < 0) {
    // remove the file
    code = tsdbDoRemoveFileObject(rtner, fobj);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (expLevel > fobj->f->did.level) {
    // Try to move the file to a new level
    for (; expLevel > fobj->f->did.level; expLevel--) {
      SDiskID diskId = {0};

      code = tsdbAllocateDiskAtLevel(rtner->tsdb, expLevel, tsdbFTypeLabel(fobj->f->type), &diskId);
      if (code) {
        tsdbTrace("vgId:%d, cannot allocate disk for file %s, level:%d, reason:%s, skip!", TD_VID(rtner->tsdb->pVnode),
                  fobj->fname, expLevel, tstrerror(code));
        code = 0;
        continue;
      } else {
        tsdbInfo("vgId:%d start to migrate file %s from level %d to %d, size:%" PRId64, TD_VID(rtner->tsdb->pVnode),
                 fobj->fname, fobj->f->did.level, diskId.level, fobj->f->size);

        code = tsdbDoMigrateFileObj(rtner, fobj, &diskId);
        TSDB_CHECK_CODE(code, lino, _exit);

        tsdbInfo("vgId:%d end to migrate file %s from level %d to %d, size:%" PRId64, TD_VID(rtner->tsdb->pVnode),
                 fobj->fname, fobj->f->did.level, diskId.level, fobj->f->size);
        break;
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbDoRetention(SRTNer *rtner) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj = NULL;
  STFileSet *fset = rtner->fset;

  // handle data file sets
  int32_t expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->now);
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ++ftype) {
    code = tsdbRemoveOrMoveFileObject(rtner, expLevel, fset->farr[ftype]);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // handle stt file
  SSttLvl *lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      code = tsdbRemoveOrMoveFileObject(rtner, expLevel, fobj);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static void tsdbRetentionCancel(void *arg) { taosMemoryFree(arg); }

static int32_t tsdbDoS3Migrate(SRTNer *rtner);

static int32_t tsdbRetention(void *arg) {
  int32_t code = 0;
  int32_t lino = 0;

  SRtnArg   *rtnArg = (SRtnArg *)arg;
  STsdb     *pTsdb = rtnArg->tsdb;
  SVnode    *pVnode = pTsdb->pVnode;
  STFileSet *fset = NULL;
  SRTNer     rtner = {
          .tsdb = pTsdb,
          .szPage = pVnode->config.tsdbPageSize,
          .now = rtnArg->now,
          .cid = tsdbFSAllocEid(pTsdb->pFS),
  };

  // begin task
  (void)taosThreadMutexLock(&pTsdb->mutex);

  // check if background task is disabled
  if (pTsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip retention", TD_VID(pTsdb->pVnode));
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
    return 0;
  }

  // set flag and copy
  tsdbBeginTaskOnFileSet(pTsdb, rtnArg->fid, EVA_TASK_RETENTION, &fset);
  if (fset && (code = tsdbTFileSetInitCopy(pTsdb, fset, &rtner.fset))) {
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  (void)taosThreadMutexUnlock(&pTsdb->mutex);

  // do retention
  if (rtner.fset) {
    if (rtnArg->s3Migrate) {
#ifdef USE_S3
      TAOS_CHECK_GOTO(tsdbDoS3Migrate(&rtner), &lino, _exit);
#endif
    } else {
      TAOS_CHECK_GOTO(tsdbDoRetention(&rtner), &lino, _exit);
    }

    TAOS_CHECK_GOTO(tsdbDoRetentionEnd(&rtner), &lino, _exit);
  }

_exit:
  if (rtner.fset) {
    (void)taosThreadMutexLock(&pTsdb->mutex);
    tsdbFinishTaskOnFileSet(pTsdb, rtnArg->fid, EVA_TASK_RETENTION);
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
  }

  // clear resources
  tsdbTFileSetClear(&rtner.fset);
  TARRAY2_DESTROY(&rtner.fopArr, NULL);
  taosMemoryFree(arg);
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(pTsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbAsyncRetentionImpl(STsdb *tsdb, int64_t now, bool s3Migrate) {
  int32_t code = 0;
  int32_t lino = 0;

  // check if background task is disabled
  if (tsdb->bgTaskDisabled) {
    tsdbInfo("vgId:%d, background task is disabled, skip retention", TD_VID(tsdb->pVnode));
    return 0;
  }

  STFileSet *fset;
  TARRAY2_FOREACH(tsdb->pFS->fSetArr, fset) {
    SRtnArg *arg = taosMemoryMalloc(sizeof(*arg));
    if (arg == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _exit);
    }

    arg->tsdb = tsdb;
    arg->now = now;
    arg->fid = fset->fid;
    arg->s3Migrate = s3Migrate;

    code = vnodeAsync(RETENTION_TASK_ASYNC, EVA_PRIORITY_LOW, tsdbRetention, tsdbRetentionCancel, arg,
                      &fset->retentionTask);
    if (code) {
      taosMemoryFree(arg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(tsdb->pVnode), __func__, __FILE__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbAsyncRetention(STsdb *tsdb, int64_t now) {
  int32_t code = 0;
  (void)taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncRetentionImpl(tsdb, now, false);
  (void)taosThreadMutexUnlock(&tsdb->mutex);
  return code;
}

#ifdef USE_S3
static int32_t tsdbS3FidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int32_t s3KeepLocal, int64_t nowSec) {
  int32_t localFid;
  TSKEY   key;

  if (pKeepCfg->precision == TSDB_TIME_PRECISION_MILLI) {
    nowSec = nowSec * 1000;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_MICRO) {
    nowSec = nowSec * 1000000l;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_NANO) {
    nowSec = nowSec * 1000000000l;
  }

  nowSec = nowSec - pKeepCfg->keepTimeOffset * tsTickPerHour[pKeepCfg->precision];

  key = nowSec - s3KeepLocal * tsTickPerMin[pKeepCfg->precision];
  localFid = tsdbKeyFid(key, pKeepCfg->days, pKeepCfg->precision);

  if (fid >= localFid) {
    return 0;
  } else {
    return 1;
  }
}

static int32_t tsdbMigrateDataFileLCS3(SRTNer *rtner, const STFileObj *fobj, int64_t size, int64_t chunksize) {
  int32_t   code = 0;
  int32_t   lino = 0;
  STFileOp  op = {0};
  TdFilePtr fdFrom = NULL, fdTo = NULL;
  int32_t   lcn = fobj->f->lcn + (size - 1) / chunksize;

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = fobj->f->did,
              .fid = fobj->f->fid,
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  char fname[TSDB_FILENAME_LEN];
  tsdbTFileName(rtner->tsdb, &op.nf, fname);
  char   *object_name = taosDirEntryBaseName(fname);
  char    object_name_prefix[TSDB_FILENAME_LEN];
  int32_t node_id = vnodeNodeId(rtner->tsdb->pVnode);
  snprintf(object_name_prefix, TSDB_FQDN_LEN, "%d/%s", node_id, object_name);

  char *dot = strrchr(object_name_prefix, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &lino, _exit);
  }

  char *dot2 = strchr(object_name, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &lino, _exit);
  }
  snprintf(dot2 + 1, TSDB_FQDN_LEN - (dot2 + 1 - object_name), "%d.data", fobj->f->lcn);

  // do copy the file
  for (int32_t cn = fobj->f->lcn; cn < lcn; ++cn) {
    snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - object_name_prefix), "%d.data", cn);
    int64_t c_offset = chunksize * (cn - fobj->f->lcn);

    TAOS_CHECK_GOTO(tcsPutObjectFromFileOffset(fname, object_name_prefix, c_offset, chunksize), &lino, _exit);
  }

  // copy last chunk
  int64_t lc_offset = chunksize * (lcn - fobj->f->lcn);
  int64_t lc_size = size - lc_offset;

  snprintf(dot2 + 1, TSDB_FQDN_LEN - (dot2 + 1 - object_name), "%d.data", fobj->f->lcn);

  fdFrom = taosOpenFile(fname, TD_FILE_READ);
  if (fdFrom == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  tsdbInfo("vgId:%d, open lcfile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, lc_size);

  snprintf(dot2 + 1, TSDB_FQDN_LEN - (dot2 + 1 - object_name), "%d.data", lcn);
  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  int64_t n = taosFSendFile(fdTo, fdFrom, &lc_offset, lc_size);
  if (n < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  if (taosCloseFile(&fdFrom) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }

  if (taosCloseFile(&fdTo) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  return code;
}

static int32_t tsdbMigrateDataFileS3(SRTNer *rtner, const STFileObj *fobj, int64_t size, int64_t chunksize) {
  int32_t   code = 0;
  int32_t   lino = 0;
  STFileOp  op = {0};
  int32_t   lcn = (size - 1) / chunksize + 1;
  TdFilePtr fdFrom = NULL, fdTo = NULL;

  // remove old
  op = (STFileOp){
      .optype = TSDB_FOP_REMOVE,
      .fid = fobj->f->fid,
      .of = fobj->f[0],
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  // create new
  op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = fobj->f->fid,
      .nf =
          {
              .type = fobj->f->type,
              .did = fobj->f->did,
              .fid = fobj->f->fid,
              .minVer = fobj->f->minVer,
              .maxVer = fobj->f->maxVer,
              .cid = fobj->f->cid,
              .size = fobj->f->size,
              .lcn = lcn,
              .stt[0] =
                  {
                      .level = fobj->f->stt[0].level,
                  },
          },
  };

  TAOS_CHECK_GOTO(TARRAY2_APPEND(&rtner->fopArr, op), &lino, _exit);

  char fname[TSDB_FILENAME_LEN];
  tsdbTFileName(rtner->tsdb, &op.nf, fname);
  char   *object_name = taosDirEntryBaseName(fname);
  char    object_name_prefix[TSDB_FILENAME_LEN];
  int32_t node_id = vnodeNodeId(rtner->tsdb->pVnode);
  snprintf(object_name_prefix, TSDB_FQDN_LEN, "%d/%s", node_id, object_name);

  char *dot = strrchr(object_name_prefix, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &lino, _exit);
  }

  // do copy the file
  for (int32_t cn = 1; cn < lcn; ++cn) {
    snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - object_name_prefix), "%d.data", cn);
    int64_t c_offset = chunksize * (cn - 1);

    TAOS_CHECK_GOTO(tcsPutObjectFromFileOffset(fobj->fname, object_name_prefix, c_offset, chunksize), &lino, _exit);
  }

  // copy last chunk
  int64_t lc_offset = (int64_t)(lcn - 1) * chunksize;
  int64_t lc_size = size - lc_offset;

  dot = strchr(object_name, '.');
  if (!dot) {
    tsdbError("vgId:%d, incorrect lcn: %d, %s at line %d", TD_VID(rtner->tsdb->pVnode), lcn, __func__, lino);
    TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &lino, _exit);
  }
  snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - object_name), "%d.data", lcn);

  fdFrom = taosOpenFile(fobj->fname, TD_FILE_READ);
  if (fdFrom == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  tsdbInfo("vgId: %d, open lcfile: %s size: %" PRId64, TD_VID(rtner->tsdb->pVnode), fname, fobj->f->size);

  fdTo = taosOpenFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  int64_t n = taosFSendFile(fdTo, fdFrom, &lc_offset, lc_size);
  if (n < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  if (taosCloseFile(&fdFrom) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  if (taosCloseFile(&fdTo) != 0) {
    tsdbTrace("vgId:%d, failed to close file", TD_VID(rtner->tsdb->pVnode));
  }
  return code;
}

static int32_t tsdbDoS3Migrate(SRTNer *rtner) {
  int32_t code = 0;
  int32_t lino = 0;

  STFileSet *fset = rtner->fset;
  STFileObj *fobj = fset->farr[TSDB_FTYPE_DATA];
  if (!fobj) {
    return 0;
  }

  int32_t expLevel = tsdbFidLevel(fset->fid, &rtner->tsdb->keepCfg, rtner->now);
  if (expLevel < 0) {  // expired
    return 0;
  }

  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  int32_t    s3KeepLocal = pCfg->s3KeepLocal;
  int32_t    s3ExpLevel = tsdbS3FidLevel(fset->fid, &rtner->tsdb->keepCfg, s3KeepLocal, rtner->now);
  if (s3ExpLevel < 1) {  // keep on local storage
    return 0;
  }

  if (!tsdbShouldCompact(fset, TD_VID(rtner->tsdb->pVnode)) && fobj->f->lcn < 0) {
    fobj->f->lcn = 0;
  }

  int64_t chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;
  int32_t lcn = fobj->f->lcn;

  if (/*lcn < 1 && */ taosCheckExistFile(fobj->fname)) {
    int64_t mtime = 0;
    int64_t size = 0;
    int32_t r = taosStatFile(fobj->fname, &size, &mtime, NULL);
    if (size > chunksize && mtime < rtner->now - tsS3UploadDelaySec) {
      if (pCfg->s3Compact && lcn < 0) {
        STimeWindow win = {0};
        tsdbFidKeyRange(fset->fid, rtner->tsdb->keepCfg.days, rtner->tsdb->keepCfg.precision, &win.skey, &win.ekey);

        tsdbInfo("vgId:%d, async compact begin lcn: %d.", TD_VID(rtner->tsdb->pVnode), lcn);
        code = tsdbAsyncCompact(rtner->tsdb, &win, true);
        tsdbInfo("vgId:%d, async compact end lcn: %d.", TD_VID(rtner->tsdb->pVnode), lcn);
        goto _exit;
        return code;
      }

      TAOS_CHECK_GOTO(tsdbMigrateDataFileS3(rtner, fobj, size, chunksize), &lino, _exit);
    }
  } else {
    if (lcn <= 1) {
      TAOS_CHECK_GOTO(TSDB_CODE_INVALID_PARA, &lino, _exit);
    }
    char fname1[TSDB_FILENAME_LEN];
    tsdbTFileLastChunkName(rtner->tsdb, fobj->f, fname1);

    if (taosCheckExistFile(fname1)) {
      int64_t mtime = 0;
      int64_t size = 0;
      if (taosStatFile(fname1, &size, &mtime, NULL) != 0) {
        tsdbError("vgId:%d, %s failed at %s:%d ", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, __LINE__);
      }
      if (size > chunksize && mtime < rtner->now - tsS3UploadDelaySec) {
        TAOS_CHECK_GOTO(tsdbMigrateDataFileLCS3(rtner, fobj, size, chunksize), &lino, _exit);
      }
    } else {
      tsdbError("vgId:%d, file: %s not found, %s at line %d", TD_VID(rtner->tsdb->pVnode), fname1, __func__, lino);
      return code;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(rtner->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbAsyncS3Migrate(STsdb *tsdb, int64_t now) {
  int32_t code = 0;

  int32_t expired = grantCheck(TSDB_GRANT_OBJECT_STORAGE);
  if (expired && tsS3Enabled) {
    tsdbWarn("s3 grant expired: %d", expired);
    tsS3Enabled = false;
  } else if (!expired && tsS3EnabledCfg) {
    tsS3Enabled = true;
  }

  if (!tsS3Enabled) {
    return 0;
  }

  (void)taosThreadMutexLock(&tsdb->mutex);
  code = tsdbAsyncRetentionImpl(tsdb, now, true);
  (void)taosThreadMutexUnlock(&tsdb->mutex);

  if (code) {
    tsdbError("vgId:%d, %s failed, reason:%s", TD_VID(tsdb->pVnode), __func__, tstrerror(code));
  }
  return code;
}

#endif

