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

#include "crypt.h"
#include "tss.h"
#include "tsdb.h"
#include "tsdbDef.h"
#include "vnd.h"

static int32_t tsdbOpenFileImpl(STsdbFD *pFD) {
  int32_t     code = 0;
  int32_t     lino;
  const char *path = pFD->path;
  int32_t     szPage = pFD->szPage;
  int32_t     flag = pFD->flag;
  int64_t     lc_size = 0;

  pFD->pFD = taosOpenFile(path, flag);
  if (pFD->pFD == NULL) {
    if (tsSsEnabled && pFD->lcn > 1 && !strncmp(path + strlen(path) - 5, ".data", 5)) {
      char lc_path[TSDB_FILENAME_LEN];
      tstrncpy(lc_path, path, TSDB_FQDN_LEN);

      int32_t     vid = 0;
      const char *object_name = taosDirEntryBaseName((char *)path);
      (void)sscanf(object_name, "v%df%dver%" PRId64, &vid, &pFD->fid, &pFD->cid);

      char *dot = strrchr(lc_path, '.');
      if (!dot) {
        tsdbError("unexpected path: %s", lc_path);
        TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(ENOENT), lino, _exit);
      }
      snprintf(dot + 1, TSDB_FQDN_LEN - (dot + 1 - lc_path), "%d.data", pFD->lcn);

      pFD->pFD = taosOpenFile(lc_path, flag);
      if (pFD->pFD == NULL) {
        TSDB_CHECK_CODE(code = terrno, lino, _exit);
      }
      if (taosStatFile(lc_path, &lc_size, NULL, NULL) < 0) {
        TSDB_CHECK_CODE(code = terrno, lino, _exit);
      }
    } else {
      tsdbInfo("no file: %s", path);
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
    pFD->ssFile = 1;
  }

  pFD->pBuf = taosMemoryCalloc(1, szPage);
  if (pFD->pBuf == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  if (lc_size > 0) {
    SVnodeCfg *pCfg = &pFD->pTsdb->pVnode->config;
    int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->ssChunkSize;

    pFD->szFile = lc_size + chunksize * (pFD->lcn - 1);
  }

  // not check file size when reading data files.
  if (flag != TD_FILE_READ /* && !pFD->s3File*/) {
    if (!lc_size && taosStatFile(path, &pFD->szFile, NULL, NULL) < 0) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
  }

  if (pFD->szFile % szPage != 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_PARA, lino, _exit);
  }
  pFD->szFile = pFD->szFile / szPage;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

// =============== PAGE-WISE FILE ===============
int32_t tsdbOpenFile(const char *path, STsdb *pTsdb, int32_t flag, STsdbFD **ppFD, int32_t lcn) {
  int32_t  code = 0;
  int32_t  lino;
  STsdbFD *pFD = NULL;
  int32_t  szPage = pTsdb->pVnode->config.tsdbPageSize;

  *ppFD = NULL;

  pFD = (STsdbFD *)taosMemoryCalloc(1, sizeof(*pFD) + strlen(path) + 1);
  if (pFD == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  pFD->path = (char *)&pFD[1];
  memcpy(pFD->path, path, strlen(path) + 1);
  pFD->szPage = szPage;
  pFD->flag = flag;
  pFD->szPage = szPage;
  pFD->pgno = 0;
  pFD->lcn = lcn;
  pFD->pTsdb = pTsdb;

  *ppFD = pFD;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pTsdb->pVnode), lino, code);
  }
  return code;
}

void tsdbCloseFile(STsdbFD **ppFD) {
  STsdbFD *pFD = *ppFD;
  if (pFD) {
    taosMemoryFree(pFD->pBuf);
    int32_t code = taosCloseFile(&pFD->pFD);
    if (code) {
      tsdbError("failed to close file: %s, code:%d reason:%s", pFD->path, code, tstrerror(code));
    } else {
      tsdbTrace("close file: %s", pFD->path);
    }
    taosMemoryFree(pFD);
    *ppFD = NULL;
  }
}

static int32_t tsdbWriteFilePage(STsdbFD *pFD, int32_t encryptAlgorithm, char *encryptKey) {
  int32_t code = 0;
  int32_t lino;

  if (!pFD->pFD) {
    code = tsdbOpenFileImpl(pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pFD->pgno > 0) {
    int64_t offset = PAGE_OFFSET(pFD->pgno, pFD->szPage);
    if (pFD->ssFile && pFD->lcn > 1) {
      SVnodeCfg *pCfg = &pFD->pTsdb->pVnode->config;
      int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->ssChunkSize;
      int64_t    chunkoffset = chunksize * (pFD->lcn - 1);

      offset -= chunkoffset;
    }

    int64_t n = taosLSeekFile(pFD->pFD, offset, SEEK_SET);
    if (n < 0) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }

    code = taosCalcChecksumAppend(0, pFD->pBuf, pFD->szPage);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (encryptAlgorithm == DND_CA_SM4) {
      // if(tsiEncryptAlgorithm == DND_CA_SM4 && (tsiEncryptScope & DND_CS_TSDB) == DND_CS_TSDB){
      unsigned char PacketData[128];
      int           NewLen;
      int32_t       count = 0;
      while (count < pFD->szPage) {
        SCryptOpts opts = {0};
        opts.len = 128;
        opts.source = pFD->pBuf + count;
        opts.result = PacketData;
        opts.unitLen = 128;
        tstrncpy(opts.key, encryptKey, ENCRYPT_KEY_LEN + 1);

        NewLen = CBC_Encrypt(&opts);

        memcpy(pFD->pBuf + count, PacketData, NewLen);
        count += NewLen;
      }
      // tsdbDebug("CBC_Encrypt count:%d %s", count, __FUNCTION__);
    }

    n = taosWriteFile(pFD->pFD, pFD->pBuf, pFD->szPage);
    if (n < 0) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }

    if (pFD->szFile < pFD->pgno) {
      pFD->szFile = pFD->pgno;
    }
  }
  pFD->pgno = 0;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbReadFilePage(STsdbFD *pFD, int64_t pgno, int32_t encryptAlgorithm, char *encryptKey) {
  int32_t code = 0;
  int32_t lino;

  if (!pFD->pFD) {
    code = tsdbOpenFileImpl(pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  int64_t offset = PAGE_OFFSET(pgno, pFD->szPage);
  if (pFD->lcn > 1) {
    SVnodeCfg *pCfg = &pFD->pTsdb->pVnode->config;
    int64_t    chunksize = (int64_t)pCfg->tsdbPageSize * pCfg->ssChunkSize;
    int64_t    chunkoffset = chunksize * (pFD->lcn - 1);

    offset -= chunkoffset;
  }

  // seek
  int64_t n = taosLSeekFile(pFD->pFD, offset, SEEK_SET);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // read
  n = taosReadFile(pFD->pFD, pFD->pBuf, pFD->szPage);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  } else if (n < pFD->szPage) {
    tsdbError(
        "vgId:%d %s failed at %s:%d since read file size is less than page size, "
        "read size: %" PRId64 ", page size: %d, fname:%s, pgno:%" PRId64,
        TD_VID(pFD->pTsdb->pVnode), __func__, __FILE__, __LINE__, n, pFD->szPage, pFD->path, pFD->pgno);
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }
  //}

  if (encryptAlgorithm == DND_CA_SM4) {
    // if(tsiEncryptAlgorithm == DND_CA_SM4 && (tsiEncryptScope & DND_CS_TSDB) == DND_CS_TSDB){
    unsigned char PacketData[128];
    int           NewLen;

    int32_t count = 0;
    while (count < pFD->szPage) {
      SCryptOpts opts = {0};
      opts.len = 128;
      opts.source = pFD->pBuf + count;
      opts.result = PacketData;
      opts.unitLen = 128;
      tstrncpy(opts.key, encryptKey, ENCRYPT_KEY_LEN + 1);

      NewLen = CBC_Decrypt(&opts);

      memcpy(pFD->pBuf + count, PacketData, NewLen);
      count += NewLen;
    }
    // tsdbDebug("CBC_Decrypt count:%d %s", count, __FUNCTION__);
  }

  // check
  if (pgno > 1 && !taosCheckChecksumWhole(pFD->pBuf, pFD->szPage)) {
    tsdbError("vgId:%d %s failed at %s:%d since checksum mismatch, fname:%s, pgno:%" PRId64, TD_VID(pFD->pTsdb->pVnode),
              __func__, __FILE__, __LINE__, pFD->path, pgno);
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

  pFD->pgno = pgno;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbWriteFile(STsdbFD *pFD, int64_t offset, const uint8_t *pBuf, int64_t size, int32_t encryptAlgorithm,
                      char *encryptKey) {
  int32_t code = 0;
  int32_t lino;
  int64_t fOffset = LOGIC_TO_FILE_OFFSET(offset, pFD->szPage);
  int64_t pgno = OFFSET_PGNO(fOffset, pFD->szPage);
  int64_t bOffset = fOffset % pFD->szPage;
  int64_t n = 0;

  do {
    if (pFD->pgno != pgno) {
      code = tsdbWriteFilePage(pFD, encryptAlgorithm, encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pgno <= pFD->szFile) {
        code = tsdbReadFilePage(pFD, pgno, encryptAlgorithm, encryptKey);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        pFD->pgno = pgno;
      }
    }

    int64_t nWrite = TMIN(PAGE_CONTENT_SIZE(pFD->szPage) - bOffset, size - n);
    memcpy(pFD->pBuf + bOffset, pBuf + n, nWrite);

    pgno++;
    bOffset = 0;
    n += nWrite;
  } while (n < size);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbReadFileImp(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size, int32_t encryptAlgorithm,
                               char *encryptKey) {
  int32_t code = 0;
  int32_t lino;
  int64_t n = 0;
  int64_t fOffset = LOGIC_TO_FILE_OFFSET(offset, pFD->szPage);
  int64_t pgno = OFFSET_PGNO(fOffset, pFD->szPage);
  int32_t szPgCont = PAGE_CONTENT_SIZE(pFD->szPage);
  int64_t bOffset = fOffset % pFD->szPage;

  if (bOffset >= szPgCont) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_PARA, lino, _exit);
  }

  while (n < size) {
    if (pFD->pgno != pgno) {
      code = tsdbReadFilePage(pFD, pgno, encryptAlgorithm, encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    int64_t nRead = TMIN(szPgCont - bOffset, size - n);
    memcpy(pBuf + n, pFD->pBuf + bOffset, nRead);

    n += nRead;
    pgno++;
    bOffset = 0;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}


#ifdef USE_SHARED_STORAGE
static int32_t tsdbReadFileBlock(STsdbFD *pFD, int64_t offset, int64_t size, uint8_t **ppBlock) {
  int32_t code = 0, lino, vid = TD_VID(pFD->pTsdb->pVnode);

  char* buf = taosMemoryCalloc(1, size);
  if (buf == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  SVnodeCfg *pCfg = &pFD->pTsdb->pVnode->config;
  int64_t szChunk = (int64_t)pCfg->tsdbPageSize * pCfg->ssChunkSize;

  for (int64_t n = 0, nRead = 0; n < size; n += nRead, offset += nRead) {
    int chunk = offset / szChunk + 1;

    if (chunk >= pFD->lcn) {
      if (taosLSeekFile(pFD->pFD, offset - szChunk * (pFD->lcn - 1), SEEK_SET) < 0) {
        TSDB_CHECK_CODE(code = terrno, lino, _exit);
      }

      int64_t toRead = size - n;
      nRead = taosReadFile(pFD->pFD, buf + n, toRead);
      if (nRead < 0) {
        TSDB_CHECK_CODE(code = terrno, lino, _exit);
      } else if (nRead < toRead) {
        TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
      }
    } else if (tsSsEnabled) {
      int64_t toRead = TMIN(szChunk - offset % szChunk, size - n);
      nRead = toRead;

      char path[TSDB_FILENAME_LEN];
      sprintf(path, "vnode%d/f%d/v%df%dver%" PRId64 ".%d.data", vid, pFD->fid, vid, pFD->fid, pFD->cid, chunk);

      code = tssReadFileFromDefault(path, offset % szChunk, buf + n, &nRead);
      TSDB_CHECK_CODE(code, lino, _exit);
      if (nRead < toRead) {
        TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
      }
    } else {
      TSDB_CHECK_CODE(code = TSDB_CODE_OPS_NOT_SUPPORT, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
    taosMemoryFree(buf);
  } else {
    *ppBlock = (uint8_t *)buf;
  }
  return code;
}
#endif



static int32_t tsdbReadFileSs(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size, int64_t szHint) {
#ifdef USE_SHARED_STORAGE
  int32_t code = 0;
  int32_t lino;
  int64_t n = 0;
  int32_t szPgCont = PAGE_CONTENT_SIZE(pFD->szPage);
  int64_t fOffset = LOGIC_TO_FILE_OFFSET(offset, pFD->szPage);
  int64_t pgno = OFFSET_PGNO(fOffset, pFD->szPage);
  int64_t bOffset = fOffset % pFD->szPage;

  if (bOffset >= szPgCont) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_PARA, lino, _exit);
  }

  // 1, find pgnoStart & pgnoEnd to fetch from s3, if all pgs are local, no need to fetch
  // 2, fetch pgnoStart ~ pgnoEnd from s3
  // 3, store pgs to pcache & last pg to pFD->pBuf
  // 4, deliver pgs to [pBuf, pBuf + size)

  while (n < size) {
    if (pFD->pgno != pgno) {
      LRUHandle *handle = NULL;
      code = tsdbCacheGetPageSs(pFD->pTsdb->pgCache, pFD, pgno, &handle);
      if (code != TSDB_CODE_SUCCESS) {
        if (handle) {
          tsdbCacheRelease(pFD->pTsdb->pgCache, handle);
        }
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (!handle) {
        break;
      }

      uint8_t *pPage = (uint8_t *)taosLRUCacheValue(pFD->pTsdb->pgCache, handle);
      memcpy(pFD->pBuf, pPage, pFD->szPage);
      tsdbCacheRelease(pFD->pTsdb->pgCache, handle);

      // check
      if (pgno > 1 && !taosCheckChecksumWhole(pFD->pBuf, pFD->szPage)) {
        TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
      }

      pFD->pgno = pgno;
    }

    int64_t nRead = TMIN(szPgCont - bOffset, size - n);
    memcpy(pBuf + n, pFD->pBuf + bOffset, nRead);

    n += nRead;
    ++pgno;
    bOffset = 0;
  }

  if (n < size) {
    // 2, retrieve pgs from s3
    uint8_t *pBlock = NULL;
    int64_t  retrieve_offset = PAGE_OFFSET(pgno, pFD->szPage);
    int64_t  pgnoEnd = pgno - 1 + (bOffset + size - n + szPgCont - 1) / szPgCont;

    if (szHint > 0) {
      pgnoEnd = pgno - 1 + (bOffset + szHint - n + szPgCont - 1) / szPgCont;
    }

    int64_t retrieve_size = (pgnoEnd - pgno + 1) * pFD->szPage;

    code = tsdbReadFileBlock(pFD, retrieve_offset, retrieve_size, &pBlock);
    TSDB_CHECK_CODE(code, lino, _exit);
    // 3, Store Pages in Cache
    int nPage = pgnoEnd - pgno + 1;
    for (int i = 0; i < nPage; ++i) {
      if (pFD->szFile != pgno) {  // DONOT cache last volatile page
        tsdbCacheSetPageSs(pFD->pTsdb->pgCache, pFD, pgno, pBlock + i * pFD->szPage);
      }

      if (szHint > 0 && n >= size) {
        ++pgno;
        continue;
      }
      memcpy(pFD->pBuf, pBlock + i * pFD->szPage, pFD->szPage);

      // check
      if (pgno > 1 && !taosCheckChecksumWhole(pFD->pBuf, pFD->szPage)) {
        TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
      }

      pFD->pgno = pgno;

      int64_t nRead = TMIN(szPgCont - bOffset, size - n);
      memcpy(pBuf + n, pFD->pBuf + bOffset, nRead);

      n += nRead;
      ++pgno;
      bOffset = 0;
    }

    taosMemoryFree(pBlock);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
#else
  return TSDB_CODE_INTERNAL_ERROR;
#endif
}

int32_t tsdbReadFile(STsdbFD *pFD, int64_t offset, uint8_t *pBuf, int64_t size, int64_t szHint,
                     int32_t encryptAlgorithm, char *encryptKey) {
  int32_t code = 0;
  int32_t lino;

  if (!pFD->pFD) {
    code = tsdbOpenFileImpl(pFD);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pFD->ssFile && pFD->lcn > 1 /* && tsSsBlockSize < 0*/) {
    code = tsdbReadFileSs(pFD, offset, pBuf, size, szHint);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbReadFileImp(pFD, offset, pBuf, size, encryptAlgorithm, encryptKey);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbReadFileToBuffer(STsdbFD *pFD, int64_t offset, int64_t size, SBuffer *buffer, int64_t szHint,
                             int32_t encryptAlgorithm, char *encryptKey) {
  int32_t code;
  int32_t lino;

  code = tBufferEnsureCapacity(buffer, buffer->size + size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(pFD, offset, (uint8_t *)tBufferGetDataEnd(buffer), size, szHint, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  buffer->size += size;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFsyncFile(STsdbFD *pFD, int32_t encryptAlgorithm, char *encryptKey) {
  int32_t code = 0;
  int32_t lino;

  code = tsdbWriteFilePage(pFD, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosFsyncFile(pFD->pFD) < 0) {
    TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(ERRNO), lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pFD->pTsdb->pVnode), lino, code);
  }
  return code;
}

// SDataFReader ====================================================
int32_t tsdbDataFReaderOpen(SDataFReader **ppReader, STsdb *pTsdb, SDFileSet *pSet) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SDataFReader *pReader = NULL;
  int32_t       szPage = pTsdb->pVnode->config.tsdbPageSize;
  char          fname[TSDB_FILENAME_LEN];

  // alloc
  pReader = (SDataFReader *)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }
  pReader->pTsdb = pTsdb;
  pReader->pSet = pSet;

  // head
  tsdbHeadFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pHeadF, fname);
  code = tsdbOpenFile(fname, pTsdb, TD_FILE_READ, &pReader->pHeadFD, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // data
  tsdbDataFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pDataF, fname);
  code = tsdbOpenFile(fname, pTsdb, TD_FILE_READ, &pReader->pDataFD, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // sma
  tsdbSmaFileName(pTsdb, pSet->diskId, pSet->fid, pSet->pSmaF, fname);
  code = tsdbOpenFile(fname, pTsdb, TD_FILE_READ, &pReader->pSmaFD, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt
  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    tsdbSttFileName(pTsdb, pSet->diskId, pSet->fid, pSet->aSttF[iStt], fname);
    code = tsdbOpenFile(fname, pTsdb, TD_FILE_READ, &pReader->aSttFD[iStt], 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    *ppReader = NULL;
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));

    if (pReader) {
      for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) tsdbCloseFile(&pReader->aSttFD[iStt]);
      tsdbCloseFile(&pReader->pSmaFD);
      tsdbCloseFile(&pReader->pDataFD);
      tsdbCloseFile(&pReader->pHeadFD);
      taosMemoryFree(pReader);
    }
  } else {
    *ppReader = pReader;
  }
  return code;
}

void tsdbDataFReaderClose(SDataFReader **ppReader) {
  if (*ppReader == NULL) return;

  // head
  tsdbCloseFile(&(*ppReader)->pHeadFD);

  // data
  tsdbCloseFile(&(*ppReader)->pDataFD);

  // sma
  tsdbCloseFile(&(*ppReader)->pSmaFD);

  // stt
  for (int32_t iStt = 0; iStt < TSDB_STT_TRIGGER_ARRAY_SIZE; iStt++) {
    if ((*ppReader)->aSttFD[iStt]) {
      tsdbCloseFile(&(*ppReader)->aSttFD[iStt]);
    }
  }

  for (int32_t iBuf = 0; iBuf < sizeof((*ppReader)->aBuf) / sizeof(uint8_t *); iBuf++) {
    tFree((*ppReader)->aBuf[iBuf]);
  }
  taosMemoryFree(*ppReader);
  *ppReader = NULL;
}

int32_t tsdbReadBlockIdx(SDataFReader *pReader, SArray *aBlockIdx) {
  int32_t    code = 0;
  int32_t    lino;
  SHeadFile *pHeadFile = pReader->pSet->pHeadF;
  int64_t    offset = pHeadFile->offset;
  int64_t    size = pHeadFile->size - offset;

  taosArrayClear(aBlockIdx);
  if (size == 0) {
    return code;
  }

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read
  int32_t encryptAlgorithm = pReader->pTsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = pReader->pTsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbReadFile(pReader->pHeadFD, offset, pReader->aBuf[0], size, 0, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decode
  int64_t n = 0;
  while (n < size) {
    SBlockIdx blockIdx;
    n += tGetBlockIdx(pReader->aBuf[0] + n, &blockIdx);

    if (taosArrayPush(aBlockIdx, &blockIdx) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
  }
  if (n != size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pReader->pTsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbReadSttBlk(SDataFReader *pReader, int32_t iStt, SArray *aSttBlk) {
  int32_t   code = 0;
  int32_t   lino;
  SSttFile *pSttFile = pReader->pSet->aSttF[iStt];
  int64_t   offset = pSttFile->offset;
  int64_t   size = pSttFile->size - offset;

  taosArrayClear(aSttBlk);
  if (size == 0) {
    return 0;
  }

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read
  int32_t encryptAlgorithm = pReader->pTsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = pReader->pTsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbReadFile(pReader->aSttFD[iStt], offset, pReader->aBuf[0], size, 0, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decode
  int64_t n = 0;
  while (n < size) {
    SSttBlk sttBlk;
    n += tGetSttBlk(pReader->aBuf[0] + n, &sttBlk);

    if (taosArrayPush(aSttBlk, &sttBlk) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
  }
  if (n != size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pReader->pTsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbReadDataBlk(SDataFReader *pReader, SBlockIdx *pBlockIdx, SMapData *mDataBlk) {
  int32_t code = 0;
  int32_t lino;
  int64_t offset = pBlockIdx->offset;
  int64_t size = pBlockIdx->size;

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read
  int32_t encryptAlgorithm = pReader->pTsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = pReader->pTsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbReadFile(pReader->pHeadFD, offset, pReader->aBuf[0], size, 0, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decode
  int32_t n;
  code = tGetMapData(pReader->aBuf[0], mDataBlk, &n);
  if (code) goto _exit;
  if (n != size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pReader->pTsdb->pVnode), lino, code);
  }
  return code;
}

// SDelFReader ====================================================
struct SDelFReader {
  STsdb   *pTsdb;
  SDelFile fDel;
  STsdbFD *pReadH;
  uint8_t *aBuf[1];
};

int32_t tsdbDelFReaderOpen(SDelFReader **ppReader, SDelFile *pFile, STsdb *pTsdb) {
  int32_t      code = 0;
  int32_t      lino = 0;
  char         fname[TSDB_FILENAME_LEN];
  SDelFReader *pDelFReader = NULL;

  // alloc
  pDelFReader = (SDelFReader *)taosMemoryCalloc(1, sizeof(*pDelFReader));
  if (pDelFReader == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // open impl
  pDelFReader->pTsdb = pTsdb;
  pDelFReader->fDel = *pFile;

  tsdbDelFileName(pTsdb, pFile, fname);
  code = tsdbOpenFile(fname, pTsdb, TD_FILE_READ, &pDelFReader->pReadH, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    *ppReader = NULL;
    TSDB_ERROR_LOG(TD_VID(pTsdb->pVnode), lino, code);
    taosMemoryFree(pDelFReader);
  } else {
    *ppReader = pDelFReader;
  }
  return code;
}

void tsdbDelFReaderClose(SDelFReader **ppReader) {
  int32_t      code = 0;
  SDelFReader *pReader = *ppReader;

  if (pReader) {
    tsdbCloseFile(&pReader->pReadH);
    for (int32_t iBuf = 0; iBuf < sizeof(pReader->aBuf) / sizeof(uint8_t *); iBuf++) {
      tFree(pReader->aBuf[iBuf]);
    }
    taosMemoryFree(pReader);
  }

  *ppReader = NULL;
}

int32_t tsdbReadDelData(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData) {
  return tsdbReadDelDatav1(pReader, pDelIdx, aDelData, INT64_MAX);
}

int32_t tsdbReadDelDatav1(SDelFReader *pReader, SDelIdx *pDelIdx, SArray *aDelData, int64_t maxVer) {
  int32_t code = 0;
  int32_t lino;
  int64_t offset = pDelIdx->offset;
  int64_t size = pDelIdx->size;
  int64_t n;

  taosArrayClear(aDelData);

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read
  int32_t encryptAlgorithm = pReader->pTsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = pReader->pTsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbReadFile(pReader->pReadH, offset, pReader->aBuf[0], size, 0, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // // decode
  n = 0;
  while (n < size) {
    SDelData delData;
    n += tGetDelData(pReader->aBuf[0] + n, &delData);

    if (delData.version > maxVer) {
      continue;
    }
    if (taosArrayPush(aDelData, &delData) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
  }

  if (n != size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pReader->pTsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbReadDelIdx(SDelFReader *pReader, SArray *aDelIdx) {
  int32_t code = 0;
  int32_t lino;
  int32_t n;
  int64_t offset = pReader->fDel.offset;
  int64_t size = pReader->fDel.size - offset;

  taosArrayClear(aDelIdx);

  // alloc
  code = tRealloc(&pReader->aBuf[0], size);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read
  int32_t encryptAlgorithm = pReader->pTsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = pReader->pTsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbReadFile(pReader->pReadH, offset, pReader->aBuf[0], size, 0, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decode
  n = 0;
  while (n < size) {
    SDelIdx delIdx;

    n += tGetDelIdx(pReader->aBuf[0] + n, &delIdx);

    if (taosArrayPush(aDelIdx, &delIdx) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _exit);
    }
  }

  if (n != size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(pReader->pTsdb->pVnode), lino, code);
  }
  return code;
}
