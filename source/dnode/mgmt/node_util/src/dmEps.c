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
#include "dmUtil.h"
#include "tjson.h"
#include "tmisce.h"

typedef struct {
  int32_t  id;
  uint16_t oldPort;
  uint16_t newPort;
  char     oldFqdn[TSDB_FQDN_LEN];
  char     newFqdn[TSDB_FQDN_LEN];
} SDnodeEpPair;

static void    dmPrintEps(SDnodeData *pData);
static bool    dmIsEpChanged(SDnodeData *pData, int32_t dnodeId, const char *ep);
static void    dmResetEps(SDnodeData *pData, SArray *dnodeEps);
static int32_t dmReadDnodePairs(SDnodeData *pData);

void dmGetDnodeEp(void *data, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  SDnodeData *pData = data;
  (void)taosThreadRwlockRdlock(&pData->lock);

  SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    if (pPort != NULL) {
      *pPort = pDnodeEp->ep.port;
    }
    if (pFqdn != NULL) {
      tstrncpy(pFqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
    }
    if (pEp != NULL) {
      snprintf(pEp, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    }
  }

  (void)taosThreadRwlockUnlock(&pData->lock);
}

static int32_t dmDecodeEps(SJson *pJson, SDnodeData *pData) {
  int32_t code = 0;

  tjsonGetInt32ValueFromDouble(pJson, "dnodeId", pData->dnodeId, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "dnodeVer", pData->dnodeVer, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "engineVer", pData->engineVer, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "clusterId", pData->clusterId, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "dropped", pData->dropped, code);
  if (code < 0) return -1;
#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
  tjsonGetInt32ValueFromDouble(pJson, "encryptAlgor", pData->encryptAlgorigthm, code);
  if (code < 0) return -1;
  tjsonGetInt32ValueFromDouble(pJson, "encryptScope", pData->encryptScope, code);
  if (code < 0) return -1;
#endif
  SJson *dnodes = tjsonGetObjectItem(pJson, "dnodes");
  if (dnodes == NULL) return 0;
  int32_t numOfDnodes = tjsonGetArraySize(dnodes);

  for (int32_t i = 0; i < numOfDnodes; ++i) {
    SJson *dnode = tjsonGetArrayItem(dnodes, i);
    if (dnode == NULL) return -1;

    SDnodeEp dnodeEp = {0};
    tjsonGetInt32ValueFromDouble(dnode, "id", dnodeEp.id, code);
    if (code < 0) return -1;
    code = tjsonGetStringValue(dnode, "fqdn", dnodeEp.ep.fqdn);
    if (code < 0) return -1;
    tjsonGetUInt16ValueFromDouble(dnode, "port", dnodeEp.ep.port, code);
    if (code < 0) return -1;
    tjsonGetInt8ValueFromDouble(dnode, "isMnode", dnodeEp.isMnode, code);
    if (code < 0) return -1;

    if (taosArrayPush(pData->dnodeEps, &dnodeEp) == NULL) return -1;
  }

  return 0;
}

int dmOccurrences(char *str, char *toSearch) {
  int   count = 0;
  char *ptr = str;
  while ((ptr = strstr(ptr, toSearch)) != NULL) {
    count++;
    ptr++;
  }
  return count;
}

void dmSplitStr(char **arr, char *str, const char *del) {
  char *lasts;
  char *s = strsep(&str, del);
  while (s != NULL) {
    *arr++ = s;
    s = strsep(&str, del);
  }
}

int32_t dmReadEps(SDnodeData *pData) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  pData->dnodeEps = taosArrayInit(1, sizeof(SDnodeEp));
  if (pData->dnodeEps == NULL) {
    code = terrno;
    dError("failed to calloc dnodeEp array since %s", terrstr());
    goto _OVER;
  }

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("dnode file:%s not exist", file);

#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
    if (strlen(tsEncryptAlgorithm) > 0) {
      if (strcmp(tsEncryptAlgorithm, "sm4") == 0) {
        pData->encryptAlgorigthm = DND_CA_SM4;
      } else {
        terrno = TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG;
        dError("invalid tsEncryptAlgorithm:%s", tsEncryptAlgorithm);
        goto _OVER;
      }

      dInfo("start to parse encryptScope:%s", tsEncryptScope);
      int32_t scopeLen = strlen(tsEncryptScope);
      if (scopeLen == 0) {
        terrno = TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG;
        dError("invalid tsEncryptScope:%s", tsEncryptScope);
        goto _OVER;
      }

      char *tmp = taosMemoryMalloc(scopeLen + 1);
      if (tmp == NULL) {
        dError("failed to malloc memory for tsEncryptScope:%s", tsEncryptScope);
        goto _OVER;
      }
      memset(tmp, 0, scopeLen + 1);
      memcpy(tmp, tsEncryptScope, scopeLen);

      int32_t count = dmOccurrences(tmp, ",");

      char **array = taosMemoryMalloc(sizeof(char *) * (count + 1));
      memset(array, 0, sizeof(char *) * (count + 1));
      dmSplitStr(array, tmp, ",");

      for (int32_t i = 0; i < count + 1; i++) {
        char *str = *(array + i);

        bool success = false;

        if (strcasecmp(str, "tsdb") == 0 || strcasecmp(str, "all") == 0) {
          pData->encryptScope |= DND_CS_TSDB;
          success = true;
        }
        if (strcasecmp(str, "vnode_wal") == 0 || strcasecmp(str, "all") == 0) {
          pData->encryptScope |= DND_CS_VNODE_WAL;
          success = true;
        }
        if (strcasecmp(str, "sdb") == 0 || strcasecmp(str, "all") == 0) {
          pData->encryptScope |= DND_CS_SDB;
          success = true;
        }
        if (strcasecmp(str, "mnode_wal") == 0 || strcasecmp(str, "all") == 0) {
          pData->encryptScope |= DND_CS_MNODE_WAL;
          success = true;
        }

        if (!success) {
          terrno = TSDB_CODE_DNODE_INVALID_ENCRYPT_CONFIG;
          taosMemoryFree(tmp);
          taosMemoryFree(array);
          dError("invalid tsEncryptScope:%s", tsEncryptScope);
          goto _OVER;
        }
      }

      taosMemoryFree(tmp);
      taosMemoryFree(array);

      dInfo("set tsCryptAlgorithm:%s, tsCryptScope:%s from cfg", tsEncryptAlgorithm, tsEncryptScope);
    }

#endif
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    dError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    dError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    code = terrno;
    dError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if ((code = dmDecodeEps(pJson, pData)) < 0) {
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read dnode file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read dnode file:%s since %s", file, terrstr());
    return terrno = code;
  }

  if (taosArrayGetSize(pData->dnodeEps) == 0) {
    SDnodeEp dnodeEp = {0};
    dnodeEp.isMnode = 1;
    if (taosGetFqdnPortFromEp(tsFirst, &dnodeEp.ep) != 0) {
      dError("failed to get fqdn and port from ep:%s", tsFirst);
    }
    if (taosArrayPush(pData->dnodeEps, &dnodeEp) == NULL) {
      return terrno;
    }
  }

  if ((code = dmReadDnodePairs(pData)) != 0) {
    return terrno = code;
  }

  dDebug("reset dnode list on startup");
  dmResetEps(pData, pData->dnodeEps);

  if (pData->oldDnodeEps == NULL && dmIsEpChanged(pData, pData->dnodeId, tsLocalEp)) {
    dError("localEp %s different with %s and need to be reconfigured", tsLocalEp, file);
    code = TSDB_CODE_INVALID_CFG;
    return terrno = code;
  }

  return code;
}

static int32_t dmEncodeEps(SJson *pJson, SDnodeData *pData) {
  if (tjsonAddDoubleToObject(pJson, "dnodeId", pData->dnodeId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "dnodeVer", pData->dnodeVer) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "engineVer", pData->engineVer) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "clusterId", pData->clusterId) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "dropped", pData->dropped) < 0) return -1;
#if defined(TD_ENTERPRISE) || defined(TD_ASTRA_TODO)
  if (tjsonAddDoubleToObject(pJson, "encryptAlgor", pData->encryptAlgorigthm) < 0) return -1;
  if (tjsonAddDoubleToObject(pJson, "encryptScope", pData->encryptScope) < 0) return -1;
#endif
  SJson *dnodes = tjsonCreateArray();
  if (dnodes == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "dnodes", dnodes) < 0) return -1;

  int32_t numOfEps = (int32_t)taosArrayGetSize(pData->dnodeEps);
  for (int32_t i = 0; i < numOfEps; ++i) {
    SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, i);
    SJson    *dnode = tjsonCreateObject();
    if (dnode == NULL) return -1;

    if (tjsonAddDoubleToObject(dnode, "id", pDnodeEp->id) < 0) return -1;
    if (tjsonAddStringToObject(dnode, "fqdn", pDnodeEp->ep.fqdn) < 0) return -1;
    if (tjsonAddDoubleToObject(dnode, "port", pDnodeEp->ep.port) < 0) return -1;
    if (tjsonAddDoubleToObject(dnode, "isMnode", pDnodeEp->isMnode) < 0) return -1;
    if (tjsonAddItemToArray(dnodes, dnode) < 0) return -1;
  }

  return 0;
}

int32_t dmWriteEps(SDnodeData *pData) {
  int32_t   code = 0;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;
  char      file[PATH_MAX] = {0};
  char      realfile[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json.bak", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  snprintf(realfile, sizeof(realfile), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  // if ((code == dmInitDndInfo(pData)) != 0) goto _OVER;
  TAOS_CHECK_GOTO(dmInitDndInfo(pData), NULL, _OVER);

  pJson = tjsonCreateObject();
  if (pJson == NULL) TAOS_CHECK_GOTO(terrno, NULL, _OVER);

  pData->engineVer = tsVersion;

  TAOS_CHECK_GOTO(dmEncodeEps(pJson, pData), NULL, _OVER);  // dmEncodeEps(pJson, pData) != 0) goto _OVER;

  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _OVER);
  }

  pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) TAOS_CHECK_GOTO(terrno, NULL, _OVER);

  int32_t len = strlen(buffer);
  if (taosWriteFile(pFile, buffer, len) <= 0) TAOS_CHECK_GOTO(terrno, NULL, _OVER);
  if (taosFsyncFile(pFile) < 0) TAOS_CHECK_GOTO(terrno, NULL, _OVER);

  (void)taosCloseFile(&pFile);
  TAOS_CHECK_GOTO(taosRenameFile(file, realfile), NULL, _OVER);

  pData->updateTime = taosGetTimestampMs();
  dInfo("succeed to write dnode file:%s, num:%d ver:%" PRId64, realfile, (int32_t)taosArrayGetSize(pData->dnodeEps),
        pData->dnodeVer);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);
  if (pFile != NULL) (void)taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to write dnode file:%s since %s, dnodeVer:%" PRId64, realfile, tstrerror(code), pData->dnodeVer);
  }
  return code;
}

int32_t dmGetDnodeSize(SDnodeData *pData) {
  int32_t size = 0;
  (void)taosThreadRwlockRdlock(&pData->lock);
  size = taosArrayGetSize(pData->dnodeEps);
  (void)taosThreadRwlockUnlock(&pData->lock);
  return size;
}

void dmUpdateEps(SDnodeData *pData, SArray *eps) {
  if (taosThreadRwlockWrlock(&pData->lock) != 0) {
    dError("failed to lock dnode lock");
  }

  dDebug("new dnode list get from mnode, dnodeVer:%" PRId64, pData->dnodeVer);
  dmResetEps(pData, eps);
  if (dmWriteEps(pData) != 0) {
    dError("failed to write dnode file");
  }

  if (taosThreadRwlockUnlock(&pData->lock) != 0) {
    dError("failed to unlock dnode lock");
  }
}

static void dmResetEps(SDnodeData *pData, SArray *dnodeEps) {
  if (pData->dnodeEps != dnodeEps) {
    SArray *tmp = pData->dnodeEps;
    pData->dnodeEps = taosArrayDup(dnodeEps, NULL);
    taosArrayDestroy(tmp);
  }

  pData->mnodeEps.inUse = 0;
  pData->mnodeEps.numOfEps = 0;

  int32_t mIndex = 0;
  int32_t numOfEps = (int32_t)taosArrayGetSize(dnodeEps);

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(dnodeEps, i);
    if (!pDnodeEp->isMnode) continue;
    if (mIndex >= TSDB_MAX_REPLICA) continue;
    pData->mnodeEps.numOfEps++;

    pData->mnodeEps.eps[mIndex] = pDnodeEp->ep;
    mIndex++;
  }
  epsetSort(&pData->mnodeEps);

  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pDnodeEp = taosArrayGet(dnodeEps, i);
    int32_t   code = taosHashPut(pData->dnodeHash, &pDnodeEp->id, sizeof(int32_t), pDnodeEp, sizeof(SDnodeEp));
    if (code) {
      dError("dnode:%d, fqdn:%s port:%u isMnode:%d failed to put into hash, reason:%s", pDnodeEp->id, pDnodeEp->ep.fqdn,
             pDnodeEp->ep.port, pDnodeEp->isMnode, tstrerror(code));
    }
  }

  pData->validMnodeEps = true;

  dmPrintEps(pData);
}

static void dmPrintEps(SDnodeData *pData) {
  int32_t numOfEps = (int32_t)taosArrayGetSize(pData->dnodeEps);
  dDebug("print dnode list, num:%d", numOfEps);
  for (int32_t i = 0; i < numOfEps; i++) {
    SDnodeEp *pEp = taosArrayGet(pData->dnodeEps, i);
    dDebug("dnode:%d, fqdn:%s port:%u isMnode:%d", pEp->id, pEp->ep.fqdn, pEp->ep.port, pEp->isMnode);
  }
}

static bool dmIsEpChanged(SDnodeData *pData, int32_t dnodeId, const char *ep) {
  bool changed = false;
  if (dnodeId == 0) return changed;
  (void)taosThreadRwlockRdlock(&pData->lock);

  SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    char epstr[TSDB_EP_LEN + 1] = {0};
    snprintf(epstr, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    changed = (strcmp(ep, epstr) != 0);
    if (changed) {
      dError("dnode:%d, localEp %s different from %s", dnodeId, ep, epstr);
    }
  }

  (void)taosThreadRwlockUnlock(&pData->lock);
  return changed;
}

void dmGetMnodeEpSet(void* data, SEpSet *pEpSet) {
  SDnodeData *pData = (SDnodeData*)data;
  (void)taosThreadRwlockRdlock(&pData->lock);
  *pEpSet = pData->mnodeEps;
  (void)taosThreadRwlockUnlock(&pData->lock);
}

void dmEpSetToStr(char *buf, int32_t len, SEpSet *epSet) {
  int32_t n = 0;
  n += tsnprintf(buf + n, len - n, "%s", "{");
  for (int i = 0; i < epSet->numOfEps; i++) {
    n += tsnprintf(buf + n, len - n, "%s:%d%s", epSet->eps[i].fqdn, epSet->eps[i].port,
                  (i + 1 < epSet->numOfEps ? ", " : ""));
  }
  n += tsnprintf(buf + n, len - n, "%s", "}");
}

static FORCE_INLINE void dmSwapEps(SEp *epLhs, SEp *epRhs) {
  SEp epTmp;

  epTmp.port = epLhs->port;
  tstrncpy(epTmp.fqdn, epLhs->fqdn, tListLen(epTmp.fqdn));

  epLhs->port = epRhs->port;
  tstrncpy(epLhs->fqdn, epRhs->fqdn, tListLen(epLhs->fqdn));

  epRhs->port = epTmp.port;
  tstrncpy(epRhs->fqdn, epTmp.fqdn, tListLen(epRhs->fqdn));
}

void dmRotateMnodeEpSet(SDnodeData *pData) {
  (void)taosThreadRwlockRdlock(&pData->lock);
  SEpSet *pEpSet = &pData->mnodeEps;
  for (int i = 1; i < pEpSet->numOfEps; i++) {
    dmSwapEps(&pEpSet->eps[i - 1], &pEpSet->eps[i]);
  }
  (void)taosThreadRwlockUnlock(&pData->lock);
}

void dmGetMnodeEpSetForRedirect(SDnodeData *pData, SRpcMsg *pMsg, SEpSet *pEpSet) {
  if (!pData->validMnodeEps) return;
  dmGetMnodeEpSet(pData, pEpSet);
  dTrace("msg is redirected, handle:%p num:%d use:%d", pMsg->info.handle, pEpSet->numOfEps, pEpSet->inUse);
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dTrace("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
    if (strcmp(pEpSet->eps[i].fqdn, tsLocalFqdn) == 0 && pEpSet->eps[i].port == tsServerPort) {
      pEpSet->inUse = (i + 1) % pEpSet->numOfEps;
    }
  }
}

void dmSetMnodeEpSet(SDnodeData *pData, SEpSet *pEpSet) {
  if (memcmp(pEpSet, &pData->mnodeEps, sizeof(SEpSet)) == 0) return;
  (void)taosThreadRwlockWrlock(&pData->lock);
  pData->mnodeEps = *pEpSet;
  (void)taosThreadRwlockUnlock(&pData->lock);

  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }
}

bool dmUpdateDnodeInfo(void *data, int32_t *did, int64_t *clusterId, char *fqdn, uint16_t *port) {
  bool        updated = false;
  SDnodeData *pData = data;
  int32_t     dnodeId = -1;
  if (did != NULL) dnodeId = *did;

  (void)taosThreadRwlockRdlock(&pData->lock);

  if (pData->oldDnodeEps != NULL) {
    int32_t size = (int32_t)taosArrayGetSize(pData->oldDnodeEps);
    for (int32_t i = 0; i < size; ++i) {
      SDnodeEpPair *pair = taosArrayGet(pData->oldDnodeEps, i);
      if (strcmp(pair->oldFqdn, fqdn) == 0 && pair->oldPort == *port) {
        dInfo("dnode:%d, update ep:%s:%u to %s:%u", dnodeId, fqdn, *port, pair->newFqdn, pair->newPort);
        tstrncpy(fqdn, pair->newFqdn, TSDB_FQDN_LEN);
        *port = pair->newPort;
        updated = true;
      }
    }
  }

  if (did != NULL && dnodeId <= 0) {
    int32_t size = (int32_t)taosArrayGetSize(pData->dnodeEps);
    for (int32_t i = 0; i < size; ++i) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, i);
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) == 0 && pDnodeEp->ep.port == *port) {
        dInfo("dnode:%s:%u, update dnodeId to dnode:%d", fqdn, *port, pDnodeEp->id);
        *did = pDnodeEp->id;
        if (clusterId != NULL) *clusterId = pData->clusterId;
      }
    }
  }

  if (dnodeId > 0) {
    SDnodeEp *pDnodeEp = taosHashGet(pData->dnodeHash, &dnodeId, sizeof(int32_t));
    if (pDnodeEp) {
      if (strcmp(pDnodeEp->ep.fqdn, fqdn) != 0 || pDnodeEp->ep.port != *port) {
        dInfo("dnode:%d, update ep:%s:%u to %s:%u", dnodeId, fqdn, *port, pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
        tstrncpy(fqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
        *port = pDnodeEp->ep.port;
        updated = true;
      }
      if (clusterId != NULL) *clusterId = pData->clusterId;
    }
  }

  (void)taosThreadRwlockUnlock(&pData->lock);
  return updated;
}

static int32_t dmDecodeEpPairs(SJson *pJson, SDnodeData *pData) {
  int32_t code = 0;

  SJson *dnodes = tjsonGetObjectItem(pJson, "dnodes");
  if (dnodes == NULL) return TSDB_CODE_INVALID_CFG_VALUE;
  int32_t numOfDnodes = tjsonGetArraySize(dnodes);

  for (int32_t i = 0; i < numOfDnodes; ++i) {
    SJson *dnode = tjsonGetArrayItem(dnodes, i);
    if (dnode == NULL) return TSDB_CODE_INVALID_CFG_VALUE;

    SDnodeEpPair pair = {0};
    tjsonGetInt32ValueFromDouble(dnode, "id", pair.id, code);
    if (code < 0) return TSDB_CODE_INVALID_CFG_VALUE;
    code = tjsonGetStringValue(dnode, "fqdn", pair.oldFqdn);
    if (code < 0) return TSDB_CODE_INVALID_CFG_VALUE;
    tjsonGetUInt16ValueFromDouble(dnode, "port", pair.oldPort, code);
    if (code < 0) return TSDB_CODE_INVALID_CFG_VALUE;
    code = tjsonGetStringValue(dnode, "new_fqdn", pair.newFqdn);
    if (code < 0) return TSDB_CODE_INVALID_CFG_VALUE;
    tjsonGetUInt16ValueFromDouble(dnode, "new_port", pair.newPort, code);
    if (code < 0) return TSDB_CODE_INVALID_CFG_VALUE;

    if (taosArrayPush(pData->oldDnodeEps, &pair) == NULL) return terrno;
  }

  return code;
}

void dmRemoveDnodePairs(SDnodeData *pData) {
  char file[PATH_MAX] = {0};
  char bak[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sep.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  snprintf(bak, sizeof(bak), "%s%sdnode%sep.json.bak", tsDataDir, TD_DIRSEP, TD_DIRSEP);
  dInfo("dnode file:%s is rename to bak file", file);
  if (taosRenameFile(file, bak) != 0) {
    dError("failed to rename dnode file:%s to bak file:%s since %s", file, bak, tstrerror(terrno));
  }
}

static int32_t dmReadDnodePairs(SDnodeData *pData) {
  int32_t   code = -1;
  TdFilePtr pFile = NULL;
  char     *content = NULL;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sep.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    code = terrno;
    dDebug("dnode file:%s not exist, reason:%s", file, tstrerror(code));
    code = 0;
    goto _OVER;
  }

  pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    dError("failed to open dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    dError("failed to fstat dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content = taosMemoryMalloc(size + 1);
  if (content == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, content, size) != size) {
    terrno = terrno;
    dError("failed to read dnode file:%s since %s", file, terrstr());
    goto _OVER;
  }

  content[size] = '\0';

  pJson = tjsonParse(content);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  pData->oldDnodeEps = taosArrayInit(1, sizeof(SDnodeEpPair));
  if (pData->oldDnodeEps == NULL) {
    code = terrno;
    dError("failed to calloc dnodeEp array since %s", strerror(ERRNO));
    goto _OVER;
  }

  if (dmDecodeEpPairs(pJson, pData) < 0) {
    taosArrayDestroy(pData->oldDnodeEps);
    pData->oldDnodeEps = NULL;

    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  code = 0;
  dInfo("succceed to read dnode file %s", file);

_OVER:
  if (content != NULL) taosMemoryFree(content);
  if (pJson != NULL) cJSON_Delete(pJson);
  if (pFile != NULL) taosCloseFile(&pFile);

  if (code != 0) {
    dError("failed to read dnode file:%s since %s", file, tstrerror(code));
    return code;
  }

  // update old fqdn and port
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pData->oldDnodeEps); ++i) {
    SDnodeEpPair *pair = taosArrayGet(pData->oldDnodeEps, i);
    for (int32_t j = 0; j < (int32_t)taosArrayGetSize(pData->dnodeEps); ++j) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, j);
      if (pDnodeEp->id == pair->id) {
        tstrncpy(pair->oldFqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
        pair->oldPort = pDnodeEp->ep.port;
      }
    }
  }

  // check new fqdn and port
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pData->oldDnodeEps); ++i) {
    SDnodeEpPair *pair = taosArrayGet(pData->oldDnodeEps, i);
    for (int32_t j = 0; j < (int32_t)taosArrayGetSize(pData->dnodeEps); ++j) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, j);
      if (pDnodeEp->id != pair->id &&
          (strcmp(pDnodeEp->ep.fqdn, pair->newFqdn) == 0 && pDnodeEp->ep.port == pair->newPort)) {
        dError("dnode:%d, can't update ep:%s:%u to %s:%u since already exists as dnode:%d", pair->id, pair->oldFqdn,
               pair->oldPort, pair->newFqdn, pair->newPort, pDnodeEp->id);
        taosArrayDestroy(pData->oldDnodeEps);
        pData->oldDnodeEps = NULL;
        code = TSDB_CODE_INVALID_CFG;
        return code;
      }
    }
  }

  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pData->oldDnodeEps); ++i) {
    SDnodeEpPair *pair = taosArrayGet(pData->oldDnodeEps, i);
    for (int32_t j = 0; j < (int32_t)taosArrayGetSize(pData->dnodeEps); ++j) {
      SDnodeEp *pDnodeEp = taosArrayGet(pData->dnodeEps, j);
      if (strcmp(pDnodeEp->ep.fqdn, pair->oldFqdn) == 0 && pDnodeEp->ep.port == pair->oldPort) {
        dInfo("dnode:%d, will update ep:%s:%u to %s:%u", pDnodeEp->id, pDnodeEp->ep.fqdn, pDnodeEp->ep.port,
              pair->newFqdn, pair->newPort);
        tstrncpy(pDnodeEp->ep.fqdn, pair->newFqdn, TSDB_FQDN_LEN);
        pDnodeEp->ep.port = pair->newPort;
      }
    }
  }

  pData->dnodeVer = 0;
  return 0;
}
