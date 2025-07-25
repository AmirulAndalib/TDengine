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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "vmInt.h"
#include "libs/function/tudf.h"
#include "osMemory.h"
#include "tfs.h"
#include "vnd.h"

int32_t vmGetPrimaryDisk(SVnodeMgmt *pMgmt, int32_t vgId) {
  int32_t    diskId = -1;
  SVnodeObj *pVnode = NULL;

  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);
  int32_t r = taosHashGetDup(pMgmt->runngingHash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode != NULL) {
    diskId = pVnode->diskPrimary;
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
  return diskId;
}

static void vmFreeVnodeObj(SVnodeObj **ppVnode) {
  if (!ppVnode || !(*ppVnode)) return;

  SVnodeObj *pVnode = *ppVnode;

  int32_t refCount = atomic_load_32(&pVnode->refCount);
  while (refCount > 0) {
    dWarn("vgId:%d, vnode is refenced, retry to free in 200ms, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
    taosMsleep(200);
    refCount = atomic_load_32(&pVnode->refCount);
  }

  taosMemoryFree(pVnode->path);
  taosMemoryFree(pVnode);
  ppVnode[0] = NULL;
}

static int32_t vmRegisterCreatingState(SVnodeMgmt *pMgmt, int32_t vgId, int32_t diskId) {
  int32_t    code = 0;
  SVnodeObj *pCreatingVnode = taosMemoryCalloc(1, sizeof(SVnodeObj));
  if (pCreatingVnode == NULL) {
    dError("failed to alloc vnode since %s", terrstr());
    return terrno;
  }
  (void)memset(pCreatingVnode, 0, sizeof(SVnodeObj));

  pCreatingVnode->vgId = vgId;
  pCreatingVnode->diskPrimary = diskId;

  code = taosThreadRwlockWrlock(&pMgmt->hashLock);
  if (code != 0) {
    taosMemoryFree(pCreatingVnode);
    return code;
  }

  dTrace("vgId:%d, put vnode into creating hash, pCreatingVnode:%p", vgId, pCreatingVnode);
  code = taosHashPut(pMgmt->creatingHash, &vgId, sizeof(int32_t), &pCreatingVnode, sizeof(SVnodeObj *));
  if (code != 0) {
    dError("vgId:%d, failed to put vnode to creatingHash", vgId);
    taosMemoryFree(pCreatingVnode);
  }

  int32_t r = taosThreadRwlockUnlock(&pMgmt->hashLock);
  if (r != 0) {
    dError("vgId:%d, failed to unlock since %s", vgId, tstrerror(r));
  }

  return code;
}

static void vmUnRegisterCreatingState(SVnodeMgmt *pMgmt, int32_t vgId) {
  SVnodeObj *pOld = NULL;

  (void)taosThreadRwlockWrlock(&pMgmt->hashLock);
  int32_t r = taosHashGetDup(pMgmt->creatingHash, &vgId, sizeof(int32_t), (void *)&pOld);
  if (r != 0) {
    dError("vgId:%d, failed to get vnode from creating Hash", vgId);
  }
  dTrace("vgId:%d, remove from creating Hash", vgId);
  r = taosHashRemove(pMgmt->creatingHash, &vgId, sizeof(int32_t));
  if (r != 0) {
    dError("vgId:%d, failed to remove vnode from creatingHash", vgId);
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);

  if (pOld) {
    dTrace("vgId:%d, free vnode pOld:%p", vgId, &pOld);
    vmFreeVnodeObj(&pOld);
  }

_OVER:
  if (r != 0) {
    dError("vgId:%d, failed to remove vnode from creatingHash since %s", vgId, tstrerror(r));
  }
}

int32_t vmAllocPrimaryDisk(SVnodeMgmt *pMgmt, int32_t vgId) {
  int32_t code = 0;
  STfs   *pTfs = pMgmt->pTfs;
  int32_t diskId = 0;
  if (!pTfs) {
    return diskId;
  }

  // search fs
  char vnodePath[TSDB_FILENAME_LEN] = {0};
  snprintf(vnodePath, TSDB_FILENAME_LEN - 1, "vnode%svnode%d", TD_DIRSEP, vgId);
  char fname[TSDB_FILENAME_LEN] = {0};
  char fnameTmp[TSDB_FILENAME_LEN] = {0};
  snprintf(fname, TSDB_FILENAME_LEN - 1, "%s%s%s", vnodePath, TD_DIRSEP, VND_INFO_FNAME);
  snprintf(fnameTmp, TSDB_FILENAME_LEN - 1, "%s%s%s", vnodePath, TD_DIRSEP, VND_INFO_FNAME_TMP);

  diskId = tfsSearch(pTfs, 0, fname);
  if (diskId >= 0) {
    return diskId;
  }
  diskId = tfsSearch(pTfs, 0, fnameTmp);
  if (diskId >= 0) {
    return diskId;
  }

  // alloc
  int32_t     disks[TFS_MAX_DISKS_PER_TIER] = {0};
  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = NULL;

  code = taosThreadMutexLock(&pMgmt->mutex);
  if (code != 0) {
    return code;
  }

  code = vmGetAllVnodeListFromHashWithCreating(pMgmt, &numOfVnodes, &ppVnodes);
  if (code != 0) {
    int32_t r = taosThreadMutexUnlock(&pMgmt->mutex);
    if (r != 0) {
      dError("vgId:%d, failed to unlock mutex since %s", vgId, tstrerror(r));
    }
    return code;
  }

  for (int32_t v = 0; v < numOfVnodes; v++) {
    SVnodeObj *pVnode = ppVnodes[v];
    disks[pVnode->diskPrimary] += 1;
  }

  int32_t minVal = INT_MAX;
  int32_t ndisk = tfsGetDisksAtLevel(pTfs, 0);
  diskId = 0;
  for (int32_t id = 0; id < ndisk; id++) {
    if (minVal > disks[id]) {
      minVal = disks[id];
      diskId = id;
    }
  }
  code = vmRegisterCreatingState(pMgmt, vgId, diskId);
  if (code != 0) {
    int32_t r = taosThreadMutexUnlock(&pMgmt->mutex);
    if (r != 0) {
      dError("vgId:%d, failed to unlock mutex since %s", vgId, tstrerror(r));
    }
    goto _OVER;
  }

  code = taosThreadMutexUnlock(&pMgmt->mutex);
  if (code != 0) {
    goto _OVER;
  }

_OVER:

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (ppVnodes == NULL || ppVnodes[i] == NULL) continue;
    vmReleaseVnode(pMgmt, ppVnodes[i]);
  }
  if (ppVnodes != NULL) {
    taosMemoryFree(ppVnodes);
  }

  if (code != 0) {
    dError("vgId:%d, failed to alloc disk since %s", vgId, tstrerror(code));
    return code;
  } else {
    dInfo("vgId:%d, alloc disk:%d of level 0. ndisk:%d, vnodes: %d", vgId, diskId, ndisk, numOfVnodes);
    return diskId;
  }
}

void vmCleanPrimaryDisk(SVnodeMgmt *pMgmt, int32_t vgId) { vmUnRegisterCreatingState(pMgmt, vgId); }

SVnodeObj *vmAcquireVnodeImpl(SVnodeMgmt *pMgmt, int32_t vgId, bool strict) {
  SVnodeObj *pVnode = NULL;

  (void)taosThreadRwlockRdlock(&pMgmt->hashLock);
  int32_t r = taosHashGetDup(pMgmt->runngingHash, &vgId, sizeof(int32_t), (void *)&pVnode);
  if (pVnode == NULL || strict && (pVnode->dropped || pVnode->failed)) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    dDebug("vgId:%d, acquire vnode failed.", vgId);
    pVnode = NULL;
  } else {
    int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);
    dTrace("vgId:%d, acquire vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);

  return pVnode;
}

SVnodeObj *vmAcquireVnode(SVnodeMgmt *pMgmt, int32_t vgId) { return vmAcquireVnodeImpl(pMgmt, vgId, true); }

void vmReleaseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  if (pVnode == NULL) return;

  //(void)taosThreadRwlockRdlock(&pMgmt->lock);
  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  dTrace("vgId:%d, release vnode, vnode:%p, ref:%d", pVnode->vgId, pVnode, refCount);
  //(void)taosThreadRwlockUnlock(&pMgmt->lock);
}

static int32_t vmRegisterRunningState(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  SVnodeObj *pOld = NULL;
  dInfo("vgId:%d, put vnode into running hash", pVnode->vgId);

  int32_t r = taosHashGetDup(pMgmt->runngingHash, &pVnode->vgId, sizeof(int32_t), (void *)&pOld);
  if (r != 0) {
    dError("vgId:%d, failed to get vnode from hash", pVnode->vgId);
  }
  if (pOld) {
    vmFreeVnodeObj(&pOld);
  }
  int32_t code = taosHashPut(pMgmt->runngingHash, &pVnode->vgId, sizeof(int32_t), &pVnode, sizeof(SVnodeObj *));

  return code;
}

static void vmUnRegisterRunningState(SVnodeMgmt *pMgmt, int32_t vgId) {
  dInfo("vgId:%d, remove from hash", vgId);
  int32_t r = taosHashRemove(pMgmt->runngingHash, &vgId, sizeof(int32_t));
  if (r != 0) {
    dError("vgId:%d, failed to remove vnode since %s", vgId, tstrerror(r));
  }
}

static int32_t vmRegisterClosedState(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  int32_t    code = 0;
  dInfo("vgId:%d, put into closed hash", pVnode->vgId);
  SVnodeObj *pClosedVnode = taosMemoryCalloc(1, sizeof(SVnodeObj));
  if (pClosedVnode == NULL) {
    dError("failed to alloc vnode since %s", terrstr());
    return terrno;
  }
  (void)memset(pClosedVnode, 0, sizeof(SVnodeObj));

  pClosedVnode->vgId = pVnode->vgId;
  pClosedVnode->dropped = pVnode->dropped;
  pClosedVnode->vgVersion = pVnode->vgVersion;
  pClosedVnode->diskPrimary = pVnode->diskPrimary;
  pClosedVnode->toVgId = pVnode->toVgId;
  pClosedVnode->mountId = pVnode->mountId;

  SVnodeObj *pOld = NULL;
  int32_t    r = taosHashGetDup(pMgmt->closedHash, &pVnode->vgId, sizeof(int32_t), (void *)&pOld);
  if (r != 0) {
    dError("vgId:%d, failed to get vnode from closedHash", pVnode->vgId);
  }
  if (pOld) {
    vmFreeVnodeObj(&pOld);
  }
  dInfo("vgId:%d, put vnode to closedHash", pVnode->vgId);
  r = taosHashPut(pMgmt->closedHash, &pVnode->vgId, sizeof(int32_t), &pClosedVnode, sizeof(SVnodeObj *));
  if (r != 0) {
    dError("vgId:%d, failed to put vnode to closedHash", pVnode->vgId);
  }

  return code;
}

static void vmUnRegisterClosedState(SVnodeMgmt *pMgmt, SVnodeObj *pVnode) {
  SVnodeObj *pOld = NULL;
  dInfo("vgId:%d, remove from closed hash", pVnode->vgId);
  int32_t r = taosHashGetDup(pMgmt->closedHash, &pVnode->vgId, sizeof(int32_t), (void *)&pOld);
  if (r != 0) {
    dError("vgId:%d, failed to get vnode from closedHash", pVnode->vgId);
  }
  if (pOld != NULL) {
    vmFreeVnodeObj(&pOld);
    dInfo("vgId:%d, remove from closedHash", pVnode->vgId);
    r = taosHashRemove(pMgmt->closedHash, &pVnode->vgId, sizeof(int32_t));
    if (r != 0) {
      dError("vgId:%d, failed to remove vnode from hash", pVnode->vgId);
    }
  }
}
#ifdef USE_MOUNT
int32_t vmAcquireMountTfs(SVnodeMgmt *pMgmt, int64_t mountId, const char *mountName, const char *mountPath,
                          STfs **ppTfs) {
  int32_t    code = 0, lino = 0;
  TdFilePtr  pFile = NULL;
  SArray    *pDisks = NULL;
  SMountTfs *pMountTfs = NULL;
  bool       unlock = false;

  pMountTfs = taosHashGet(pMgmt->mountTfsHash, &mountId, sizeof(mountId));
  if (pMountTfs && *(SMountTfs **)pMountTfs) {
    if (!(*ppTfs = (*(SMountTfs **)pMountTfs)->pTfs)) {
      TAOS_CHECK_EXIT(TSDB_CODE_INTERNAL_ERROR);
    }
    (void)atomic_add_fetch_32(&(*(SMountTfs **)pMountTfs)->nRef, 1);
    TAOS_RETURN(code);
  }
  if (!mountPath || mountPath[0] == 0 || mountId == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  (void)(taosThreadMutexLock(&pMgmt->mutex));
  unlock = true;
  pMountTfs = taosHashGet(pMgmt->mountTfsHash, &mountId, sizeof(mountId));
  if (pMountTfs && *(SMountTfs **)pMountTfs) {
    if (!(*ppTfs = (*(SMountTfs **)pMountTfs)->pTfs)) {
      TAOS_CHECK_EXIT(TSDB_CODE_INTERNAL_ERROR);
    }
    (void)taosThreadMutexUnlock(&pMgmt->mutex);
    (void)atomic_add_fetch_32(&(*(SMountTfs **)pMountTfs)->nRef, 1);
    TAOS_RETURN(code);
  }

  TAOS_CHECK_EXIT(vmMountCheckRunning(mountName, mountPath, &pFile, 3));
  TAOS_CHECK_EXIT(vmGetMountDisks(pMgmt, mountPath, &pDisks));
  int32_t numOfDisks = taosArrayGetSize(pDisks);
  if (numOfDisks <= 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
  }
  TSDB_CHECK_NULL((pMountTfs = taosMemoryCalloc(1, sizeof(SMountTfs))), code, lino, _exit, terrno);
  if (mountName) (void)snprintf(pMountTfs->name, sizeof(pMountTfs->name), "%s", mountName);
  if (mountPath) (void)snprintf(pMountTfs->path, sizeof(pMountTfs->path), "%s", mountPath);
  pMountTfs->pFile = pFile;
  atomic_store_32(&pMountTfs->nRef, 2);  // init and acquire
  TAOS_CHECK_EXIT(tfsOpen(TARRAY_GET_ELEM(pDisks, 0), numOfDisks, &pMountTfs->pTfs));
  TAOS_CHECK_EXIT(taosHashPut(pMgmt->mountTfsHash, &mountId, sizeof(mountId), &pMountTfs, POINTER_BYTES));
_exit:
  if (unlock) {
    (void)taosThreadMutexUnlock(&pMgmt->mutex);
  }
  taosArrayDestroy(pDisks);
  if (code != 0) {
    dError("mount:%" PRIi64 ",%s, failed at line %d to get mount tfs since %s", mountId, mountPath ? mountPath : "NULL",
           lino, tstrerror(code));
    if (pFile) {
      (void)taosUnLockFile(pFile);
      (void)taosCloseFile(&pFile);
    }
    if (pMountTfs) {
      tfsClose(pMountTfs->pTfs);
      taosMemoryFree(pMountTfs);
    }
    *ppTfs = NULL;
  } else {
    *ppTfs = pMountTfs->pTfs;
  }

  TAOS_RETURN(code);
}
#endif

bool vmReleaseMountTfs(SVnodeMgmt *pMgmt, int64_t mountId, int32_t minRef) {
#ifdef USE_MOUNT
  SMountTfs *pMountTfs = NULL;
  int32_t    nRef = INT32_MAX, code = 0;

  pMountTfs = taosHashGet(pMgmt->mountTfsHash, &mountId, sizeof(mountId));
  if (pMountTfs && *(SMountTfs **)pMountTfs) {
    if ((nRef = atomic_sub_fetch_32(&(*(SMountTfs **)pMountTfs)->nRef, 1)) <= minRef) {
      (void)(taosThreadMutexLock(&pMgmt->mutex));
      SMountTfs *pTmp = taosHashGet(pMgmt->mountTfsHash, &mountId, sizeof(mountId));
      if (pTmp && *(SMountTfs **)pTmp) {
        dInfo("mount:%" PRIi64 ", ref:%d, release mount tfs", mountId, nRef);
        tfsClose((*(SMountTfs **)pTmp)->pTfs);
        if ((*(SMountTfs **)pTmp)->pFile) {
          (void)taosUnLockFile((*(SMountTfs **)pTmp)->pFile);
          (void)taosCloseFile(&(*(SMountTfs **)pTmp)->pFile);
        }
        taosMemoryFree(*(SMountTfs **)pTmp);
        if ((code = taosHashRemove(pMgmt->mountTfsHash, &mountId, sizeof(mountId))) < 0) {
          dError("failed at line %d to remove mountId:%" PRIi64 " from mount tfs hash", __LINE__, mountId);
        }
      }
      (void)taosThreadMutexUnlock(&pMgmt->mutex);
      return true;
    }
  }
#endif
  return false;
}

int32_t vmOpenVnode(SVnodeMgmt *pMgmt, SWrapperCfg *pCfg, SVnode *pImpl) {
  SVnodeObj *pVnode = taosMemoryCalloc(1, sizeof(SVnodeObj));
  if (pVnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pVnode->vgId = pCfg->vgId;
  pVnode->vgVersion = pCfg->vgVersion;
  pVnode->diskPrimary = pCfg->diskPrimary;
  pVnode->mountId = pCfg->mountId;
  pVnode->refCount = 0;
  pVnode->dropped = 0;
  pVnode->failed = 0;
  pVnode->path = taosStrdup(pCfg->path);
  pVnode->pImpl = pImpl;

  if (pVnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    taosMemoryFree(pVnode);
    return -1;
  }

  if (pImpl) {
    if (vmAllocQueue(pMgmt, pVnode) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pVnode->path);
      taosMemoryFree(pVnode);
      return -1;
    }
  } else {
    pVnode->failed = 1;
  }

  (void)taosThreadRwlockWrlock(&pMgmt->hashLock);
  int32_t code = vmRegisterRunningState(pMgmt, pVnode);
  vmUnRegisterClosedState(pMgmt, pVnode);
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);

  TAOS_RETURN(code);
}

void vmCloseVnode(SVnodeMgmt *pMgmt, SVnodeObj *pVnode, bool commitAndRemoveWal, bool keepClosed) {
  char path[TSDB_FILENAME_LEN] = {0};
  bool atExit = true;

  if (pVnode->pImpl && vnodeIsLeader(pVnode->pImpl)) {
    vnodeProposeCommitOnNeed(pVnode->pImpl, atExit);
  }

  (void)taosThreadRwlockWrlock(&pMgmt->hashLock);
  vmUnRegisterRunningState(pMgmt, pVnode->vgId);
  if (keepClosed) {
    if (vmRegisterClosedState(pMgmt, pVnode) != 0) {
      (void)taosThreadRwlockUnlock(&pMgmt->hashLock);
      return;
    };
  }
  (void)taosThreadRwlockUnlock(&pMgmt->hashLock);

  vmReleaseVnode(pMgmt, pVnode);

  if (pVnode->failed) {
    goto _closed;
  }
  dInfo("vgId:%d, pre close", pVnode->vgId);
  vnodePreClose(pVnode->pImpl);

  dInfo("vgId:%d, wait for vnode ref become 0", pVnode->vgId);
  while (pVnode->refCount > 0) taosMsleep(10);

  dInfo("vgId:%d, wait for vnode write queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pWriteW.queue,
        taosQueueGetThreadId(pVnode->pWriteW.queue));
  tMultiWorkerCleanup(&pVnode->pWriteW);

  dInfo("vgId:%d, wait for vnode sync queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pSyncW.queue,
        taosQueueGetThreadId(pVnode->pSyncW.queue));
  tMultiWorkerCleanup(&pVnode->pSyncW);

  dInfo("vgId:%d, wait for vnode sync rd queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pSyncRdW.queue,
        taosQueueGetThreadId(pVnode->pSyncRdW.queue));
  tMultiWorkerCleanup(&pVnode->pSyncRdW);

  dInfo("vgId:%d, wait for vnode apply queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pApplyW.queue,
        taosQueueGetThreadId(pVnode->pApplyW.queue));
  tMultiWorkerCleanup(&pVnode->pApplyW);

  dInfo("vgId:%d, wait for vnode fetch queue:%p is empty, thread:%08" PRId64, pVnode->vgId, pVnode->pFetchQ,
        taosQueueGetThreadId(pVnode->pFetchQ));
  while (!taosQueueEmpty(pVnode->pFetchQ)) taosMsleep(10);

  dInfo("vgId:%d, wait for vnode query queue:%p is empty", pVnode->vgId, pVnode->pQueryQ);
  while (!taosQueueEmpty(pVnode->pQueryQ)) taosMsleep(10);

  dInfo("vgId:%d, wait for vnode stream reader queue:%p is empty", pVnode->vgId, pVnode->pStreamReaderQ);
  while (!taosQueueEmpty(pVnode->pStreamReaderQ)) taosMsleep(10);

  dInfo("vgId:%d, all vnode queues is empty", pVnode->vgId);

  dInfo("vgId:%d, post close", pVnode->vgId);
  vnodePostClose(pVnode->pImpl);

  vmFreeQueue(pMgmt, pVnode);

  if (commitAndRemoveWal) {
    dInfo("vgId:%d, commit data for vnode split", pVnode->vgId);
    if (vnodeSyncCommit(pVnode->pImpl) != 0) {
      dError("vgId:%d, failed to commit data", pVnode->vgId);
    }
    if (vnodeBegin(pVnode->pImpl) != 0) {
      dError("vgId:%d, failed to begin", pVnode->vgId);
    }
    dInfo("vgId:%d, commit data finished", pVnode->vgId);
  }

  int32_t nodeId = vnodeNodeId(pVnode->pImpl);
  vnodeClose(pVnode->pImpl);
  pVnode->pImpl = NULL;

_closed:
  dInfo("vgId:%d, vnode is closed", pVnode->vgId);

  if (commitAndRemoveWal) {
    snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d%swal", TD_DIRSEP, pVnode->vgId, TD_DIRSEP);
    dInfo("vgId:%d, remove all wals, path:%s", pVnode->vgId, path);
    if (tfsRmdir(pMgmt->pTfs, path) != 0) {
      dTrace("vgId:%d, failed to remove wals, path:%s", pVnode->vgId, path);
    }
    if (tfsMkdir(pMgmt->pTfs, path) != 0) {
      dTrace("vgId:%d, failed to create wals, path:%s", pVnode->vgId, path);
    }
  }

  if (pVnode->dropped) {
    dInfo("vgId:%d, vnode is destroyed, dropped:%d", pVnode->vgId, pVnode->dropped);
    snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, pVnode->vgId);
    vnodeDestroy(pVnode->vgId, path, pMgmt->pTfs, nodeId);
  }
  if (pVnode->mountId && vmReleaseMountTfs(pMgmt, pVnode->mountId, pVnode->dropped ? 1 : 0)) {
    if (vmWriteMountListToFile(pMgmt) != 0) {
      dError("vgId:%d, failed at line %d to write mount list since %s", pVnode->vgId, __LINE__, terrstr());
    }
  }

  vmFreeVnodeObj(&pVnode);
}

void vmCloseFailedVnode(SVnodeMgmt *pMgmt, int32_t vgId) {
  int32_t r = 0;
  r = taosThreadRwlockWrlock(&pMgmt->hashLock);
  if (r != 0) {
    dError("vgId:%d, failed to lock since %s", vgId, tstrerror(r));
  }
  if (r == 0) {
    vmUnRegisterRunningState(pMgmt, vgId);
  }
  r = taosThreadRwlockUnlock(&pMgmt->hashLock);
  if (r != 0) {
    dError("vgId:%d, failed to unlock since %s", vgId, tstrerror(r));
  }
}

static int32_t vmRestoreVgroupId(SWrapperCfg *pCfg, STfs *pTfs) {
  int32_t srcVgId = pCfg->vgId;
  int32_t dstVgId = pCfg->toVgId;
  if (dstVgId == 0) return 0;

  char srcPath[TSDB_FILENAME_LEN];
  char dstPath[TSDB_FILENAME_LEN];

  snprintf(srcPath, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, srcVgId);
  snprintf(dstPath, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, dstVgId);

  int32_t diskPrimary = pCfg->diskPrimary;
  int32_t vgId = vnodeRestoreVgroupId(srcPath, dstPath, srcVgId, dstVgId, diskPrimary, pTfs);
  if (vgId <= 0) {
    dError("vgId:%d, failed to restore vgroup id. srcPath: %s", pCfg->vgId, srcPath);
    return -1;
  }

  pCfg->vgId = vgId;
  pCfg->toVgId = 0;
  return 0;
}

static void *vmOpenVnodeInThread(void *param) {
  SVnodeThread *pThread = param;
  SVnodeMgmt   *pMgmt = pThread->pMgmt;
  char          path[TSDB_FILENAME_LEN];

  dInfo("thread:%d, start to open or destroy %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("open-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SWrapperCfg *pCfg = &pThread->pCfgs[v];
    if (pCfg->dropped) {
      char stepDesc[TSDB_STEP_DESC_LEN] = {0};
      snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to destroy, %d of %d have been dropped", pCfg->vgId,
               pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
      tmsgReportStartup("vnode-destroy", stepDesc);

      snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, pCfg->vgId);
      vnodeDestroy(pCfg->vgId, path, pMgmt->pTfs, 0);
      pThread->updateVnodesList = true;
      pThread->dropped++;
      (void)atomic_add_fetch_32(&pMgmt->state.dropVnodes, 1);
      continue;
    }

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", pCfg->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    tmsgReportStartup("vnode-open", stepDesc);

    if (pCfg->toVgId) {
      if (vmRestoreVgroupId(pCfg, pMgmt->pTfs) != 0) {
        dError("vgId:%d, failed to restore vgroup id by thread:%d", pCfg->vgId, pThread->threadIndex);
        pThread->failed++;
        continue;
      }
      pThread->updateVnodesList = true;
    }

    int32_t diskPrimary = pCfg->mountId == 0 ? pCfg->diskPrimary : 0;
    snprintf(path, TSDB_FILENAME_LEN, "vnode%svnode%d", TD_DIRSEP, pCfg->vgId);

    STfs *pMountTfs = NULL;
#ifdef USE_MOUNT
    bool releaseTfs = false;
    if (pCfg->mountId) {
      if (vmAcquireMountTfs(pMgmt, pCfg->mountId, NULL, NULL, &pMountTfs) != 0) {
        dError("vgId:%d, failed to get mount tfs by thread:%d", pCfg->vgId, pThread->threadIndex);
        pThread->failed++;
        continue;
      }
      releaseTfs = true;
    }
#endif

    SVnode *pImpl = vnodeOpen(path, diskPrimary, pMgmt->pTfs, pMountTfs, pMgmt->msgCb, false);

    if (pImpl == NULL) {
      dError("vgId:%d, failed to open vnode by thread:%d since %s", pCfg->vgId, pThread->threadIndex, terrstr());
      if (terrno != TSDB_CODE_NEED_RETRY) {
        pThread->failed++;
#ifdef USE_MOUNT
        if (releaseTfs) vmReleaseMountTfs(pMgmt, pCfg->mountId, 0);
#endif
        continue;
      }
    }

    if (pImpl != NULL) {
      if (vmOpenVnode(pMgmt, pCfg, pImpl) != 0) {
        dError("vgId:%d, failed to open vnode by thread:%d", pCfg->vgId, pThread->threadIndex);
        pThread->failed++;
#ifdef USE_MOUNT
        if (releaseTfs) vmReleaseMountTfs(pMgmt, pCfg->mountId, 0);
#endif
        continue;
      }
    }

    dInfo("vgId:%d, is opened by thread:%d", pCfg->vgId, pThread->threadIndex);
    pThread->opened++;
    (void)atomic_add_fetch_32(&pMgmt->state.openVnodes, 1);
  }

  dInfo("thread:%d, numOfVnodes:%d, opened:%d dropped:%d failed:%d", pThread->threadIndex, pThread->vnodeNum,
        pThread->opened, pThread->dropped, pThread->failed);
  return NULL;
}

#ifdef USE_MOUNT
static int32_t vmOpenMountTfs(SVnodeMgmt *pMgmt) {
  int32_t    code = 0, lino = 0;
  int32_t    numOfMounts = 0;
  SMountCfg *pMountCfgs = NULL;
  SArray    *pDisks = NULL;
  TdFilePtr  pFile = NULL;
  SMountTfs *pMountTfs = NULL;

  TAOS_CHECK_EXIT(vmGetMountListFromFile(pMgmt, &pMountCfgs, &numOfMounts));
  for (int32_t i = 0; i < numOfMounts; ++i) {
    SMountCfg *pCfg = &pMountCfgs[i];
    if (taosHashGet(pMgmt->mountTfsHash, &pCfg->mountId, sizeof(pCfg->mountId))) {
      TAOS_CHECK_EXIT(TSDB_CODE_INTERNAL_ERROR);
    }
    TAOS_CHECK_EXIT(vmMountCheckRunning(pCfg->name, pCfg->path, &pFile, 3));
    TAOS_CHECK_EXIT(vmGetMountDisks(pMgmt, pCfg->path, &pDisks));
    int32_t nDisks = taosArrayGetSize(pDisks);
    if (nDisks < 1 || nDisks > TFS_MAX_DISKS) {
      dError("mount:%s, %" PRIi64 ", %s, invalid number of disks:%d, expect 1 to %d", pCfg->name, pCfg->mountId,
             pCfg->path, nDisks, TFS_MAX_DISKS);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_JSON_FORMAT);
    }

    TSDB_CHECK_NULL((pMountTfs = taosMemoryCalloc(1, sizeof(SMountTfs))), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(tfsOpen(TARRAY_GET_ELEM(pDisks, 0), TARRAY_SIZE(pDisks), &pMountTfs->pTfs));
    (void)snprintf(pMountTfs->name, sizeof(pMountTfs->name), "%s", pCfg->name);
    (void)snprintf(pMountTfs->path, sizeof(pMountTfs->path), "%s", pCfg->path);
    pMountTfs->pFile = pFile;
    pMountTfs->nRef = 1;
    TAOS_CHECK_EXIT(taosHashPut(pMgmt->mountTfsHash, &pCfg->mountId, sizeof(pCfg->mountId), &pMountTfs, POINTER_BYTES));
    taosArrayDestroy(pDisks);
    pDisks = NULL;
    pMountTfs = NULL;
    pFile = NULL;
  }
_exit:
  if (code != 0) {
    dError("failed to open mount tfs at line %d since %s", lino, tstrerror(code));
    if (pFile) {
      (void)taosUnLockFile(pFile);
      (void)taosCloseFile(&pFile);
    }
    if (pMountTfs) {
      tfsClose(pMountTfs->pTfs);
      taosMemoryFree(pMountTfs);
    }
    taosArrayDestroy(pDisks);
  }
  taosMemoryFree(pMountCfgs);
  TAOS_RETURN(code);
}
#endif
static int32_t vmOpenVnodes(SVnodeMgmt *pMgmt) {
  pMgmt->runngingHash =
      taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMgmt->runngingHash == NULL) {
    dError("failed to init vnode hash since %s", terrstr());
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMgmt->closedHash =
      taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMgmt->closedHash == NULL) {
    dError("failed to init vnode closed hash since %s", terrstr());
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMgmt->creatingHash =
      taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pMgmt->creatingHash == NULL) {
    dError("failed to init vnode creatingHash hash since %s", terrstr());
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SWrapperCfg *pCfgs = NULL;
  int32_t      numOfVnodes = 0;
  int32_t      code = 0;
  if ((code = vmGetVnodeListFromFile(pMgmt, &pCfgs, &numOfVnodes)) != 0) {
    dInfo("failed to get vnode list from disk since %s", tstrerror(code));
    return code;
  }

  pMgmt->state.totalVnodes = numOfVnodes;

  int32_t threadNum = tsNumOfCores / 2;
  if (threadNum < 1) threadNum = 1;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = taosMemoryCalloc(threadNum, sizeof(SVnodeThread));
  if (threads == NULL) {
    dError("failed to allocate memory for threads since %s", terrstr());
    taosMemoryFree(pCfgs);
    return terrno;
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].pCfgs = taosMemoryCalloc(vnodesPerThread, sizeof(SWrapperCfg));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    pThread->pCfgs[pThread->vnodeNum++] = pCfgs[v];
  }

  dInfo("open %d vnodes with %d threads", numOfVnodes, threadNum);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
    (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
    if (taosThreadCreate(&pThread->thread, &thAttr, vmOpenVnodeInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(ERRNO));
    }

    (void)taosThreadAttrDestroy(&thAttr);
  }

  bool updateVnodesList = false;

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      (void)taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->pCfgs);
    if (pThread->updateVnodesList) updateVnodesList = true;
  }
  taosMemoryFree(threads);
  taosMemoryFree(pCfgs);

  if ((pMgmt->state.openVnodes + pMgmt->state.dropVnodes) != pMgmt->state.totalVnodes) {
    dError("there are total vnodes:%d, opened:%d", pMgmt->state.totalVnodes, pMgmt->state.openVnodes);
    return terrno = TSDB_CODE_VND_INIT_FAILED;
  }

  if (updateVnodesList && (code = vmWriteVnodeListToFile(pMgmt)) != 0) {
    dError("failed to write vnode list since %s", tstrerror(code));
    return code;
  }

#ifdef USE_MOUNT
  bool  updateMountList = false;
  void *pIter = NULL;
  while ((pIter = taosHashIterate(pMgmt->mountTfsHash, pIter))) {
    SMountTfs *pMountTfs = *(SMountTfs **)pIter;
    if (pMountTfs && atomic_load_32(&pMountTfs->nRef) <= 1) {
      size_t  keyLen = 0;
      int64_t mountId = *(int64_t *)taosHashGetKey(pIter, &keyLen);
      dInfo("mount:%s, %s, %" PRIi64 ", ref:%d, remove unused mount tfs", pMountTfs->name, pMountTfs->path, mountId,
            atomic_load_32(&pMountTfs->nRef));
      if (pMountTfs->pFile) {
        (void)taosUnLockFile(pMountTfs->pFile);
        (void)taosCloseFile(&pMountTfs->pFile);
      }
      tfsClose(pMountTfs->pTfs);
      taosMemoryFree(pMountTfs);
      if ((code = taosHashRemove(pMgmt->mountTfsHash, &mountId, keyLen)) != 0) {
        dWarn("failed at line %d to remove mount:%s, %s, %" PRIi64 " from mount tfs hash since %s", __LINE__,
              pMountTfs->name, pMountTfs->path, mountId, tstrerror(code));
      }
      updateMountList = true;
    }
  }
  if (updateMountList && (code = vmWriteMountListToFile(pMgmt)) != 0) {
    dError("failed to write mount list at line %d since %s", __LINE__, tstrerror(code));
    return code;
  }
#endif

  dInfo("successfully opened %d vnodes", pMgmt->state.totalVnodes);
  return 0;
}

static void *vmCloseVnodeInThread(void *param) {
  SVnodeThread *pThread = param;
  SVnodeMgmt   *pMgmt = pThread->pMgmt;

  dInfo("thread:%d, start to close %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("close-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SVnodeObj *pVnode = pThread->ppVnodes[v];

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to close, %d of %d have been closed", pVnode->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    tmsgReportStartup("vnode-close", stepDesc);

    vmCloseVnode(pMgmt, pVnode, false, false);
  }

  dInfo("thread:%d, numOfVnodes:%d is closed", pThread->threadIndex, pThread->vnodeNum);
  return NULL;
}

static void vmCloseVnodes(SVnodeMgmt *pMgmt) {
  int32_t code = 0;
  dInfo("start to close all vnodes");
  tSingleWorkerCleanup(&pMgmt->mgmtWorker);
  dInfo("vnodes mgmt worker is stopped");
  tSingleWorkerCleanup(&pMgmt->mgmtMultiWorker);
  dInfo("vnodes multiple mgmt worker is stopped");

  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = NULL;
  code = vmGetVnodeListFromHash(pMgmt, &numOfVnodes, &ppVnodes);
  if (code != 0) {
    dError("failed to get vnode list since %s", tstrerror(code));
    return;
  }

  int32_t threadNum = tsNumOfCores / 2;
  if (threadNum < 1) threadNum = 1;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = taosMemoryCalloc(threadNum, sizeof(SVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].ppVnodes = taosMemoryCalloc(vnodesPerThread, sizeof(SVnode *));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    if (pThread->ppVnodes != NULL && ppVnodes != NULL) {
      pThread->ppVnodes[pThread->vnodeNum++] = ppVnodes[v];
    }
  }

  pMgmt->state.openVnodes = 0;
  dInfo("close %d vnodes with %d threads", numOfVnodes, threadNum);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
    (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
    if (taosThreadCreate(&pThread->thread, &thAttr, vmCloseVnodeInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to close vnode since %s", pThread->threadIndex, strerror(ERRNO));
    }

    (void)taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      (void)taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->ppVnodes);
  }
  taosMemoryFree(threads);

  if (ppVnodes != NULL) {
    taosMemoryFree(ppVnodes);
  }

  if (pMgmt->runngingHash != NULL) {
    taosHashCleanup(pMgmt->runngingHash);
    pMgmt->runngingHash = NULL;
  }

  void *pIter = taosHashIterate(pMgmt->closedHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    vmFreeVnodeObj(ppVnode);
    pIter = taosHashIterate(pMgmt->closedHash, pIter);
  }

  if (pMgmt->closedHash != NULL) {
    taosHashCleanup(pMgmt->closedHash);
    pMgmt->closedHash = NULL;
  }

  pIter = taosHashIterate(pMgmt->creatingHash, NULL);
  while (pIter) {
    SVnodeObj **ppVnode = pIter;
    vmFreeVnodeObj(ppVnode);
    pIter = taosHashIterate(pMgmt->creatingHash, pIter);
  }

  if (pMgmt->creatingHash != NULL) {
    taosHashCleanup(pMgmt->creatingHash);
    pMgmt->creatingHash = NULL;
  }

#ifdef USE_MOUNT
  pIter = NULL;
  while ((pIter = taosHashIterate(pMgmt->mountTfsHash, pIter))) {
    SMountTfs *mountTfs = *(SMountTfs **)pIter;
    if (mountTfs->pFile) {
      (void)taosUnLockFile(mountTfs->pFile);
      (void)taosCloseFile(&mountTfs->pFile);
    }
    tfsClose(mountTfs->pTfs);
    taosMemoryFree(mountTfs);
  }
  taosHashCleanup(pMgmt->mountTfsHash);
  pMgmt->mountTfsHash = NULL;
#endif

  dInfo("total vnodes:%d are all closed", numOfVnodes);
}

static void vmCleanup(SVnodeMgmt *pMgmt) {
  vmCloseVnodes(pMgmt);
  vmStopWorker(pMgmt);
  vnodeCleanup();
  (void)taosThreadRwlockDestroy(&pMgmt->hashLock);
  (void)taosThreadMutexDestroy(&pMgmt->mutex);
  taosMemoryFree(pMgmt);
}

static void vmCheckSyncTimeout(SVnodeMgmt *pMgmt) {
  int32_t     code = 0;
  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = NULL;
  code = vmGetVnodeListFromHash(pMgmt, &numOfVnodes, &ppVnodes);
  if (code != 0) {
    dError("failed to get vnode list since %s", tstrerror(code));
    return;
  }

  if (ppVnodes != NULL) {
    for (int32_t i = 0; i < numOfVnodes; ++i) {
      SVnodeObj *pVnode = ppVnodes[i];
      if (!pVnode->failed) {
        vnodeSyncCheckTimeout(pVnode->pImpl);
      }
      vmReleaseVnode(pMgmt, pVnode);
    }
    taosMemoryFree(ppVnodes);
  }
}

static void *vmThreadFp(void *param) {
  SVnodeMgmt *pMgmt = param;
  int64_t     lastTime = 0;
  setThreadName("vnode-timer");

  while (1) {
    lastTime++;
    taosMsleep(100);
    if (pMgmt->stop) break;
    if (lastTime % 10 != 0) continue;

    int64_t sec = lastTime / 10;
    if (sec % (VNODE_TIMEOUT_SEC / 2) == 0) {
      vmCheckSyncTimeout(pMgmt);
    }
  }

  return NULL;
}

static int32_t vmInitTimer(SVnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
#ifdef TD_COMPACT_OS
  (void)taosThreadAttrSetStackSize(&thAttr, STACK_SIZE_SMALL);
#endif
  if (taosThreadCreate(&pMgmt->thread, &thAttr, vmThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    dError("failed to create vnode timer thread since %s", tstrerror(code));
    return code;
  }

  (void)taosThreadAttrDestroy(&thAttr);
  return 0;
}

static void vmCleanupTimer(SVnodeMgmt *pMgmt) {
  pMgmt->stop = true;
  if (taosCheckPthreadValid(pMgmt->thread)) {
    (void)taosThreadJoin(pMgmt->thread, NULL);
    taosThreadClear(&pMgmt->thread);
  }
}

static int32_t vmInit(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t code = -1;

  SVnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SVnodeMgmt));
  if (pMgmt == NULL) {
    code = terrno;
    goto _OVER;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)vmPutRpcMsgToQueue;
  pMgmt->msgCb.qsizeFp = (GetQueueSizeFp)vmGetQueueSize;
  pMgmt->msgCb.mgmt = pMgmt;

  code = taosThreadRwlockInit(&pMgmt->hashLock, NULL);
  if (code != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _OVER;
  }

  code = taosThreadMutexInit(&pMgmt->mutex, NULL);
  if (code != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    goto _OVER;
  }

  pMgmt->pTfs = pInput->pTfs;
  if (pMgmt->pTfs == NULL) {
    dError("tfs is null.");
    goto _OVER;
  }
#ifdef USE_MOUNT
  if (!(pMgmt->mountTfsHash =
            taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK))) {
    dError("failed to init mountTfsHash since %s", terrstr());
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if ((code = vmOpenMountTfs(pMgmt)) != 0) {
    goto _OVER;
  }
#endif
  tmsgReportStartup("vnode-tfs", "initialized");
  if ((code = walInit(pInput->stopDnodeFp)) != 0) {
    dError("failed to init wal since %s", tstrerror(code));
    goto _OVER;
  }

  tmsgReportStartup("vnode-wal", "initialized");

  if ((code = syncInit()) != 0) {
    dError("failed to open sync since %s", tstrerror(code));
    goto _OVER;
  }
  tmsgReportStartup("vnode-sync", "initialized");

  if ((code = vnodeInit(pInput->stopDnodeFp)) != 0) {
    dError("failed to init vnode since %s", tstrerror(code));
    goto _OVER;
  }
  tmsgReportStartup("vnode-commit", "initialized");

  if ((code = vmStartWorker(pMgmt)) != 0) {
    dError("failed to init workers since %s", tstrerror(code));
    goto _OVER;
  }
  tmsgReportStartup("vnode-worker", "initialized");

  if ((code = vmOpenVnodes(pMgmt)) != 0) {
    dError("failed to open all vnodes since %s", tstrerror(code));
    goto _OVER;
  }
  tmsgReportStartup("vnode-vnodes", "initialized");

  if ((code = udfcOpen()) != 0) {
    dError("failed to open udfc in vnode since %s", tstrerror(code));
    goto _OVER;
  }

  code = 0;

_OVER:
  if (code == 0) {
    pOutput->pMgmt = pMgmt;
  } else {
    dError("failed to init vnodes-mgmt since %s", tstrerror(code));
    vmCleanup(pMgmt);
  }

  return code;
}

static int32_t vmRequire(const SMgmtInputOpt *pInput, bool *required) {
  *required = tsNumOfSupportVnodes > 0;
  return 0;
}

static void *vmRestoreVnodeInThread(void *param) {
  SVnodeThread *pThread = param;
  SVnodeMgmt   *pMgmt = pThread->pMgmt;

  dInfo("thread:%d, start to restore %d vnodes", pThread->threadIndex, pThread->vnodeNum);
  setThreadName("restore-vnodes");

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    SVnodeObj *pVnode = pThread->ppVnodes[v];
    if (pVnode->failed) {
      dError("vgId:%d, cannot restore a vnode in failed mode.", pVnode->vgId);
      continue;
    }

    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been restored", pVnode->vgId,
             pMgmt->state.openVnodes, pMgmt->state.totalVnodes);
    tmsgReportStartup("vnode-restore", stepDesc);

    int32_t code = vnodeStart(pVnode->pImpl);
    if (code != 0) {
      dError("vgId:%d, failed to restore vnode by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dInfo("vgId:%d, is restored by thread:%d", pVnode->vgId, pThread->threadIndex);
      pThread->opened++;
      (void)atomic_add_fetch_32(&pMgmt->state.openVnodes, 1);
    }
  }

  dInfo("thread:%d, numOfVnodes:%d, restored:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
        pThread->failed);
  return NULL;
}

static int32_t vmStartVnodes(SVnodeMgmt *pMgmt) {
  int32_t     code = 0;
  int32_t     numOfVnodes = 0;
  SVnodeObj **ppVnodes = NULL;
  code = vmGetVnodeListFromHash(pMgmt, &numOfVnodes, &ppVnodes);
  if (code != 0) {
    dError("failed to get vnode list since %s", tstrerror(code));
    return code;
  }

  int32_t threadNum = tsNumOfCores / 2;
  if (threadNum < 1) threadNum = 1;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;

  SVnodeThread *threads = taosMemoryCalloc(threadNum, sizeof(SVnodeThread));
  if (threads == NULL) {
    return terrno;
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].pMgmt = pMgmt;
    threads[t].ppVnodes = taosMemoryCalloc(vnodesPerThread, sizeof(SVnode *));
    if (threads[t].ppVnodes == NULL) {
      code = terrno;
      break;
    }
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t       t = v % threadNum;
    SVnodeThread *pThread = &threads[t];
    if (pThread->ppVnodes != NULL && ppVnodes != NULL) {
      pThread->ppVnodes[pThread->vnodeNum++] = ppVnodes[v];
    }
  }

  pMgmt->state.openVnodes = 0;
  pMgmt->state.dropVnodes = 0;
  dInfo("restore %d vnodes with %d threads", numOfVnodes, threadNum);

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    TdThreadAttr thAttr;
    (void)taosThreadAttrInit(&thAttr);
    (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&pThread->thread, &thAttr, vmRestoreVnodeInThread, pThread) != 0) {
      dError("thread:%d, failed to create thread to restore vnode since %s", pThread->threadIndex, strerror(ERRNO));
    }

    (void)taosThreadAttrDestroy(&thAttr);
  }

  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      (void)taosThreadJoin(pThread->thread, NULL);
      taosThreadClear(&pThread->thread);
    }
    taosMemoryFree(pThread->ppVnodes);
  }
  taosMemoryFree(threads);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    if (ppVnodes == NULL || ppVnodes[i] == NULL) continue;
    vmReleaseVnode(pMgmt, ppVnodes[i]);
  }

  if (ppVnodes != NULL) {
    taosMemoryFree(ppVnodes);
  }

  return vmInitTimer(pMgmt);

_exit:
  for (int32_t t = 0; t < threadNum; ++t) {
    SVnodeThread *pThread = &threads[t];
    taosMemoryFree(pThread->ppVnodes);
  }
  taosMemoryFree(threads);
  return code;
}

static void vmStop(SVnodeMgmt *pMgmt) { vmCleanupTimer(pMgmt); }

SMgmtFunc vmGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = vmInit;
  mgmtFunc.closeFp = (NodeCloseFp)vmCleanup;
  mgmtFunc.startFp = (NodeStartFp)vmStartVnodes;
  mgmtFunc.stopFp = (NodeStopFp)vmStop;
  mgmtFunc.requiredFp = vmRequire;
  mgmtFunc.getHandlesFp = vmGetMsgHandles;

  return mgmtFunc;
}
