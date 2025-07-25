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
// clang-format off
//#include "storageapi.h"
#include "dmMgmt.h"
#include "audit.h"
#include "libs/function/tudf.h"
#include "metrics.h"
#include "tgrant.h"
#include "tcompare.h"
#include "tss.h"
#include "tanalytics.h"
#include "stream.h"
// clang-format on

#define DM_INIT_AUDIT()                       \
  do {                                        \
    auditCfg.port = tsMonitorPort;            \
    auditCfg.server = tsMonitorFqdn;          \
    auditCfg.comp = tsMonitorComp;            \
    if ((code = auditInit(&auditCfg)) != 0) { \
      return code;                            \
    }                                         \
  } while (0)

static SDnode globalDnode = {0};

SDnode *dmInstance() { return &globalDnode; }

static int32_t dmCheckRepeatInit(SDnode *pDnode) {
  int32_t code = 0;
  if (atomic_val_compare_exchange_8(&pDnode->once, DND_ENV_INIT, DND_ENV_READY) != DND_ENV_INIT) {
    dError("env is already initialized");
    code = TSDB_CODE_REPEAT_INIT;
    return code;
  }
  return 0;
}

static int32_t dmInitSystem() {
  if (taosIgnSIGPIPE() != 0) {
    dError("failed to ignore SIGPIPE");
  }

  if (taosBlockSIGPIPE() != 0) {
    dError("failed to block SIGPIPE");
  }

  taosResolveCRC();
  return 0;
}

static int32_t dmInitMonitor() {
  int32_t code = 0;
  SMonCfg monCfg = {0};

  monCfg.maxLogs = tsMonitorMaxLogs;
  monCfg.port = tsMonitorPort;
  monCfg.server = tsMonitorFqdn;
  monCfg.comp = tsMonitorComp;
  if ((code = monInit(&monCfg)) != 0) {
    dError("failed to init monitor since %s", tstrerror(code));
  }
  return code;
}

static int32_t dmInitMetrics() {
  int32_t code = 0;
  if ((code = initMetricsManager()) != 0) {
    dError("failed to init metrics since %s", tstrerror(code));
  }
  return code;
}

static int32_t dmInitAudit() {
  SAuditCfg auditCfg = {0};
  int32_t   code = 0;

  DM_INIT_AUDIT();

  return 0;
}

static bool dmDataSpaceAvailable() {
  SDnode *pDnode = dmInstance();
  if (pDnode->pTfs) {
    return tfsDiskSpaceAvailable(pDnode->pTfs, 0);
  }
  if (!osDataSpaceAvailable()) {
    dError("data disk space unavailable, i.e. %s", tsDataDir);
    return false;
  }
  return true;
}

static int32_t dmCheckDiskSpace() {
  // availability
  int32_t code = 0;
  code = osUpdate();
  if (code != 0) {
    dError("failed to update os info since %s", tstrerror(code));
    code = 0;  // ignore the error, just log it
  }
  if (!dmDataSpaceAvailable()) {
    code = TSDB_CODE_NO_DISKSPACE;
    return code;
  }
  if (!osLogSpaceAvailable()) {
    dError("log disk space unavailable, i.e. %s", tsLogDir);
    code = TSDB_CODE_NO_DISKSPACE;
    return code;
  }
  if (!osTempSpaceAvailable()) {
    dError("temp disk space unavailable, i.e. %s", tsTempDir);
    code = TSDB_CODE_NO_DISKSPACE;
    return code;
  }
  return code;
}

int32_t dmDiskInit() {
  SDnode  *pDnode = dmInstance();
  SDiskCfg dCfg = {.level = 0, .primary = 1, .disable = 0};
  tstrncpy(dCfg.dir, tsDataDir, TSDB_FILENAME_LEN);
  SDiskCfg *pDisks = tsDiskCfg;
  int32_t   numOfDisks = tsDiskCfgNum;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  int32_t code = tfsOpen(pDisks, numOfDisks, &pDnode->pTfs);
  if (code != 0) {
    dError("failed to init tfs since %s", tstrerror(code));
    TAOS_RETURN(code);
  }
  return 0;
}

int32_t dmDiskClose() {
  SDnode *pDnode = dmInstance();
  tfsClose(pDnode->pTfs);
  pDnode->pTfs = NULL;
  return 0;
}

static bool dmCheckDataDirVersion() {
  char checkDataDirJsonFileName[PATH_MAX] = {0};
  snprintf(checkDataDirJsonFileName, PATH_MAX, "%s/dnode/dnodeCfg.json", tsDataDir);
  if (taosCheckExistFile(checkDataDirJsonFileName)) {
    dError("The default data directory %s contains old data of tdengine 2.x, please clear it before running!",
           tsDataDir);
    return false;
  }
  return true;
}

static int32_t dmCheckDataDirVersionWrapper() {
  if (!dmCheckDataDirVersion()) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  return 0;
}

int32_t dmInit() {
  dInfo("start to init dnode env");
  int32_t code = 0;

#ifdef USE_SHARED_STORAGE
  if (tsSsEnabled) {
    if ((code = tssInit()) != 0) return code;
    if ((code = tssCreateDefaultInstance()) != 0) return code;
  }
#endif

  if ((code = dmDiskInit()) != 0) return code;
  if (!dmCheckDataDirVersion()) {
    code = TSDB_CODE_INVALID_DATA_FMT;
    return code;
  }
  SDnode* pDnode = dmInstance();
  if ((code = dmCheckDiskSpace()) != 0) return code;
  if ((code = dmCheckRepeatInit(pDnode)) != 0) return code;
  if ((code = dmInitSystem()) != 0) return code;
  if ((code = dmInitMonitor()) != 0) return code;
  if ((code = dmInitMetrics()) != 0) return code;
  if ((code = dmInitAudit()) != 0) return code;
  if ((code = dmInitDnode(pDnode)) != 0) return code;
  if ((code = InitRegexCache() != 0)) return code;

  gExecInfoInit(&pDnode->data, (getDnodeId_f)dmGetDnodeId, dmGetMnodeEpSet);
  if ((code = streamInit(&pDnode->data, (getDnodeId_f)dmGetDnodeId, dmGetMnodeEpSet, dmGetSynEpset)) != 0) return code;

  dInfo("dnode env is initialized");
  return 0;
}

static int32_t dmCheckRepeatCleanup(SDnode *pDnode) {
  if (atomic_val_compare_exchange_8(&pDnode->once, DND_ENV_READY, DND_ENV_CLEANUP) != DND_ENV_READY) {
    dError("dnode env is already cleaned up");
    return -1;
  }
  return 0;
}

void dmCleanup() {
  dDebug("start to cleanup dnode env");
  SDnode *pDnode = dmInstance();
  if (dmCheckRepeatCleanup(pDnode) != 0) return;
  dmCleanupDnode(pDnode);
  monCleanup();
  auditCleanup();
  syncCleanUp();
  walCleanUp();
  cleanupMetrics();
  if (udfcClose() != 0) {
    dError("failed to close udfc");
  }
  udfStopUdfd();
  taosAnalyticsCleanup();
  taosStopCacheRefreshWorker();
  (void)dmDiskClose();
  DestroyRegexCache();

#ifdef USE_SHARED_STORAGE
  if (tsSsEnabled) {
    tssCloseDefaultInstance();
    tssUninit();
  }
#endif

  dInfo("dnode env is cleaned up");

  taosMemPoolClose(gMemPoolHandle);
  taosCleanupCfg();
  taosCloseLog();
}

void dmStop() {
  SDnode *pDnode = dmInstance();
  pDnode->stop = true;
}

int32_t dmRun() {
  SDnode *pDnode = dmInstance();
  return dmRunDnode(pDnode);
}

static int32_t dmProcessCreateNodeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  int32_t code = 0;
  SDnode *pDnode = dmInstance();

  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper != NULL) {
    dmReleaseWrapper(pWrapper);
    switch (ntype) {
      case MNODE:
        code = TSDB_CODE_MNODE_ALREADY_DEPLOYED;
        break;
      case QNODE:
        code = TSDB_CODE_QNODE_ALREADY_DEPLOYED;
        break;
      case SNODE:
        code = TSDB_CODE_SNODE_ALREADY_DEPLOYED;
        break;
      case BNODE:
        code = TSDB_CODE_BNODE_ALREADY_DEPLOYED;
        break;
      default:
        code = TSDB_CODE_APP_ERROR;
    }
    dError("failed to create node since %s", tstrerror(code));
    return code;
  }

  dInfo("start to process create-node-request");

  pWrapper = &pDnode->wrappers[ntype];

  if (taosMulMkDir(pWrapper->path) != 0) {
    dmReleaseWrapper(pWrapper);
    code = terrno;
    dError("failed to create dir:%s since %s", pWrapper->path, tstrerror(code));
    return code;
  }

  dInfo("path %s created", pWrapper->path);

  (void)taosThreadMutexLock(&pDnode->mutex);
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to create", pWrapper->name);
  code = (*pWrapper->func.createFp)(&input, pMsg);
  if (code != 0) {
    dError("node:%s, failed to create since %s", pWrapper->name, tstrerror(code));
  } else {
    dInfo("node:%s, has been created", pWrapper->name);
    code = dmOpenNode(pWrapper);
    if (code == 0) {
      code = dmStartNode(pWrapper);
    }
    pWrapper->deployed = true;
    pWrapper->required = true;
  }

  (void)taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}


static int32_t dmProcessAlterNodeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  int32_t code = 0;
  if (SNODE != ntype) {
    dError("failed to process msgType %d since node type is NOT snode", pMsg->msgType);
    return TSDB_CODE_INVALID_MSG;
  }
  
  SDnode *pDnode = dmInstance();
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);

  dInfo("start to process alter-node-request");

  pWrapper = &pDnode->wrappers[ntype];

  (void)taosThreadMutexLock(&pDnode->mutex);
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to update", pWrapper->name);
  code = (*pWrapper->func.createFp)(&input, pMsg);
  if (code != 0) {
    dError("node:%s, failed to update since %s", pWrapper->name, tstrerror(code));
  } else {
    dInfo("node:%s, has been updated", pWrapper->name);
  }

  (void)taosThreadMutexUnlock(&pDnode->mutex);

  dmReleaseWrapper(pWrapper);
  
  return code;
}


static int32_t dmProcessAlterNodeTypeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  int32_t code = 0;
  SDnode *pDnode = dmInstance();

  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    dError("fail to process alter node type since node not exist");
    return TSDB_CODE_INVALID_MSG;
  }
  dmReleaseWrapper(pWrapper);

  dInfo("node:%s, start to process alter-node-type-request", pWrapper->name);

  pWrapper = &pDnode->wrappers[ntype];

  if (pWrapper->func.nodeRoleFp != NULL) {
    ESyncRole role = (*pWrapper->func.nodeRoleFp)(pWrapper->pMgmt);
    dInfo("node:%s, checking node role:%d", pWrapper->name, role);
    if (role == TAOS_SYNC_ROLE_VOTER) {
      dError("node:%s, failed to alter node type since node already is role:%d", pWrapper->name, role);
      code = TSDB_CODE_MNODE_ALREADY_IS_VOTER;
      return code;
    }
  }

  if (pWrapper->func.isCatchUpFp != NULL) {
    dInfo("node:%s, checking node catch up", pWrapper->name);
    if ((*pWrapper->func.isCatchUpFp)(pWrapper->pMgmt) != 1) {
      code = TSDB_CODE_MNODE_NOT_CATCH_UP;
      return code;
    }
  }

  dInfo("node:%s, catched up leader, continue to process alter-node-type-request", pWrapper->name);

  (void)taosThreadMutexLock(&pDnode->mutex);

  dInfo("node:%s, stopping node", pWrapper->name);
  dmStopNode(pWrapper);
  dInfo("node:%s, closing node", pWrapper->name);
  dmCloseNode(pWrapper);

  pWrapper = &pDnode->wrappers[ntype];
  if (taosMkDir(pWrapper->path) != 0) {
    (void)taosThreadMutexUnlock(&pDnode->mutex);
    code = terrno;
    dError("failed to create dir:%s since %s", pWrapper->path, tstrerror(code));
    return code;
  }

  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to create", pWrapper->name);
  code = (*pWrapper->func.createFp)(&input, pMsg);
  if (code != 0) {
    dError("node:%s, failed to create since %s", pWrapper->name, tstrerror(code));
  } else {
    dInfo("node:%s, has been created", pWrapper->name);
    code = dmOpenNode(pWrapper);
    if (code == 0) {
      code = dmStartNode(pWrapper);
    }
    pWrapper->deployed = true;
    pWrapper->required = true;
  }

  (void)taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

static int32_t dmProcessDropNodeReq(EDndNodeType ntype, SRpcMsg *pMsg) {
  int32_t code = 0;
  SDnode *pDnode = dmInstance();

  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    switch (ntype) {
      case MNODE:
        code = TSDB_CODE_MNODE_NOT_DEPLOYED;
        break;
      case QNODE:
        code = TSDB_CODE_QNODE_NOT_DEPLOYED;
        break;
      case SNODE:
        code = TSDB_CODE_SNODE_NOT_DEPLOYED;
        break;
      case BNODE:
        code = TSDB_CODE_BNODE_NOT_DEPLOYED;
        break;
      default:
        code = TSDB_CODE_APP_ERROR;
    }

    dError("failed to drop node since %s", tstrerror(code));
    return terrno = code;
  }

  (void)taosThreadMutexLock(&pDnode->mutex);
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to drop", pWrapper->name);
  code = (*pWrapper->func.dropFp)(&input, pMsg);
  if (code != 0) {
    dError("node:%s, failed to drop since %s", pWrapper->name, tstrerror(code));
  } else {
    dInfo("node:%s, has been dropped", pWrapper->name);
    pWrapper->required = false;
    pWrapper->deployed = false;
  }

  dmReleaseWrapper(pWrapper);

  if (code == 0) {
    dmStopNode(pWrapper);
    dmCloseNode(pWrapper);
    taosRemoveDir(pWrapper->path);
  }
  (void)taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

SMgmtInputOpt dmBuildMgmtInputOpt(SMgmtWrapper *pWrapper) {
  SMgmtInputOpt opt = {
      .path = pWrapper->path,
      .name = pWrapper->name,
      .pTfs = pWrapper->pDnode->pTfs,
      .pData = &pWrapper->pDnode->data,
      .processCreateNodeFp = dmProcessCreateNodeReq,
      .processAlterNodeFp = dmProcessAlterNodeReq,
      .processAlterNodeTypeFp = dmProcessAlterNodeTypeReq,
      .processDropNodeFp = dmProcessDropNodeReq,
      .sendMonitorReportFp = dmSendMonitorReport,
      .sendMetricsReportFp = dmSendMetricsReport,
      .monitorCleanExpiredSamplesFp = dmMonitorCleanExpiredSamples,
      .metricsCleanExpiredSamplesFp = dmMetricsCleanExpiredSamples,
      .sendAuditRecordFp = auditSendRecordsInBatch,
      .getVnodeLoadsFp = dmGetVnodeLoads,
      .getVnodeLoadsLiteFp = dmGetVnodeLoadsLite,
      .getMnodeLoadsFp = dmGetMnodeLoads,
      .getQnodeLoadsFp = dmGetQnodeLoads,
      .stopDnodeFp = dmStop,
  };

  opt.msgCb = dmGetMsgcb(pWrapper->pDnode);
  return opt;
}

void dmReportStartup(const char *pName, const char *pDesc) {
  SStartupInfo *pStartup = &(dmInstance()->startup);
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  dDebug("step:%s, %s", pStartup->name, pStartup->desc);
}

int64_t dmGetClusterId() { return globalDnode.data.clusterId; }

bool dmReadyForTest() { return dmInstance()->data.dnodeVer > 0; }
