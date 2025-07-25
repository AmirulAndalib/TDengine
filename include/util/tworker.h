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

#ifndef _TD_UTIL_WORKER_H_
#define _TD_UTIL_WORKER_H_

#include "tlist.h"
#include "tqueue.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SWWorkerPool SWWorkerPool;

typedef struct SQueueWorker {
  int32_t  id;      // worker id
  int64_t  pid;     // thread pid
  TdThread thread;  // thread id
  void    *pool;
} SQueueWorker;

typedef struct SQWorkerPool {
  int32_t       max;  // max number of workers
  int32_t       min;  // min number of workers
  int32_t       num;  // current number of workers
  STaosQset    *qset;
  const char   *name;
  SQueueWorker *workers;
  TdThreadMutex mutex;
} SQWorkerPool;

typedef struct SAutoQWorkerPool {
  float         ratio;
  STaosQset    *qset;
  const char   *name;
  SArray       *workers;
  TdThreadMutex mutex;
} SAutoQWorkerPool;

typedef struct SWWorker {
  int32_t       id;      // worker id
  int64_t       pid;     // thread pid
  TdThread      thread;  // thread id
  STaosQall    *qall;
  STaosQset    *qset;
  SWWorkerPool *pool;
} SWWorker;

struct SWWorkerPool {
  int32_t       max;  // max number of workers
  int32_t       num;
  int32_t       nextId;  // from 0 to max-1, cyclic
  const char   *name;
  SWWorker     *workers;
  TdThreadMutex mutex;
};

int32_t     tQWorkerInit(SQWorkerPool *pool);
void        tQWorkerCleanup(SQWorkerPool *pool);
STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp);
void        tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue);

int32_t     tAutoQWorkerInit(SAutoQWorkerPool *pool);
void        tAutoQWorkerCleanup(SAutoQWorkerPool *pool);
STaosQueue *tAutoQWorkerAllocQueue(SAutoQWorkerPool *pool, void *ahandle, FItem fp, int32_t minNum);
void        tAutoQWorkerFreeQueue(SAutoQWorkerPool *pool, STaosQueue *queue);

int32_t     tWWorkerInit(SWWorkerPool *pool);
void        tWWorkerCleanup(SWWorkerPool *pool);
STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FItems fp);
void        tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue);

typedef enum SQWorkerPoolType {
  QWORKER_POOL = 0,
  QUERY_AUTO_QWORKER_POOL,
} SQWorkerPoolType;

typedef struct {
  const char      *name;
  int32_t          min;
  int32_t          max;
  FItem            fp;
  void            *param;
  SQWorkerPoolType poolType;
  bool             stopNoWaitQueue;
} SSingleWorkerCfg;

typedef struct {
  const char      *name;
  STaosQueue      *queue;
  SQWorkerPoolType poolType;  // default to QWORKER_POOL
  void            *pool;
  bool             stopNoWaitQueue;
} SSingleWorker;

typedef struct {
  const char *name;
  int32_t     max;
  FItems      fp;
  void       *param;
} SMultiWorkerCfg;

typedef struct {
  const char  *name;
  STaosQueue  *queue;
  SWWorkerPool pool;
} SMultiWorker;

int32_t tSingleWorkerInit(SSingleWorker *pWorker, const SSingleWorkerCfg *pCfg);
void    tSingleWorkerCleanup(SSingleWorker *pWorker);
int32_t tMultiWorkerInit(SMultiWorker *pWorker, const SMultiWorkerCfg *pCfg);
void    tMultiWorkerCleanup(SMultiWorker *pWorker);

struct SQueryAutoQWorkerPoolCB;

typedef struct SQueryAutoQWorker {
  int32_t  id;      // worker id
  int32_t  backupIdx;// the idx when put into backup pool
  int64_t  pid;     // thread pid
  TdThread thread;  // thread id
  void    *pool;
} SQueryAutoQWorker;

typedef struct SQueryAutoQWorkerPool {
  int32_t       num;
  int32_t       max;
  int32_t       min;
  int32_t       maxInUse;

  int64_t       activeRunningN; // 4 bytes for activeN, 4 bytes for runningN
  // activeN are running workers and workers waiting at reading new queue msgs
  // runningN are workers processing queue msgs, not include blocking/waitingAfterBlock/waitingBeforeProcessMsg workers.

  int32_t       waitingAfterBlockN; // workers that recovered from blocking but waiting for too many running workers
  TdThreadMutex waitingAfterBlockLock;
  TdThreadCond  waitingAfterBlockCond;

  int32_t       waitingBeforeProcessMsgN; // workers that get msg from queue, but waiting for too many running workers
  TdThreadMutex waitingBeforeProcessMsgLock;
  TdThreadCond  waitingBeforeProcessMsgCond;

  int32_t       backupNum; // workers that are in backup pool, not reading msg from queue
  TdThreadMutex backupLock;
  TdThreadCond  backupCond;

  const char                     *name;
  TdThreadMutex                   poolLock;
  SList                          *workers;
  SList                          *backupWorkers;
  SList                          *exitedWorkers;
  STaosQset                      *qset;
  struct SQueryAutoQWorkerPoolCB *pCb;
  volatile bool                   exit;
} SQueryAutoQWorkerPool;

int32_t     tQueryAutoQWorkerInit(SQueryAutoQWorkerPool *pPool);
void        tQueryAutoQWorkerCleanup(SQueryAutoQWorkerPool *pPool);
STaosQueue *tQueryAutoQWorkerAllocQueue(SQueryAutoQWorkerPool *pPool, void *ahandle, FItem fp);
void        tQueryAutoQWorkerFreeQueue(SQueryAutoQWorkerPool* pPool, STaosQueue* pQ);

typedef struct SQueryAutoQWorkerPoolCB {
  void *pPool;
  int32_t (*beforeBlocking)(void *pPool);
  int32_t (*afterRecoverFromBlocking)(void *pPool);
} SQueryAutoQWorkerPoolCB;

typedef struct SDispatchWorker {
  int32_t     id;
  int32_t     pid;
  TdThread    thread;
  void       *pool;
  STaosQueue *queue;
  STaosQset  *qset;
} SDispatchWorker;

struct SDispatchWorkerPool;

typedef int32_t (*DispatchFp)(struct SDispatchWorkerPool* pPool, void* pParam, int32_t *pWorkerIdx);

typedef struct SDispatchWorkerPool {
  const char      *name;
  int32_t          num;
  int32_t          max;
  FItems           fp;
  void            *param;
  SDispatchWorker *pWorkers;
  DispatchFp       dispatchFp;
  TdThreadMutex    poolLock;
  bool             exit;
} SDispatchWorkerPool;

int32_t tDispatchWorkerInit(SDispatchWorkerPool *pPool);
void    tDispatchWorkerCleanup(SDispatchWorkerPool *pPool);
int32_t tDispatchWorkerAllocQueue(SDispatchWorkerPool *pPool, void *ahandle, FItem fp, DispatchFp dispatchFp);
int32_t tAddTaskIntoDispatchWorkerPool(SDispatchWorkerPool* pPool, void* pTask);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_WORKER_H_*/
