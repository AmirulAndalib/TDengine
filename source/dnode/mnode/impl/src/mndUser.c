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
#ifndef TD_ASTRA
#include <uv.h>
#endif
#include "crypt.h"
#include "mndUser.h"
#include "audit.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tbase64.h"

// clang-format on

#define USER_VER_NUMBER                      7
#define USER_VER_SUPPORT_WHITELIST           5
#define USER_VER_SUPPORT_WHITELIT_DUAL_STACK 7
#define USER_RESERVE_SIZE                    63

#define BIT_FLAG_MASK(n)              (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)  ((val) |= (mask))
#define BIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

#define PRIVILEGE_TYPE_ALL       BIT_FLAG_MASK(0)
#define PRIVILEGE_TYPE_READ      BIT_FLAG_MASK(1)
#define PRIVILEGE_TYPE_WRITE     BIT_FLAG_MASK(2)
#define PRIVILEGE_TYPE_SUBSCRIBE BIT_FLAG_MASK(3)
#define PRIVILEGE_TYPE_ALTER     BIT_FLAG_MASK(4)

#define ALTER_USER_ADD_PRIVS(_type) ((_type) == TSDB_ALTER_USER_ADD_PRIVILEGES)
#define ALTER_USER_DEL_PRIVS(_type) ((_type) == TSDB_ALTER_USER_DEL_PRIVILEGES)

#define ALTER_USER_ALL_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_READ_PRIV(_priv) \
  (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_READ) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_WRITE_PRIV(_priv) \
  (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_WRITE) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_ALTER_PRIV(_priv) \
  (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALTER) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_SUBSCRIBE_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_SUBSCRIBE))

#define ALTER_USER_TARGET_DB(_tbname) (0 == (_tbname)[0])
#define ALTER_USER_TARGET_TB(_tbname) (0 != (_tbname)[0])

#define ALTER_USER_ADD_READ_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_READ_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_WRITE_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_WRITE_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALTER_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALTER_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALL_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALL_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))

#define ALTER_USER_ADD_READ_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_READ_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_WRITE_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_WRITE_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALTER_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALTER_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALL_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALL_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))

#define ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(_type, _priv) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))
#define ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(_type, _priv) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))

static int32_t createDefaultIpWhiteList(SIpWhiteListDual **ppWhiteList);
static int32_t createIpWhiteList(void *buf, int32_t len, SIpWhiteListDual **ppWhiteList);

static bool isIpWhiteListEqual(SIpWhiteListDual *a, SIpWhiteListDual *b);
static bool isIpRangeEqual(SIpRange *a, SIpRange *b);

void destroyIpWhiteTab(SHashObj *pIpWhiteTab);

#define MND_MAX_USE_HOST (TSDB_PRIVILEDGE_HOST_LEN / 24)

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq);
static int32_t  mndProcessCreateUserReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterUserReq(SRpcMsg *pReq);
static int32_t  mndProcessDropUserReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserAuthReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserWhiteListReq(SRpcMsg *pReq);
static int32_t  mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static int32_t  mndRetrieveUsersFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextUser(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);
static int32_t  mndFetchAllIpWhite(SMnode *pMnode, SHashObj **ppIpWhiteTab);
static int32_t  mndProcesSRetrieveIpWhiteReq(SRpcMsg *pReq);
static int32_t  mndUpdateIpWhiteImpl(SHashObj *pIpWhiteTab, char *user, char *fqdn, int8_t type, bool *pUpdate);

static int32_t ipWhiteMgtUpdateAll(SMnode *pMnode);
static int32_t ipWhiteMgtRemove(char *user);

static int32_t createIpWhiteListFromOldVer(void *buf, int32_t len, SIpWhiteList **ppList);
static int32_t tDerializeIpWhileListFromOldVer(void *buf, int32_t len, SIpWhiteList *pList);
typedef struct {
  SHashObj      *pIpWhiteTab;
  int64_t        ver;
  TdThreadRwlock rw;
} SIpWhiteMgt;

static SIpWhiteMgt ipWhiteMgt;

const static SIpV4Range defaultIpRange = {.ip = 16777343, .mask = 32};

static int32_t ipWhiteMgtInit() {
  ipWhiteMgt.pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);
  if (ipWhiteMgt.pIpWhiteTab == NULL) {
    TAOS_RETURN(terrno);
  }
  ipWhiteMgt.ver = 0;
  (void)taosThreadRwlockInit(&ipWhiteMgt.rw, NULL);
  TAOS_RETURN(0);
}
void ipWhiteMgtCleanup() {
  destroyIpWhiteTab(ipWhiteMgt.pIpWhiteTab);
  (void)taosThreadRwlockDestroy(&ipWhiteMgt.rw);
}

int32_t ipWhiteMgtUpdate(SMnode *pMnode, char *user, SIpWhiteListDual *pNew) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    update = true;
  SArray *fqdns = NULL;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  SIpWhiteListDual **ppList = taosHashGet(ipWhiteMgt.pIpWhiteTab, user, strlen(user));

  if (ppList == NULL || *ppList == NULL) {
    SIpWhiteListDual *p = cloneIpWhiteList(pNew);
    if (p == NULL) {
      update = false;
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    if ((code = taosHashPut(ipWhiteMgt.pIpWhiteTab, user, strlen(user), &p, sizeof(void *))) != 0) {
      update = false;
      taosMemoryFree(p);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
  } else {
    SIpWhiteListDual *pOld = *ppList;
    if (isIpWhiteListEqual(pOld, pNew)) {
      update = false;
    } else {
      taosMemoryFree(pOld);
      SIpWhiteListDual *p = cloneIpWhiteList(pNew);
      if (p == NULL) {
        update = false;
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
      }
      if ((code = taosHashPut(ipWhiteMgt.pIpWhiteTab, user, strlen(user), &p, sizeof(void *))) != 0) {
        update = false;
        taosMemoryFree(p);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
    }
  }

  fqdns = mndGetAllDnodeFqdns(pMnode);  // TODO: update this line after refactor api
  if (fqdns == NULL) {
    update = false;
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);
    bool  upd = false;
    TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, TSDB_DEFAULT_USER, fqdn, IP_WHITE_ADD, &upd), &lino,
                    _OVER);
    update |= upd;
    TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, user, fqdn, IP_WHITE_ADD, &upd), &lino, _OVER);
    update |= upd;
  }

  if (update) ipWhiteMgt.ver++;

_OVER:
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  taosArrayDestroyP(fqdns, NULL);
  if (code < 0) {
    mError("failed to update ip white list for user: %s at line %d since %s", user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
int32_t ipWhiteMgtRemove(char *user) {
  bool    update = true;
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  SIpWhiteListDual **ppList = taosHashGet(ipWhiteMgt.pIpWhiteTab, user, strlen(user));
  if (ppList == NULL || *ppList == NULL) {
    update = false;
  } else {
    taosMemoryFree(*ppList);
    code = taosHashRemove(ipWhiteMgt.pIpWhiteTab, user, strlen(user));
    if (code != 0) {
      update = false;
    }
  }

  if (update) ipWhiteMgt.ver++;
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  return 0;
}

bool isRangeInWhiteList(SIpWhiteListDual *pList, SIpRange *range) {
  for (int i = 0; i < pList->num; i++) {
    if (isIpRangeEqual(&pList->pIpRanges[i], range)) {
      return true;
    }
  }
  return false;
}

static int32_t ipWhiteMgtUpdateAll(SMnode *pMnode) {
  SHashObj *pNew = NULL;
  TAOS_CHECK_RETURN(mndFetchAllIpWhite(pMnode, &pNew));

  SHashObj *pOld = ipWhiteMgt.pIpWhiteTab;

  ipWhiteMgt.pIpWhiteTab = pNew;
  ipWhiteMgt.ver++;

  destroyIpWhiteTab(pOld);
  TAOS_RETURN(0);
}

int64_t mndGetIpWhiteVer(SMnode *pMnode) {
  int64_t ver = 0;
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  if (ipWhiteMgt.ver == 0) {
    // get user and dnode ip white list
    if ((code = ipWhiteMgtUpdateAll(pMnode)) != 0) {
      (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
      mError("%s failed to update ip white list since %s", __func__, tstrerror(code));
      return ver;
    }
    ipWhiteMgt.ver = taosGetTimestampMs();
  }
  ver = ipWhiteMgt.ver;
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);

  if (mndEnableIpWhiteList(pMnode) == 0 || tsEnableWhiteList == false) {
    ver = 0;
  }
  mDebug("ip-white-list on mnode ver: %" PRId64, ver);
  return ver;
}

int32_t mndUpdateIpWhiteImpl(SHashObj *pIpWhiteTab, char *user, char *fqdn, int8_t type, bool *pUpdate) {
  int32_t lino = 0;
  bool    update = false;

  SIpRange range = {0};
  SIpAddr  addr = {0};
  int32_t  code = taosGetIpFromFqdn(tsEnableIpv6, fqdn, &addr);
  if (code) {
    mError("failed to get ip from fqdn: %s at line %d since %s", fqdn, lino, tstrerror(code));
    TAOS_RETURN(TSDB_CODE_TSC_INVALID_FQDN);
  }

  code = tIpStrToUint(&addr, &range);
  if (code) {
    TAOS_RETURN(code);
  }

  tIpRangeSetMask(&range, 32);

  mDebug("ip-white-list may update for user: %s, fqdn: %s", user, fqdn);
  SIpWhiteListDual **ppList = taosHashGet(pIpWhiteTab, user, strlen(user));
  SIpWhiteListDual  *pList = NULL;
  if (ppList != NULL && *ppList != NULL) {
    pList = *ppList;
  }

  if (type == IP_WHITE_ADD) {
    if (pList == NULL) {
      SIpWhiteListDual *pNewList = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + sizeof(SIpRange));
      if (pNewList == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memcpy(pNewList->pIpRanges, &range, sizeof(SIpRange));
      pNewList->num = 1;

      if ((code = taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *))) != 0) {
        taosMemoryFree(pNewList);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      update = true;
    } else {
      if (!isRangeInWhiteList(pList, &range)) {
        int32_t           sz = sizeof(SIpWhiteListDual) + sizeof(SIpRange) * (pList->num + 1);
        SIpWhiteListDual *pNewList = taosMemoryCalloc(1, sz);
        if (pNewList == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memcpy(pNewList->pIpRanges, pList->pIpRanges, sizeof(SIpRange) * (pList->num));
        memcpy(&pNewList->pIpRanges[pList->num], &range, sizeof(SIpRange));

        pNewList->num = pList->num + 1;

        if ((code = taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *))) != 0) {
          taosMemoryFree(pNewList);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        taosMemoryFree(pList);
        update = true;
      }
    }
  } else if (type == IP_WHITE_DROP) {
    if (pList != NULL) {
      if (isRangeInWhiteList(pList, &range)) {
        if (pList->num == 1) {
          if (taosHashRemove(pIpWhiteTab, user, strlen(user)) < 0) {
            mError("failed to remove ip-white-list for user: %s at line %d", user, lino);
          }
          taosMemoryFree(pList);
        } else {
          int32_t           idx = 0;
          int32_t           sz = sizeof(SIpWhiteListDual) + sizeof(SIpRange) * (pList->num - 1);
          SIpWhiteListDual *pNewList = taosMemoryCalloc(1, sz);
          if (pNewList == NULL) {
            TAOS_CHECK_GOTO(terrno, &lino, _OVER);
          }
          for (int i = 0; i < pList->num; i++) {
            SIpRange *e = &pList->pIpRanges[i];
            if (!isIpRangeEqual(e, &range)) {
              memcpy(&pNewList->pIpRanges[idx], e, sizeof(SIpRange));
              idx++;
            }
          }
          pNewList->num = idx;
          if ((code = taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *)) != 0)) {
            taosMemoryFree(pNewList);
            TAOS_CHECK_GOTO(code, &lino, _OVER);
          }
          taosMemoryFree(pList);
        }
        update = true;
      }
    }
  }
  if (update) {
    mDebug("ip-white-list update for user: %s, fqdn: %s", user, fqdn);
  }

_OVER:
  if (pUpdate) *pUpdate = update;
  if (code < 0) {
    mError("failed to update ip-white-list for user: %s, fqdn: %s at line %d since %s", user, fqdn, lino,
           tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndRefreshUserIpWhiteList(SMnode *pMnode) {
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);

  if ((code = ipWhiteMgtUpdateAll(pMnode)) != 0) {
    (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
    TAOS_RETURN(code);
  }
  ipWhiteMgt.ver = taosGetTimestampMs();
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);

  TAOS_RETURN(code);
}

int32_t mndUpdateIpWhiteForAllUser(SMnode *pMnode, char *user, char *fqdn, int8_t type, int8_t lock) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    update = false;

  if (lock) {
    (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
    if (ipWhiteMgt.ver == 0) {
      TAOS_CHECK_GOTO(ipWhiteMgtUpdateAll(pMnode), &lino, _OVER);
      ipWhiteMgt.ver = taosGetTimestampMs();
      mInfo("update ip-white-list, user: %s, ver: %" PRId64, user, ipWhiteMgt.ver);
    }
  }

  TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, user, fqdn, type, &update), &lino, _OVER);

  void *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);
  while (pIter) {
    size_t klen = 0;
    char  *key = taosHashGetKey(pIter, &klen);

    char *keyDup = taosMemoryCalloc(1, klen + 1);
    if (keyDup == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    (void)memcpy(keyDup, key, klen);
    bool upd = false;
    code = mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, keyDup, fqdn, type, &upd);
    update |= upd;
    if (code < 0) {
      taosMemoryFree(keyDup);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    taosMemoryFree(keyDup);

    pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, pIter);
  }

_OVER:
  if (update) ipWhiteMgt.ver++;
  if (lock) (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  if (code < 0) {
    mError("failed to update ip-white-list for user: %s, fqdn: %s at line %d since %s", user, fqdn, lino,
           tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int64_t ipWhiteMgtFillMsg(SUpdateIpWhite *pUpdate) {
  int64_t ver = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  ver = ipWhiteMgt.ver;
  int32_t num = taosHashGetSize(ipWhiteMgt.pIpWhiteTab);

  pUpdate->pUserIpWhite = taosMemoryCalloc(1, num * sizeof(SUpdateUserIpWhite));
  if (pUpdate->pUserIpWhite == NULL) {
    (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
    TAOS_RETURN(terrno);
  }

  void   *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);
  int32_t i = 0;
  while (pIter) {
    SUpdateUserIpWhite *pUser = &pUpdate->pUserIpWhite[i];
    SIpWhiteListDual   *list = *(SIpWhiteListDual **)pIter;

    size_t klen;
    char  *key = taosHashGetKey(pIter, &klen);
    if (list->num != 0) {
      pUser->ver = ver;
      (void)memcpy(pUser->user, key, klen);
      pUser->numOfRange = list->num;
      pUser->pIpRanges = taosMemoryCalloc(1, list->num * sizeof(SIpRange));
      if (pUser->pIpRanges == NULL) {
        (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
        TAOS_RETURN(terrno);
      }
      (void)memcpy(pUser->pIpRanges, list->pIpRanges, list->num * sizeof(SIpRange));
      i++;
    }
    pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, pIter);
  }
  pUpdate->numOfUser = i;
  pUpdate->ver = ver;

  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  TAOS_RETURN(0);
}

void destroyIpWhiteTab(SHashObj *pIpWhiteTab) {
  if (pIpWhiteTab == NULL) return;

  void *pIter = taosHashIterate(pIpWhiteTab, NULL);
  while (pIter) {
    SIpWhiteListDual *list = *(SIpWhiteListDual **)pIter;
    taosMemoryFree(list);
    pIter = taosHashIterate(pIpWhiteTab, pIter);
  }

  taosHashCleanup(pIpWhiteTab);
}
int32_t mndFetchAllIpWhite(SMnode *pMnode, SHashObj **ppIpWhiteTab) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SHashObj *pIpWhiteTab = NULL;
  SArray   *pUserNames = NULL;
  SArray   *fqdns = NULL;

  pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);
  if (pIpWhiteTab == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  pUserNames = taosArrayInit(8, sizeof(void *));
  if (pUserNames == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  while (1) {
    SUserObj *pUser = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    SIpWhiteListDual *pWhiteList = cloneIpWhiteList(pUser->pIpWhiteListDual);
    if (pWhiteList == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    if ((code = taosHashPut(pIpWhiteTab, pUser->user, strlen(pUser->user), &pWhiteList, sizeof(void *))) != 0) {
      taosMemoryFree(pWhiteList);
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }

    char *name = taosStrdup(pUser->user);
    if (name == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    if (taosArrayPush(pUserNames, &name) == NULL) {
      taosMemoryFree(name);
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    sdbRelease(pSdb, pUser);
  }

  bool found = false;
  for (int i = 0; i < taosArrayGetSize(pUserNames); i++) {
    char *name = taosArrayGetP(pUserNames, i);
    if (strlen(name) == strlen(TSDB_DEFAULT_USER) && strncmp(name, TSDB_DEFAULT_USER, strlen(TSDB_DEFAULT_USER)) == 0) {
      found = true;
      break;
    }
  }
  if (found == false) {
    char *name = taosStrdup(TSDB_DEFAULT_USER);
    if (name == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    if (taosArrayPush(pUserNames, &name) == NULL) {
      taosMemoryFree(name);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
  }

  fqdns = mndGetAllDnodeFqdns(pMnode);  // TODO: refactor this line after refactor api
  if (fqdns == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);

    for (int j = 0; j < taosArrayGetSize(pUserNames); j++) {
      char *name = taosArrayGetP(pUserNames, j);
      TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(pIpWhiteTab, name, fqdn, IP_WHITE_ADD, NULL), &lino, _OVER);
    }
  }

_OVER:
  taosArrayDestroyP(fqdns, NULL);
  taosArrayDestroyP(pUserNames, NULL);

  if (code < 0) {
    mError("failed to fetch all ip white list at line %d since %s", lino, tstrerror(code));
    destroyIpWhiteTab(pIpWhiteTab);
    pIpWhiteTab = NULL;
  }
  *ppIpWhiteTab = pIpWhiteTab;
  TAOS_RETURN(code);
}

int32_t mndInitUser(SMnode *pMnode) {
  TAOS_CHECK_RETURN(ipWhiteMgtInit());

  SSdbTable table = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
      .encodeFp = (SdbEncodeFp)mndUserActionEncode,
      .decodeFp = (SdbDecodeFp)mndUserActionDecode,
      .insertFp = (SdbInsertFp)mndUserActionInsert,
      .updateFp = (SdbUpdateFp)mndUserActionUpdate,
      .deleteFp = (SdbDeleteFp)mndUserActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_USER, mndProcessCreateUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_USER, mndProcessAlterUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_USER, mndProcessDropUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_AUTH, mndProcessGetUserAuthReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_WHITELIST, mndProcessGetUserWhiteListReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_WHITELIST_DUAL, mndProcessGetUserWhiteListReq);

  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_IP_WHITE, mndProcesSRetrieveIpWhiteReq);
  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_IP_WHITE_DUAL, mndProcesSRetrieveIpWhiteReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER, mndRetrieveUsers);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER_FULL, mndRetrieveUsersFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER_FULL, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndCancelGetNextPrivileges);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupUser(SMnode *pMnode) { ipWhiteMgtCleanup(); }

static bool isDefaultRange(SIpRange *pRange) {
  int32_t code = 0;
  int32_t lino = 0;

  SIpRange range4 = {0};
  SIpRange range6 = {0};

  code = createDefaultIp4Range(&range4);
  TSDB_CHECK_CODE(code, lino, _error);

  code = createDefaultIp6Range(&range6);
  TSDB_CHECK_CODE(code, lino, _error);

  if (isIpRangeEqual(pRange, &range4) || (isIpRangeEqual(pRange, &range6))) {
    return true;
  }
_error:
  return false;
};

static int32_t ipRangeListToStr(SIpRange *range, int32_t num, char *buf, int64_t bufLen) {
  int32_t len = 0;
  for (int i = 0; i < num; i++) {
    SIpRange *pRange = &range[i];
    SIpAddr   addr = {0};
    tIpUintToStr(pRange, &addr);

    len += tsnprintf(buf + len, bufLen - len, "%s/%d,", IP_ADDR_STR(&addr), addr.mask);
  }
  if (len > 0) buf[len - 1] = 0;
  return len;
}

static bool isIpRangeEqual(SIpRange *a, SIpRange *b) {
  // equal or not
  if (a->type != b->type) {
    return false;
  }
  if (a->type == 0) {
    SIpV4Range *aP4 = &a->ipV4;
    SIpV4Range *bP4 = &b->ipV4;
    if (aP4->ip != bP4->ip || aP4->mask != bP4->mask) {
      return false;
    } else {
      return true;
    }
  } else {
    SIpV6Range *aP6 = &a->ipV6;
    SIpV6Range *bP6 = &b->ipV6;
    if (aP6->addr[0] != bP6->addr[0] || aP6->addr[1] != bP6->addr[1] || aP6->mask != bP6->mask) {
      return false;
    } else {
      return true;
    }
  }

  return true;
}
static bool isRangeInIpWhiteList(SIpWhiteListDual *pList, SIpRange *tgt) {
  for (int i = 0; i < pList->num; i++) {
    if (isIpRangeEqual(&pList->pIpRanges[i], tgt)) return true;
  }
  return false;
}
static bool isIpWhiteListEqual(SIpWhiteListDual *a, SIpWhiteListDual *b) {
  if (a->num != b->num) {
    return false;
  }
  for (int i = 0; i < a->num; i++) {
    if (!isIpRangeEqual(&a->pIpRanges[i], &b->pIpRanges[i])) {
      return false;
    }
  }
  return true;
}
int32_t convertIpWhiteListToStr(SIpWhiteListDual *pList, char **buf) {
  if (pList->num == 0) {
    *buf = NULL;
    return 0;
  }
  int64_t bufLen = pList->num * 256;
  *buf = taosMemoryCalloc(1, bufLen);
  if (*buf == NULL) {
    return 0;
  }

  int32_t len = ipRangeListToStr(pList->pIpRanges, pList->num, *buf, bufLen);
  if (len == 0) {
    taosMemoryFreeClear(*buf);
    return 0;
  }
  return strlen(*buf);
}
int32_t tSerializeIpWhiteList(void *buf, int32_t len, SIpWhiteListDual *pList, uint32_t *pLen) {
  int32_t  code = 0;
  int32_t  lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);

  TAOS_CHECK_GOTO(tStartEncode(&encoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tEncodeI32(&encoder, pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpRange *pRange = &(pList->pIpRanges[i]);
    TAOS_CHECK_GOTO(tSerializeIpRange(&encoder, pRange), &lino, _OVER);
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
_OVER:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("failed to serialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  if (pLen) *pLen = tlen;
  TAOS_RETURN(code);
}

int32_t tDerializeIpWhileList(void *buf, int32_t len, SIpWhiteListDual *pList) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpRange *pRange = &(pList->pIpRanges[i]);
    TAOS_CHECK_GOTO(tDeserializeIpRange(&decoder, pRange), &lino, _OVER);
  }

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("failed to deserialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t tDerializeIpWhileListFromOldVer(void *buf, int32_t len, SIpWhiteList *pList) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range *pIp4 = &(pList->pIpRange[i]);
    TAOS_CHECK_GOTO(tDecodeU32(&decoder, &pIp4->ip), &lino, _OVER);
    TAOS_CHECK_GOTO(tDecodeU32(&decoder, &pIp4->mask), &lino, _OVER);
  }

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("failed to deserialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t createIpWhiteList(void *buf, int32_t len, SIpWhiteListDual **ppList) {
  int32_t           code = 0;
  int32_t           lino = 0;
  int32_t           num = 0;
  SIpWhiteListDual *p = NULL;
  SDecoder          decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &num), &lino, _OVER);

  p = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + num * sizeof(SIpRange));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(tDerializeIpWhileList(buf, len, p), &lino, _OVER);

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    taosMemoryFreeClear(p);
    mError("failed to create ip white list at line %d since %s", lino, tstrerror(code));
  }
  *ppList = p;
  TAOS_RETURN(code);
}

static int32_t createIpWhiteListFromOldVer(void *buf, int32_t len, SIpWhiteList **ppList) {
  int32_t       code = 0;
  int32_t       lino = 0;
  int32_t       num = 0;
  SIpWhiteList *p = NULL;
  SDecoder      decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &num), &lino, _OVER);

  p = taosMemoryCalloc(1, sizeof(SIpWhiteList) + num * sizeof(SIpV4Range));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(tDerializeIpWhileListFromOldVer(buf, len, p), &lino, _OVER);

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    taosMemoryFreeClear(p);
    mError("failed to create ip white list at line %d since %s", lino, tstrerror(code));
  }
  *ppList = p;
  TAOS_RETURN(code);
}

static int32_t createDefaultIpWhiteList(SIpWhiteListDual **ppWhiteList) {
  int32_t code = 0;
  int32_t lino = 0;
  *ppWhiteList = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + sizeof(SIpRange) * 2);
  if (*ppWhiteList == NULL) {
    TAOS_RETURN(terrno);
  }
  (*ppWhiteList)->num = 2;

  SIpRange v4 = {0};
  SIpRange v6 = {0};

#ifndef TD_ASTRA
  code = createDefaultIp4Range(&v4);
  TSDB_CHECK_CODE(code, lino, _error);

  code = createDefaultIp6Range(&v6);
  TSDB_CHECK_CODE(code, lino, _error);

#endif

_error:
  if (code != 0) {
    taosMemoryFree(*ppWhiteList);
    *ppWhiteList = NULL;
    mError("failed to create default ip white list at line %d since %s", __LINE__, tstrerror(code));
  } else {
    memcpy(&(*ppWhiteList)->pIpRanges[0], &v4, sizeof(SIpRange));
    memcpy(&(*ppWhiteList)->pIpRanges[1], &v6, sizeof(SIpRange));
  }
  return 0;
}

static int32_t mndCreateDefaultUser(SMnode *pMnode, char *acct, char *user, char *pass) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pass, strlen(pass), userObj.pass);
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.sysInfo = 1;
  userObj.enable = 1;
  userObj.ipWhiteListVer = taosGetTimestampMs();
  TAOS_CHECK_RETURN(createDefaultIpWhiteList(&userObj.pIpWhiteListDual));
  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
    userObj.createdb = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) goto _ERROR;
  TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_READY), &lino, _ERROR);

  mInfo("user:%s, will be created when deploying, raw:%p", userObj.user, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-user");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("user:%s, failed to create since %s", userObj.user, terrstr());
    goto _ERROR;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, userObj.user);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _ERROR;
  }
  TAOS_CHECK_GOTO(sdbSetRawStatus(pRaw, SDB_STATUS_READY), &lino, _ERROR);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _ERROR;
  }

  mndTransDrop(pTrans);
  taosMemoryFree(userObj.pIpWhiteListDual);
  return 0;
_ERROR:
  taosMemoryFree(userObj.pIpWhiteListDual);
  TAOS_RETURN(terrno ? terrno : TSDB_CODE_APP_ERROR);
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  return mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS);
}

SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t ipWhiteReserve =
      pUser->pIpWhiteListDual ? (sizeof(SIpRange) * pUser->pIpWhiteListDual->num + sizeof(SIpWhiteListDual) + 4) : 16;
  int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
  int32_t numOfReadTbs = taosHashGetSize(pUser->readTbs);
  int32_t numOfWriteTbs = taosHashGetSize(pUser->writeTbs);
  int32_t numOfAlterTbs = taosHashGetSize(pUser->alterTbs);
  int32_t numOfReadViews = taosHashGetSize(pUser->readViews);
  int32_t numOfWriteViews = taosHashGetSize(pUser->writeViews);
  int32_t numOfAlterViews = taosHashGetSize(pUser->alterViews);
  int32_t numOfTopics = taosHashGetSize(pUser->topics);
  int32_t numOfUseDbs = taosHashGetSize(pUser->useDbs);
  int32_t size = sizeof(SUserObj) + USER_RESERVE_SIZE + (numOfReadDbs + numOfWriteDbs) * TSDB_DB_FNAME_LEN +
                 numOfTopics * TSDB_TOPIC_FNAME_LEN + ipWhiteReserve;
  char    *buf = NULL;
  SSdbRaw *pRaw = NULL;

  char *stb = taosHashIterate(pUser->readTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->readTbs, stb);
  }

  stb = taosHashIterate(pUser->writeTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->writeTbs, stb);
  }

  stb = taosHashIterate(pUser->alterTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->alterTbs, stb);
  }

  stb = taosHashIterate(pUser->readViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->readViews, stb);
  }

  stb = taosHashIterate(pUser->writeViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->writeViews, stb);
  }

  stb = taosHashIterate(pUser->alterViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->alterViews, stb);
  }

  int32_t *useDb = taosHashIterate(pUser->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;
    size += sizeof(int32_t);
    useDb = taosHashIterate(pUser->useDbs, useDb);
  }

  pRaw = sdbAllocRaw(SDB_USER, USER_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->sysInfo, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->enable, _OVER)
  SDB_SET_UINT8(pRaw, dataPos, pUser->flag, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->authVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->passVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfTopics, _OVER)

  char *db = taosHashIterate(pUser->readDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->readDbs, db);
  }

  db = taosHashIterate(pUser->writeDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->writeDbs, db);
  }

  char *topic = taosHashIterate(pUser->topics, NULL);
  while (topic != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER);
    topic = taosHashIterate(pUser->topics, topic);
  }

  SDB_SET_INT32(pRaw, dataPos, numOfReadTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfAlterTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfAlterViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfUseDbs, _OVER)

  stb = taosHashIterate(pUser->readTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->readTbs, stb);
  }

  stb = taosHashIterate(pUser->writeTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->writeTbs, stb);
  }

  stb = taosHashIterate(pUser->alterTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->alterTbs, stb);
  }

  stb = taosHashIterate(pUser->readViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->readViews, stb);
  }

  stb = taosHashIterate(pUser->writeViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->writeViews, stb);
  }

  stb = taosHashIterate(pUser->alterViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->alterViews, stb);
  }

  useDb = taosHashIterate(pUser->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    SDB_SET_INT32(pRaw, dataPos, *useDb, _OVER)
    useDb = taosHashIterate(pUser->useDbs, useDb);
  }

  // save white list
  int32_t num = pUser->pIpWhiteListDual->num;
  int32_t tlen = sizeof(SIpWhiteListDual) + num * sizeof(SIpRange) + 4;
  if ((buf = taosMemoryCalloc(1, tlen)) == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _OVER);
  }
  int32_t len = 0;
  TAOS_CHECK_GOTO(tSerializeIpWhiteList(buf, tlen, pUser->pIpWhiteListDual, &len), &lino, _OVER);

  SDB_SET_INT32(pRaw, dataPos, len, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, len, _OVER);

  SDB_SET_INT64(pRaw, dataPos, pUser->ipWhiteListVer, _OVER);
  SDB_SET_INT8(pRaw, dataPos, pUser->passEncryptAlgorithm, _OVER);

  SDB_SET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

_OVER:
  taosMemoryFree(buf);
  if (code < 0) {
    mError("user:%s, failed to encode user action to raw:%p at line %d since %s", pUser->user, pRaw, lino,
           tstrerror(code));
    sdbFreeRaw(pRaw);
    pRaw = NULL;
    terrno = code;
  }

  mTrace("user:%s, encode user action to raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SSdbRow  *pRow = NULL;
  SUserObj *pUser = NULL;
  char     *key = NULL;
  char     *value = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_PTR, &lino, _OVER);
  }

  if (sver < 1 || sver > USER_VER_NUMBER) {
    TAOS_CHECK_GOTO(TSDB_CODE_SDB_INVALID_DATA_VER, &lino, _OVER);
  }

  pRow = sdbAllocRow(sizeof(SUserObj));
  if (pRow == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->updateTime, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->superUser, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->sysInfo, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->enable, _OVER)
  SDB_GET_UINT8(pRaw, dataPos, &pUser->flag, _OVER)
  if (pUser->superUser) pUser->createdb = 1;
  SDB_GET_INT32(pRaw, dataPos, &pUser->authVersion, _OVER)
  if (sver >= 4) {
    SDB_GET_INT32(pRaw, dataPos, &pUser->passVersion, _OVER)
  }

  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  int32_t numOfTopics = 0;
  SDB_GET_INT32(pRaw, dataPos, &numOfReadDbs, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &numOfWriteDbs, _OVER)
  if (sver >= 2) {
    SDB_GET_INT32(pRaw, dataPos, &numOfTopics, _OVER)
  }

  pUser->readDbs = taosHashInit(numOfReadDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pUser->writeDbs =
      taosHashInit(numOfWriteDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pUser->topics = taosHashInit(numOfTopics, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pUser->readDbs == NULL || pUser->writeDbs == NULL || pUser->topics == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    goto _OVER;
  }

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    TAOS_CHECK_GOTO(taosHashPut(pUser->readDbs, db, len, db, TSDB_DB_FNAME_LEN), &lino, _OVER);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    TAOS_CHECK_GOTO(taosHashPut(pUser->writeDbs, db, len, db, TSDB_DB_FNAME_LEN), &lino, _OVER);
  }

  if (sver >= 2) {
    for (int32_t i = 0; i < numOfTopics; ++i) {
      char topic[TSDB_TOPIC_FNAME_LEN] = {0};
      SDB_GET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER)
      int32_t len = strlen(topic) + 1;
      TAOS_CHECK_GOTO(taosHashPut(pUser->topics, topic, len, topic, TSDB_TOPIC_FNAME_LEN), &lino, _OVER);
    }
  }

  if (sver >= 3) {
    int32_t numOfReadTbs = 0;
    int32_t numOfWriteTbs = 0;
    int32_t numOfAlterTbs = 0;
    int32_t numOfReadViews = 0;
    int32_t numOfWriteViews = 0;
    int32_t numOfAlterViews = 0;
    int32_t numOfUseDbs = 0;
    SDB_GET_INT32(pRaw, dataPos, &numOfReadTbs, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &numOfWriteTbs, _OVER)
    if (sver >= 6) {
      SDB_GET_INT32(pRaw, dataPos, &numOfAlterTbs, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfReadViews, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfWriteViews, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfAlterViews, _OVER)
    }
    SDB_GET_INT32(pRaw, dataPos, &numOfUseDbs, _OVER)

    pUser->readTbs =
        taosHashInit(numOfReadTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->writeTbs =
        taosHashInit(numOfWriteTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->alterTbs =
        taosHashInit(numOfAlterTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    pUser->readViews =
        taosHashInit(numOfReadViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->writeViews =
        taosHashInit(numOfWriteViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->alterViews =
        taosHashInit(numOfAlterViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    pUser->useDbs = taosHashInit(numOfUseDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    if (pUser->readTbs == NULL || pUser->writeTbs == NULL || pUser->alterTbs == NULL || pUser->readViews == NULL ||
        pUser->writeViews == NULL || pUser->alterViews == NULL || pUser->useDbs == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      goto _OVER;
    }

    for (int32_t i = 0; i < numOfReadTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
      if (value == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      TAOS_CHECK_GOTO(taosHashPut(pUser->readTbs, key, keyLen, value, valuelen), &lino, _OVER);
    }

    for (int32_t i = 0; i < numOfWriteTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
      if (value == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      TAOS_CHECK_GOTO(taosHashPut(pUser->writeTbs, key, keyLen, value, valuelen), &lino, _OVER);
    }

    if (sver >= 6) {
      for (int32_t i = 0; i < numOfAlterTbs; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->alterTbs, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfReadViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->readViews, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfWriteViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->writeViews, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfAlterViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->alterViews, key, keyLen, value, valuelen), &lino, _OVER);
      }
    }

    for (int32_t i = 0; i < numOfUseDbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t ref = 0;
      SDB_GET_INT32(pRaw, dataPos, &ref, _OVER);

      TAOS_CHECK_GOTO(taosHashPut(pUser->useDbs, key, keyLen, &ref, sizeof(ref)), &lino, _OVER);
    }
  }
  // decoder white list
  if (sver >= USER_VER_SUPPORT_WHITELIST) {
    if (sver < USER_VER_SUPPORT_WHITELIT_DUAL_STACK) {
      int32_t len = 0;
      SDB_GET_INT32(pRaw, dataPos, &len, _OVER);

      TAOS_MEMORY_REALLOC(key, len);
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      SDB_GET_BINARY(pRaw, dataPos, key, len, _OVER);

      SIpWhiteList *pIpWhiteList = NULL;
      TAOS_CHECK_GOTO(createIpWhiteListFromOldVer(key, len, &pIpWhiteList), &lino, _OVER);

      SDB_GET_INT64(pRaw, dataPos, &pUser->ipWhiteListVer, _OVER);

      code = cvtIpWhiteListToDual(pIpWhiteList, &pUser->pIpWhiteListDual);
      if (code != 0) {
        taosMemoryFreeClear(pIpWhiteList);
      }
      TAOS_CHECK_GOTO(code, &lino, _OVER);

      taosMemoryFreeClear(pIpWhiteList);

    } else if (sver >= USER_VER_SUPPORT_WHITELIT_DUAL_STACK) {
      int32_t len = 0;
      SDB_GET_INT32(pRaw, dataPos, &len, _OVER);

      TAOS_MEMORY_REALLOC(key, len);
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      SDB_GET_BINARY(pRaw, dataPos, key, len, _OVER);

      TAOS_CHECK_GOTO(createIpWhiteList(key, len, &pUser->pIpWhiteListDual), &lino, _OVER);
      SDB_GET_INT64(pRaw, dataPos, &pUser->ipWhiteListVer, _OVER);
    }
  }

  if (pUser->pIpWhiteListDual == NULL) {
    TAOS_CHECK_GOTO(createDefaultIpWhiteList(&pUser->pIpWhiteListDual), &lino, _OVER);
    pUser->ipWhiteListVer = taosGetTimestampMs();
  }

  SDB_GET_INT8(pRaw, dataPos, &pUser->passEncryptAlgorithm, _OVER);

  SDB_GET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pUser->lock);

_OVER:
  taosMemoryFree(key);
  taosMemoryFree(value);
  if (code < 0) {
    terrno = code;
    mError("user:%s, failed to decode at line %d from raw:%p since %s", pUser == NULL ? "null" : pUser->user, lino,
           pRaw, tstrerror(code));
    if (pUser != NULL) {
      taosHashCleanup(pUser->readDbs);
      taosHashCleanup(pUser->writeDbs);
      taosHashCleanup(pUser->topics);
      taosHashCleanup(pUser->readTbs);
      taosHashCleanup(pUser->writeTbs);
      taosHashCleanup(pUser->alterTbs);
      taosHashCleanup(pUser->readViews);
      taosHashCleanup(pUser->writeViews);
      taosHashCleanup(pUser->alterViews);
      taosHashCleanup(pUser->useDbs);
      taosMemoryFreeClear(pUser->pIpWhiteListDual);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("user:%s, decode from raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRow;
}

static int32_t mndUserActionInsert(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform insert action, row:%p", pUser->user, pUser);

  SAcctObj *pAcct = sdbAcquire(pSdb, SDB_ACCT, pUser->acct);
  if (pAcct == NULL) {
    terrno = TSDB_CODE_MND_ACCT_NOT_EXIST;
    mError("user:%s, failed to perform insert action since %s", pUser->user, terrstr());
    TAOS_RETURN(terrno);
  }
  pUser->acctId = pAcct->acctId;
  sdbRelease(pSdb, pAcct);

  return 0;
}

int32_t mndDupTableHash(SHashObj *pOld, SHashObj **ppNew) {
  int32_t code = 0;
  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    TAOS_RETURN(terrno);
  }

  char *tb = taosHashIterate(pOld, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(tb, &keyLen);

    int32_t valueLen = strlen(tb) + 1;
    if ((code = taosHashPut(*ppNew, key, keyLen, tb, valueLen)) != 0) {
      taosHashCancelIterate(pOld, tb);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    tb = taosHashIterate(pOld, tb);
  }

  TAOS_RETURN(code);
}

int32_t mndDupUseDbHash(SHashObj *pOld, SHashObj **ppNew) {
  int32_t code = 0;
  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    TAOS_RETURN(terrno);
  }

  int32_t *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(db, &keyLen);

    if ((code = taosHashPut(*ppNew, key, keyLen, db, sizeof(*db))) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    db = taosHashIterate(pOld, db);
  }

  TAOS_RETURN(code);
}

int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew) {
  int32_t code = 0;
  (void)memcpy(pNew, pUser, sizeof(SUserObj));
  pNew->authVersion++;
  pNew->updateTime = taosGetTimestampMs();

  taosRLockLatch(&pUser->lock);
  TAOS_CHECK_GOTO(mndDupDbHash(pUser->readDbs, &pNew->readDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupDbHash(pUser->writeDbs, &pNew->writeDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->readTbs, &pNew->readTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->writeTbs, &pNew->writeTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->alterTbs, &pNew->alterTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->readViews, &pNew->readViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->writeViews, &pNew->writeViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->alterViews, &pNew->alterViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTopicHash(pUser->topics, &pNew->topics), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupUseDbHash(pUser->useDbs, &pNew->useDbs), NULL, _OVER);
  pNew->pIpWhiteListDual = cloneIpWhiteList(pUser->pIpWhiteListDual);
  if (pNew->pIpWhiteListDual == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

_OVER:
  taosRUnLockLatch(&pUser->lock);
  TAOS_RETURN(code);
}

void mndUserFreeObj(SUserObj *pUser) {
  taosHashCleanup(pUser->readDbs);
  taosHashCleanup(pUser->writeDbs);
  taosHashCleanup(pUser->topics);
  taosHashCleanup(pUser->readTbs);
  taosHashCleanup(pUser->writeTbs);
  taosHashCleanup(pUser->alterTbs);
  taosHashCleanup(pUser->readViews);
  taosHashCleanup(pUser->writeViews);
  taosHashCleanup(pUser->alterViews);
  taosHashCleanup(pUser->useDbs);
  taosMemoryFreeClear(pUser->pIpWhiteListDual);
  pUser->readDbs = NULL;
  pUser->writeDbs = NULL;
  pUser->topics = NULL;
  pUser->readTbs = NULL;
  pUser->writeTbs = NULL;
  pUser->alterTbs = NULL;
  pUser->readViews = NULL;
  pUser->writeViews = NULL;
  pUser->alterViews = NULL;
  pUser->useDbs = NULL;
}

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform delete action, row:%p", pUser->user, pUser);
  mndUserFreeObj(pUser);
  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew) {
  mTrace("user:%s, perform update action, old row:%p new row:%p", pOld->user, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->authVersion = pNew->authVersion;
  pOld->passVersion = pNew->passVersion;
  pOld->sysInfo = pNew->sysInfo;
  pOld->enable = pNew->enable;
  pOld->flag = pNew->flag;
  (void)memcpy(pOld->pass, pNew->pass, TSDB_PASSWORD_LEN);
  TSWAP(pOld->readDbs, pNew->readDbs);
  TSWAP(pOld->writeDbs, pNew->writeDbs);
  TSWAP(pOld->topics, pNew->topics);
  TSWAP(pOld->readTbs, pNew->readTbs);
  TSWAP(pOld->writeTbs, pNew->writeTbs);
  TSWAP(pOld->alterTbs, pNew->alterTbs);
  TSWAP(pOld->readViews, pNew->readViews);
  TSWAP(pOld->writeViews, pNew->writeViews);
  TSWAP(pOld->alterViews, pNew->alterViews);
  TSWAP(pOld->useDbs, pNew->useDbs);

  int32_t sz = sizeof(SIpWhiteListDual) + pNew->pIpWhiteListDual->num * sizeof(SIpRange);
  TAOS_MEMORY_REALLOC(pOld->pIpWhiteListDual, sz);
  if (pOld->pIpWhiteListDual == NULL) {
    taosWUnLockLatch(&pOld->lock);
    return terrno;
  }
  (void)memcpy(pOld->pIpWhiteListDual, pNew->pIpWhiteListDual, sz);
  pOld->ipWhiteListVer = pNew->ipWhiteListVer;

  taosWUnLockLatch(&pOld->lock);

  return 0;
}

int32_t mndAcquireUser(SMnode *pMnode, const char *userName, SUserObj **ppUser) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (*ppUser == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_USER_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

void mndReleaseUser(SMnode *pMnode, SUserObj *pUser) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pUser);
}

int32_t mndEncryptPass(char *pass, int8_t *algo) {
  int32_t code = 0;
  if (tsiEncryptPassAlgorithm == DND_CA_SM4) {
    if (strlen(tsEncryptKey) == 0) {
      code = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
      goto _OVER;
    }
    unsigned char packetData[TSDB_PASSWORD_LEN] = {0};
    int           newLen = 0;

    SCryptOpts opts = {0};
    opts.len = TSDB_PASSWORD_LEN;
    opts.source = pass;
    opts.result = packetData;
    opts.unitLen = TSDB_PASSWORD_LEN;
    tstrncpy(opts.key, tsEncryptKey, ENCRYPT_KEY_LEN + 1);

    newLen = CBC_Encrypt(&opts);

    memcpy(pass, packetData, newLen);

    if (algo != NULL) *algo = DND_CA_SM4;
  }
_OVER:
  return code;
}

static int32_t addDefaultIpToTable(int8_t enableIpv6, SHashObj *pUniqueTab) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t dummpy = 0;

  SIpRange ipv4 = {0}, ipv6 = {0};
  code = createDefaultIp4Range(&ipv4);
  TSDB_CHECK_CODE(code, lino, _error);

  code = taosHashPut(pUniqueTab, &ipv4, sizeof(ipv4), &dummpy, sizeof(dummpy));
  TSDB_CHECK_CODE(code, lino, _error);

  if (enableIpv6) {
    code = createDefaultIp6Range(&ipv6);
    TSDB_CHECK_CODE(code, lino, _error);

    code = taosHashPut(pUniqueTab, &ipv6, sizeof(ipv6), &dummpy, sizeof(dummpy));
    TSDB_CHECK_CODE(code, lino, _error);
  }
_error:
  if (code != 0) {
    mError("failed to add default ip range to table since %s", tstrerror(code));
  }
  return code;
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SUserObj userObj = {0};

  if (pCreate->passIsMd5 == 1) {
    memcpy(userObj.pass, pCreate->pass, TSDB_PASSWORD_LEN - 1);
    TAOS_CHECK_RETURN(mndEncryptPass(userObj.pass, &userObj.passEncryptAlgorithm));
  } else {
    if (pCreate->isImport != 1) {
      taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.pass);
      userObj.pass[TSDB_PASSWORD_LEN - 1] = 0;
      TAOS_CHECK_RETURN(mndEncryptPass(userObj.pass, &userObj.passEncryptAlgorithm));
    } else {
      memcpy(userObj.pass, pCreate->pass, TSDB_PASSWORD_LEN);
    }
  }

  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = 0;  // pCreate->superUser;
  userObj.sysInfo = pCreate->sysInfo;
  userObj.enable = pCreate->enable;
  userObj.createdb = pCreate->createDb;

  if (pCreate->numIpRanges == 0) {
    TAOS_CHECK_RETURN(createDefaultIpWhiteList(&userObj.pIpWhiteListDual));
  } else {
    SHashObj *pUniqueTab = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
    if (pUniqueTab == NULL) {
      TAOS_RETURN(terrno);
    }
    int32_t dummpy = 0;
    for (int i = 0; i < pCreate->numIpRanges; i++) {
      SIpRange range = {0};
      if (pCreate->pIpDualRanges == NULL) {
        range.type = 0;
        memcpy(&range.ipV4, &(pCreate->pIpRanges[i]), sizeof(SIpV4Range));
      } else {
        memcpy(&range, pCreate->pIpDualRanges + i, sizeof(SIpRange));
      }

      if ((code = taosHashPut(pUniqueTab, &range, sizeof(range), &dummpy, sizeof(dummpy))) != 0) {
        taosHashCleanup(pUniqueTab);
        TAOS_RETURN(code);
      }
    }
    code = addDefaultIpToTable(tsEnableIpv6, pUniqueTab);
    if (code != 0) {
      taosHashCleanup(pUniqueTab);
      TAOS_RETURN(code);
    }

    if (taosHashGetSize(pUniqueTab) > MND_MAX_USE_HOST) {
      taosHashCleanup(pUniqueTab);
      TAOS_RETURN(TSDB_CODE_MND_TOO_MANY_USER_HOST);
    }

    int32_t           numOfRanges = taosHashGetSize(pUniqueTab);
    SIpWhiteListDual *p = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + numOfRanges * sizeof(SIpRange));
    if (p == NULL) {
      taosHashCleanup(pUniqueTab);
      TAOS_RETURN(terrno);
    }
    void   *pIter = taosHashIterate(pUniqueTab, NULL);
    int32_t i = 0;
    while (pIter) {
      size_t    len = 0;
      SIpRange *key = taosHashGetKey(pIter, &len);
      memcpy(p->pIpRanges + i, key, sizeof(SIpRange));
      pIter = taosHashIterate(pUniqueTab, pIter);
      i++;
    }

    taosHashCleanup(pUniqueTab);
    p->num = numOfRanges;
    userObj.pIpWhiteListDual = p;
  }

  userObj.ipWhiteListVer = taosGetTimestampMs();

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());
    taosMemoryFree(userObj.pIpWhiteListDual);
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY), &lino, _OVER);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  if ((code = ipWhiteMgtUpdate(pMnode, userObj.user, userObj.pIpWhiteListDual)) != 0) {
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  taosMemoryFree(userObj.pIpWhiteListDual);
  mndTransDrop(pTrans);
  return 0;
_OVER:
  taosMemoryFree(userObj.pIpWhiteListDual);

  TAOS_RETURN(code);
}

static int32_t mndCheckPasswordMinLen(const char *pwd, int32_t len) {
  if (len < TSDB_PASSWORD_MIN_LEN) {
    return -1;
  }
  return 0;
}

static int32_t mndCheckPasswordMaxLen(const char *pwd, int32_t len) {
  if (len > TSDB_PASSWORD_MAX_LEN) {
    return -1;
  }
  return 0;
}

static int32_t mndCheckPasswordFmt(const char *pwd, int32_t len) {
  if (strcmp(pwd, "taosdata") == 0) {
    return 0;
  }

  bool charTypes[4] = {0};
  for (int32_t i = 0; i < len; ++i) {
    if (taosIsBigChar(pwd[i])) {
      charTypes[0] = true;
    } else if (taosIsSmallChar(pwd[i])) {
      charTypes[1] = true;
    } else if (taosIsNumberChar(pwd[i])) {
      charTypes[2] = true;
    } else if (taosIsSpecialChar(pwd[i])) {
      charTypes[3] = true;
    } else {
      return -1;
    }
  }

  int32_t numOfTypes = 0;
  for (int32_t i = 0; i < 4; ++i) {
    numOfTypes += charTypes[i];
  }

  if (numOfTypes < 3) {
    return -1;
  }

  return 0;
}

static int32_t mndProcessCreateUserReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = 0;
  int32_t        lino = 0;
  SUserObj      *pUser = NULL;
  SUserObj      *pOperUser = NULL;
  SCreateUserReq createReq = {0};

  if (tDeserializeSCreateUserReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }

  mInfo("user:%s, start to create, createdb:%d, is_import:%d", createReq.user, createReq.createDb, createReq.isImport);

#ifndef TD_ENTERPRISE
  if (createReq.isImport == 1) {
    TAOS_CHECK_GOTO(TSDB_CODE_OPS_NOT_SUPPORT, &lino, _OVER);  // enterprise feature
  }
#endif

  if (createReq.isImport != 1) {
    TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_USER), &lino, _OVER);
  } else {
    if (strcmp(pReq->info.conn.user, "root") != 0) {
      mError("The operation is not permitted, user:%s", pReq->info.conn.user);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
    }
  }

  if (createReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  if (createReq.passIsMd5 == 0) {
    int32_t len = strlen(createReq.pass);
    if (createReq.isImport != 1) {
      if (mndCheckPasswordMinLen(createReq.pass, len) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY, &lino, _OVER);
      }
      if (mndCheckPasswordMaxLen(createReq.pass, len) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG, &lino, _OVER);
      }
      if (mndCheckPasswordFmt(createReq.pass, len) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_PASS_FORMAT, &lino, _OVER);
      }
    }
  }

  code = mndAcquireUser(pMnode, createReq.user, &pUser);
  if (pUser != NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_ALREADY_EXIST, &lino, _OVER);
  }

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(grantCheck(TSDB_GRANT_USER), &lino, _OVER);

  code = mndCreateUser(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char detail[1000] = {0};
  (void)tsnprintf(detail, sizeof(detail), "enable:%d, superUser:%d, sysInfo:%d, password:xxx", createReq.enable,
                  createReq.superUser, createReq.sysInfo);
  char operation[15] = {0};
  if (createReq.isImport == 1) {
    tstrncpy(operation, "importUser", sizeof(operation));
  } else {
    tstrncpy(operation, "createUser", sizeof(operation));
  }

  auditRecord(pReq, pMnode->clusterId, operation, "", createReq.user, detail, strlen(detail));

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create at line %d since %s", createReq.user, lino, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSCreateUserReq(&createReq);

  TAOS_RETURN(code);
}

int32_t mndProcessGetUserWhiteListReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = 0;
  int32_t              lino = 0;
  int32_t              contLen = 0;
  void                *pRsp = NULL;
  SUserObj            *pUser = NULL;
  SGetUserWhiteListReq wlReq = {0};
  SGetUserWhiteListRsp wlRsp = {0};

  int32_t (*serialFn)(void *, int32_t, SGetUserWhiteListRsp *) = NULL;
  int32_t (*setRspFn)(SMnode * pMnode, SUserObj * pUser, SGetUserWhiteListRsp * pRsp) = NULL;

  if (pReq->msgType == TDMT_MND_GET_USER_WHITELIST_DUAL) {
    serialFn = tSerializeSGetUserWhiteListDualRsp;
    setRspFn = mndSetUserWhiteListDualRsp;
  } else {
    serialFn = tSerializeSGetUserWhiteListRsp;
    setRspFn = mndSetUserWhiteListRsp;
  }
  if (tDeserializeSGetUserWhiteListReq(pReq->pCont, pReq->contLen, &wlReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }
  mTrace("user: %s, start to get whitelist", wlReq.user);

  code = mndAcquireUser(pMnode, wlReq.user, &pUser);
  if (pUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_NOT_EXIST, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(setRspFn(pMnode, pUser, &wlRsp), &lino, _OVER);
  contLen = serialFn(NULL, 0, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  contLen = serialFn(pRsp, contLen, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserWhiteListDualRsp(&wlRsp);
  if (code < 0) {
    mError("user:%s, failed to get whitelist at line %d since %s", wlReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  TAOS_RETURN(code);
}

int32_t mndProcesSRetrieveIpWhiteReq(SRpcMsg *pReq) {
  int32_t        code = 0;
  int32_t        lino = 0;
  int32_t        len = 0;
  void          *pRsp = NULL;
  SUpdateIpWhite ipWhite = {0};

  // impl later
  SRetrieveIpWhiteReq req = {0};
  if (tDeserializeRetrieveIpWhite(pReq->pCont, pReq->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  int32_t (*fn)(void *, int32_t, SUpdateIpWhite *) = NULL;
  if (pReq->msgType == TDMT_MND_RETRIEVE_IP_WHITE) {
    fn = tSerializeSUpdateIpWhite;
  } else if (pReq->msgType == TDMT_MND_RETRIEVE_IP_WHITE_DUAL) {
    fn = tSerializeSUpdateIpWhiteDual;
  }

  TAOS_CHECK_GOTO(ipWhiteMgtFillMsg(&ipWhite), &lino, _OVER);

  len = fn(NULL, 0, &ipWhite);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

  pRsp = rpcMallocCont(len);
  if (!pRsp) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  len = fn(pRsp, len, &ipWhite);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

_OVER:
  if (code < 0) {
    mError("failed to process retrieve ip white request at line %d since %s", lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    len = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = len;

  tFreeSUpdateIpWhiteReq(&ipWhite);
  TAOS_RETURN(code);
}

static int32_t mndAlterUser(SMnode *pMnode, SUserObj *pOld, SUserObj *pNew, SRpcMsg *pReq) {
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "alter-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to alter since %s", pOld->user, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to alter user:%s", pTrans->id, pOld->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pNew);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  if (code < 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if ((code = ipWhiteMgtUpdate(pMnode, pNew->user, pNew->pIpWhiteListDual)) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndDupObjHash(SHashObj *pOld, int32_t dataLen, SHashObj **ppNew) {
  int32_t code = 0;

  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    code = terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  char *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    int32_t len = strlen(db) + 1;
    if ((code = taosHashPut(*ppNew, db, len, db, dataLen)) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    db = taosHashIterate(pOld, db);
  }

  TAOS_RETURN(code);
}

int32_t mndDupDbHash(SHashObj *pOld, SHashObj **ppNew) { return mndDupObjHash(pOld, TSDB_DB_FNAME_LEN, ppNew); }

int32_t mndDupTopicHash(SHashObj *pOld, SHashObj **ppNew) { return mndDupObjHash(pOld, TSDB_TOPIC_FNAME_LEN, ppNew); }

static int32_t mndTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                  SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};

  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (alterReq->tagCond != NULL && alterReq->tagCondLen != 0) {
    char *value = taosHashGet(hash, tbFName, len);
    if (value != NULL) {
      TAOS_RETURN(TSDB_CODE_MND_PRIVILEDGE_EXIST);
    }

    int32_t condLen = alterReq->tagCondLen;
    TAOS_CHECK_RETURN(taosHashPut(hash, tbFName, len, alterReq->tagCond, condLen));
  } else {
    TAOS_CHECK_RETURN(taosHashPut(hash, tbFName, len, alterReq->isView ? "v" : "t", 2));
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t  ref = 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL != currRef) {
    ref = (*currRef) + 1;
  }
  TAOS_CHECK_RETURN(taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)));

  TAOS_RETURN(0);
}

static int32_t mndRemoveTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                        SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (taosHashRemove(hash, tbFName, len) != 0) {
    TAOS_RETURN(0);  // not found
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL == currRef) {
    return 0;
  }

  if (1 == *currRef) {
    if (taosHashRemove(useDbHash, alterReq->objname, dbKeyLen) != 0) {
      TAOS_RETURN(0);  // not found
    }
    return 0;
  }
  int32_t ref = (*currRef) - 1;
  TAOS_CHECK_RETURN(taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)));

  return 0;
}

static char *mndUserAuditTypeStr(int32_t type) {
  if (type == TSDB_ALTER_USER_PASSWD) {
    return "changePassword";
  }
  if (type == TSDB_ALTER_USER_SUPERUSER) {
    return "changeSuperUser";
  }
  if (type == TSDB_ALTER_USER_ENABLE) {
    return "enableUser";
  }
  if (type == TSDB_ALTER_USER_SYSINFO) {
    return "userSysInfo";
  }
  if (type == TSDB_ALTER_USER_CREATEDB) {
    return "userCreateDB";
  }
  return "error";
}

static int32_t mndProcessAlterUserPrivilegesReq(SAlterUserReq *pAlterReq, SMnode *pMnode, SUserObj *pNewUser) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t code = 0;
  int32_t lino = 0;

  if (ALTER_USER_ADD_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      if ((code = taosHashPut(pNewUser->readDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN)) !=
          0) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        if ((code = taosHashPut(pNewUser->readDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN)) != 0) {
          sdbRelease(pSdb, pDb);
          sdbCancelFetch(pSdb, pIter);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_ADD_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      if ((code = taosHashPut(pNewUser->writeDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN)) !=
          0) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        if ((code = taosHashPut(pNewUser->writeDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN)) != 0) {
          sdbRelease(pSdb, pDb);
          sdbCancelFetch(pSdb, pIter);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_DEL_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      code = taosHashRemove(pNewUser->readDbs, pAlterReq->objname, len);
      if (code < 0) {
        mError("read db:%s, failed to remove db:%s since %s", pNewUser->user, pAlterReq->objname, terrstr());
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(pNewUser->readDbs);
    }
  }

  if (ALTER_USER_DEL_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      code = taosHashRemove(pNewUser->writeDbs, pAlterReq->objname, len);
      if (code < 0) {
        mError("user:%s, failed to remove db:%s since %s", pNewUser->user, pAlterReq->objname, terrstr());
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(pNewUser->writeDbs);
    }
  }

  SHashObj *pReadTbs = pNewUser->readTbs;
  SHashObj *pWriteTbs = pNewUser->writeTbs;
  SHashObj *pAlterTbs = pNewUser->alterTbs;

#ifdef TD_ENTERPRISE
  if (pAlterReq->isView) {
    pReadTbs = pNewUser->readViews;
    pWriteTbs = pNewUser->writeViews;
    pAlterTbs = pNewUser->alterViews;
  }
#endif

  if (ALTER_USER_ADD_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pReadTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pWriteTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pAlterTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pReadTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pWriteTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pAlterTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

#ifdef USE_TOPIC
  if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = NULL;
    if ((code = mndAcquireTopic(pMnode, pAlterReq->objname, &pTopic)) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    if ((code = taosHashPut(pNewUser->topics, pTopic->name, len, pTopic->name, TSDB_TOPIC_FNAME_LEN)) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    mndReleaseTopic(pMnode, pTopic);
  }

  if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = NULL;
    if ((code = mndAcquireTopic(pMnode, pAlterReq->objname, &pTopic)) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    code = taosHashRemove(pNewUser->topics, pAlterReq->objname, len);
    if (code < 0) {
      mError("user:%s, failed to remove topic:%s since %s", pNewUser->user, pAlterReq->objname, tstrerror(code));
    }
    mndReleaseTopic(pMnode, pTopic);
  }
#endif
_OVER:
  if (code < 0) {
    mError("user:%s, failed to alter user privileges at line %d since %s", pAlterReq->user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessAlterUserReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  void         *pIter = NULL;
  int32_t       code = 0;
  int32_t       lino = 0;
  SUserObj     *pUser = NULL;
  SUserObj     *pOperUser = NULL;
  SUserObj      newUser = {0};
  SAlterUserReq alterReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSAlterUserReq(pReq->pCont, pReq->contLen, &alterReq), &lino, _OVER);

  mInfo("user:%s, start to alter", alterReq.user);

  if (alterReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }
  if (alterReq.passIsMd5 == 0) {
    if (TSDB_ALTER_USER_PASSWD == alterReq.alterType) {
      int32_t len = strlen(alterReq.pass);
      if (mndCheckPasswordMinLen(alterReq.pass, len) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY, &lino, _OVER);
      }
      if (mndCheckPasswordMaxLen(alterReq.pass, len) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG, &lino, _OVER);
      }
      if (mndCheckPasswordFmt(alterReq.pass, len) != 0) {
        TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_PASS_FORMAT, &lino, _OVER);
      }
    }
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, alterReq.user, &pUser), &lino, _OVER);

  (void)mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndCheckAlterUserPrivilege(pOperUser, pUser, &alterReq), &lino, _OVER);

  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    if (alterReq.passIsMd5 == 1) {
      (void)memcpy(newUser.pass, alterReq.pass, TSDB_PASSWORD_LEN);
    } else {
      taosEncryptPass_c((uint8_t *)alterReq.pass, strlen(alterReq.pass), newUser.pass);
    }

    TAOS_CHECK_GOTO(mndEncryptPass(newUser.pass, &newUser.passEncryptAlgorithm), &lino, _OVER);

    if (0 != strncmp(pUser->pass, newUser.pass, TSDB_PASSWORD_LEN)) {
      ++newUser.passVersion;
    }
  }

  if (alterReq.alterType == TSDB_ALTER_USER_SUPERUSER) {
    newUser.superUser = alterReq.superUser;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ENABLE) {
    newUser.enable = alterReq.enable;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_SYSINFO) {
    newUser.sysInfo = alterReq.sysInfo;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_CREATEDB) {
    newUser.createdb = alterReq.createdb;
  }

  if (ALTER_USER_ADD_PRIVS(alterReq.alterType) || ALTER_USER_DEL_PRIVS(alterReq.alterType)) {
    TAOS_CHECK_GOTO(mndProcessAlterUserPrivilegesReq(&alterReq, pMnode, &newUser), &lino, _OVER);
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ADD_WHITE_LIST) {
    taosMemoryFreeClear(newUser.pIpWhiteListDual);

    int32_t           num = pUser->pIpWhiteListDual->num + alterReq.numIpRanges;
    int32_t           idx = pUser->pIpWhiteListDual->num;
    SIpWhiteListDual *pNew = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + sizeof(SIpRange) * num);

    if (pNew == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    bool exist = false;
    (void)memcpy(pNew->pIpRanges, pUser->pIpWhiteListDual->pIpRanges, sizeof(SIpRange) * idx);
    for (int i = 0; i < alterReq.numIpRanges; i++) {
      SIpRange range = {0};
      if (alterReq.pIpDualRanges == NULL) {
        range.type = 0;
        memcpy(&range.ipV4, &alterReq.pIpRanges[i], sizeof(SIpV4Range));
      } else {
        memcpy(&range, &alterReq.pIpDualRanges[i], sizeof(SIpRange));
        range = alterReq.pIpDualRanges[i];
      }
      if (!isRangeInIpWhiteList(pUser->pIpWhiteListDual, &range)) {
        // already exist, just ignore;
        (void)memcpy(&pNew->pIpRanges[idx], &range, sizeof(SIpRange));
        idx++;
        continue;
      } else {
        exist = true;
      }
    }
    if (exist) {
      taosMemoryFree(pNew);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_HOST_EXIST, &lino, _OVER);
    }
    pNew->num = idx;
    newUser.pIpWhiteListDual = pNew;
    newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;

    if (pNew->num > MND_MAX_USE_HOST) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_USER_HOST, &lino, _OVER);
    }
  }
  if (alterReq.alterType == TSDB_ALTER_USER_DROP_WHITE_LIST) {
    taosMemoryFreeClear(newUser.pIpWhiteListDual);

    int32_t           num = pUser->pIpWhiteListDual->num;
    bool              noexist = true;
    bool              localHost = false;
    SIpWhiteListDual *pNew = taosMemoryCalloc(1, sizeof(SIpWhiteListDual) + sizeof(SIpRange) * num);

    if (pNew == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    if (pUser->pIpWhiteListDual->num > 0) {
      int idx = 0;
      for (int i = 0; i < pUser->pIpWhiteListDual->num; i++) {
        SIpRange *oldRange = &pUser->pIpWhiteListDual->pIpRanges[i];

        bool found = false;
        for (int j = 0; j < alterReq.numIpRanges; j++) {
          SIpRange range = {0};
          if (alterReq.pIpDualRanges == NULL) {
            SIpV4Range *trange = &alterReq.pIpRanges[j];
            memcpy(&range.ipV4, trange, sizeof(SIpV4Range));
          } else {
            memcpy(&range, &alterReq.pIpDualRanges[j], sizeof(SIpRange));
          }

          if (isDefaultRange(&range)) {
            localHost = true;
            break;
          }
          if (isIpRangeEqual(oldRange, &range)) {
            found = true;
            break;
          }
        }
        if (localHost) break;

        if (found == false) {
          (void)memcpy(&pNew->pIpRanges[idx], oldRange, sizeof(SIpRange));
          idx++;
        } else {
          noexist = false;
        }
      }
      pNew->num = idx;
      newUser.pIpWhiteListDual = pNew;
      newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;

    } else {
      pNew->num = 0;
      newUser.pIpWhiteListDual = pNew;
      newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;
    }

    if (localHost) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_LOCAL_HOST_NOT_DROP, &lino, _OVER);
    }
    if (noexist) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_HOST_NOT_EXIST, &lino, _OVER);
    }
  }

  code = mndAlterUser(pMnode, pUser, &newUser, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    char detail[1000] = {0};
    (void)tsnprintf(detail, sizeof(detail),
                    "alterType:%s, enable:%d, superUser:%d, sysInfo:%d, createdb:%d, tabName:%s, password:xxx",
                    mndUserAuditTypeStr(alterReq.alterType), alterReq.enable, alterReq.superUser, alterReq.sysInfo,
                    alterReq.createdb ? 1 : 0, alterReq.tabName);
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", alterReq.user, detail, strlen(detail));
  } else if (alterReq.alterType == TSDB_ALTER_USER_SUPERUSER || alterReq.alterType == TSDB_ALTER_USER_ENABLE ||
             alterReq.alterType == TSDB_ALTER_USER_SYSINFO || alterReq.alterType == TSDB_ALTER_USER_CREATEDB) {
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
  } else if (ALTER_USER_ADD_READ_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_WRITE_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_ALL_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_READ_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_WRITE_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_ALL_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)) {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, alterReq.objname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", name.dbname, alterReq.user, alterReq.sql,
                  alterReq.sqlLen);
    } else {
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
    }
  } else if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(alterReq.alterType, alterReq.privileges)) {
    auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", alterReq.objname, alterReq.user, alterReq.sql,
                alterReq.sqlLen);
  } else if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(alterReq.alterType, alterReq.privileges)) {
    auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", alterReq.objname, alterReq.user, alterReq.sql,
                alterReq.sqlLen);
  } else {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      SName name = {0};
      TAOS_CHECK_GOTO(tNameFromString(&name, alterReq.objname, T_NAME_ACCT | T_NAME_DB), &lino, _OVER);
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", name.dbname, alterReq.user, alterReq.sql,
                  alterReq.sqlLen);
    } else {
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
    }
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter at line %d since %s", alterReq.user, lino, tstrerror(code));
  }

  tFreeSAlterUserReq(&alterReq);
  mndReleaseUser(pMnode, pOperUser);
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);

  TAOS_RETURN(code);
}

static int32_t mndDropUser(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to drop since %s", pUser->user, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to drop user:%s", pTrans->id, pUser->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) < 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  (void)ipWhiteMgtRemove(pUser->user);

  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

static int32_t mndProcessDropUserReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = 0;
  int32_t      lino = 0;
  SUserObj    *pUser = NULL;
  SDropUserReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSDropUserReq(pReq->pCont, pReq->contLen, &dropReq), &lino, _OVER);

  mInfo("user:%s, start to drop", dropReq.user);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_USER), &lino, _OVER);

  if (dropReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, dropReq.user, &pUser), &lino, _OVER);

  TAOS_CHECK_GOTO(mndDropUser(pMnode, pReq, pUser), &lino, _OVER);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "dropUser", "", dropReq.user, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop at line %d since %s", dropReq.user, lino, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  tFreeSDropUserReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessGetUserAuthReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = 0;
  int32_t         lino = 0;
  int32_t         contLen = 0;
  void           *pRsp = NULL;
  SUserObj       *pUser = NULL;
  SGetUserAuthReq authReq = {0};
  SGetUserAuthRsp authRsp = {0};

  TAOS_CHECK_EXIT(tDeserializeSGetUserAuthReq(pReq->pCont, pReq->contLen, &authReq));
  mTrace("user:%s, start to get auth", authReq.user);

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, authReq.user, &pUser));

  TAOS_CHECK_EXIT(mndSetUserAuthRsp(pMnode, pUser, &authRsp));

  contLen = tSerializeSGetUserAuthRsp(NULL, 0, &authRsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  contLen = tSerializeSGetUserAuthRsp(pRsp, contLen, &authRsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }

_exit:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserAuthRsp(&authRsp);
  if (code < 0) {
    mError("user:%s, failed to get auth at line %d since %s", authReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  pReq->code = code;

  TAOS_RETURN(code);
}

static int32_t mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->superUser, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, pShow->pIter, _exit);

    cols++;
    flag = pUser->createdb ? 1 : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->createdTime, false, pUser, pShow->pIter, _exit);

    cols++;

    int32_t tlen = convertIpWhiteListToStr(pUser->pIpWhiteListDual, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, pShow->pIter, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, pShow->pIter, _exit);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static int32_t mndRetrieveUsersFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
#ifdef TD_ENTERPRISE
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = NULL;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->superUser, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, pShow->pIter, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, pShow->pIter, _exit);

    cols++;
    flag = pUser->createdb ? 1 : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, pShow->pIter, _exit);

    // mInfo("pUser->pass:%s", pUser->pass);
    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char pass[TSDB_PASSWORD_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(pass, pUser->pass, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)pass, false, pUser, pShow->pIter, _exit);

    cols++;

    int32_t tlen = convertIpWhiteListToStr(pUser->pIpWhiteListDual, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, pShow->pIter, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, pShow->pIter, _exit);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
#endif
  return numOfRows;
}

static void mndCancelGetNextUser(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_USER);
}

static int32_t mndLoopHash(SHashObj *hash, char *priType, SSDataBlock *pBlock, int32_t *pNumOfRows, SSdb *pSdb,
                           SUserObj *pUser, SShowObj *pShow, char **condition, char **sql) {
  char   *value = taosHashIterate(hash, NULL);
  char   *user = pUser->user;
  int32_t code = 0;
  int32_t lino = 0;
  int32_t cols = 0;
  int32_t numOfRows = *pNumOfRows;

  while (value != NULL) {
    cols = 0;
    char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(userName, user, pShow->pMeta->pSchemas[cols].bytes);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)userName, false, NULL, NULL, _exit);

    char privilege[20] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(privilege, priType, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)privilege, false, NULL, NULL, _exit);

    size_t keyLen = 0;
    void  *key = taosHashGetKey(value, &keyLen);

    char dbName[TSDB_DB_NAME_LEN] = {0};
    (void)mndExtractShortDbNameFromStbFullName(key, dbName);
    char dbNameContent[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbNameContent, dbName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)dbNameContent, false, NULL, NULL, _exit);

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    mndExtractTbNameFromStbFullName(key, tableName, TSDB_TABLE_NAME_LEN);
    char tableNameContent[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(tableNameContent, tableName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)tableNameContent, false, NULL, NULL, _exit);

    if (strcmp("t", value) != 0 && strcmp("v", value) != 0) {
      SNode  *pAst = NULL;
      int32_t sqlLen = 0;
      size_t  bufSz = strlen(value) + 1;
      if (bufSz < 6) bufSz = 6;
      TAOS_MEMORY_REALLOC(*sql, bufSz);
      if (*sql == NULL) {
        code = terrno;
        goto _exit;
      }
      TAOS_MEMORY_REALLOC(*condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if ((*condition) == NULL) {
        code = terrno;
        goto _exit;
      }

      if (nodesStringToNode(value, &pAst) == 0) {
        if (nodesNodeToSQLFormat(pAst, *sql, bufSz, &sqlLen, true) != 0) {
          sqlLen = tsnprintf(*sql, bufSz, "error");
        }
        nodesDestroyNode(pAst);
      }

      if (sqlLen == 0) {
        sqlLen = tsnprintf(*sql, bufSz, "error");
      }

      STR_WITH_MAXSIZE_TO_VARSTR((*condition), (*sql), pShow->pMeta->pSchemas[cols].bytes);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)(*condition), false, NULL, NULL, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, NULL, NULL, _exit);
    } else {
      TAOS_MEMORY_REALLOC(*condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if ((*condition) == NULL) {
        code = terrno;
        goto _exit;
      }
      STR_WITH_MAXSIZE_TO_VARSTR((*condition), "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)(*condition), false, NULL, NULL, _exit);

      char notes[64 + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, value[0] == 'v' ? "view" : "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, NULL, NULL, _exit);
    }

    numOfRows++;
    value = taosHashIterate(hash, value);
  }
  *pNumOfRows = numOfRows;
_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    sdbRelease(pSdb, pUser);
    sdbCancelFetch(pSdb, pShow->pIter);
  }
  TAOS_RETURN(code);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite = NULL;
  char     *condition = NULL;
  char     *sql = NULL;

  bool fetchNextUser = pShow->restore ? false : true;
  pShow->restore = false;

  while (numOfRows < rows) {
    if (fetchNextUser) {
      pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
      if (pShow->pIter == NULL) break;
    } else {
      fetchNextUser = true;
      void *pKey = taosHashGetKey(pShow->pIter, NULL);
      pUser = sdbAcquire(pSdb, SDB_USER, pKey);
      if (!pUser) {
        continue;
      }
    }

    int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
    int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
    int32_t numOfTopics = taosHashGetSize(pUser->topics);
    int32_t numOfReadTbs = taosHashGetSize(pUser->readTbs);
    int32_t numOfWriteTbs = taosHashGetSize(pUser->writeTbs);
    int32_t numOfAlterTbs = taosHashGetSize(pUser->alterTbs);
    int32_t numOfReadViews = taosHashGetSize(pUser->readViews);
    int32_t numOfWriteViews = taosHashGetSize(pUser->writeViews);
    int32_t numOfAlterViews = taosHashGetSize(pUser->alterViews);
    if (numOfRows + numOfReadDbs + numOfWriteDbs + numOfTopics + numOfReadTbs + numOfWriteTbs + numOfAlterTbs +
            numOfReadViews + numOfWriteViews + numOfAlterViews >=
        rows) {
      mInfo(
          "will restore. current num of rows: %d, read dbs %d, write dbs %d, topics %d, read tables %d, write tables "
          "%d, alter tables %d, read views %d, write views %d, alter views %d",
          numOfRows, numOfReadDbs, numOfWriteDbs, numOfTopics, numOfReadTbs, numOfWriteTbs, numOfAlterTbs,
          numOfReadViews, numOfWriteViews, numOfAlterViews);
      pShow->restore = true;
      sdbRelease(pSdb, pUser);
      break;
    }

    if (pUser->superUser) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      char objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(objName, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
    }

    char *db = taosHashIterate(pUser->readDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "read", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      code = tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      if (code < 0) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      (void)tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
      db = taosHashIterate(pUser->readDbs, db);
    }

    db = taosHashIterate(pUser->writeDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "write", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      code = tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      if (code < 0) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(code, &lino, _exit);
      }
      (void)tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        sdbCancelFetch(pSdb, pShow->pIter);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
      db = taosHashIterate(pUser->writeDbs, db);
    }

    TAOS_CHECK_EXIT(mndLoopHash(pUser->readTbs, "read", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->writeTbs, "write", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->alterTbs, "alter", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->readViews, "read", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->writeViews, "write", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->alterViews, "alter", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    char *topic = taosHashIterate(pUser->topics, NULL);
    while (topic != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, pShow->pIter, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "subscribe", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, pShow->pIter, _exit);

      char topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE + 5] = {0};
      tstrncpy(varDataVal(topicName), mndGetDbStr(topic), TSDB_TOPIC_NAME_LEN - 2);
      varDataSetLen(topicName, strlen(varDataVal(topicName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)topicName, false, pUser, pShow->pIter, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, pShow->pIter, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, pShow->pIter, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, pShow->pIter, _exit);

      numOfRows++;
      topic = taosHashIterate(pUser->topics, topic);
    }

    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(condition);
  taosMemoryFreeClear(sql);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_USER);
}

int32_t mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                int32_t *pRspLen, int64_t ipWhiteListVer) {
  int32_t           code = 0;
  int32_t           lino = 0;
  int32_t           rspLen = 0;
  void             *pRsp = NULL;
  SUserAuthBatchRsp batchRsp = {0};

  batchRsp.pArray = taosArrayInit(numOfUses, sizeof(SGetUserAuthRsp));
  if (batchRsp.pArray == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  for (int32_t i = 0; i < numOfUses; ++i) {
    SUserObj *pUser = NULL;
    code = mndAcquireUser(pMnode, pUsers[i].user, &pUser);
    if (pUser == NULL) {
      if (TSDB_CODE_MND_USER_NOT_EXIST == code) {
        SGetUserAuthRsp rsp = {.dropped = 1};
        (void)memcpy(rsp.user, pUsers[i].user, TSDB_USER_LEN);
        TSDB_CHECK_NULL(taosArrayPush(batchRsp.pArray, &rsp), code, lino, _OVER, TSDB_CODE_OUT_OF_MEMORY);
      }
      mError("user:%s, failed to auth user since %s", pUsers[i].user, tstrerror(code));
      code = 0;
      continue;
    }

    pUsers[i].version = ntohl(pUsers[i].version);
    if (pUser->authVersion <= pUsers[i].version && ipWhiteListVer == pMnode->ipWhiteVer) {
      mndReleaseUser(pMnode, pUser);
      continue;
    }

    SGetUserAuthRsp rsp = {0};
    code = mndSetUserAuthRsp(pMnode, pUser, &rsp);
    if (code) {
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }

    if (!(taosArrayPush(batchRsp.pArray, &rsp))) {
      code = terrno;
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    mndReleaseUser(pMnode, pUser);
  }

  if (taosArrayGetSize(batchRsp.pArray) <= 0) {
    *ppRsp = NULL;
    *pRspLen = 0;

    tFreeSUserAuthBatchRsp(&batchRsp);
    return 0;
  }

  rspLen = tSerializeSUserAuthBatchRsp(NULL, 0, &batchRsp);
  if (rspLen < 0) {
    TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  }
  pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  rspLen = tSerializeSUserAuthBatchRsp(pRsp, rspLen, &batchRsp);
  if (rspLen < 0) {
    TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  }
_OVER:
  tFreeSUserAuthBatchRsp(&batchRsp);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    taosMemoryFreeClear(pRsp);
    rspLen = 0;
  }
  *ppRsp = pRsp;
  *pRspLen = rspLen;

  TAOS_RETURN(code);
}

static int32_t mndRemoveDbPrivileges(SHashObj *pHash, const char *dbFName, int32_t dbFNameLen, int32_t *nRemoved) {
  void *pVal = NULL;
  while ((pVal = taosHashIterate(pHash, pVal))) {
    size_t keyLen = 0;
    char  *pKey = (char *)taosHashGetKey(pVal, &keyLen);
    if (pKey == NULL || keyLen <= dbFNameLen) continue;
    if ((*(pKey + dbFNameLen) == '.') && strncmp(pKey, dbFName, dbFNameLen) == 0) {
      TAOS_CHECK_RETURN(taosHashRemove(pHash, pKey, keyLen));
      if (nRemoved) ++(*nRemoved);
    }
  }
  TAOS_RETURN(0);
}

int32_t mndUserRemoveDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SSHashObj **ppUsers) {
  int32_t    code = 0, lino = 0;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    dbLen = strlen(pDb->name);
  void      *pIter = NULL;
  SUserObj  *pUser = NULL;
  SUserObj   newUser = {0};
  SSHashObj *pUsers = ppUsers ? *ppUsers : NULL;
  bool       output = (ppUsers != NULL);

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    bool update = false;
    bool inReadDb = (taosHashGet(pUser->readDbs, pDb->name, dbLen + 1) != NULL);
    bool inWriteDb = (taosHashGet(pUser->writeDbs, pDb->name, dbLen + 1) != NULL);
    bool inUseDb = (taosHashGet(pUser->useDbs, pDb->name, dbLen + 1) != NULL);
    bool inReadTbs = taosHashGetSize(pUser->readTbs) > 0;
    bool inWriteTbs = taosHashGetSize(pUser->writeTbs) > 0;
    bool inAlterTbs = taosHashGetSize(pUser->alterTbs) > 0;
    bool inReadViews = taosHashGetSize(pUser->readViews) > 0;
    bool inWriteViews = taosHashGetSize(pUser->writeViews) > 0;
    bool inAlterViews = taosHashGetSize(pUser->alterViews) > 0;
    // no need remove pUser->topics since topics must be dropped ahead of db
    if (!inReadDb && !inWriteDb && !inReadTbs && !inWriteTbs && !inAlterTbs && !inReadViews && !inWriteViews &&
        !inAlterViews) {
      sdbRelease(pSdb, pUser);
      continue;
    }
    SUserObj *pTargetUser = &newUser;
    if (output) {
      if (!pUsers) {
        TSDB_CHECK_NULL(pUsers = tSimpleHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY)), code, lino,
                        _exit, TSDB_CODE_OUT_OF_MEMORY);
        *ppUsers = pUsers;
      }
      void   *pVal = NULL;
      int32_t userLen = strlen(pUser->user) + 1;
      if ((pVal = tSimpleHashGet(pUsers, pUser->user, userLen)) != NULL) {
        pTargetUser = (SUserObj *)pVal;
      } else {
        TAOS_CHECK_EXIT(mndUserDupObj(pUser, &newUser));
        TAOS_CHECK_EXIT(tSimpleHashPut(pUsers, pUser->user, userLen, &newUser, sizeof(SUserObj)));
        TSDB_CHECK_NULL((pVal = tSimpleHashGet(pUsers, pUser->user, userLen)), code, lino, _exit,
                        TSDB_CODE_OUT_OF_MEMORY);
        pTargetUser = (SUserObj *)pVal;
      }
    } else {
      TAOS_CHECK_EXIT(mndUserDupObj(pUser, &newUser));
    }
    if (inReadDb) {
      TAOS_CHECK_EXIT(taosHashRemove(pTargetUser->readDbs, pDb->name, dbLen + 1));
    }
    if (inWriteDb) {
      TAOS_CHECK_EXIT(taosHashRemove(pTargetUser->writeDbs, pDb->name, dbLen + 1));
    }
    if (inUseDb) {
      TAOS_CHECK_EXIT(taosHashRemove(pTargetUser->useDbs, pDb->name, dbLen + 1));
    }
    update = inReadDb || inWriteDb || inUseDb;

    int32_t nRemovedReadTbs = 0;
    int32_t nRemovedWriteTbs = 0;
    int32_t nRemovedAlterTbs = 0;
    if (inReadTbs || inWriteTbs || inAlterTbs) {
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->readTbs, pDb->name, dbLen, &nRemovedReadTbs));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->writeTbs, pDb->name, dbLen, &nRemovedWriteTbs));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->alterTbs, pDb->name, dbLen, &nRemovedAlterTbs));
      if (!update) update = nRemovedReadTbs > 0 || nRemovedWriteTbs > 0 || nRemovedAlterTbs > 0;
    }

    int32_t nRemovedReadViews = 0;
    int32_t nRemovedWriteViews = 0;
    int32_t nRemovedAlterViews = 0;
    if (inReadViews || inWriteViews || inAlterViews) {
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->readViews, pDb->name, dbLen, &nRemovedReadViews));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->writeViews, pDb->name, dbLen, &nRemovedWriteViews));
      TAOS_CHECK_EXIT(mndRemoveDbPrivileges(pTargetUser->alterViews, pDb->name, dbLen, &nRemovedAlterViews));
      if (!update) update = nRemovedReadViews > 0 || nRemovedWriteViews > 0 || nRemovedAlterViews > 0;
    }

    if (!output) {
      if (update) {
        SSdbRaw *pCommitRaw = mndUserActionEncode(pTargetUser);
        if (pCommitRaw == NULL) {
          TAOS_CHECK_EXIT(terrno);
        }
        TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pCommitRaw));
        TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
      }
      mndUserFreeObj(&newUser);
    }
    sdbRelease(pSdb, pUser);
  }

_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    mndUserFreeObj(&newUser);
  }
  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  if (!output) mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int32_t mndUserRemoveStb(SMnode *pMnode, STrans *pTrans, char *stb) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(stb) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readTbs, stb, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeTbs, stb, len) != NULL);
    bool inAlter = (taosHashGet(newUser.alterTbs, stb, len) != NULL);
    if (inRead || inWrite || inAlter) {
      code = taosHashRemove(newUser.readTbs, stb, len);
      if (code < 0) {
        mError("failed to remove readTbs:%s from user:%s", stb, pUser->user);
      }
      code = taosHashRemove(newUser.writeTbs, stb, len);
      if (code < 0) {
        mError("failed to remove writeTbs:%s from user:%s", stb, pUser->user);
      }
      code = taosHashRemove(newUser.alterTbs, stb, len);
      if (code < 0) {
        mError("failed to remove alterTbs:%s from user:%s", stb, pUser->user);
      }

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
      if (code != 0) {
        mndUserFreeObj(&newUser);
        sdbRelease(pSdb, pUser);
        TAOS_RETURN(code);
      }
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int32_t mndUserRemoveView(SMnode *pMnode, STrans *pTrans, char *view) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(view) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readViews, view, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeViews, view, len) != NULL);
    bool inAlter = (taosHashGet(newUser.alterViews, view, len) != NULL);
    if (inRead || inWrite || inAlter) {
      code = taosHashRemove(newUser.readViews, view, len);
      if (code < 0) {
        mError("failed to remove readViews:%s from user:%s", view, pUser->user);
      }
      code = taosHashRemove(newUser.writeViews, view, len);
      if (code < 0) {
        mError("failed to remove writeViews:%s from user:%s", view, pUser->user);
      }
      code = taosHashRemove(newUser.alterViews, view, len);
      if (code < 0) {
        mError("failed to remove alterViews:%s from user:%s", view, pUser->user);
      }

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
      if (code < 0) {
        mndUserFreeObj(&newUser);
        sdbRelease(pSdb, pUser);
        TAOS_RETURN(code);
      }
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int32_t mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(topic) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) {
      break;
    }

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inTopic = (taosHashGet(newUser.topics, topic, len) != NULL);
    if (inTopic) {
      code = taosHashRemove(newUser.topics, topic, len);
      if (code < 0) {
        mError("failed to remove topic:%s from user:%s", topic, pUser->user);
      }
      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
      if (code < 0) {
        mndUserFreeObj(&newUser);
        sdbRelease(pSdb, pUser);
        TAOS_RETURN(code);
      }
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int64_t mndGetUserIpWhiteListVer(SMnode *pMnode, SUserObj *pUser) {
  // ver = 0, disable ip white list
  // ver > 0, enable ip white list
  return tsEnableWhiteList ? pUser->ipWhiteListVer : 0;
}
