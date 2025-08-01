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

#ifndef _TD_COMMON_TAOS_MSG_H_
#define _TD_COMMON_TAOS_MSG_H_

#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcoding.h"
#include "tcol.h"
#include "tencode.h"
#include "thash.h"
#include "tlist.h"
#include "tname.h"
#include "trow.h"
#include "tuuid.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ MESSAGE DEFINITIONS ------------------------ */

#define TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#undef TD_MSG_TYPE_INFO_
#undef TD_MSG_RANGE_CODE_
#undef TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#undef TD_MSG_TYPE_INFO_
#undef TD_MSG_RANGE_CODE_
#define TD_MSG_SEG_CODE_
#include "tmsgdef.h"

#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_INFO_
#undef TD_MSG_TYPE_INFO_
#undef TD_MSG_SEG_CODE_
#undef TD_MSG_RANGE_CODE_
#include "tmsgdef.h"

extern char*   tMsgInfo[];
extern int32_t tMsgDict[];
extern int32_t tMsgRangeDict[];

typedef uint16_t tmsg_t;

#define TMSG_SEG_CODE(TYPE) (((TYPE)&0xff00) >> 8)
#define TMSG_SEG_SEQ(TYPE)  ((TYPE)&0xff)
#define TMSG_INDEX(TYPE)    (tMsgDict[TMSG_SEG_CODE(TYPE)] + TMSG_SEG_SEQ(TYPE))


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


static inline bool tmsgIsValid(tmsg_t type) {
  // static int8_t sz = sizeof(tMsgRangeDict) / sizeof(tMsgRangeDict[0]);
  int8_t maxSegIdx = TMSG_SEG_CODE(TDMT_MAX_MSG_MIN);
  int    segIdx = TMSG_SEG_CODE(type);
  if (segIdx >= 0 && segIdx < maxSegIdx) {
    return type < tMsgRangeDict[segIdx];
  }
  return false;
}

#define TMSG_INFO(type) (tmsgIsValid(type) ? tMsgInfo[TMSG_INDEX(type)] : "unKnown")

static inline bool vnodeIsMsgBlock(tmsg_t type) {
  return (type == TDMT_VND_CREATE_TABLE) || (type == TDMT_VND_ALTER_TABLE) || (type == TDMT_VND_DROP_TABLE) ||
         (type == TDMT_VND_UPDATE_TAG_VAL) || (type == TDMT_VND_ALTER_CONFIRM) || (type == TDMT_VND_COMMIT) ||
         (type == TDMT_SYNC_CONFIG_CHANGE);
}

static inline bool syncUtilUserCommit(tmsg_t msgType) {
  return msgType != TDMT_SYNC_NOOP && msgType != TDMT_SYNC_LEADER_TRANSFER;
}

/* ------------------------ OTHER DEFINITIONS ------------------------ */
// IE type
#define TSDB_IE_TYPE_SEC         1
#define TSDB_IE_TYPE_META        2
#define TSDB_IE_TYPE_MGMT_IP     3
#define TSDB_IE_TYPE_DNODE_CFG   4
#define TSDB_IE_TYPE_NEW_VERSION 5
#define TSDB_IE_TYPE_DNODE_EXT   6
#define TSDB_IE_TYPE_DNODE_STATE 7

enum {
  CONN_TYPE__QUERY = 1,
  CONN_TYPE__TMQ,
  CONN_TYPE__UDFD,
  CONN_TYPE__MAX,
};

enum {
  HEARTBEAT_KEY_USER_AUTHINFO = 1,
  HEARTBEAT_KEY_DBINFO,
  HEARTBEAT_KEY_STBINFO,
  HEARTBEAT_KEY_TMQ,
  HEARTBEAT_KEY_DYN_VIEW,
  HEARTBEAT_KEY_VIEWINFO,
  HEARTBEAT_KEY_TSMA,
};

typedef enum _mgmt_table {
  TSDB_MGMT_TABLE_START,
  TSDB_MGMT_TABLE_DNODE,
  TSDB_MGMT_TABLE_MNODE,
  TSDB_MGMT_TABLE_MODULE,
  TSDB_MGMT_TABLE_QNODE,
  TSDB_MGMT_TABLE_SNODE,
  TSDB_MGMT_TABLE_BACKUP_NODE,  // no longer used
  TSDB_MGMT_TABLE_CLUSTER,
  TSDB_MGMT_TABLE_DB,
  TSDB_MGMT_TABLE_FUNC,
  TSDB_MGMT_TABLE_INDEX,
  TSDB_MGMT_TABLE_STB,
  TSDB_MGMT_TABLE_STREAMS,
  TSDB_MGMT_TABLE_TABLE,
  TSDB_MGMT_TABLE_TAG,
  TSDB_MGMT_TABLE_COL,
  TSDB_MGMT_TABLE_USER,
  TSDB_MGMT_TABLE_GRANTS,
  TSDB_MGMT_TABLE_VGROUP,
  TSDB_MGMT_TABLE_TOPICS,
  TSDB_MGMT_TABLE_CONSUMERS,
  TSDB_MGMT_TABLE_SUBSCRIPTIONS,
  TSDB_MGMT_TABLE_TRANS,
  TSDB_MGMT_TABLE_SMAS,
  TSDB_MGMT_TABLE_CONFIGS,
  TSDB_MGMT_TABLE_CONNS,
  TSDB_MGMT_TABLE_QUERIES,
  TSDB_MGMT_TABLE_VNODES,
  TSDB_MGMT_TABLE_APPS,
  TSDB_MGMT_TABLE_STREAM_TASKS,
  TSDB_MGMT_TABLE_STREAM_RECALCULATES,
  TSDB_MGMT_TABLE_PRIVILEGES,
  TSDB_MGMT_TABLE_VIEWS,
  TSDB_MGMT_TABLE_TSMAS,
  TSDB_MGMT_TABLE_COMPACT,
  TSDB_MGMT_TABLE_COMPACT_DETAIL,
  TSDB_MGMT_TABLE_GRANTS_FULL,
  TSDB_MGMT_TABLE_GRANTS_LOGS,
  TSDB_MGMT_TABLE_MACHINES,
  TSDB_MGMT_TABLE_ARBGROUP,
  TSDB_MGMT_TABLE_ENCRYPTIONS,
  TSDB_MGMT_TABLE_USER_FULL,
  TSDB_MGMT_TABLE_ANODE,
  TSDB_MGMT_TABLE_ANODE_FULL,
  TSDB_MGMT_TABLE_USAGE,
  TSDB_MGMT_TABLE_FILESETS,
  TSDB_MGMT_TABLE_TRANSACTION_DETAIL,
  TSDB_MGMT_TABLE_VC_COL,
  TSDB_MGMT_TABLE_BNODE,
  TSDB_MGMT_TABLE_MOUNT,
  TSDB_MGMT_TABLE_SSMIGRATE,
  TSDB_MGMT_TABLE_MAX,
} EShowType;

#define TSDB_ALTER_TABLE_ADD_TAG                         1
#define TSDB_ALTER_TABLE_DROP_TAG                        2
#define TSDB_ALTER_TABLE_UPDATE_TAG_NAME                 3
#define TSDB_ALTER_TABLE_UPDATE_TAG_VAL                  4
#define TSDB_ALTER_TABLE_ADD_COLUMN                      5
#define TSDB_ALTER_TABLE_DROP_COLUMN                     6
#define TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES             7
#define TSDB_ALTER_TABLE_UPDATE_TAG_BYTES                8
#define TSDB_ALTER_TABLE_UPDATE_OPTIONS                  9
#define TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME              10
#define TSDB_ALTER_TABLE_ADD_TAG_INDEX                   11
#define TSDB_ALTER_TABLE_DROP_TAG_INDEX                  12
#define TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS          13
#define TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COMPRESS_OPTION 14
#define TSDB_ALTER_TABLE_UPDATE_MULTI_TAG_VAL            15
#define TSDB_ALTER_TABLE_ALTER_COLUMN_REF                16
#define TSDB_ALTER_TABLE_REMOVE_COLUMN_REF               17
#define TSDB_ALTER_TABLE_ADD_COLUMN_WITH_COLUMN_REF      18

#define TSDB_FILL_NONE        0
#define TSDB_FILL_NULL        1
#define TSDB_FILL_NULL_F      2
#define TSDB_FILL_SET_VALUE   3
#define TSDB_FILL_SET_VALUE_F 4
#define TSDB_FILL_LINEAR      5
#define TSDB_FILL_PREV        6
#define TSDB_FILL_NEXT        7
#define TSDB_FILL_NEAR        8

#define TSDB_ALTER_USER_PASSWD          0x1
#define TSDB_ALTER_USER_SUPERUSER       0x2
#define TSDB_ALTER_USER_ENABLE          0x3
#define TSDB_ALTER_USER_SYSINFO         0x4
#define TSDB_ALTER_USER_ADD_PRIVILEGES  0x5
#define TSDB_ALTER_USER_DEL_PRIVILEGES  0x6
#define TSDB_ALTER_USER_ADD_WHITE_LIST  0x7
#define TSDB_ALTER_USER_DROP_WHITE_LIST 0x8
#define TSDB_ALTER_USER_CREATEDB        0x9

#define TSDB_KILL_MSG_LEN 30

#define TSDB_TABLE_NUM_UNIT 100000

#define TSDB_VN_READ_ACCCESS  ((char)0x1)
#define TSDB_VN_WRITE_ACCCESS ((char)0x2)
#define TSDB_VN_ALL_ACCCESS   (TSDB_VN_READ_ACCCESS | TSDB_VN_WRITE_ACCCESS)

#define TSDB_COL_NORMAL 0x0u  // the normal column of the table
#define TSDB_COL_TAG    0x1u  // the tag column type
#define TSDB_COL_UDC    0x2u  // the user specified normal string column, it is a dummy column
#define TSDB_COL_TMP    0x4u  // internal column generated by the previous operators
#define TSDB_COL_NULL   0x8u  // the column filter NULL or not

#define TSDB_COL_IS_TAG(f)        (((f & (~(TSDB_COL_NULL))) & TSDB_COL_TAG) != 0)
#define TSDB_COL_IS_NORMAL_COL(f) ((f & (~(TSDB_COL_NULL))) == TSDB_COL_NORMAL)
#define TSDB_COL_IS_UD_COL(f)     ((f & (~(TSDB_COL_NULL))) == TSDB_COL_UDC)
#define TSDB_COL_REQ_NULL(f)      (((f)&TSDB_COL_NULL) != 0)

#define TD_SUPER_TABLE          TSDB_SUPER_TABLE
#define TD_CHILD_TABLE          TSDB_CHILD_TABLE
#define TD_NORMAL_TABLE         TSDB_NORMAL_TABLE
#define TD_VIRTUAL_NORMAL_TABLE TSDB_VIRTUAL_NORMAL_TABLE
#define TD_VIRTUAL_CHILD_TABLE  TSDB_VIRTUAL_CHILD_TABLE

typedef enum ENodeType {
  // Syntax nodes are used in parser and planner module, and some are also used in executor module, such as COLUMN,
  // VALUE, OPERATOR, FUNCTION and so on.
  QUERY_NODE_COLUMN = 1,
  QUERY_NODE_VALUE,
  QUERY_NODE_OPERATOR,
  QUERY_NODE_LOGIC_CONDITION,
  QUERY_NODE_FUNCTION,
  QUERY_NODE_REAL_TABLE,
  QUERY_NODE_TEMP_TABLE,
  QUERY_NODE_JOIN_TABLE,
  QUERY_NODE_GROUPING_SET,
  QUERY_NODE_ORDER_BY_EXPR,
  QUERY_NODE_LIMIT,
  QUERY_NODE_STATE_WINDOW,
  QUERY_NODE_SESSION_WINDOW,
  QUERY_NODE_INTERVAL_WINDOW,
  QUERY_NODE_NODE_LIST,
  QUERY_NODE_FILL,
  QUERY_NODE_RAW_EXPR,  // Only be used in parser module.
  QUERY_NODE_TARGET,
  QUERY_NODE_DATABLOCK_DESC,
  QUERY_NODE_SLOT_DESC,
  QUERY_NODE_COLUMN_DEF,
  QUERY_NODE_DOWNSTREAM_SOURCE,
  QUERY_NODE_DATABASE_OPTIONS,
  QUERY_NODE_TABLE_OPTIONS,
  QUERY_NODE_INDEX_OPTIONS,
  QUERY_NODE_EXPLAIN_OPTIONS,
  QUERY_NODE_LEFT_VALUE,
  QUERY_NODE_COLUMN_REF,
  QUERY_NODE_WHEN_THEN,
  QUERY_NODE_CASE_WHEN,
  QUERY_NODE_EVENT_WINDOW,
  QUERY_NODE_HINT,
  QUERY_NODE_VIEW,
  QUERY_NODE_WINDOW_OFFSET,
  QUERY_NODE_COUNT_WINDOW,
  QUERY_NODE_COLUMN_OPTIONS,
  QUERY_NODE_TSMA_OPTIONS,
  QUERY_NODE_ANOMALY_WINDOW,
  QUERY_NODE_RANGE_AROUND,
  QUERY_NODE_STREAM_NOTIFY_OPTIONS,
  QUERY_NODE_VIRTUAL_TABLE,
  QUERY_NODE_SLIDING_WINDOW,
  QUERY_NODE_PERIOD_WINDOW,
  QUERY_NODE_STREAM_TRIGGER,
  QUERY_NODE_STREAM,
  QUERY_NODE_STREAM_TAG_DEF,
  QUERY_NODE_EXTERNAL_WINDOW,
  QUERY_NODE_STREAM_TRIGGER_OPTIONS,
  QUERY_NODE_PLACE_HOLDER_TABLE,
  QUERY_NODE_TIME_RANGE,
  QUERY_NODE_STREAM_OUT_TABLE,
  QUERY_NODE_STREAM_CALC_RANGE,
  QUERY_NODE_COUNT_WINDOW_ARGS,
  QUERY_NODE_BNODE_OPTIONS,

  // Statement nodes are used in parser and planner module.
  QUERY_NODE_SET_OPERATOR = 100,
  QUERY_NODE_SELECT_STMT,
  QUERY_NODE_VNODE_MODIFY_STMT,
  QUERY_NODE_CREATE_DATABASE_STMT,
  QUERY_NODE_DROP_DATABASE_STMT,
  QUERY_NODE_ALTER_DATABASE_STMT,
  QUERY_NODE_FLUSH_DATABASE_STMT,
  QUERY_NODE_TRIM_DATABASE_STMT,
  QUERY_NODE_CREATE_TABLE_STMT,
  QUERY_NODE_CREATE_SUBTABLE_CLAUSE,
  QUERY_NODE_CREATE_MULTI_TABLES_STMT,
  QUERY_NODE_DROP_TABLE_CLAUSE,
  QUERY_NODE_DROP_TABLE_STMT,
  QUERY_NODE_DROP_SUPER_TABLE_STMT,
  QUERY_NODE_ALTER_TABLE_STMT,
  QUERY_NODE_ALTER_SUPER_TABLE_STMT,
  QUERY_NODE_CREATE_USER_STMT,
  QUERY_NODE_ALTER_USER_STMT,
  QUERY_NODE_DROP_USER_STMT,
  QUERY_NODE_USE_DATABASE_STMT,
  QUERY_NODE_CREATE_DNODE_STMT,
  QUERY_NODE_DROP_DNODE_STMT,
  QUERY_NODE_ALTER_DNODE_STMT,
  QUERY_NODE_CREATE_INDEX_STMT,
  QUERY_NODE_DROP_INDEX_STMT,
  QUERY_NODE_CREATE_QNODE_STMT,
  QUERY_NODE_DROP_QNODE_STMT,
  QUERY_NODE_CREATE_BACKUP_NODE_STMT,  // no longer used
  QUERY_NODE_DROP_BACKUP_NODE_STMT,    // no longer used
  QUERY_NODE_CREATE_SNODE_STMT,
  QUERY_NODE_DROP_SNODE_STMT,
  QUERY_NODE_CREATE_MNODE_STMT,
  QUERY_NODE_DROP_MNODE_STMT,
  QUERY_NODE_CREATE_TOPIC_STMT,
  QUERY_NODE_DROP_TOPIC_STMT,
  QUERY_NODE_DROP_CGROUP_STMT,
  QUERY_NODE_ALTER_LOCAL_STMT,
  QUERY_NODE_EXPLAIN_STMT,
  QUERY_NODE_DESCRIBE_STMT,
  QUERY_NODE_RESET_QUERY_CACHE_STMT,
  QUERY_NODE_COMPACT_DATABASE_STMT,
  QUERY_NODE_COMPACT_VGROUPS_STMT,
  QUERY_NODE_CREATE_FUNCTION_STMT,
  QUERY_NODE_DROP_FUNCTION_STMT,
  QUERY_NODE_CREATE_STREAM_STMT,
  QUERY_NODE_DROP_STREAM_STMT,
  QUERY_NODE_BALANCE_VGROUP_STMT,
  QUERY_NODE_MERGE_VGROUP_STMT,
  QUERY_NODE_REDISTRIBUTE_VGROUP_STMT,
  QUERY_NODE_SPLIT_VGROUP_STMT,
  QUERY_NODE_SYNCDB_STMT,
  QUERY_NODE_GRANT_STMT,
  QUERY_NODE_REVOKE_STMT,
  QUERY_NODE_ALTER_CLUSTER_STMT,
  QUERY_NODE_SSMIGRATE_DATABASE_STMT,
  QUERY_NODE_CREATE_TSMA_STMT,
  QUERY_NODE_DROP_TSMA_STMT,
  QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT,
  QUERY_NODE_CREATE_VIRTUAL_SUBTABLE_STMT,
  QUERY_NODE_DROP_VIRTUAL_TABLE_STMT,
  QUERY_NODE_ALTER_VIRTUAL_TABLE_STMT,
  QUERY_NODE_CREATE_MOUNT_STMT,
  QUERY_NODE_DROP_MOUNT_STMT,

  // placeholder for [154, 180]
  QUERY_NODE_SHOW_CREATE_VIEW_STMT = 181,
  QUERY_NODE_SHOW_CREATE_DATABASE_STMT,
  QUERY_NODE_SHOW_CREATE_TABLE_STMT,
  QUERY_NODE_SHOW_CREATE_STABLE_STMT,
  QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT,
  QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT,
  QUERY_NODE_SHOW_SCORES_STMT,
  QUERY_NODE_SHOW_TABLE_TAGS_STMT,
  QUERY_NODE_KILL_CONNECTION_STMT,
  QUERY_NODE_KILL_QUERY_STMT,
  QUERY_NODE_KILL_TRANSACTION_STMT,
  QUERY_NODE_KILL_COMPACT_STMT,
  QUERY_NODE_DELETE_STMT,
  QUERY_NODE_INSERT_STMT,
  QUERY_NODE_QUERY,
  QUERY_NODE_SHOW_DB_ALIVE_STMT,
  QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT,
  QUERY_NODE_BALANCE_VGROUP_LEADER_STMT,
  QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT,
  QUERY_NODE_RESTORE_DNODE_STMT,
  QUERY_NODE_RESTORE_QNODE_STMT,
  QUERY_NODE_RESTORE_MNODE_STMT,
  QUERY_NODE_RESTORE_VNODE_STMT,
  QUERY_NODE_PAUSE_STREAM_STMT,
  QUERY_NODE_RESUME_STREAM_STMT,
  QUERY_NODE_CREATE_VIEW_STMT,
  QUERY_NODE_DROP_VIEW_STMT,
  QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE,
  QUERY_NODE_CREATE_ANODE_STMT,
  QUERY_NODE_DROP_ANODE_STMT,
  QUERY_NODE_UPDATE_ANODE_STMT,
  QUERY_NODE_ASSIGN_LEADER_STMT,
  QUERY_NODE_SHOW_CREATE_TSMA_STMT,
  QUERY_NODE_SHOW_CREATE_VTABLE_STMT,
  QUERY_NODE_RECALCULATE_STREAM_STMT,
  QUERY_NODE_CREATE_BNODE_STMT,
  QUERY_NODE_DROP_BNODE_STMT,

  // show statement nodes
  // see 'sysTableShowAdapter', 'SYSTABLE_SHOW_TYPE_OFFSET'
  QUERY_NODE_SHOW_DNODES_STMT = 400,
  QUERY_NODE_SHOW_MNODES_STMT,
  QUERY_NODE_SHOW_MODULES_STMT,
  QUERY_NODE_SHOW_QNODES_STMT,
  QUERY_NODE_SHOW_SNODES_STMT,
  QUERY_NODE_SHOW_BACKUP_NODES_STMT,  // no longer used
  QUERY_NODE_SHOW_ARBGROUPS_STMT,
  QUERY_NODE_SHOW_CLUSTER_STMT,
  QUERY_NODE_SHOW_DATABASES_STMT,
  QUERY_NODE_SHOW_FUNCTIONS_STMT,
  QUERY_NODE_SHOW_INDEXES_STMT,
  QUERY_NODE_SHOW_STABLES_STMT,
  QUERY_NODE_SHOW_STREAMS_STMT,
  QUERY_NODE_SHOW_TABLES_STMT,
  QUERY_NODE_SHOW_TAGS_STMT,
  QUERY_NODE_SHOW_USERS_STMT,
  QUERY_NODE_SHOW_USERS_FULL_STMT,
  QUERY_NODE_SHOW_LICENCES_STMT,
  QUERY_NODE_SHOW_VGROUPS_STMT,
  QUERY_NODE_SHOW_TOPICS_STMT,
  QUERY_NODE_SHOW_CONSUMERS_STMT,
  QUERY_NODE_SHOW_CONNECTIONS_STMT,
  QUERY_NODE_SHOW_QUERIES_STMT,
  QUERY_NODE_SHOW_APPS_STMT,
  QUERY_NODE_SHOW_VARIABLES_STMT,
  QUERY_NODE_SHOW_DNODE_VARIABLES_STMT,
  QUERY_NODE_SHOW_TRANSACTIONS_STMT,
  QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT,
  QUERY_NODE_SHOW_VNODES_STMT,
  QUERY_NODE_SHOW_USER_PRIVILEGES_STMT,
  QUERY_NODE_SHOW_VIEWS_STMT,
  QUERY_NODE_SHOW_COMPACTS_STMT,
  QUERY_NODE_SHOW_COMPACT_DETAILS_STMT,
  QUERY_NODE_SHOW_GRANTS_FULL_STMT,
  QUERY_NODE_SHOW_GRANTS_LOGS_STMT,
  QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT,
  QUERY_NODE_SHOW_ENCRYPTIONS_STMT,
  QUERY_NODE_SHOW_TSMAS_STMT,
  QUERY_NODE_SHOW_ANODES_STMT,
  QUERY_NODE_SHOW_ANODES_FULL_STMT,
  QUERY_NODE_SHOW_USAGE_STMT,
  QUERY_NODE_SHOW_FILESETS_STMT,
  QUERY_NODE_SHOW_TRANSACTION_DETAILS_STMT,
  QUERY_NODE_SHOW_VTABLES_STMT,
  QUERY_NODE_SHOW_BNODES_STMT,
  QUERY_NODE_SHOW_MOUNTS_STMT,

  // logic plan node
  QUERY_NODE_LOGIC_PLAN_SCAN = 1000,
  QUERY_NODE_LOGIC_PLAN_JOIN,
  QUERY_NODE_LOGIC_PLAN_AGG,
  QUERY_NODE_LOGIC_PLAN_PROJECT,
  QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY,
  QUERY_NODE_LOGIC_PLAN_EXCHANGE,
  QUERY_NODE_LOGIC_PLAN_MERGE,
  QUERY_NODE_LOGIC_PLAN_WINDOW,
  QUERY_NODE_LOGIC_PLAN_FILL,
  QUERY_NODE_LOGIC_PLAN_SORT,
  QUERY_NODE_LOGIC_PLAN_PARTITION,
  QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC,
  QUERY_NODE_LOGIC_PLAN_INTERP_FUNC,
  QUERY_NODE_LOGIC_SUBPLAN,
  QUERY_NODE_LOGIC_PLAN,
  QUERY_NODE_LOGIC_PLAN_GROUP_CACHE,
  QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL,
  QUERY_NODE_LOGIC_PLAN_FORECAST_FUNC,
  QUERY_NODE_LOGIC_PLAN_VIRTUAL_TABLE_SCAN,

  // physical plan node
  QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN = 1100,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN,  // INACTIVE
  QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_PROJECT,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN,
  QUERY_NODE_PHYSICAL_PLAN_HASH_AGG,
  QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
  QUERY_NODE_PHYSICAL_PLAN_MERGE,
  QUERY_NODE_PHYSICAL_PLAN_SORT,
  QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT,
  QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL,  // INACTIVE
  QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_1,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_2,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_3,
  QUERY_NODE_PHYSICAL_PLAN_FILL,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_4,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_5,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_6,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_7,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_8,
  QUERY_NODE_PHYSICAL_PLAN_PARTITION,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_9,
  QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_DISPATCH,
  QUERY_NODE_PHYSICAL_PLAN_INSERT,
  QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT,
  QUERY_NODE_PHYSICAL_PLAN_DELETE,
  QUERY_NODE_PHYSICAL_SUBPLAN,
  QUERY_NODE_PHYSICAL_PLAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_10,
  QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN,
  QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE,
  QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_11,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_12,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_13,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_ANOMALY,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_14,
  QUERY_NODE_PHYSICAL_PLAN_FORECAST_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_15,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_16,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_17,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_18,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_19,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_20,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_21,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_22,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_23,
  QUERY_NODE_PHYSICAL_PLAN_UNUSED_24,
  QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_EXTERNAL_WINDOW,
  QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_INSERT,
} ENodeType;

typedef struct {
  int32_t     vgId;
  uint8_t     option;         // 0x0 REQ_OPT_TBNAME, 0x01 REQ_OPT_TBUID
  uint8_t     autoCreateCtb;  // 0x0 not auto create, 0x01 auto create
  const char* dbFName;
  const char* tbName;
} SBuildTableInput;

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t vgVersion;
  int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
  int64_t stateTs;
} SBuildUseDBInput;

typedef struct SField {
  char    name[TSDB_COL_NAME_LEN];
  uint8_t type;
  int8_t  flags;
  int32_t bytes;
} SField;

typedef struct SFieldWithOptions {
  char     name[TSDB_COL_NAME_LEN];
  uint8_t  type;
  int8_t   flags;
  int32_t  bytes;
  uint32_t compress;
  STypeMod typeMod;
} SFieldWithOptions;

typedef struct SRetention {
  int64_t freq;
  int64_t keep;
  int8_t  freqUnit;
  int8_t  keepUnit;
} SRetention;

#define RETENTION_VALID(l, r) ((((l) == 0 && (r)->freq >= 0) || ((r)->freq > 0)) && ((r)->keep > 0))

#pragma pack(push, 1)
// null-terminated string instead of char array to avoid too many memory consumption in case of more than 1M tableMeta
typedef struct SEp {
  char     fqdn[TSDB_FQDN_LEN];
  uint16_t port;
} SEp;

typedef struct {
  int32_t contLen;
  int32_t vgId;
} SMsgHead;

// Submit message for one table
typedef struct SSubmitBlk {
  int64_t uid;        // table unique id
  int64_t suid;       // stable id
  int32_t sversion;   // data schema version
  int32_t dataLen;    // data part length, not including the SSubmitBlk head
  int32_t schemaLen;  // schema length, if length is 0, no schema exists
  int32_t numOfRows;  // total number of rows in current submit block
  char    data[];
} SSubmitBlk;

// Submit message for this TSDB
typedef struct {
  SMsgHead header;
  int64_t  version;
  int32_t  length;
  int32_t  numOfBlocks;
  char     blocks[];
} SSubmitReq;

typedef struct {
  int32_t totalLen;
  int32_t len;
  STSRow* row;
} SSubmitBlkIter;

typedef struct {
  int32_t totalLen;
  int32_t len;
  // head of SSubmitBlk
  int64_t uid;        // table unique id
  int64_t suid;       // stable id
  int32_t sversion;   // data schema version
  int32_t dataLen;    // data part length, not including the SSubmitBlk head
  int32_t schemaLen;  // schema length, if length is 0, no schema exists
  int32_t numOfRows;  // total number of rows in current submit block
  // head of SSubmitBlk
  int32_t     numOfBlocks;
  const void* pMsg;
} SSubmitMsgIter;

int32_t tInitSubmitMsgIter(const SSubmitReq* pMsg, SSubmitMsgIter* pIter);
int32_t tGetSubmitMsgNext(SSubmitMsgIter* pIter, SSubmitBlk** pPBlock);
int32_t tInitSubmitBlkIter(SSubmitMsgIter* pMsgIter, SSubmitBlk* pBlock, SSubmitBlkIter* pIter);
STSRow* tGetSubmitBlkNext(SSubmitBlkIter* pIter);
// for debug
int32_t tPrintFixedSchemaSubmitReq(SSubmitReq* pReq, STSchema* pSchema);

typedef struct {
  bool     hasRef;
  col_id_t id;
  char     refDbName[TSDB_DB_NAME_LEN];
  char     refTableName[TSDB_TABLE_NAME_LEN];
  char     refColName[TSDB_COL_NAME_LEN];
} SColRef;

typedef struct {
  int32_t  nCols;
  int32_t  version;
  SColRef* pColRef;
} SColRefWrapper;

int32_t tEncodeSColRefWrapper(SEncoder *pCoder, const SColRefWrapper *pWrapper);
int32_t tDecodeSColRefWrapperEx(SDecoder *pDecoder, SColRefWrapper *pWrapper, bool decoderMalloc);
typedef struct {
  int32_t vgId;
  SColRef colRef;
} SColRefEx;

typedef struct {
  int16_t colId;
  char    refDbName[TSDB_DB_NAME_LEN];
  char    refTableName[TSDB_TABLE_NAME_LEN];
  char    refColName[TSDB_COL_NAME_LEN];
} SRefColInfo;

typedef struct SVCTableRefCols {
  uint64_t     uid;
  int32_t      numOfSrcTbls;
  int32_t      numOfColRefs;
  SRefColInfo* refCols;
} SVCTableRefCols;

typedef struct SVCTableMergeInfo {
  uint64_t uid;
  int32_t  numOfSrcTbls;
} SVCTableMergeInfo;

typedef struct {
  int32_t    nCols;
  SColRefEx* pColRefEx;
} SColRefExWrapper;

struct SSchema {
  int8_t   type;
  int8_t   flags;
  col_id_t colId;
  int32_t  bytes;
  char     name[TSDB_COL_NAME_LEN];
};
struct SSchemaExt {
  col_id_t colId;
  uint32_t compress;
  STypeMod typeMod;
};

//

struct SSchema2 {
  int8_t   type;
  int8_t   flags;
  col_id_t colId;
  int32_t  bytes;
  char     name[TSDB_COL_NAME_LEN];
  uint32_t compress;
};

typedef struct {
  char        tbName[TSDB_TABLE_NAME_LEN];
  char        stbName[TSDB_TABLE_NAME_LEN];
  char        dbFName[TSDB_DB_FNAME_LEN];
  int64_t     dbId;
  int32_t     numOfTags;
  int32_t     numOfColumns;
  int8_t      precision;
  int8_t      tableType;
  int32_t     sversion;
  int32_t     tversion;
  int32_t     rversion;
  uint64_t    suid;
  uint64_t    tuid;
  int32_t     vgId;
  int8_t      sysInfo;
  SSchema*    pSchemas;
  SSchemaExt* pSchemaExt;
  int8_t      virtualStb;
  int32_t     numOfColRefs;
  SColRef*    pColRefs;
} STableMetaRsp;

typedef struct {
  int32_t        code;
  int64_t        uid;
  char*          tblFName;
  int32_t        numOfRows;
  int32_t        affectedRows;
  int64_t        sver;
  STableMetaRsp* pMeta;
} SSubmitBlkRsp;

typedef struct {
  int32_t numOfRows;
  int32_t affectedRows;
  int32_t nBlocks;
  union {
    SArray*        pArray;
    SSubmitBlkRsp* pBlocks;
  };
} SSubmitRsp;

// int32_t tEncodeSSubmitRsp(SEncoder* pEncoder, const SSubmitRsp* pRsp);
// int32_t tDecodeSSubmitRsp(SDecoder* pDecoder, SSubmitRsp* pRsp);
// void    tFreeSSubmitBlkRsp(void* param);
void tFreeSSubmitRsp(SSubmitRsp* pRsp);

#define COL_SMA_ON       ((int8_t)0x1)
#define COL_IDX_ON       ((int8_t)0x2)
#define COL_IS_KEY       ((int8_t)0x4)
#define COL_SET_NULL     ((int8_t)0x10)
#define COL_SET_VAL      ((int8_t)0x20)
#define COL_IS_SYSINFO   ((int8_t)0x40)
#define COL_HAS_TYPE_MOD ((int8_t)0x80)
#define COL_REF_BY_STM   ((int8_t)0x08)

#define COL_IS_SET(FLG)  (((FLG) & (COL_SET_VAL | COL_SET_NULL)) != 0)
#define COL_CLR_SET(FLG) ((FLG) &= (~(COL_SET_VAL | COL_SET_NULL)))

#define IS_BSMA_ON(s)  (((s)->flags & 0x01) == COL_SMA_ON)
#define IS_IDX_ON(s)   (((s)->flags & 0x02) == COL_IDX_ON)
#define IS_SET_NULL(s) (((s)->flags & COL_SET_NULL) == COL_SET_NULL)

#define SSCHMEA_SET_IDX_ON(s) \
  do {                        \
    (s)->flags |= COL_IDX_ON; \
  } while (0)

#define SSCHMEA_SET_IDX_OFF(s)   \
  do {                           \
    (s)->flags &= (~COL_IDX_ON); \
  } while (0)

#define SSCHEMA_SET_TYPE_MOD(s)     \
  do {                              \
    (s)->flags |= COL_HAS_TYPE_MOD; \
  } while (0)

#define HAS_TYPE_MOD(s) (((s)->flags & COL_HAS_TYPE_MOD))

#define SSCHMEA_TYPE(s)  ((s)->type)
#define SSCHMEA_FLAGS(s) ((s)->flags)
#define SSCHMEA_COLID(s) ((s)->colId)
#define SSCHMEA_BYTES(s) ((s)->bytes)
#define SSCHMEA_NAME(s)  ((s)->name)

typedef struct {
  bool    tsEnableMonitor;
  int32_t tsMonitorInterval;
  int32_t tsSlowLogThreshold;
  int32_t tsSlowLogMaxLen;
  int32_t tsSlowLogScope;
  int32_t tsSlowLogThresholdTest;  // Obsolete
  char    tsSlowLogExceptDb[TSDB_DB_NAME_LEN];
} SMonitorParas;

typedef struct {
  STypeMod typeMod;
} SExtSchema;

bool hasExtSchema(const SExtSchema* pExtSchema);

typedef struct {
  int32_t  nCols;
  int32_t  version;
  SSchema* pSchema;
} SSchemaWrapper;

typedef struct {
  col_id_t id;
  uint32_t alg;
} SColCmpr;

typedef struct {
  int32_t   nCols;
  int32_t   version;
  SColCmpr* pColCmpr;
} SColCmprWrapper;

static FORCE_INLINE int32_t tInitDefaultSColRefWrapperByCols(SColRefWrapper* pRef, int32_t nCols) {
  if (pRef->pColRef) {
    return TSDB_CODE_INVALID_PARA;
  }
  pRef->pColRef = (SColRef*)taosMemoryCalloc(nCols, sizeof(SColRef));
  if (pRef->pColRef == NULL) {
    return terrno;
  }
  pRef->nCols = nCols;
  for (int32_t i = 0; i < nCols; i++) {
    pRef->pColRef[i].hasRef = false;
    pRef->pColRef[i].id = (col_id_t)(i + 1);
  }
  return 0;
}

static FORCE_INLINE SColCmprWrapper* tCloneSColCmprWrapper(const SColCmprWrapper* pSrcWrapper) {
  if (pSrcWrapper->pColCmpr == NULL || pSrcWrapper->nCols == 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  SColCmprWrapper* pDstWrapper = (SColCmprWrapper*)taosMemoryMalloc(sizeof(SColCmprWrapper));
  if (pDstWrapper == NULL) {
    return NULL;
  }
  pDstWrapper->nCols = pSrcWrapper->nCols;
  pDstWrapper->version = pSrcWrapper->version;

  int32_t size = sizeof(SColCmpr) * pDstWrapper->nCols;
  pDstWrapper->pColCmpr = (SColCmpr*)taosMemoryCalloc(1, size);
  if (pDstWrapper->pColCmpr == NULL) {
    taosMemoryFree(pDstWrapper);
    return NULL;
  }
  (void)memcpy(pDstWrapper->pColCmpr, pSrcWrapper->pColCmpr, size);

  return pDstWrapper;
}

static FORCE_INLINE int32_t tInitDefaultSColCmprWrapperByCols(SColCmprWrapper* pCmpr, int32_t nCols) {
  if (!(!pCmpr->pColCmpr)) {
    return TSDB_CODE_INVALID_PARA;
  }
  pCmpr->pColCmpr = (SColCmpr*)taosMemoryCalloc(nCols, sizeof(SColCmpr));
  if (pCmpr->pColCmpr == NULL) {
    return terrno;
  }
  pCmpr->nCols = nCols;
  return 0;
}

static FORCE_INLINE int32_t tInitDefaultSColCmprWrapper(SColCmprWrapper* pCmpr, SSchemaWrapper* pSchema) {
  pCmpr->nCols = pSchema->nCols;
  if (!(!pCmpr->pColCmpr)) {
    return TSDB_CODE_INVALID_PARA;
  }
  pCmpr->pColCmpr = (SColCmpr*)taosMemoryCalloc(pCmpr->nCols, sizeof(SColCmpr));
  if (pCmpr->pColCmpr == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < pCmpr->nCols; i++) {
    SColCmpr* pColCmpr = &pCmpr->pColCmpr[i];
    SSchema*  pColSchema = &pSchema->pSchema[i];
    pColCmpr->id = pColSchema->colId;
    pColCmpr->alg = 0;
  }
  return 0;
}

static FORCE_INLINE void tDeleteSColCmprWrapper(SColCmprWrapper* pWrapper) {
  if (pWrapper == NULL) return;

  taosMemoryFreeClear(pWrapper->pColCmpr);
  taosMemoryFreeClear(pWrapper);
}
static FORCE_INLINE SSchemaWrapper* tCloneSSchemaWrapper(const SSchemaWrapper* pSchemaWrapper) {
  if (pSchemaWrapper->pSchema == NULL) return NULL;

  SSchemaWrapper* pSW = (SSchemaWrapper*)taosMemoryMalloc(sizeof(SSchemaWrapper));
  if (pSW == NULL) {
    return NULL;
  }
  pSW->nCols = pSchemaWrapper->nCols;
  pSW->version = pSchemaWrapper->version;
  pSW->pSchema = (SSchema*)taosMemoryCalloc(pSW->nCols, sizeof(SSchema));
  if (pSW->pSchema == NULL) {
    taosMemoryFree(pSW);
    return NULL;
  }

  (void)memcpy(pSW->pSchema, pSchemaWrapper->pSchema, pSW->nCols * sizeof(SSchema));
  return pSW;
}

static FORCE_INLINE void tDeleteSchemaWrapper(SSchemaWrapper* pSchemaWrapper) {
  if (pSchemaWrapper) {
    taosMemoryFree(pSchemaWrapper->pSchema);
    taosMemoryFree(pSchemaWrapper);
  }
}

static FORCE_INLINE void tDeleteSSchemaWrapperForHash(void* pSchemaWrapper) {
  if (pSchemaWrapper != NULL && *(SSchemaWrapper**)pSchemaWrapper != NULL) {
    taosMemoryFree((*(SSchemaWrapper**)pSchemaWrapper)->pSchema);
    taosMemoryFree(*(SSchemaWrapper**)pSchemaWrapper);
  }
}

static FORCE_INLINE int32_t taosEncodeSSchema(void** buf, const SSchema* pSchema) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI8(buf, pSchema->type);
  tlen += taosEncodeFixedI8(buf, pSchema->flags);
  tlen += taosEncodeFixedI32(buf, pSchema->bytes);
  tlen += taosEncodeFixedI16(buf, pSchema->colId);
  tlen += taosEncodeString(buf, pSchema->name);
  return tlen;
}

static FORCE_INLINE void* taosDecodeSSchema(const void* buf, SSchema* pSchema) {
  buf = taosDecodeFixedI8(buf, &pSchema->type);
  buf = taosDecodeFixedI8(buf, &pSchema->flags);
  buf = taosDecodeFixedI32(buf, &pSchema->bytes);
  buf = taosDecodeFixedI16(buf, &pSchema->colId);
  buf = taosDecodeStringTo(buf, pSchema->name);
  return (void*)buf;
}

static FORCE_INLINE int32_t tEncodeSSchema(SEncoder* pEncoder, const SSchema* pSchema) {
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pSchema->type));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pSchema->flags));
  TAOS_CHECK_RETURN(tEncodeI32v(pEncoder, pSchema->bytes));
  TAOS_CHECK_RETURN(tEncodeI16v(pEncoder, pSchema->colId));
  TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pSchema->name));
  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchema(SDecoder* pDecoder, SSchema* pSchema) {
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pSchema->type));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pSchema->flags));
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pSchema->bytes));
  TAOS_CHECK_RETURN(tDecodeI16v(pDecoder, &pSchema->colId));
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pSchema->name));
  return 0;
}

static FORCE_INLINE int32_t tEncodeSSchemaExt(SEncoder* pEncoder, const SSchemaExt* pSchemaExt) {
  TAOS_CHECK_RETURN(tEncodeI16v(pEncoder, pSchemaExt->colId));
  TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pSchemaExt->compress));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pSchemaExt->typeMod));
  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchemaExt(SDecoder* pDecoder, SSchemaExt* pSchemaExt) {
  TAOS_CHECK_RETURN(tDecodeI16v(pDecoder, &pSchemaExt->colId));
  TAOS_CHECK_RETURN(tDecodeU32(pDecoder, &pSchemaExt->compress));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pSchemaExt->typeMod));
  return 0;
}

static FORCE_INLINE int32_t tEncodeSColRef(SEncoder* pEncoder, const SColRef* pColRef) {
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pColRef->hasRef));
  TAOS_CHECK_RETURN(tEncodeI16(pEncoder, pColRef->id));
  if (pColRef->hasRef) {
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pColRef->refDbName));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pColRef->refTableName));
    TAOS_CHECK_RETURN(tEncodeCStr(pEncoder, pColRef->refColName));
  }
  return 0;
}

static FORCE_INLINE int32_t tDecodeSColRef(SDecoder* pDecoder, SColRef* pColRef) {
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, (int8_t*)&pColRef->hasRef));
  TAOS_CHECK_RETURN(tDecodeI16(pDecoder, &pColRef->id));
  if (pColRef->hasRef) {
    TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pColRef->refDbName));
    TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pColRef->refTableName));
    TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, pColRef->refColName));
  }

  return 0;
}

static FORCE_INLINE int32_t taosEncodeSSchemaWrapper(void** buf, const SSchemaWrapper* pSW) {
  int32_t tlen = 0;
  tlen += taosEncodeVariantI32(buf, pSW->nCols);
  tlen += taosEncodeVariantI32(buf, pSW->version);
  for (int32_t i = 0; i < pSW->nCols; i++) {
    tlen += taosEncodeSSchema(buf, &pSW->pSchema[i]);
  }
  return tlen;
}

static FORCE_INLINE void* taosDecodeSSchemaWrapper(const void* buf, SSchemaWrapper* pSW) {
  buf = taosDecodeVariantI32(buf, &pSW->nCols);
  buf = taosDecodeVariantI32(buf, &pSW->version);
  if (pSW->nCols > 0) {
    pSW->pSchema = (SSchema*)taosMemoryCalloc(pSW->nCols, sizeof(SSchema));
    if (pSW->pSchema == NULL) {
      return NULL;
    }

    for (int32_t i = 0; i < pSW->nCols; i++) {
      buf = taosDecodeSSchema(buf, &pSW->pSchema[i]);
    }
  } else {
    pSW->pSchema = NULL;
  }
  return (void*)buf;
}

static FORCE_INLINE int32_t tEncodeSSchemaWrapper(SEncoder* pEncoder, const SSchemaWrapper* pSW) {
  if (pSW == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  TAOS_CHECK_RETURN(tEncodeI32v(pEncoder, pSW->nCols));
  TAOS_CHECK_RETURN(tEncodeI32v(pEncoder, pSW->version));
  for (int32_t i = 0; i < pSW->nCols; i++) {
    TAOS_CHECK_RETURN(tEncodeSSchema(pEncoder, &pSW->pSchema[i]));
  }
  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchemaWrapper(SDecoder* pDecoder, SSchemaWrapper* pSW) {
  if (pSW == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pSW->nCols));
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pSW->version));

  pSW->pSchema = (SSchema*)taosMemoryCalloc(pSW->nCols, sizeof(SSchema));
  if (pSW->pSchema == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  for (int32_t i = 0; i < pSW->nCols; i++) {
    TAOS_CHECK_RETURN(tDecodeSSchema(pDecoder, &pSW->pSchema[i]));
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeSSchemaWrapperEx(SDecoder* pDecoder, SSchemaWrapper* pSW) {
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pSW->nCols));
  TAOS_CHECK_RETURN(tDecodeI32v(pDecoder, &pSW->version));

  pSW->pSchema = (SSchema*)tDecoderMalloc(pDecoder, pSW->nCols * sizeof(SSchema));
  if (pSW->pSchema == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  for (int32_t i = 0; i < pSW->nCols; i++) {
    TAOS_CHECK_RETURN(tDecodeSSchema(pDecoder, &pSW->pSchema[i]));
  }

  return 0;
}

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  int8_t   igExists;
  int8_t   source;  // TD_REQ_FROM_TAOX-taosX or TD_REQ_FROM_APP-taosClient
  int8_t   reserved[6];
  tb_uid_t suid;
  int64_t  delay1;
  int64_t  delay2;
  int64_t  watermark1;
  int64_t  watermark2;
  int32_t  ttl;
  int32_t  colVer;
  int32_t  tagVer;
  int32_t  numOfColumns;
  int32_t  numOfTags;
  int32_t  numOfFuncs;
  int32_t  commentLen;
  int32_t  ast1Len;
  int32_t  ast2Len;
  SArray*  pColumns;  // array of SFieldWithOptions
  SArray*  pTags;     // array of SField
  SArray*  pFuncs;
  char*    pComment;
  char*    pAst1;
  char*    pAst2;
  int64_t  deleteMark1;
  int64_t  deleteMark2;
  int32_t  sqlLen;
  char*    sql;
  int64_t  keep;
  int8_t   virtualStb;
} SMCreateStbReq;

int32_t tSerializeSMCreateStbReq(void* buf, int32_t bufLen, SMCreateStbReq* pReq);
int32_t tDeserializeSMCreateStbReq(void* buf, int32_t bufLen, SMCreateStbReq* pReq);
void    tFreeSMCreateStbReq(SMCreateStbReq* pReq);

typedef struct {
  STableMetaRsp* pMeta;
} SMCreateStbRsp;

int32_t tEncodeSMCreateStbRsp(SEncoder* pEncoder, const SMCreateStbRsp* pRsp);
int32_t tDecodeSMCreateStbRsp(SDecoder* pDecoder, SMCreateStbRsp* pRsp);
void    tFreeSMCreateStbRsp(SMCreateStbRsp* pRsp);

typedef struct {
  char     name[TSDB_TABLE_FNAME_LEN];
  int8_t   igNotExists;
  int8_t   source;  // TD_REQ_FROM_TAOX-taosX or TD_REQ_FROM_APP-taosClient
  int8_t   reserved[6];
  tb_uid_t suid;
  int32_t  sqlLen;
  char*    sql;
} SMDropStbReq;

int32_t tSerializeSMDropStbReq(void* buf, int32_t bufLen, SMDropStbReq* pReq);
int32_t tDeserializeSMDropStbReq(void* buf, int32_t bufLen, SMDropStbReq* pReq);
void    tFreeSMDropStbReq(SMDropStbReq* pReq);

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  int8_t  alterType;
  int32_t numOfFields;
  SArray* pFields;
  int32_t ttl;
  int32_t commentLen;
  char*   comment;
  int32_t sqlLen;
  char*   sql;
  int64_t keep;
  SArray* pTypeMods;
} SMAlterStbReq;

int32_t tSerializeSMAlterStbReq(void* buf, int32_t bufLen, SMAlterStbReq* pReq);
int32_t tDeserializeSMAlterStbReq(void* buf, int32_t bufLen, SMAlterStbReq* pReq);
void    tFreeSMAltertbReq(SMAlterStbReq* pReq);

typedef struct SEpSet {
  int8_t inUse;
  int8_t numOfEps;
  SEp    eps[TSDB_MAX_REPLICA];
} SEpSet;

int32_t tEncodeSEpSet(SEncoder* pEncoder, const SEpSet* pEp);
int32_t tDecodeSEpSet(SDecoder* pDecoder, SEpSet* pEp);
int32_t taosEncodeSEpSet(void** buf, const SEpSet* pEp);
void*   taosDecodeSEpSet(const void* buf, SEpSet* pEp);

int32_t tSerializeSEpSet(void* buf, int32_t bufLen, const SEpSet* pEpset);
int32_t tDeserializeSEpSet(void* buf, int32_t buflen, SEpSet* pEpset);

typedef struct {
  int8_t  connType;
  int32_t pid;
  char    app[TSDB_APP_NAME_LEN];
  char    db[TSDB_DB_NAME_LEN];
  char    user[TSDB_USER_LEN];
  char    passwd[TSDB_PASSWORD_LEN];
  int64_t startTime;
  char    sVer[TSDB_VERSION_LEN];
} SConnectReq;

int32_t tSerializeSConnectReq(void* buf, int32_t bufLen, SConnectReq* pReq);
int32_t tDeserializeSConnectReq(void* buf, int32_t bufLen, SConnectReq* pReq);

typedef struct {
  int32_t       acctId;
  int64_t       clusterId;
  uint32_t      connId;
  int32_t       dnodeNum;
  int8_t        superUser;
  int8_t        sysInfo;
  int8_t        connType;
  SEpSet        epSet;
  int32_t       svrTimestamp;
  int32_t       passVer;
  int32_t       authVer;
  char          sVer[TSDB_VERSION_LEN];
  char          sDetailVer[128];
  int64_t       whiteListVer;
  SMonitorParas monitorParas;
  int8_t        enableAuditDelete;
} SConnectRsp;

int32_t tSerializeSConnectRsp(void* buf, int32_t bufLen, SConnectRsp* pRsp);
int32_t tDeserializeSConnectRsp(void* buf, int32_t bufLen, SConnectRsp* pRsp);

typedef struct {
  char    user[TSDB_USER_LEN];
  char    pass[TSDB_PASSWORD_LEN];
  int32_t maxUsers;
  int32_t maxDbs;
  int32_t maxTimeSeries;
  int32_t maxStreams;
  int32_t accessState;  // Configured only by command
  int64_t maxStorage;
} SCreateAcctReq, SAlterAcctReq;

// int32_t tSerializeSCreateAcctReq(void* buf, int32_t bufLen, SCreateAcctReq* pReq);
// int32_t tDeserializeSCreateAcctReq(void* buf, int32_t bufLen, SCreateAcctReq* pReq);

typedef struct {
  char    user[TSDB_USER_LEN];
  int32_t sqlLen;
  char*   sql;
} SDropUserReq, SDropAcctReq;

int32_t tSerializeSDropUserReq(void* buf, int32_t bufLen, SDropUserReq* pReq);
int32_t tDeserializeSDropUserReq(void* buf, int32_t bufLen, SDropUserReq* pReq);
void    tFreeSDropUserReq(SDropUserReq* pReq);

typedef struct SIpV4Range {
  uint32_t ip;
  uint32_t mask;
} SIpV4Range;

typedef struct SIpv6Range {
  uint64_t addr[2];
  uint32_t mask;
} SIpV6Range;

typedef struct {
  int8_t type;
  union {
    SIpV4Range ipV4;
    SIpV6Range ipV6;
  };
} SIpRange;

typedef struct {
  int32_t    num;
  SIpV4Range pIpRange[];
} SIpWhiteList;

typedef struct {
  int32_t  num;
  SIpRange pIpRanges[];
} SIpWhiteListDual;

SIpWhiteListDual* cloneIpWhiteList(SIpWhiteListDual* pIpWhiteList);
int32_t           cvtIpWhiteListToDual(SIpWhiteList* pWhiteList, SIpWhiteListDual** pWhiteListDual);
int32_t           cvtIpWhiteListDualToV4(SIpWhiteListDual* pWhiteListDual, SIpWhiteList** pWhiteList);
int32_t           createDefaultIp6Range(SIpRange* pRange);
int32_t           createDefaultIp4Range(SIpRange* pRange);

typedef struct {
  int8_t  createType;
  int8_t  superUser;  // denote if it is a super user or not
  int8_t  sysInfo;
  int8_t  enable;
  char    user[TSDB_USER_LEN];
  char    pass[TSDB_USET_PASSWORD_LEN];
  int32_t numIpRanges;

  SIpV4Range* pIpRanges;
  int32_t     sqlLen;
  char*       sql;
  int8_t      isImport;
  int8_t      createDb;
  int8_t      passIsMd5;
  SIpRange*   pIpDualRanges;
} SCreateUserReq;

int32_t tSerializeSCreateUserReq(void* buf, int32_t bufLen, SCreateUserReq* pReq);
int32_t tDeserializeSCreateUserReq(void* buf, int32_t bufLen, SCreateUserReq* pReq);
void    tFreeSCreateUserReq(SCreateUserReq* pReq);

typedef struct {
  int32_t dnodeId;
  int64_t analVer;
} SRetrieveAnalyticsAlgoReq;

typedef struct {
  int64_t   ver;
  SHashObj* hash;  // algoname:algotype -> SAnalUrl
} SRetrieveAnalyticAlgoRsp;

int32_t tSerializeRetrieveAnalyticAlgoReq(void* buf, int32_t bufLen, SRetrieveAnalyticsAlgoReq* pReq);
int32_t tDeserializeRetrieveAnalyticAlgoReq(void* buf, int32_t bufLen, SRetrieveAnalyticsAlgoReq* pReq);
int32_t tSerializeRetrieveAnalyticAlgoRsp(void* buf, int32_t bufLen, SRetrieveAnalyticAlgoRsp* pRsp);
int32_t tDeserializeRetrieveAnalyticAlgoRsp(void* buf, int32_t bufLen, SRetrieveAnalyticAlgoRsp* pRsp);
void    tFreeRetrieveAnalyticAlgoRsp(SRetrieveAnalyticAlgoRsp* pRsp);

typedef struct {
  int8_t alterType;
  int8_t superUser;
  int8_t sysInfo;
  int8_t enable;
  int8_t isView;
  union {
    uint8_t flag;
    struct {
      uint8_t createdb : 1;
      uint8_t reserve : 7;
    };
  };
  char        user[TSDB_USER_LEN];
  char        pass[TSDB_USET_PASSWORD_LEN];
  char        objname[TSDB_DB_FNAME_LEN];  // db or topic
  char        tabName[TSDB_TABLE_NAME_LEN];
  char*       tagCond;
  int32_t     tagCondLen;
  int32_t     numIpRanges;
  SIpV4Range* pIpRanges;
  int64_t     privileges;
  int32_t     sqlLen;
  char*       sql;
  int8_t      passIsMd5;
  SIpRange*   pIpDualRanges;
} SAlterUserReq;

int32_t tSerializeSAlterUserReq(void* buf, int32_t bufLen, SAlterUserReq* pReq);
int32_t tDeserializeSAlterUserReq(void* buf, int32_t bufLen, SAlterUserReq* pReq);
void    tFreeSAlterUserReq(SAlterUserReq* pReq);

typedef struct {
  char user[TSDB_USER_LEN];
} SGetUserAuthReq;

int32_t tSerializeSGetUserAuthReq(void* buf, int32_t bufLen, SGetUserAuthReq* pReq);
int32_t tDeserializeSGetUserAuthReq(void* buf, int32_t bufLen, SGetUserAuthReq* pReq);

typedef struct {
  char      user[TSDB_USER_LEN];
  int32_t   version;
  int32_t   passVer;
  int8_t    superAuth;
  int8_t    sysInfo;
  int8_t    enable;
  int8_t    dropped;
  SHashObj* createdDbs;
  SHashObj* readDbs;
  SHashObj* writeDbs;
  SHashObj* readTbs;
  SHashObj* writeTbs;
  SHashObj* alterTbs;
  SHashObj* readViews;
  SHashObj* writeViews;
  SHashObj* alterViews;
  SHashObj* useDbs;
  int64_t   whiteListVer;
} SGetUserAuthRsp;

int32_t tSerializeSGetUserAuthRsp(void* buf, int32_t bufLen, SGetUserAuthRsp* pRsp);
int32_t tDeserializeSGetUserAuthRsp(void* buf, int32_t bufLen, SGetUserAuthRsp* pRsp);
void    tFreeSGetUserAuthRsp(SGetUserAuthRsp* pRsp);

int32_t tSerializeIpRange(SEncoder* encoder, SIpRange* pRange);
int32_t tDeserializeIpRange(SDecoder* decoder, SIpRange* pRange);
typedef struct {
  int64_t ver;
  char    user[TSDB_USER_LEN];
  int32_t numOfRange;
  union {
    SIpV4Range* pIpRanges;
    SIpRange*   pIpDualRanges;
  };
} SUpdateUserIpWhite;

typedef struct {
  int64_t             ver;
  int                 numOfUser;
  SUpdateUserIpWhite* pUserIpWhite;
} SUpdateIpWhite;

int32_t tSerializeSUpdateIpWhite(void* buf, int32_t bufLen, SUpdateIpWhite* pReq);
int32_t tDeserializeSUpdateIpWhite(void* buf, int32_t bufLen, SUpdateIpWhite* pReq);
void    tFreeSUpdateIpWhiteReq(SUpdateIpWhite* pReq);
int32_t cloneSUpdateIpWhiteReq(SUpdateIpWhite* pReq, SUpdateIpWhite** pUpdate);

int32_t tSerializeSUpdateIpWhiteDual(void* buf, int32_t bufLen, SUpdateIpWhite* pReq);
int32_t tDeserializeSUpdateIpWhiteDual(void* buf, int32_t bufLen, SUpdateIpWhite* pReq);
void    tFreeSUpdateIpWhiteDualReq(SUpdateIpWhite* pReq);

typedef struct {
  int64_t ipWhiteVer;
} SRetrieveIpWhiteReq;

int32_t tSerializeRetrieveIpWhite(void* buf, int32_t bufLen, SRetrieveIpWhiteReq* pReq);
int32_t tDeserializeRetrieveIpWhite(void* buf, int32_t bufLen, SRetrieveIpWhiteReq* pReq);
typedef struct {
  char user[TSDB_USER_LEN];
} SGetUserWhiteListReq;

int32_t tSerializeSGetUserWhiteListReq(void* buf, int32_t bufLen, SGetUserWhiteListReq* pReq);
int32_t tDeserializeSGetUserWhiteListReq(void* buf, int32_t bufLen, SGetUserWhiteListReq* pReq);

typedef struct {
  char    user[TSDB_USER_LEN];
  int32_t numWhiteLists;
  union {
    SIpV4Range* pWhiteLists;
    SIpRange*   pWhiteListsDual;
  };
} SGetUserWhiteListRsp;

int32_t tIpStrToUint(const SIpAddr* addr, SIpRange* range);
int32_t tIpUintToStr(const SIpRange* range, SIpAddr* addr);
int32_t tIpRangeSetMask(SIpRange* range, int32_t mask);
void    tIpRangeSetDefaultMask(SIpRange* range);

int32_t tSerializeSGetUserWhiteListRsp(void* buf, int32_t bufLen, SGetUserWhiteListRsp* pRsp);
int32_t tDeserializeSGetUserWhiteListRsp(void* buf, int32_t bufLen, SGetUserWhiteListRsp* pRsp);
void    tFreeSGetUserWhiteListRsp(SGetUserWhiteListRsp* pRsp);

int32_t tSerializeSGetUserWhiteListDualRsp(void* buf, int32_t bufLen, SGetUserWhiteListRsp* pRsp);
int32_t tDeserializeSGetUserWhiteListDualRsp(void* buf, int32_t bufLen, SGetUserWhiteListRsp* pRsp);
void    tFreeSGetUserWhiteListDualRsp(SGetUserWhiteListRsp* pRsp);

/*
 * for client side struct, only column id, type, bytes are necessary
 * But for data in vnode side, we need all the following information.
 */
typedef struct {
  union {
    col_id_t colId;
    int16_t  slotId;
  };

  uint8_t precision;
  uint8_t scale;
  int32_t bytes;
  int8_t  type;
  uint8_t pk;
  bool    noData;
} SColumnInfo;

typedef struct STimeWindow {
  TSKEY skey;
  TSKEY ekey;
} STimeWindow;

typedef struct SQueryHint {
  bool batchScan;
} SQueryHint;

typedef struct {
  int32_t tsOffset;       // offset value in current msg body, NOTE: ts list is compressed
  int32_t tsLen;          // total length of ts comp block
  int32_t tsNumOfBlocks;  // ts comp block numbers
  int32_t tsOrder;        // ts comp block order
} STsBufInfo;

typedef struct {
  void*       timezone;
  char        intervalUnit;
  char        slidingUnit;
  char        offsetUnit;
  int8_t      precision;
  int64_t     interval;
  int64_t     sliding;
  int64_t     offset;
  STimeWindow timeRange;
} SInterval;

typedef struct STbVerInfo {
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t sversion;
  int32_t tversion;
  int32_t rversion;  // virtual table's column ref's version
} STbVerInfo;

typedef struct {
  int32_t code;
  int64_t affectedRows;
  SArray* tbVerInfo;  // STbVerInfo
} SQueryTableRsp;

int32_t tSerializeSQueryTableRsp(void* buf, int32_t bufLen, SQueryTableRsp* pRsp);

int32_t tDeserializeSQueryTableRsp(void* buf, int32_t bufLen, SQueryTableRsp* pRsp);

typedef struct {
  SMsgHead header;
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     tbName[TSDB_TABLE_NAME_LEN];
} STableCfgReq;

typedef struct {
  char        tbName[TSDB_TABLE_NAME_LEN];
  char        stbName[TSDB_TABLE_NAME_LEN];
  char        dbFName[TSDB_DB_FNAME_LEN];
  int32_t     numOfTags;
  int32_t     numOfColumns;
  int8_t      tableType;
  int64_t     delay1;
  int64_t     delay2;
  int64_t     watermark1;
  int64_t     watermark2;
  int32_t     ttl;
  int32_t     keep;
  SArray*     pFuncs;
  int32_t     commentLen;
  char*       pComment;
  SSchema*    pSchemas;
  int32_t     tagsLen;
  char*       pTags;
  SSchemaExt* pSchemaExt;
  int8_t      virtualStb;
  SColRef*    pColRefs;
} STableCfg;

typedef STableCfg STableCfgRsp;

int32_t tSerializeSTableCfgReq(void* buf, int32_t bufLen, STableCfgReq* pReq);
int32_t tDeserializeSTableCfgReq(void* buf, int32_t bufLen, STableCfgReq* pReq);

int32_t tSerializeSTableCfgRsp(void* buf, int32_t bufLen, STableCfgRsp* pRsp);
int32_t tDeserializeSTableCfgRsp(void* buf, int32_t bufLen, STableCfgRsp* pRsp);
void    tFreeSTableCfgRsp(STableCfgRsp* pRsp);

typedef struct {
  SMsgHead header;
  tb_uid_t suid;
} SVSubTablesReq;

int32_t tSerializeSVSubTablesReq(void* buf, int32_t bufLen, SVSubTablesReq* pReq);
int32_t tDeserializeSVSubTablesReq(void* buf, int32_t bufLen, SVSubTablesReq* pReq);

typedef struct {
  int32_t vgId;
  SArray* pTables;  // SArray<SVCTableRefCols*>
} SVSubTablesRsp;

int32_t tSerializeSVSubTablesRsp(void* buf, int32_t bufLen, SVSubTablesRsp* pRsp);
int32_t tDeserializeSVSubTablesRsp(void* buf, int32_t bufLen, SVSubTablesRsp* pRsp);
void    tDestroySVSubTablesRsp(void* rsp);

typedef struct {
  SMsgHead header;
  tb_uid_t suid;
} SVStbRefDbsReq;

int32_t tSerializeSVStbRefDbsReq(void* buf, int32_t bufLen, SVStbRefDbsReq* pReq);
int32_t tDeserializeSVStbRefDbsReq(void* buf, int32_t bufLen, SVStbRefDbsReq* pReq);

typedef struct {
  int32_t vgId;
  SArray* pDbs;  // SArray<char* (db name)>
} SVStbRefDbsRsp;

int32_t tSerializeSVStbRefDbsRsp(void* buf, int32_t bufLen, SVStbRefDbsRsp* pRsp);
int32_t tDeserializeSVStbRefDbsRsp(void* buf, int32_t bufLen, SVStbRefDbsRsp* pRsp);
void    tDestroySVStbRefDbsRsp(void* rsp);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int32_t numOfVgroups;
  int32_t numOfStables;  // single_stable
  int32_t buffer;        // MB
  int32_t pageSize;
  int32_t pages;
  int32_t cacheLastSize;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t keepTimeOffset;
  int32_t minRows;
  int32_t maxRows;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  precision;  // time resolution
  int8_t  compression;
  int8_t  replications;
  int8_t  strict;
  int8_t  cacheLast;
  int8_t  schemaless;
  int8_t  ignoreExist;
  int32_t numOfRetensions;
  SArray* pRetensions;  // SRetention
  int32_t walRetentionPeriod;
  int64_t walRetentionSize;
  int32_t walRollPeriod;
  int64_t walSegmentSize;
  int32_t sstTrigger;
  int16_t hashPrefix;
  int16_t hashSuffix;
  int32_t ssChunkSize;
  int32_t ssKeepLocal;
  int8_t  ssCompact;
  int32_t tsdbPageSize;
  int32_t sqlLen;
  char*   sql;
  int8_t  withArbitrator;
  int8_t  encryptAlgorithm;
  char    dnodeListStr[TSDB_DNODE_LIST_LEN];
  // 1. add auto-compact parameters
  int32_t compactInterval;    // minutes
  int32_t compactStartTime;   // minutes
  int32_t compactEndTime;     // minutes
  int8_t  compactTimeOffset;  // hour
} SCreateDbReq;

int32_t tSerializeSCreateDbReq(void* buf, int32_t bufLen, SCreateDbReq* pReq);
int32_t tDeserializeSCreateDbReq(void* buf, int32_t bufLen, SCreateDbReq* pReq);
void    tFreeSCreateDbReq(SCreateDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int32_t buffer;
  int32_t pageSize;
  int32_t pages;
  int32_t cacheLastSize;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t keepTimeOffset;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  strict;
  int8_t  cacheLast;
  int8_t  replications;
  int32_t sstTrigger;
  int32_t minRows;
  int32_t walRetentionPeriod;
  int32_t walRetentionSize;
  int32_t ssKeepLocal;
  int8_t  ssCompact;
  int32_t sqlLen;
  char*   sql;
  int8_t  withArbitrator;
  // 1. add auto-compact parameters
  int32_t compactInterval;
  int32_t compactStartTime;
  int32_t compactEndTime;
  int8_t  compactTimeOffset;
} SAlterDbReq;

int32_t tSerializeSAlterDbReq(void* buf, int32_t bufLen, SAlterDbReq* pReq);
int32_t tDeserializeSAlterDbReq(void* buf, int32_t bufLen, SAlterDbReq* pReq);
void    tFreeSAlterDbReq(SAlterDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int8_t  ignoreNotExists;
  int8_t  force;
  int32_t sqlLen;
  char*   sql;
} SDropDbReq;

int32_t tSerializeSDropDbReq(void* buf, int32_t bufLen, SDropDbReq* pReq);
int32_t tDeserializeSDropDbReq(void* buf, int32_t bufLen, SDropDbReq* pReq);
void    tFreeSDropDbReq(SDropDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t uid;
} SDropDbRsp;

int32_t tSerializeSDropDbRsp(void* buf, int32_t bufLen, SDropDbRsp* pRsp);
int32_t tDeserializeSDropDbRsp(void* buf, int32_t bufLen, SDropDbRsp* pRsp);

typedef struct {
  char    name[TSDB_MOUNT_NAME_LEN];
  int64_t uid;
} SDropMountRsp;

int32_t tSerializeSDropMountRsp(void* buf, int32_t bufLen, SDropMountRsp* pRsp);
int32_t tDeserializeSDropMountRsp(void* buf, int32_t bufLen, SDropMountRsp* pRsp);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t vgVersion;
  int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
  int64_t stateTs;     // ms
} SUseDbReq;

int32_t tSerializeSUseDbReq(void* buf, int32_t bufLen, SUseDbReq* pReq);
int32_t tDeserializeSUseDbReq(void* buf, int32_t bufLen, SUseDbReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t uid;
  int32_t vgVersion;
  int32_t vgNum;
  int16_t hashPrefix;
  int16_t hashSuffix;
  int8_t  hashMethod;
  union {
    uint8_t flags;
    struct {
      uint8_t isMount : 1;  // TS-5868
      uint8_t padding : 7;
    };
  };
  SArray* pVgroupInfos;  // Array of SVgroupInfo
  int32_t errCode;
  int64_t stateTs;  // ms
} SUseDbRsp;

int32_t tSerializeSUseDbRsp(void* buf, int32_t bufLen, const SUseDbRsp* pRsp);
int32_t tDeserializeSUseDbRsp(void* buf, int32_t bufLen, SUseDbRsp* pRsp);
int32_t tSerializeSUseDbRspImp(SEncoder* pEncoder, const SUseDbRsp* pRsp);
int32_t tDeserializeSUseDbRspImp(SDecoder* pDecoder, SUseDbRsp* pRsp);
void    tFreeSUsedbRsp(SUseDbRsp* pRsp);

typedef struct {
  char db[TSDB_DB_FNAME_LEN];
} SDbCfgReq;

int32_t tSerializeSDbCfgReq(void* buf, int32_t bufLen, SDbCfgReq* pReq);
int32_t tDeserializeSDbCfgReq(void* buf, int32_t bufLen, SDbCfgReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int32_t maxSpeed;
} STrimDbReq;

int32_t tSerializeSTrimDbReq(void* buf, int32_t bufLen, STrimDbReq* pReq);
int32_t tDeserializeSTrimDbReq(void* buf, int32_t bufLen, STrimDbReq* pReq);

typedef struct {
  int32_t timestamp;
} SVTrimDbReq;

int32_t tSerializeSVTrimDbReq(void* buf, int32_t bufLen, SVTrimDbReq* pReq);
int32_t tDeserializeSVTrimDbReq(void* buf, int32_t bufLen, SVTrimDbReq* pReq);

typedef struct {
  char db[TSDB_DB_FNAME_LEN];
} SSsMigrateDbReq;

int32_t tSerializeSSsMigrateDbReq(void* buf, int32_t bufLen, SSsMigrateDbReq* pReq);
int32_t tDeserializeSSsMigrateDbReq(void* buf, int32_t bufLen, SSsMigrateDbReq* pReq);

typedef struct {
  int32_t ssMigrateId;
  bool    bAccepted;
} SSsMigrateDbRsp;

int32_t tSerializeSSsMigrateDbRsp(void* buf, int32_t bufLen, SSsMigrateDbRsp* pRsp);
int32_t tDeserializeSSsMigrateDbRsp(void* buf, int32_t bufLen, SSsMigrateDbRsp* pRsp);

typedef struct {
  int32_t ssMigrateId;
  int32_t nodeId; // node id of the leader vnode
  int64_t timestamp;
} SSsMigrateVgroupReq;

int32_t tSerializeSSsMigrateVgroupReq(void* buf, int32_t bufLen, SSsMigrateVgroupReq* pReq);
int32_t tDeserializeSSsMigrateVgroupReq(void* buf, int32_t bufLen, SSsMigrateVgroupReq* pReq);

typedef struct {
  int32_t ssMigrateId;
  int32_t vgId;   // vgroup id
  int32_t nodeId; // node id of the leader vnode
} SSsMigrateVgroupRsp;

int32_t tSerializeSSsMigrateVgroupRsp(void* buf, int32_t bufLen, SSsMigrateVgroupRsp* pRsp);
int32_t tDeserializeSSsMigrateVgroupRsp(void* buf, int32_t bufLen, SSsMigrateVgroupRsp* pRsp);

typedef struct {
  int32_t ssMigrateId;
  int32_t vgId;
} SQuerySsMigrateProgressReq;

int32_t tSerializeSQuerySsMigrateProgressReq(void* buf, int32_t bufLen, SQuerySsMigrateProgressReq* pReq);
int32_t tDeserializeSQuerySsMigrateProgressReq(void* buf, int32_t bufLen, SQuerySsMigrateProgressReq* pReq);

#define FILE_SET_MIGRATE_STATE_IN_PROGRESS  0
#define FILE_SET_MIGRATE_STATE_SUCCEEDED    1
#define FILE_SET_MIGRATE_STATE_SKIPPED      2
#define FILE_SET_MIGRATE_STATE_FAILED       3

typedef struct {
  int32_t fid;  // file set id
  int32_t state;
} SFileSetSsMigrateState;

typedef struct {
  // if all taosd were restarted during the migration, then vnode migrate id will be 0,
  // that's mnode will repeatedly request the migration progress. to avoid this, we need
  // to save mnode migrate id in the response, so that mnode can drop it.
  int32_t mnodeMigrateId; // migrate id requested by mnode
  int32_t vnodeMigrateId; // migrate id reponsed by vnode
  int32_t dnodeId;
  int32_t vgId;
  int64_t startTimeSec;
  SArray* pFileSetStates;
} SVnodeSsMigrateState;

int32_t tSerializeSVnodeSsMigrateState(void* buf, int32_t bufLen, SVnodeSsMigrateState* pState);
int32_t tDeserializeSVnodeSsMigrateState(void* buf, int32_t bufLen, SVnodeSsMigrateState* pState);
void tFreeSVnodeSsMigrateState(SVnodeSsMigrateState* pState);

typedef SVnodeSsMigrateState SQuerySsMigrateProgressRsp;
#define tSerializeSQuerySsMigrateProgressRsp(buf, bufLen, pRsp) \
  tSerializeSVnodeSsMigrateState(buf, bufLen, pRsp)
#define tDeserializeSQuerySsMigrateProgressRsp(buf, bufLen, pRsp) \
  tDeserializeSVnodeSsMigrateState(buf, bufLen, pRsp)
#define tFreeSQuerySsMigrateProgressRsp(pRsp) \
  tFreeSVnodeSsMigrateState(pRsp)

typedef SVnodeSsMigrateState SFollowerSsMigrateReq;
#define tSerializeSFollowerSsMigrateReq(buf, bufLen, pReq) \
  tSerializeSVnodeSsMigrateState(buf, bufLen, pReq)
#define tDeserializeSFollowerSsMigrateReq(buf, bufLen, pReq) \
  tDeserializeSVnodeSsMigrateState(buf, bufLen, pReq)
#define tFreeSFollowerSsMigrateReq(pReq) \
  tFreeSVnodeSsMigrateState(pReq)

typedef struct {
  int32_t timestampSec;
  int32_t ttlDropMaxCount;
  int32_t nUids;
  SArray* pTbUids;
} SVDropTtlTableReq;

int32_t tSerializeSVDropTtlTableReq(void* buf, int32_t bufLen, SVDropTtlTableReq* pReq);
int32_t tDeserializeSVDropTtlTableReq(void* buf, int32_t bufLen, SVDropTtlTableReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  int64_t dbId;
  int32_t cfgVersion;
  int32_t numOfVgroups;
  int32_t numOfStables;
  int32_t buffer;
  int32_t cacheSize;
  int32_t pageSize;
  int32_t pages;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t keepTimeOffset;
  int32_t minRows;
  int32_t maxRows;
  int32_t walFsyncPeriod;
  int16_t hashPrefix;
  int16_t hashSuffix;
  int8_t  hashMethod;
  int8_t  walLevel;
  int8_t  precision;
  int8_t  compression;
  int8_t  replications;
  int8_t  strict;
  int8_t  cacheLast;
  int8_t  encryptAlgorithm;
  int32_t ssChunkSize;
  int32_t ssKeepLocal;
  int8_t  ssCompact;
  union {
    uint8_t flags;
    struct {
      uint8_t isMount : 1;  // TS-5868
      uint8_t padding : 7;
    };
  };
  int8_t  compactTimeOffset;
  int32_t compactInterval;
  int32_t compactStartTime;
  int32_t compactEndTime;
  int32_t tsdbPageSize;
  int32_t walRetentionPeriod;
  int32_t walRollPeriod;
  int64_t walRetentionSize;
  int64_t walSegmentSize;
  int32_t numOfRetensions;
  SArray* pRetensions;
  int8_t  schemaless;
  int16_t sstTrigger;
  int8_t  withArbitrator;
} SDbCfgRsp;

typedef SDbCfgRsp SDbCfgInfo;

int32_t tSerializeSDbCfgRspImpl(SEncoder* encoder, const SDbCfgRsp* pRsp);
int32_t tSerializeSDbCfgRsp(void* buf, int32_t bufLen, const SDbCfgRsp* pRsp);
int32_t tDeserializeSDbCfgRsp(void* buf, int32_t bufLen, SDbCfgRsp* pRsp);
int32_t tDeserializeSDbCfgRspImpl(SDecoder* decoder, SDbCfgRsp* pRsp);
void    tFreeSDbCfgRsp(SDbCfgRsp* pRsp);

typedef struct {
  int32_t rowNum;
} SQnodeListReq;

int32_t tSerializeSQnodeListReq(void* buf, int32_t bufLen, SQnodeListReq* pReq);
int32_t tDeserializeSQnodeListReq(void* buf, int32_t bufLen, SQnodeListReq* pReq);

typedef struct {
  int32_t rowNum;
} SDnodeListReq;

int32_t tSerializeSDnodeListReq(void* buf, int32_t bufLen, SDnodeListReq* pReq);

typedef struct {
  int32_t useless;  // useless
} SServerVerReq;

int32_t tSerializeSServerVerReq(void* buf, int32_t bufLen, SServerVerReq* pReq);
// int32_t tDeserializeSServerVerReq(void* buf, int32_t bufLen, SServerVerReq* pReq);

typedef struct {
  char ver[TSDB_VERSION_LEN];
} SServerVerRsp;

int32_t tSerializeSServerVerRsp(void* buf, int32_t bufLen, SServerVerRsp* pRsp);
int32_t tDeserializeSServerVerRsp(void* buf, int32_t bufLen, SServerVerRsp* pRsp);

typedef struct SQueryNodeAddr {
  int32_t nodeId;  // vgId or qnodeId
  SEpSet  epSet;
} SQueryNodeAddr;

typedef struct {
  SQueryNodeAddr addr;
  uint64_t       load;
} SQueryNodeLoad;

typedef struct {
  SArray* qnodeList;  // SArray<SQueryNodeLoad>
} SQnodeListRsp;

int32_t tSerializeSQnodeListRsp(void* buf, int32_t bufLen, SQnodeListRsp* pRsp);
int32_t tDeserializeSQnodeListRsp(void* buf, int32_t bufLen, SQnodeListRsp* pRsp);
void    tFreeSQnodeListRsp(SQnodeListRsp* pRsp);

typedef struct SDNodeAddr {
  int32_t nodeId;  // dnodeId
  SEpSet  epSet;
} SDNodeAddr;

typedef struct {
  SArray* dnodeList;  // SArray<SDNodeAddr>
} SDnodeListRsp;

int32_t tSerializeSDnodeListRsp(void* buf, int32_t bufLen, SDnodeListRsp* pRsp);
int32_t tDeserializeSDnodeListRsp(void* buf, int32_t bufLen, SDnodeListRsp* pRsp);
void    tFreeSDnodeListRsp(SDnodeListRsp* pRsp);

typedef struct {
  SArray* pTsmas;  // SArray<STableTSMAInfo*>
} STableTSMAInfoRsp;

typedef struct {
  SUseDbRsp*         useDbRsp;
  SDbCfgRsp*         cfgRsp;
  STableTSMAInfoRsp* pTsmaRsp;
  int32_t            dbTsmaVersion;
  char               db[TSDB_DB_FNAME_LEN];
  int64_t            dbId;
} SDbHbRsp;

typedef struct {
  SArray* pArray;  // Array of SDbHbRsp
} SDbHbBatchRsp;

int32_t tSerializeSDbHbBatchRsp(void* buf, int32_t bufLen, SDbHbBatchRsp* pRsp);
int32_t tDeserializeSDbHbBatchRsp(void* buf, int32_t bufLen, SDbHbBatchRsp* pRsp);
void    tFreeSDbHbBatchRsp(SDbHbBatchRsp* pRsp);

typedef struct {
  SArray* pArray;  // Array of SGetUserAuthRsp
} SUserAuthBatchRsp;

int32_t tSerializeSUserAuthBatchRsp(void* buf, int32_t bufLen, SUserAuthBatchRsp* pRsp);
int32_t tDeserializeSUserAuthBatchRsp(void* buf, int32_t bufLen, SUserAuthBatchRsp* pRsp);
void    tFreeSUserAuthBatchRsp(SUserAuthBatchRsp* pRsp);

typedef struct {
  char        db[TSDB_DB_FNAME_LEN];
  STimeWindow timeRange;
  int32_t     sqlLen;
  char*       sql;
  SArray*     vgroupIds;
  int8_t      metaOnly;
} SCompactDbReq;

int32_t tSerializeSCompactDbReq(void* buf, int32_t bufLen, SCompactDbReq* pReq);
int32_t tDeserializeSCompactDbReq(void* buf, int32_t bufLen, SCompactDbReq* pReq);
void    tFreeSCompactDbReq(SCompactDbReq* pReq);

typedef struct {
  int32_t compactId;
  int8_t  bAccepted;
} SCompactDbRsp;

int32_t tSerializeSCompactDbRsp(void* buf, int32_t bufLen, SCompactDbRsp* pRsp);
int32_t tDeserializeSCompactDbRsp(void* buf, int32_t bufLen, SCompactDbRsp* pRsp);

typedef struct {
  int32_t compactId;
  int32_t sqlLen;
  char*   sql;
} SKillCompactReq;

int32_t tSerializeSKillCompactReq(void* buf, int32_t bufLen, SKillCompactReq* pReq);
int32_t tDeserializeSKillCompactReq(void* buf, int32_t bufLen, SKillCompactReq* pReq);
void    tFreeSKillCompactReq(SKillCompactReq* pReq);

typedef struct {
  char    name[TSDB_FUNC_NAME_LEN];
  int8_t  igExists;
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
  int32_t codeLen;
  int64_t signature;
  char*   pComment;
  char*   pCode;
  int8_t  orReplace;
} SCreateFuncReq;

int32_t tSerializeSCreateFuncReq(void* buf, int32_t bufLen, SCreateFuncReq* pReq);
int32_t tDeserializeSCreateFuncReq(void* buf, int32_t bufLen, SCreateFuncReq* pReq);
void    tFreeSCreateFuncReq(SCreateFuncReq* pReq);

typedef struct {
  char   name[TSDB_FUNC_NAME_LEN];
  int8_t igNotExists;
} SDropFuncReq;

int32_t tSerializeSDropFuncReq(void* buf, int32_t bufLen, SDropFuncReq* pReq);
int32_t tDeserializeSDropFuncReq(void* buf, int32_t bufLen, SDropFuncReq* pReq);

typedef struct {
  int32_t numOfFuncs;
  bool    ignoreCodeComment;
  SArray* pFuncNames;
} SRetrieveFuncReq;

int32_t tSerializeSRetrieveFuncReq(void* buf, int32_t bufLen, SRetrieveFuncReq* pReq);
int32_t tDeserializeSRetrieveFuncReq(void* buf, int32_t bufLen, SRetrieveFuncReq* pReq);
void    tFreeSRetrieveFuncReq(SRetrieveFuncReq* pReq);

typedef struct {
  char    name[TSDB_FUNC_NAME_LEN];
  int8_t  funcType;
  int8_t  scriptType;
  int8_t  outputType;
  int32_t outputLen;
  int32_t bufSize;
  int64_t signature;
  int32_t commentSize;
  int32_t codeSize;
  char*   pComment;
  char*   pCode;
} SFuncInfo;

typedef struct {
  int32_t funcVersion;
  int64_t funcCreatedTime;
} SFuncExtraInfo;

typedef struct {
  int32_t numOfFuncs;
  SArray* pFuncInfos;
  SArray* pFuncExtraInfos;
} SRetrieveFuncRsp;

int32_t tSerializeSRetrieveFuncRsp(void* buf, int32_t bufLen, SRetrieveFuncRsp* pRsp);
int32_t tDeserializeSRetrieveFuncRsp(void* buf, int32_t bufLen, SRetrieveFuncRsp* pRsp);
void    tFreeSFuncInfo(SFuncInfo* pInfo);
void    tFreeSRetrieveFuncRsp(SRetrieveFuncRsp* pRsp);

typedef struct {
  int32_t       statusInterval;
  int64_t       checkTime;                  // 1970-01-01 00:00:00.000
  char          timezone[TD_TIMEZONE_LEN];  // tsTimezone
  char          locale[TD_LOCALE_LEN];      // tsLocale
  char          charset[TD_LOCALE_LEN];     // tsCharset
  int8_t        ttlChangeOnWrite;
  int8_t        enableWhiteList;
  int8_t        encryptionKeyStat;
  uint32_t      encryptionKeyChksum;
  SMonitorParas monitorParas;
} SClusterCfg;

typedef struct {
  int32_t openVnodes;
  int32_t dropVnodes;
  int32_t totalVnodes;
  int32_t masterNum;
  int64_t numOfSelectReqs;
  int64_t numOfInsertReqs;
  int64_t numOfInsertSuccessReqs;
  int64_t numOfBatchInsertReqs;
  int64_t numOfBatchInsertSuccessReqs;
  int64_t errors;
} SVnodesStat;

typedef struct {
  int32_t vgId;
  int8_t  syncState;
  int8_t  syncRestore;
  int64_t syncTerm;
  int64_t roleTimeMs;
  int64_t startTimeMs;
  int8_t  syncCanRead;
  int64_t cacheUsage;
  int64_t numOfTables;
  int64_t numOfTimeSeries;
  int64_t totalStorage;
  int64_t compStorage;
  int64_t pointsWritten;
  int64_t numOfSelectReqs;
  int64_t numOfInsertReqs;
  int64_t numOfInsertSuccessReqs;
  int64_t numOfBatchInsertReqs;
  int64_t numOfBatchInsertSuccessReqs;
  int32_t numOfCachedTables;
  int32_t learnerProgress;  // use one reservered
  int64_t syncAppliedIndex;
  int64_t syncCommitIndex;
  int64_t bufferSegmentUsed;
  int64_t bufferSegmentSize;
} SVnodeLoad;

typedef struct {
  int64_t total_requests;
  int64_t total_rows;
  int64_t total_bytes;
  double  write_size;
  double  cache_hit_ratio;
  int64_t rpc_queue_wait;
  int64_t preprocess_time;

  int64_t memory_table_size;
  int64_t commit_count;
  int64_t merge_count;
  double  commit_time;
  double  merge_time;
  int64_t block_commit_time;
  int64_t memtable_wait_time;
} SVnodeMetrics;

typedef struct {
  int32_t     vgId;
  int64_t     numOfTables;
  int64_t     memSize;
  int64_t     l1Size;
  int64_t     l2Size;
  int64_t     l3Size;
  int64_t     cacheSize;
  int64_t     walSize;
  int64_t     metaSize;
  int64_t     rawDataSize;
  int64_t     ssSize;
  const char* dbname;
} SDbSizeStatisInfo;

typedef struct {
  int32_t vgId;
  int64_t nTimeSeries;
} SVnodeLoadLite;

typedef struct {
  int8_t  syncState;
  int64_t syncTerm;
  int8_t  syncRestore;
  int64_t roleTimeMs;
} SMnodeLoad;

typedef struct {
  int32_t dnodeId;
  int64_t numOfProcessedQuery;
  int64_t numOfProcessedCQuery;
  int64_t numOfProcessedFetch;
  int64_t numOfProcessedDrop;
  int64_t numOfProcessedNotify;
  int64_t numOfProcessedHb;
  int64_t numOfProcessedDelete;
  int64_t cacheDataSize;
  int64_t numOfQueryInQueue;
  int64_t numOfFetchInQueue;
  int64_t timeInQueryQueue;
  int64_t timeInFetchQueue;
} SQnodeLoad;

typedef struct {
  int32_t     sver;      // software version
  int64_t     dnodeVer;  // dnode table version in sdb
  int32_t     dnodeId;
  int64_t     clusterId;
  int64_t     rebootTime;
  int64_t     updateTime;
  float       numOfCores;
  int32_t     numOfSupportVnodes;
  int32_t     numOfDiskCfg;
  int64_t     memTotal;
  int64_t     memAvail;
  char        dnodeEp[TSDB_EP_LEN];
  char        machineId[TSDB_MACHINE_ID_LEN + 1];
  SMnodeLoad  mload;
  SQnodeLoad  qload;
  SClusterCfg clusterCfg;
  SArray*     pVloads;  // array of SVnodeLoad
  int32_t     statusSeq;
  int64_t     ipWhiteVer;
  int64_t     analVer;
  int64_t     timestamp;
} SStatusReq;

int32_t tSerializeSStatusReq(void* buf, int32_t bufLen, SStatusReq* pReq);
int32_t tDeserializeSStatusReq(void* buf, int32_t bufLen, SStatusReq* pReq);
void    tFreeSStatusReq(SStatusReq* pReq);

typedef struct {
  int32_t forceReadConfig;
  int32_t cver;
  SArray* array;
} SConfigReq;

int32_t tSerializeSConfigReq(void* buf, int32_t bufLen, SConfigReq* pReq);
int32_t tDeserializeSConfigReq(void* buf, int32_t bufLen, SConfigReq* pReq);
void    tFreeSConfigReq(SConfigReq* pReq);

typedef struct {
  int32_t dnodeId;
  char    machineId[TSDB_MACHINE_ID_LEN + 1];
} SDnodeInfoReq;

int32_t tSerializeSDnodeInfoReq(void* buf, int32_t bufLen, SDnodeInfoReq* pReq);
int32_t tDeserializeSDnodeInfoReq(void* buf, int32_t bufLen, SDnodeInfoReq* pReq);

typedef enum {
  MONITOR_TYPE_COUNTER = 0,
  MONITOR_TYPE_SLOW_LOG = 1,
} MONITOR_TYPE;

typedef struct {
  int32_t      contLen;
  char*        pCont;
  MONITOR_TYPE type;
} SStatisReq;

int32_t tSerializeSStatisReq(void* buf, int32_t bufLen, SStatisReq* pReq);
int32_t tDeserializeSStatisReq(void* buf, int32_t bufLen, SStatisReq* pReq);
void    tFreeSStatisReq(SStatisReq* pReq);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  char    table[TSDB_TABLE_NAME_LEN];
  char    operation[AUDIT_OPERATION_LEN];
  int32_t sqlLen;
  char*   pSql;
} SAuditReq;
int32_t tSerializeSAuditReq(void* buf, int32_t bufLen, SAuditReq* pReq);
int32_t tDeserializeSAuditReq(void* buf, int32_t bufLen, SAuditReq* pReq);
void    tFreeSAuditReq(SAuditReq* pReq);

typedef struct {
  int32_t dnodeId;
  int64_t clusterId;
  SArray* pVloads;
} SNotifyReq;

int32_t tSerializeSNotifyReq(void* buf, int32_t bufLen, SNotifyReq* pReq);
int32_t tDeserializeSNotifyReq(void* buf, int32_t bufLen, SNotifyReq* pReq);
void    tFreeSNotifyReq(SNotifyReq* pReq);

typedef struct {
  int32_t dnodeId;
  int64_t clusterId;
} SDnodeCfg;

typedef struct {
  int32_t id;
  int8_t  isMnode;
  SEp     ep;
} SDnodeEp;

typedef struct {
  int32_t id;
  int8_t  isMnode;
  int8_t  offlineReason;
  SEp     ep;
  char    active[TSDB_ACTIVE_KEY_LEN];
  char    connActive[TSDB_CONN_ACTIVE_KEY_LEN];
} SDnodeInfo;

typedef struct {
  int64_t   dnodeVer;
  SDnodeCfg dnodeCfg;
  SArray*   pDnodeEps;  // Array of SDnodeEp
  int32_t   statusSeq;
  int64_t   ipWhiteVer;
  int64_t   analVer;
} SStatusRsp;

int32_t tSerializeSStatusRsp(void* buf, int32_t bufLen, SStatusRsp* pRsp);
int32_t tDeserializeSStatusRsp(void* buf, int32_t bufLen, SStatusRsp* pRsp);
void    tFreeSStatusRsp(SStatusRsp* pRsp);

typedef struct {
  int32_t forceReadConfig;
  int32_t isConifgVerified;
  int32_t isVersionVerified;
  int32_t cver;
  SArray* array;
} SConfigRsp;

int32_t tSerializeSConfigRsp(void* buf, int32_t bufLen, SConfigRsp* pRsp);
int32_t tDeserializeSConfigRsp(void* buf, int32_t bufLen, SConfigRsp* pRsp);
void    tFreeSConfigRsp(SConfigRsp* pRsp);

typedef struct {
  int32_t reserved;
} SMTimerReq;

int32_t tSerializeSMTimerMsg(void* buf, int32_t bufLen, SMTimerReq* pReq);
// int32_t tDeserializeSMTimerMsg(void* buf, int32_t bufLen, SMTimerReq* pReq);

typedef struct SOrphanTask {
  int64_t streamId;
  int32_t taskId;
  int32_t nodeId;
} SOrphanTask;

typedef struct SMStreamDropOrphanMsg {
  SArray* pList;  // SArray<SOrphanTask>
} SMStreamDropOrphanMsg;

int32_t tSerializeDropOrphanTaskMsg(void* buf, int32_t bufLen, SMStreamDropOrphanMsg* pMsg);
int32_t tDeserializeDropOrphanTaskMsg(void* buf, int32_t bufLen, SMStreamDropOrphanMsg* pMsg);
void    tDestroyDropOrphanTaskMsg(SMStreamDropOrphanMsg* pMsg);

typedef struct {
  int32_t  id;
  uint16_t port;                 // node sync Port
  char     fqdn[TSDB_FQDN_LEN];  // node FQDN
} SReplica;

typedef struct {
  int32_t  vgId;
  char     db[TSDB_DB_FNAME_LEN];
  int64_t  dbUid;
  int32_t  vgVersion;
  int32_t  numOfStables;
  int32_t  buffer;
  int32_t  pageSize;
  int32_t  pages;
  int32_t  cacheLastSize;
  int32_t  daysPerFile;
  int32_t  daysToKeep0;
  int32_t  daysToKeep1;
  int32_t  daysToKeep2;
  int32_t  keepTimeOffset;
  int32_t  minRows;
  int32_t  maxRows;
  int32_t  walFsyncPeriod;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int8_t   hashMethod;
  int8_t   walLevel;
  int8_t   precision;
  int8_t   compression;
  int8_t   strict;
  int8_t   cacheLast;
  int8_t   isTsma;
  int8_t   replica;
  int8_t   selfIndex;
  SReplica replicas[TSDB_MAX_REPLICA];
  int32_t  numOfRetensions;
  SArray*  pRetensions;  // SRetention
  void*    pTsma;
  int32_t  walRetentionPeriod;
  int64_t  walRetentionSize;
  int32_t  walRollPeriod;
  int64_t  walSegmentSize;
  int16_t  sstTrigger;
  int16_t  hashPrefix;
  int16_t  hashSuffix;
  int32_t  tsdbPageSize;
  int32_t  ssChunkSize;
  int32_t  ssKeepLocal;
  int8_t   ssCompact;
  int64_t  reserved[6];
  int8_t   learnerReplica;
  int8_t   learnerSelfIndex;
  SReplica learnerReplicas[TSDB_MAX_LEARNER_REPLICA];
  int32_t  changeVersion;
  int8_t   encryptAlgorithm;
} SCreateVnodeReq;

int32_t tSerializeSCreateVnodeReq(void* buf, int32_t bufLen, SCreateVnodeReq* pReq);
int32_t tDeserializeSCreateVnodeReq(void* buf, int32_t bufLen, SCreateVnodeReq* pReq);
int32_t tFreeSCreateVnodeReq(SCreateVnodeReq* pReq);

typedef struct {
  int32_t compactId;
  int32_t vgId;
  int32_t dnodeId;
} SQueryCompactProgressReq;

int32_t tSerializeSQueryCompactProgressReq(void* buf, int32_t bufLen, SQueryCompactProgressReq* pReq);
int32_t tDeserializeSQueryCompactProgressReq(void* buf, int32_t bufLen, SQueryCompactProgressReq* pReq);

typedef struct {
  int32_t compactId;
  int32_t vgId;
  int32_t dnodeId;
  int32_t numberFileset;
  int32_t finished;
  int32_t progress;
  int64_t remainingTime;
} SQueryCompactProgressRsp;

int32_t tSerializeSQueryCompactProgressRsp(void* buf, int32_t bufLen, SQueryCompactProgressRsp* pReq);
int32_t tDeserializeSQueryCompactProgressRsp(void* buf, int32_t bufLen, SQueryCompactProgressRsp* pReq);

typedef struct {
  int32_t vgId;
  int32_t dnodeId;
  int64_t dbUid;
  char    db[TSDB_DB_FNAME_LEN];
  int64_t reserved[8];
} SDropVnodeReq;

int32_t tSerializeSDropVnodeReq(void* buf, int32_t bufLen, SDropVnodeReq* pReq);
int32_t tDeserializeSDropVnodeReq(void* buf, int32_t bufLen, SDropVnodeReq* pReq);

typedef struct {
  char    colName[TSDB_COL_NAME_LEN];
  char    stb[TSDB_TABLE_FNAME_LEN];
  int64_t stbUid;
  int64_t dbUid;
  int64_t reserved[8];
} SDropIndexReq;

int32_t tSerializeSDropIdxReq(void* buf, int32_t bufLen, SDropIndexReq* pReq);
int32_t tDeserializeSDropIdxReq(void* buf, int32_t bufLen, SDropIndexReq* pReq);

typedef struct {
  int64_t     dbUid;
  char        db[TSDB_DB_FNAME_LEN];
  int64_t     compactStartTime;
  STimeWindow tw;
  int32_t     compactId;
  int8_t      metaOnly;
} SCompactVnodeReq;

int32_t tSerializeSCompactVnodeReq(void* buf, int32_t bufLen, SCompactVnodeReq* pReq);
int32_t tDeserializeSCompactVnodeReq(void* buf, int32_t bufLen, SCompactVnodeReq* pReq);

typedef struct {
  int32_t compactId;
  int32_t vgId;
  int32_t dnodeId;
} SVKillCompactReq;

int32_t tSerializeSVKillCompactReq(void* buf, int32_t bufLen, SVKillCompactReq* pReq);
int32_t tDeserializeSVKillCompactReq(void* buf, int32_t bufLen, SVKillCompactReq* pReq);

typedef struct {
  int32_t vgVersion;
  int32_t buffer;
  int32_t pageSize;
  int32_t pages;
  int32_t cacheLastSize;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t keepTimeOffset;
  int32_t walFsyncPeriod;
  int8_t  walLevel;
  int8_t  strict;
  int8_t  cacheLast;
  int64_t reserved[7];
  // 1st modification
  int16_t sttTrigger;
  int32_t minRows;
  // 2nd modification
  int32_t walRetentionPeriod;
  int32_t walRetentionSize;
  int32_t ssKeepLocal;
  int8_t  ssCompact;
} SAlterVnodeConfigReq;

int32_t tSerializeSAlterVnodeConfigReq(void* buf, int32_t bufLen, SAlterVnodeConfigReq* pReq);
int32_t tDeserializeSAlterVnodeConfigReq(void* buf, int32_t bufLen, SAlterVnodeConfigReq* pReq);

typedef struct {
  int32_t  vgId;
  int8_t   strict;
  int8_t   selfIndex;
  int8_t   replica;
  SReplica replicas[TSDB_MAX_REPLICA];
  int64_t  reserved[8];
  int8_t   learnerSelfIndex;
  int8_t   learnerReplica;
  SReplica learnerReplicas[TSDB_MAX_LEARNER_REPLICA];
  int32_t  changeVersion;
} SAlterVnodeReplicaReq, SAlterVnodeTypeReq, SCheckLearnCatchupReq;

int32_t tSerializeSAlterVnodeReplicaReq(void* buf, int32_t bufLen, SAlterVnodeReplicaReq* pReq);
int32_t tDeserializeSAlterVnodeReplicaReq(void* buf, int32_t bufLen, SAlterVnodeReplicaReq* pReq);

typedef struct {
  int32_t vgId;
  int8_t  disable;
} SDisableVnodeWriteReq;

int32_t tSerializeSDisableVnodeWriteReq(void* buf, int32_t bufLen, SDisableVnodeWriteReq* pReq);
int32_t tDeserializeSDisableVnodeWriteReq(void* buf, int32_t bufLen, SDisableVnodeWriteReq* pReq);

typedef struct {
  int32_t  srcVgId;
  int32_t  dstVgId;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int32_t  changeVersion;
  int32_t  reserved;
} SAlterVnodeHashRangeReq;

int32_t tSerializeSAlterVnodeHashRangeReq(void* buf, int32_t bufLen, SAlterVnodeHashRangeReq* pReq);
int32_t tDeserializeSAlterVnodeHashRangeReq(void* buf, int32_t bufLen, SAlterVnodeHashRangeReq* pReq);

#define REQ_OPT_TBNAME 0x0
#define REQ_OPT_TBUID  0x01
typedef struct {
  SMsgHead header;
  char     dbFName[TSDB_DB_FNAME_LEN];
  char     tbName[TSDB_TABLE_NAME_LEN];
  uint8_t  option;
  uint8_t  autoCreateCtb;
} STableInfoReq;

int32_t tSerializeSTableInfoReq(void* buf, int32_t bufLen, STableInfoReq* pReq);
int32_t tDeserializeSTableInfoReq(void* buf, int32_t bufLen, STableInfoReq* pReq);

typedef struct {
  int8_t  metaClone;  // create local clone of the cached table meta
  int32_t numOfVgroups;
  int32_t numOfTables;
  int32_t numOfUdfs;
  char    tableNames[];
} SMultiTableInfoReq;

// todo refactor
typedef struct SVgroupInfo {
  int32_t  vgId;
  uint32_t hashBegin;
  uint32_t hashEnd;
  SEpSet   epSet;
  union {
    int32_t numOfTable;  // unit is TSDB_TABLE_NUM_UNIT
    int32_t taskId;      // used in stream
  };
} SVgroupInfo;

typedef struct {
  int32_t     numOfVgroups;
  SVgroupInfo vgroups[];
} SVgroupsInfo;

typedef struct {
  STableMetaRsp* pMeta;
} SMAlterStbRsp;

int32_t tEncodeSMAlterStbRsp(SEncoder* pEncoder, const SMAlterStbRsp* pRsp);
int32_t tDecodeSMAlterStbRsp(SDecoder* pDecoder, SMAlterStbRsp* pRsp);
void    tFreeSMAlterStbRsp(SMAlterStbRsp* pRsp);

int32_t tSerializeSTableMetaRsp(void* buf, int32_t bufLen, STableMetaRsp* pRsp);
int32_t tDeserializeSTableMetaRsp(void* buf, int32_t bufLen, STableMetaRsp* pRsp);
void    tFreeSTableMetaRsp(void* pRsp);
void    tFreeSTableIndexRsp(void* info);

typedef struct {
  SArray* pMetaRsp;   // Array of STableMetaRsp
  SArray* pIndexRsp;  // Array of STableIndexRsp;
} SSTbHbRsp;

int32_t tSerializeSSTbHbRsp(void* buf, int32_t bufLen, SSTbHbRsp* pRsp);
int32_t tDeserializeSSTbHbRsp(void* buf, int32_t bufLen, SSTbHbRsp* pRsp);
void    tFreeSSTbHbRsp(SSTbHbRsp* pRsp);

typedef struct {
  SArray* pViewRsp;  // Array of SViewMetaRsp*;
} SViewHbRsp;

int32_t tSerializeSViewHbRsp(void* buf, int32_t bufLen, SViewHbRsp* pRsp);
int32_t tDeserializeSViewHbRsp(void* buf, int32_t bufLen, SViewHbRsp* pRsp);
void    tFreeSViewHbRsp(SViewHbRsp* pRsp);

typedef struct {
  int32_t numOfTables;
  int32_t numOfVgroup;
  int32_t numOfUdf;
  int32_t contLen;
  int8_t  compressed;  // denote if compressed or not
  int32_t rawLen;      // size before compress
  uint8_t metaClone;   // make meta clone after retrieve meta from mnode
  char    meta[];
} SMultiTableMeta;

typedef struct {
  int32_t dataLen;
  char    name[TSDB_TABLE_FNAME_LEN];
  char*   data;
} STagData;

typedef struct {
  int32_t  opType;
  uint32_t valLen;
  char*    val;
} SShowVariablesReq;

int32_t tSerializeSShowVariablesReq(void* buf, int32_t bufLen, SShowVariablesReq* pReq);
int32_t tDeserializeSShowVariablesReq(void* buf, int32_t bufLen, SShowVariablesReq* pReq);
void    tFreeSShowVariablesReq(SShowVariablesReq* pReq);

typedef struct {
  char name[TSDB_CONFIG_OPTION_LEN + 1];
  char value[TSDB_CONFIG_PATH_LEN + 1];
  char scope[TSDB_CONFIG_SCOPE_LEN + 1];
  char category[TSDB_CONFIG_CATEGORY_LEN + 1];
  char info[TSDB_CONFIG_INFO_LEN + 1];
} SVariablesInfo;

typedef struct {
  SArray* variables;  // SArray<SVariablesInfo>
} SShowVariablesRsp;

int32_t tSerializeSShowVariablesRsp(void* buf, int32_t bufLen, SShowVariablesRsp* pReq);
int32_t tDeserializeSShowVariablesRsp(void* buf, int32_t bufLen, SShowVariablesRsp* pReq);

void tFreeSShowVariablesRsp(SShowVariablesRsp* pRsp);

/*
 * sql: show tables like '%a_%'
 * payload is the query condition, e.g., '%a_%'
 * payloadLen is the length of payload
 */
typedef struct {
  int32_t type;
  char    db[TSDB_DB_FNAME_LEN];
  int32_t payloadLen;
  char*   payload;
} SShowReq;

int32_t tSerializeSShowReq(void* buf, int32_t bufLen, SShowReq* pReq);
// int32_t tDeserializeSShowReq(void* buf, int32_t bufLen, SShowReq* pReq);
void tFreeSShowReq(SShowReq* pReq);

typedef struct {
  int64_t       showId;
  STableMetaRsp tableMeta;
} SShowRsp, SVShowTablesRsp;

// int32_t tSerializeSShowRsp(void* buf, int32_t bufLen, SShowRsp* pRsp);
// int32_t tDeserializeSShowRsp(void* buf, int32_t bufLen, SShowRsp* pRsp);
// void    tFreeSShowRsp(SShowRsp* pRsp);

typedef struct {
  char    db[TSDB_DB_FNAME_LEN];
  char    tb[TSDB_TABLE_NAME_LEN];
  char    user[TSDB_USER_LEN];
  char    filterTb[TSDB_TABLE_NAME_LEN];  // for ins_columns
  int64_t showId;
  int64_t compactId;  // for compact
  bool    withFull;   // for show users full
} SRetrieveTableReq;

typedef struct SSysTableSchema {
  int8_t   type;
  col_id_t colId;
  int32_t  bytes;
} SSysTableSchema;

int32_t tSerializeSRetrieveTableReq(void* buf, int32_t bufLen, SRetrieveTableReq* pReq);
int32_t tDeserializeSRetrieveTableReq(void* buf, int32_t bufLen, SRetrieveTableReq* pReq);

#define RETRIEVE_TABLE_RSP_VERSION         0
#define RETRIEVE_TABLE_RSP_TMQ_VERSION     1
#define RETRIEVE_TABLE_RSP_TMQ_RAW_VERSION 2

typedef struct {
  int64_t useconds;
  int8_t  completed;  // all results are returned to client
  int8_t  precision;
  int8_t  compressed;
  int8_t  streamBlockType;
  int32_t payloadLen;
  int32_t compLen;
  int32_t numOfBlocks;
  int64_t numOfRows;  // from int32_t change to int64_t
  int64_t numOfCols;
  int64_t skey;
  int64_t ekey;
  int64_t version;                         // for stream
  TSKEY   watermark;                       // for stream
  char    parTbName[TSDB_TABLE_NAME_LEN];  // for stream
  char    data[];
} SRetrieveTableRsp;

#define PAYLOAD_PREFIX_LEN ((sizeof(int32_t)) << 1)

#define SET_PAYLOAD_LEN(_p, _compLen, _fullLen) \
  do {                                          \
    ((int32_t*)(_p))[0] = (_compLen);           \
    ((int32_t*)(_p))[1] = (_fullLen);           \
  } while (0);

typedef struct {
  int64_t version;
  int64_t numOfRows;
  int8_t  compressed;
  int8_t  precision;
  char    data[];
} SRetrieveTableRspForTmq;

typedef struct {
  int64_t handle;
  int64_t useconds;
  int8_t  completed;  // all results are returned to client
  int8_t  precision;
  int8_t  compressed;
  int32_t compLen;
  int32_t numOfRows;
  int32_t fullLen;
  char    data[];
} SRetrieveMetaTableRsp;

typedef struct SExplainExecInfo {
  double   startupCost;
  double   totalCost;
  uint64_t numOfRows;
  uint32_t verboseLen;
  void*    verboseInfo;
} SExplainExecInfo;

typedef struct {
  int32_t           numOfPlans;
  SExplainExecInfo* subplanInfo;
} SExplainRsp;

typedef struct {
  SExplainRsp rsp;
  uint64_t    qId;
  uint64_t    cId;
  uint64_t    tId;
  int64_t     rId;
  int32_t     eId;
} SExplainLocalRsp;

typedef struct STableScanAnalyzeInfo {
  uint64_t totalRows;
  uint64_t totalCheckedRows;
  uint32_t totalBlocks;
  uint32_t loadBlocks;
  uint32_t loadBlockStatis;
  uint32_t skipBlocks;
  uint32_t filterOutBlocks;
  double   elapsedTime;
  double   filterTime;
} STableScanAnalyzeInfo;

int32_t tSerializeSExplainRsp(void* buf, int32_t bufLen, SExplainRsp* pRsp);
int32_t tDeserializeSExplainRsp(void* buf, int32_t bufLen, SExplainRsp* pRsp);
void    tFreeSExplainRsp(SExplainRsp* pRsp);

typedef struct {
  char    config[TSDB_DNODE_CONFIG_LEN];
  char    value[TSDB_CLUSTER_VALUE_LEN];
  int32_t sqlLen;
  char*   sql;
} SMCfgClusterReq;

int32_t tSerializeSMCfgClusterReq(void* buf, int32_t bufLen, SMCfgClusterReq* pReq);
int32_t tDeserializeSMCfgClusterReq(void* buf, int32_t bufLen, SMCfgClusterReq* pReq);
void    tFreeSMCfgClusterReq(SMCfgClusterReq* pReq);

typedef struct {
  char    fqdn[TSDB_FQDN_LEN];  // end point, hostname:port
  int32_t port;
  int32_t sqlLen;
  char*   sql;
} SCreateDnodeReq;

int32_t tSerializeSCreateDnodeReq(void* buf, int32_t bufLen, SCreateDnodeReq* pReq);
int32_t tDeserializeSCreateDnodeReq(void* buf, int32_t bufLen, SCreateDnodeReq* pReq);
void    tFreeSCreateDnodeReq(SCreateDnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  char    fqdn[TSDB_FQDN_LEN];
  int32_t port;
  int8_t  force;
  int8_t  unsafe;
  int32_t sqlLen;
  char*   sql;
} SDropDnodeReq;

int32_t tSerializeSDropDnodeReq(void* buf, int32_t bufLen, SDropDnodeReq* pReq);
int32_t tDeserializeSDropDnodeReq(void* buf, int32_t bufLen, SDropDnodeReq* pReq);
void    tFreeSDropDnodeReq(SDropDnodeReq* pReq);

enum {
  RESTORE_TYPE__ALL = 1,
  RESTORE_TYPE__MNODE,
  RESTORE_TYPE__VNODE,
  RESTORE_TYPE__QNODE,
};

typedef struct {
  int32_t dnodeId;
  int8_t  restoreType;
  int32_t sqlLen;
  char*   sql;
} SRestoreDnodeReq;

int32_t tSerializeSRestoreDnodeReq(void* buf, int32_t bufLen, SRestoreDnodeReq* pReq);
int32_t tDeserializeSRestoreDnodeReq(void* buf, int32_t bufLen, SRestoreDnodeReq* pReq);
void    tFreeSRestoreDnodeReq(SRestoreDnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  char    config[TSDB_DNODE_CONFIG_LEN];
  char    value[TSDB_DNODE_VALUE_LEN];
  int32_t sqlLen;
  char*   sql;
} SMCfgDnodeReq;

int32_t tSerializeSMCfgDnodeReq(void* buf, int32_t bufLen, SMCfgDnodeReq* pReq);
int32_t tDeserializeSMCfgDnodeReq(void* buf, int32_t bufLen, SMCfgDnodeReq* pReq);
void    tFreeSMCfgDnodeReq(SMCfgDnodeReq* pReq);

typedef struct {
  char    config[TSDB_DNODE_CONFIG_LEN];
  char    value[TSDB_DNODE_VALUE_LEN];
  int32_t version;
} SDCfgDnodeReq;

int32_t tSerializeSDCfgDnodeReq(void* buf, int32_t bufLen, SDCfgDnodeReq* pReq);
int32_t tDeserializeSDCfgDnodeReq(void* buf, int32_t bufLen, SDCfgDnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  int32_t sqlLen;
  char*   sql;
} SMCreateMnodeReq, SMDropMnodeReq, SDDropMnodeReq, SMCreateQnodeReq, SMDropQnodeReq, SDCreateQnodeReq, SDDropQnodeReq,
    SMCreateSnodeReq, SMDropSnodeReq,     SDDropSnodeReq;


int32_t tSerializeSCreateDropMQSNodeReq(void* buf, int32_t bufLen, SMCreateQnodeReq* pReq);
int32_t tDeserializeSCreateDropMQSNodeReq(void* buf, int32_t bufLen, SMCreateQnodeReq* pReq);


typedef struct {
  int32_t nodeId;
  SEpSet  epSet;
} SNodeEpSet;


typedef struct {
  int32_t    snodeId;
  SNodeEpSet leaders[2];
  SNodeEpSet replica;
  int32_t    sqlLen;
  char*      sql;
} SDCreateSnodeReq;

int32_t tSerializeSDCreateSNodeReq(void *buf, int32_t bufLen, SDCreateSnodeReq *pReq);
int32_t tDeserializeSDCreateSNodeReq(void *buf, int32_t bufLen, SDCreateSnodeReq *pReq);
void tFreeSDCreateSnodeReq(SDCreateSnodeReq *pReq);


void    tFreeSMCreateQnodeReq(SMCreateQnodeReq* pReq);
void    tFreeSDDropQnodeReq(SDDropQnodeReq* pReq);
typedef struct {
  int8_t   replica;
  SReplica replicas[TSDB_MAX_REPLICA];
  int8_t   learnerReplica;
  SReplica learnerReplicas[TSDB_MAX_LEARNER_REPLICA];
  int64_t  lastIndex;
} SDCreateMnodeReq, SDAlterMnodeReq, SDAlterMnodeTypeReq;

int32_t tSerializeSDCreateMnodeReq(void* buf, int32_t bufLen, SDCreateMnodeReq* pReq);
int32_t tDeserializeSDCreateMnodeReq(void* buf, int32_t bufLen, SDCreateMnodeReq* pReq);

typedef struct {
  int32_t urlLen;
  int32_t sqlLen;
  char*   url;
  char*   sql;
} SMCreateAnodeReq;

int32_t tSerializeSMCreateAnodeReq(void* buf, int32_t bufLen, SMCreateAnodeReq* pReq);
int32_t tDeserializeSMCreateAnodeReq(void* buf, int32_t bufLen, SMCreateAnodeReq* pReq);
void    tFreeSMCreateAnodeReq(SMCreateAnodeReq* pReq);

typedef struct {
  int32_t anodeId;
  int32_t sqlLen;
  char*   sql;
} SMDropAnodeReq, SMUpdateAnodeReq;

int32_t tSerializeSMDropAnodeReq(void* buf, int32_t bufLen, SMDropAnodeReq* pReq);
int32_t tDeserializeSMDropAnodeReq(void* buf, int32_t bufLen, SMDropAnodeReq* pReq);
void    tFreeSMDropAnodeReq(SMDropAnodeReq* pReq);
int32_t tSerializeSMUpdateAnodeReq(void* buf, int32_t bufLen, SMUpdateAnodeReq* pReq);
int32_t tDeserializeSMUpdateAnodeReq(void* buf, int32_t bufLen, SMUpdateAnodeReq* pReq);
void    tFreeSMUpdateAnodeReq(SMUpdateAnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  int32_t bnodeProto;
  int32_t sqlLen;
  char*   sql;
} SMCreateBnodeReq, SDCreateBnodeReq;

int32_t tSerializeSMCreateBnodeReq(void* buf, int32_t bufLen, SMCreateBnodeReq* pReq);
int32_t tDeserializeSMCreateBnodeReq(void* buf, int32_t bufLen, SMCreateBnodeReq* pReq);
void    tFreeSMCreateBnodeReq(SMCreateBnodeReq* pReq);

typedef struct {
  int32_t dnodeId;
  int32_t sqlLen;
  char*   sql;
} SMDropBnodeReq, SDDropBnodeReq;

int32_t tSerializeSMDropBnodeReq(void* buf, int32_t bufLen, SMDropBnodeReq* pReq);
int32_t tDeserializeSMDropBnodeReq(void* buf, int32_t bufLen, SMDropBnodeReq* pReq);
void    tFreeSMDropBnodeReq(SMDropBnodeReq* pReq);

typedef struct {
  int32_t vgId;
  int32_t hbSeq;
} SVArbHbReqMember;

typedef struct {
  int32_t dnodeId;
  char*   arbToken;
  int64_t arbTerm;
  SArray* hbMembers;  // SVArbHbReqMember
} SVArbHeartBeatReq;

int32_t tSerializeSVArbHeartBeatReq(void* buf, int32_t bufLen, SVArbHeartBeatReq* pReq);
int32_t tDeserializeSVArbHeartBeatReq(void* buf, int32_t bufLen, SVArbHeartBeatReq* pReq);
void    tFreeSVArbHeartBeatReq(SVArbHeartBeatReq* pReq);

typedef struct {
  int32_t vgId;
  char    memberToken[TSDB_ARB_TOKEN_SIZE];
  int32_t hbSeq;
} SVArbHbRspMember;

typedef struct {
  char    arbToken[TSDB_ARB_TOKEN_SIZE];
  int32_t dnodeId;
  SArray* hbMembers;  // SVArbHbRspMember
} SVArbHeartBeatRsp;

int32_t tSerializeSVArbHeartBeatRsp(void* buf, int32_t bufLen, SVArbHeartBeatRsp* pRsp);
int32_t tDeserializeSVArbHeartBeatRsp(void* buf, int32_t bufLen, SVArbHeartBeatRsp* pRsp);
void    tFreeSVArbHeartBeatRsp(SVArbHeartBeatRsp* pRsp);

typedef struct {
  char*   arbToken;
  int64_t arbTerm;
  char*   member0Token;
  char*   member1Token;
} SVArbCheckSyncReq;

int32_t tSerializeSVArbCheckSyncReq(void* buf, int32_t bufLen, SVArbCheckSyncReq* pReq);
int32_t tDeserializeSVArbCheckSyncReq(void* buf, int32_t bufLen, SVArbCheckSyncReq* pReq);
void    tFreeSVArbCheckSyncReq(SVArbCheckSyncReq* pRsp);

typedef struct {
  char*   arbToken;
  char*   member0Token;
  char*   member1Token;
  int32_t vgId;
  int32_t errCode;
} SVArbCheckSyncRsp;

int32_t tSerializeSVArbCheckSyncRsp(void* buf, int32_t bufLen, SVArbCheckSyncRsp* pRsp);
int32_t tDeserializeSVArbCheckSyncRsp(void* buf, int32_t bufLen, SVArbCheckSyncRsp* pRsp);
void    tFreeSVArbCheckSyncRsp(SVArbCheckSyncRsp* pRsp);

typedef struct {
  char*   arbToken;
  int64_t arbTerm;
  char*   memberToken;
  int8_t  force;
} SVArbSetAssignedLeaderReq;

int32_t tSerializeSVArbSetAssignedLeaderReq(void* buf, int32_t bufLen, SVArbSetAssignedLeaderReq* pReq);
int32_t tDeserializeSVArbSetAssignedLeaderReq(void* buf, int32_t bufLen, SVArbSetAssignedLeaderReq* pReq);
void    tFreeSVArbSetAssignedLeaderReq(SVArbSetAssignedLeaderReq* pReq);

typedef struct {
  char*   arbToken;
  char*   memberToken;
  int32_t vgId;
} SVArbSetAssignedLeaderRsp;

int32_t tSerializeSVArbSetAssignedLeaderRsp(void* buf, int32_t bufLen, SVArbSetAssignedLeaderRsp* pRsp);
int32_t tDeserializeSVArbSetAssignedLeaderRsp(void* buf, int32_t bufLen, SVArbSetAssignedLeaderRsp* pRsp);
void    tFreeSVArbSetAssignedLeaderRsp(SVArbSetAssignedLeaderRsp* pRsp);

typedef struct {
  int32_t dnodeId;
  char*   token;
} SMArbUpdateGroupMember;

typedef struct {
  int32_t dnodeId;
  char*   token;
  int8_t  acked;
} SMArbUpdateGroupAssigned;

typedef struct {
  int32_t                  vgId;
  int64_t                  dbUid;
  SMArbUpdateGroupMember   members[2];
  int8_t                   isSync;
  int8_t                   assignedAcked;
  SMArbUpdateGroupAssigned assignedLeader;
  int64_t                  version;
  int32_t                  code;
  int64_t                  updateTimeMs;
} SMArbUpdateGroup;

typedef struct {
  SArray* updateArray;  // SMArbUpdateGroup
} SMArbUpdateGroupBatchReq;

int32_t tSerializeSMArbUpdateGroupBatchReq(void* buf, int32_t bufLen, SMArbUpdateGroupBatchReq* pReq);
int32_t tDeserializeSMArbUpdateGroupBatchReq(void* buf, int32_t bufLen, SMArbUpdateGroupBatchReq* pReq);
void    tFreeSMArbUpdateGroupBatchReq(SMArbUpdateGroupBatchReq* pReq);

typedef struct {
  char queryStrId[TSDB_QUERY_ID_LEN];
} SKillQueryReq;

int32_t tSerializeSKillQueryReq(void* buf, int32_t bufLen, SKillQueryReq* pReq);
int32_t tDeserializeSKillQueryReq(void* buf, int32_t bufLen, SKillQueryReq* pReq);

typedef struct {
  uint32_t connId;
} SKillConnReq;

int32_t tSerializeSKillConnReq(void* buf, int32_t bufLen, SKillConnReq* pReq);
int32_t tDeserializeSKillConnReq(void* buf, int32_t bufLen, SKillConnReq* pReq);

typedef struct {
  int32_t transId;
} SKillTransReq;

int32_t tSerializeSKillTransReq(void* buf, int32_t bufLen, SKillTransReq* pReq);
int32_t tDeserializeSKillTransReq(void* buf, int32_t bufLen, SKillTransReq* pReq);

typedef struct {
  int32_t useless;  // useless
  int32_t sqlLen;
  char*   sql;
} SBalanceVgroupReq;

int32_t tSerializeSBalanceVgroupReq(void* buf, int32_t bufLen, SBalanceVgroupReq* pReq);
int32_t tDeserializeSBalanceVgroupReq(void* buf, int32_t bufLen, SBalanceVgroupReq* pReq);
void    tFreeSBalanceVgroupReq(SBalanceVgroupReq* pReq);

typedef struct {
  int32_t useless;  // useless
  int32_t sqlLen;
  char*   sql;
} SAssignLeaderReq;

int32_t tSerializeSAssignLeaderReq(void* buf, int32_t bufLen, SAssignLeaderReq* pReq);
int32_t tDeserializeSAssignLeaderReq(void* buf, int32_t bufLen, SAssignLeaderReq* pReq);
void    tFreeSAssignLeaderReq(SAssignLeaderReq* pReq);
typedef struct {
  int32_t vgId1;
  int32_t vgId2;
} SMergeVgroupReq;

int32_t tSerializeSMergeVgroupReq(void* buf, int32_t bufLen, SMergeVgroupReq* pReq);
int32_t tDeserializeSMergeVgroupReq(void* buf, int32_t bufLen, SMergeVgroupReq* pReq);

typedef struct {
  int32_t vgId;
  int32_t dnodeId1;
  int32_t dnodeId2;
  int32_t dnodeId3;
  int32_t sqlLen;
  char*   sql;
} SRedistributeVgroupReq;

int32_t tSerializeSRedistributeVgroupReq(void* buf, int32_t bufLen, SRedistributeVgroupReq* pReq);
int32_t tDeserializeSRedistributeVgroupReq(void* buf, int32_t bufLen, SRedistributeVgroupReq* pReq);
void    tFreeSRedistributeVgroupReq(SRedistributeVgroupReq* pReq);

typedef struct {
  int32_t reserved;
  int32_t vgId;
  int32_t sqlLen;
  char*   sql;
  char    db[TSDB_DB_FNAME_LEN];
} SBalanceVgroupLeaderReq;

int32_t tSerializeSBalanceVgroupLeaderReq(void* buf, int32_t bufLen, SBalanceVgroupLeaderReq* pReq);
int32_t tDeserializeSBalanceVgroupLeaderReq(void* buf, int32_t bufLen, SBalanceVgroupLeaderReq* pReq);
void    tFreeSBalanceVgroupLeaderReq(SBalanceVgroupLeaderReq* pReq);

typedef struct {
  int32_t vgId;
} SForceBecomeFollowerReq;

int32_t tSerializeSForceBecomeFollowerReq(void* buf, int32_t bufLen, SForceBecomeFollowerReq* pReq);
// int32_t tDeserializeSForceBecomeFollowerReq(void* buf, int32_t bufLen, SForceBecomeFollowerReq* pReq);

typedef struct {
  int32_t vgId;
  bool    force;
} SSplitVgroupReq;

int32_t tSerializeSSplitVgroupReq(void* buf, int32_t bufLen, SSplitVgroupReq* pReq);
int32_t tDeserializeSSplitVgroupReq(void* buf, int32_t bufLen, SSplitVgroupReq* pReq);

typedef struct {
  char user[TSDB_USER_LEN];
  char spi;
  char encrypt;
  char secret[TSDB_PASSWORD_LEN];
  char ckey[TSDB_PASSWORD_LEN];
} SAuthReq, SAuthRsp;

// int32_t tSerializeSAuthReq(void* buf, int32_t bufLen, SAuthReq* pReq);
// int32_t tDeserializeSAuthReq(void* buf, int32_t bufLen, SAuthReq* pReq);

typedef struct {
  int32_t statusCode;
  char    details[1024];
} SServerStatusRsp;

int32_t tSerializeSServerStatusRsp(void* buf, int32_t bufLen, SServerStatusRsp* pRsp);
int32_t tDeserializeSServerStatusRsp(void* buf, int32_t bufLen, SServerStatusRsp* pRsp);

/**
 * The layout of the query message payload is as following:
 * +--------------------+---------------------------------+
 * |Sql statement       | Physical plan                   |
 * |(denoted by sqlLen) |(In JSON, denoted by contentLen) |
 * +--------------------+---------------------------------+
 */
typedef struct SSubQueryMsg {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t clientId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
  int32_t  msgMask;
  int8_t   taskType;
  int8_t   explain;
  int8_t   needFetch;
  int8_t   compress;
  uint32_t sqlLen;
  char*    sql;
  uint32_t msgLen;
  char*    msg;
} SSubQueryMsg;

int32_t tSerializeSSubQueryMsg(void* buf, int32_t bufLen, SSubQueryMsg* pReq);
int32_t tDeserializeSSubQueryMsg(void* buf, int32_t bufLen, SSubQueryMsg* pReq);
void    tFreeSSubQueryMsg(SSubQueryMsg* pReq);

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
} SSinkDataReq;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t clientId;
  uint64_t taskId;
  int32_t  execId;
} SQueryContinueReq;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t taskId;
} SResReadyReq;

typedef struct {
  int32_t code;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t sversion;
  int32_t tversion;
} SResReadyRsp;

typedef struct SOperatorParam {
  int32_t opType;
  int32_t downstreamIdx;
  void*   value;
  SArray* pChildren;  // SArray<SOperatorParam*>
  bool    reUse;
} SOperatorParam;

typedef struct SColIdNameKV {
  col_id_t colId;
  char     colName[TSDB_COL_NAME_LEN];
} SColIdNameKV;

typedef struct SColIdPair {
  col_id_t vtbColId;
  col_id_t orgColId;
} SColIdPair;

typedef struct SOrgTbInfo {
  int32_t vgId;
  char    tbName[TSDB_TABLE_FNAME_LEN];
  SArray* colMap;  // SArray<SColIdNameKV>
} SOrgTbInfo;

typedef struct STableScanOperatorParam {
  bool        tableSeq;
  SArray*     pUidList;
  SOrgTbInfo* pOrgTbInfo;
  STimeWindow window;
} STableScanOperatorParam;

typedef struct STagScanOperatorParam {
  tb_uid_t       vcUid;
} STagScanOperatorParam;

typedef struct SVTableScanOperatorParam {
  uint64_t        uid;
  SOperatorParam* pTagScanOp;
  SArray*         pOpParamArray;  // SArray<SOperatorParam>
} SVTableScanOperatorParam;

struct SStreamRuntimeFuncInfo;
typedef struct {
  SMsgHead                       header;
  uint64_t                       sId;
  uint64_t                       queryId;
  uint64_t                       clientId;
  uint64_t                       taskId;
  int32_t                        execId;
  SOperatorParam*                pOpParam;

  // used for new-stream
  struct SStreamRuntimeFuncInfo* pStRtFuncInfo;
  bool                           reset;
  bool                           dynTbname;
  // used for new-stream
} SResFetchReq;

int32_t tSerializeSResFetchReq(void* buf, int32_t bufLen, SResFetchReq* pReq);
int32_t tDeserializeSResFetchReq(void* buf, int32_t bufLen, SResFetchReq* pReq);
void    tDestroySResFetchReq(SResFetchReq* pReq);
typedef struct {
  SMsgHead header;
  uint64_t clientId;
} SSchTasksStatusReq;

typedef struct {
  uint64_t queryId;
  uint64_t clientId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
  int8_t   status;
} STaskStatus;

typedef struct {
  int64_t refId;
  SArray* taskStatus;  // SArray<STaskStatus>
} SSchedulerStatusRsp;

typedef struct {
  uint64_t queryId;
  uint64_t taskId;
  int8_t   action;
} STaskAction;

typedef struct SQueryNodeEpId {
  int32_t nodeId;  // vgId or qnodeId
  SEp     ep;
} SQueryNodeEpId;

typedef struct {
  SMsgHead       header;
  uint64_t       clientId;
  SQueryNodeEpId epId;
  SArray*        taskAction;  // SArray<STaskAction>
} SSchedulerHbReq;

int32_t tSerializeSSchedulerHbReq(void* buf, int32_t bufLen, SSchedulerHbReq* pReq);
int32_t tDeserializeSSchedulerHbReq(void* buf, int32_t bufLen, SSchedulerHbReq* pReq);
void    tFreeSSchedulerHbReq(SSchedulerHbReq* pReq);

typedef struct {
  SQueryNodeEpId epId;
  SArray*        taskStatus;  // SArray<STaskStatus>
} SSchedulerHbRsp;

int32_t tSerializeSSchedulerHbRsp(void* buf, int32_t bufLen, SSchedulerHbRsp* pRsp);
int32_t tDeserializeSSchedulerHbRsp(void* buf, int32_t bufLen, SSchedulerHbRsp* pRsp);
void    tFreeSSchedulerHbRsp(SSchedulerHbRsp* pRsp);

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t clientId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
} STaskCancelReq;

typedef struct {
  int32_t code;
} STaskCancelRsp;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t clientId;
  uint64_t taskId;
  int64_t  refId;
  int32_t  execId;
} STaskDropReq;

int32_t tSerializeSTaskDropReq(void* buf, int32_t bufLen, STaskDropReq* pReq);
int32_t tDeserializeSTaskDropReq(void* buf, int32_t bufLen, STaskDropReq* pReq);

typedef enum {
  TASK_NOTIFY_FINISHED = 1,
} ETaskNotifyType;

typedef struct {
  SMsgHead        header;
  uint64_t        sId;
  uint64_t        queryId;
  uint64_t        clientId;
  uint64_t        taskId;
  int64_t         refId;
  int32_t         execId;
  ETaskNotifyType type;
} STaskNotifyReq;

int32_t tSerializeSTaskNotifyReq(void* buf, int32_t bufLen, STaskNotifyReq* pReq);
int32_t tDeserializeSTaskNotifyReq(void* buf, int32_t bufLen, STaskNotifyReq* pReq);

int32_t tSerializeSQueryTableRsp(void* buf, int32_t bufLen, SQueryTableRsp* pRsp);
int32_t tDeserializeSQueryTableRsp(void* buf, int32_t bufLen, SQueryTableRsp* pRsp);

typedef struct {
  int32_t code;
} STaskDropRsp;

#define STREAM_TRIGGER_AT_ONCE                 1
#define STREAM_TRIGGER_WINDOW_CLOSE            2
#define STREAM_TRIGGER_MAX_DELAY               3
#define STREAM_TRIGGER_FORCE_WINDOW_CLOSE      4
#define STREAM_TRIGGER_CONTINUOUS_WINDOW_CLOSE 5

#define STREAM_DEFAULT_IGNORE_EXPIRED 1
#define STREAM_FILL_HISTORY_ON        1
#define STREAM_FILL_HISTORY_OFF       0
#define STREAM_DEFAULT_FILL_HISTORY   STREAM_FILL_HISTORY_OFF
#define STREAM_DEFAULT_IGNORE_UPDATE  1
#define STREAM_CREATE_STABLE_TRUE     1
#define STREAM_CREATE_STABLE_FALSE    0

typedef struct SVgroupVer {
  int32_t vgId;
  int64_t ver;
} SVgroupVer;

typedef struct STaskNotifyEventStat {
  int64_t notifyEventAddTimes;     // call times of add function
  int64_t notifyEventAddElems;     // elements added by add function
  double  notifyEventAddCostSec;   // time cost of add function
  int64_t notifyEventPushTimes;    // call times of push function
  int64_t notifyEventPushElems;    // elements pushed by push function
  double  notifyEventPushCostSec;  // time cost of push function
  int64_t notifyEventPackTimes;    // call times of pack function
  int64_t notifyEventPackElems;    // elements packed by pack function
  double  notifyEventPackCostSec;  // time cost of pack function
  int64_t notifyEventSendTimes;    // call times of send function
  int64_t notifyEventSendElems;    // elements sent by send function
  double  notifyEventSendCostSec;  // time cost of send function
  int64_t notifyEventHoldElems;    // elements hold due to watermark
} STaskNotifyEventStat;

enum {
  TOPIC_SUB_TYPE__DB = 1,
  TOPIC_SUB_TYPE__TABLE,
  TOPIC_SUB_TYPE__COLUMN,
};

#define DEFAULT_MAX_POLL_INTERVAL  300000
#define DEFAULT_SESSION_TIMEOUT    12000
#define DEFAULT_MAX_POLL_WAIT_TIME 1000
#define DEFAULT_MIN_POLL_ROWS      4096

typedef struct {
  char   name[TSDB_TOPIC_FNAME_LEN];  // accout.topic
  int8_t igExists;
  int8_t subType;
  int8_t withMeta;
  char*  sql;
  char   subDbName[TSDB_DB_FNAME_LEN];
  char*  ast;
  char   subStbName[TSDB_TABLE_FNAME_LEN];
} SCMCreateTopicReq;

int32_t tSerializeSCMCreateTopicReq(void* buf, int32_t bufLen, const SCMCreateTopicReq* pReq);
int32_t tDeserializeSCMCreateTopicReq(void* buf, int32_t bufLen, SCMCreateTopicReq* pReq);
void    tFreeSCMCreateTopicReq(SCMCreateTopicReq* pReq);

typedef struct {
  int64_t consumerId;
} SMqConsumerRecoverMsg, SMqConsumerClearMsg;

typedef struct {
  int64_t consumerId;
  char    cgroup[TSDB_CGROUP_LEN];
  char    clientId[TSDB_CLIENT_ID_LEN];
  char    user[TSDB_USER_LEN];
  char    fqdn[TSDB_FQDN_LEN];
  SArray* topicNames;  // SArray<char**>

  int8_t  withTbName;
  int8_t  autoCommit;
  int32_t autoCommitInterval;
  int8_t  resetOffsetCfg;
  int8_t  enableReplay;
  int8_t  enableBatchMeta;
  int32_t sessionTimeoutMs;
  int32_t maxPollIntervalMs;
} SCMSubscribeReq;

static FORCE_INLINE int32_t tSerializeSCMSubscribeReq(void** buf, const SCMSubscribeReq* pReq) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pReq->consumerId);
  tlen += taosEncodeString(buf, pReq->cgroup);
  tlen += taosEncodeString(buf, pReq->clientId);

  int32_t topicNum = taosArrayGetSize(pReq->topicNames);
  tlen += taosEncodeFixedI32(buf, topicNum);

  for (int32_t i = 0; i < topicNum; i++) {
    tlen += taosEncodeString(buf, (char*)taosArrayGetP(pReq->topicNames, i));
  }

  tlen += taosEncodeFixedI8(buf, pReq->withTbName);
  tlen += taosEncodeFixedI8(buf, pReq->autoCommit);
  tlen += taosEncodeFixedI32(buf, pReq->autoCommitInterval);
  tlen += taosEncodeFixedI8(buf, pReq->resetOffsetCfg);
  tlen += taosEncodeFixedI8(buf, pReq->enableReplay);
  tlen += taosEncodeFixedI8(buf, pReq->enableBatchMeta);
  tlen += taosEncodeFixedI32(buf, pReq->sessionTimeoutMs);
  tlen += taosEncodeFixedI32(buf, pReq->maxPollIntervalMs);
  tlen += taosEncodeString(buf, pReq->user);
  tlen += taosEncodeString(buf, pReq->fqdn);

  return tlen;
}

static FORCE_INLINE int32_t tDeserializeSCMSubscribeReq(void* buf, SCMSubscribeReq* pReq, int32_t len) {
  void* start = buf;
  buf = taosDecodeFixedI64(buf, &pReq->consumerId);
  buf = taosDecodeStringTo(buf, pReq->cgroup);
  buf = taosDecodeStringTo(buf, pReq->clientId);

  int32_t topicNum = 0;
  buf = taosDecodeFixedI32(buf, &topicNum);

  pReq->topicNames = taosArrayInit(topicNum, sizeof(void*));
  if (pReq->topicNames == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < topicNum; i++) {
    char* name = NULL;
    buf = taosDecodeString(buf, &name);
    if (taosArrayPush(pReq->topicNames, &name) == NULL) {
      return terrno;
    }
  }

  buf = taosDecodeFixedI8(buf, &pReq->withTbName);
  buf = taosDecodeFixedI8(buf, &pReq->autoCommit);
  buf = taosDecodeFixedI32(buf, &pReq->autoCommitInterval);
  buf = taosDecodeFixedI8(buf, &pReq->resetOffsetCfg);
  buf = taosDecodeFixedI8(buf, &pReq->enableReplay);
  buf = taosDecodeFixedI8(buf, &pReq->enableBatchMeta);
  if ((char*)buf - (char*)start < len) {
    buf = taosDecodeFixedI32(buf, &pReq->sessionTimeoutMs);
    buf = taosDecodeFixedI32(buf, &pReq->maxPollIntervalMs);
    buf = taosDecodeStringTo(buf, pReq->user);
    buf = taosDecodeStringTo(buf, pReq->fqdn);
  } else {
    pReq->sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT;
    pReq->maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL;
  }

  return 0;
}

typedef struct {
  char    key[TSDB_SUBSCRIBE_KEY_LEN];
  SArray* removedConsumers;  // SArray<int64_t>
  SArray* newConsumers;      // SArray<int64_t>
} SMqRebInfo;

static FORCE_INLINE SMqRebInfo* tNewSMqRebSubscribe(const char* key) {
  SMqRebInfo* pRebInfo = (SMqRebInfo*)taosMemoryCalloc(1, sizeof(SMqRebInfo));
  if (pRebInfo == NULL) {
    return NULL;
  }
  tstrncpy(pRebInfo->key, key, TSDB_SUBSCRIBE_KEY_LEN);
  pRebInfo->removedConsumers = taosArrayInit(0, sizeof(int64_t));
  if (pRebInfo->removedConsumers == NULL) {
    goto _err;
  }
  pRebInfo->newConsumers = taosArrayInit(0, sizeof(int64_t));
  if (pRebInfo->newConsumers == NULL) {
    goto _err;
  }
  return pRebInfo;
_err:
  taosArrayDestroy(pRebInfo->removedConsumers);
  taosArrayDestroy(pRebInfo->newConsumers);
  taosMemoryFreeClear(pRebInfo);
  return NULL;
}

typedef struct {
  int64_t streamId;
  int64_t checkpointId;
  char    streamName[TSDB_STREAM_FNAME_LEN];
} SMStreamDoCheckpointMsg;

typedef struct {
  int64_t status;
} SMVSubscribeRsp;

typedef struct {
  char    name[TSDB_TOPIC_FNAME_LEN];
  int8_t  igNotExists;
  int32_t sqlLen;
  char*   sql;
  int8_t  force;
} SMDropTopicReq;

int32_t tSerializeSMDropTopicReq(void* buf, int32_t bufLen, SMDropTopicReq* pReq);
int32_t tDeserializeSMDropTopicReq(void* buf, int32_t bufLen, SMDropTopicReq* pReq);
void    tFreeSMDropTopicReq(SMDropTopicReq* pReq);

typedef struct {
  char   topic[TSDB_TOPIC_FNAME_LEN];
  char   cgroup[TSDB_CGROUP_LEN];
  int8_t igNotExists;
  int8_t force;
} SMDropCgroupReq;

int32_t tSerializeSMDropCgroupReq(void* buf, int32_t bufLen, SMDropCgroupReq* pReq);
int32_t tDeserializeSMDropCgroupReq(void* buf, int32_t bufLen, SMDropCgroupReq* pReq);

typedef struct {
  int8_t reserved;
} SMDropCgroupRsp;

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  int8_t  alterType;
  SSchema schema;
} SAlterTopicReq;

typedef struct {
  SMsgHead head;
  char     name[TSDB_TABLE_FNAME_LEN];
  int64_t  tuid;
  int32_t  sverson;
  int32_t  execLen;
  char*    executor;
  int32_t  sqlLen;
  char*    sql;
} SDCreateTopicReq;

typedef struct {
  SMsgHead head;
  char     name[TSDB_TABLE_FNAME_LEN];
  int64_t  tuid;
} SDDropTopicReq;

typedef struct {
  int64_t maxdelay[2];
  int64_t watermark[2];
  int64_t deleteMark[2];
  int32_t qmsgLen[2];
  char*   qmsg[2];  // pAst:qmsg:SRetention => trigger aggr task1/2
} SRSmaParam;

int32_t tEncodeSRSmaParam(SEncoder* pCoder, const SRSmaParam* pRSmaParam);
int32_t tDecodeSRSmaParam(SDecoder* pCoder, SRSmaParam* pRSmaParam);

// TDMT_VND_CREATE_STB ==============
typedef struct SVCreateStbReq {
  char*           name;
  tb_uid_t        suid;
  int8_t          rollup;
  SSchemaWrapper  schemaRow;
  SSchemaWrapper  schemaTag;
  SRSmaParam      rsmaParam;
  int32_t         alterOriDataLen;
  void*           alterOriData;
  int8_t          source;
  int8_t          colCmpred;
  SColCmprWrapper colCmpr;
  int64_t         keep;
  SExtSchema*     pExtSchemas;
  int8_t          virtualStb;
} SVCreateStbReq;

int tEncodeSVCreateStbReq(SEncoder* pCoder, const SVCreateStbReq* pReq);
int tDecodeSVCreateStbReq(SDecoder* pCoder, SVCreateStbReq* pReq);

// TDMT_VND_DROP_STB ==============
typedef struct SVDropStbReq {
  char*    name;
  tb_uid_t suid;
} SVDropStbReq;

int32_t tEncodeSVDropStbReq(SEncoder* pCoder, const SVDropStbReq* pReq);
int32_t tDecodeSVDropStbReq(SDecoder* pCoder, SVDropStbReq* pReq);

// TDMT_VND_CREATE_TABLE ==============
#define TD_CREATE_IF_NOT_EXISTS       0x1
#define TD_CREATE_NORMAL_TB_IN_STREAM 0x2
#define TD_CREATE_SUB_TB_IN_STREAM    0x4
typedef struct SVCreateTbReq {
  int32_t  flags;
  char*    name;
  tb_uid_t uid;
  int64_t  btime;
  int32_t  ttl;
  int32_t  commentLen;
  char*    comment;
  int8_t   type;
  union {
    struct {
      char*    stbName;  // super table name
      uint8_t  tagNum;
      tb_uid_t suid;
      SArray*  tagName;
      uint8_t* pTag;
    } ctb;
    struct {
      SSchemaWrapper schemaRow;
    } ntb;
  };
  int32_t         sqlLen;
  char*           sql;
  SColCmprWrapper colCmpr;
  SExtSchema*     pExtSchemas;
  SColRefWrapper  colRef;  // col reference for virtual table
} SVCreateTbReq;

int  tEncodeSVCreateTbReq(SEncoder* pCoder, const SVCreateTbReq* pReq);
int  tDecodeSVCreateTbReq(SDecoder* pCoder, SVCreateTbReq* pReq);
void tDestroySVCreateTbReq(SVCreateTbReq* pReq, int32_t flags);

static FORCE_INLINE void tdDestroySVCreateTbReq(SVCreateTbReq* req) {
  if (NULL == req) {
    return;
  }

  taosMemoryFreeClear(req->sql);
  taosMemoryFreeClear(req->name);
  taosMemoryFreeClear(req->comment);
  if (req->type == TSDB_CHILD_TABLE || req->type == TSDB_VIRTUAL_CHILD_TABLE) {
    taosMemoryFreeClear(req->ctb.pTag);
    taosMemoryFreeClear(req->ctb.stbName);
    taosArrayDestroy(req->ctb.tagName);
    req->ctb.tagName = NULL;
  } else if (req->type == TSDB_NORMAL_TABLE || req->type == TSDB_VIRTUAL_NORMAL_TABLE) {
    taosMemoryFreeClear(req->ntb.schemaRow.pSchema);
  }
  taosMemoryFreeClear(req->colCmpr.pColCmpr);
  taosMemoryFreeClear(req->pExtSchemas);
  taosMemoryFreeClear(req->colRef.pColRef);
}

typedef struct {
  int32_t nReqs;
  union {
    SVCreateTbReq* pReqs;
    SArray*        pArray;
  };
  int8_t source;  // TD_REQ_FROM_TAOX-taosX or TD_REQ_FROM_APP-taosClient
} SVCreateTbBatchReq;

int  tEncodeSVCreateTbBatchReq(SEncoder* pCoder, const SVCreateTbBatchReq* pReq);
int  tDecodeSVCreateTbBatchReq(SDecoder* pCoder, SVCreateTbBatchReq* pReq);
void tDeleteSVCreateTbBatchReq(SVCreateTbBatchReq* pReq);

typedef struct {
  int32_t        code;
  STableMetaRsp* pMeta;
} SVCreateTbRsp, SVUpdateTbRsp;

int  tEncodeSVCreateTbRsp(SEncoder* pCoder, const SVCreateTbRsp* pRsp);
int  tDecodeSVCreateTbRsp(SDecoder* pCoder, SVCreateTbRsp* pRsp);
void tFreeSVCreateTbRsp(void* param);

int32_t tSerializeSVCreateTbReq(void** buf, SVCreateTbReq* pReq);
void*   tDeserializeSVCreateTbReq(void* buf, SVCreateTbReq* pReq);

typedef struct {
  int32_t nRsps;
  union {
    SVCreateTbRsp* pRsps;
    SArray*        pArray;
  };
} SVCreateTbBatchRsp;

int tEncodeSVCreateTbBatchRsp(SEncoder* pCoder, const SVCreateTbBatchRsp* pRsp);
int tDecodeSVCreateTbBatchRsp(SDecoder* pCoder, SVCreateTbBatchRsp* pRsp);

// int32_t tSerializeSVCreateTbBatchRsp(void* buf, int32_t bufLen, SVCreateTbBatchRsp* pRsp);
// int32_t tDeserializeSVCreateTbBatchRsp(void* buf, int32_t bufLen, SVCreateTbBatchRsp* pRsp);

// TDMT_VND_DROP_TABLE =================
typedef struct {
  char*    name;
  uint64_t suid;  // for tmq in wal format
  int64_t  uid;
  int8_t   igNotExists;
  int8_t   isVirtual;
} SVDropTbReq;

typedef struct {
  int32_t code;
} SVDropTbRsp;

typedef struct {
  int32_t nReqs;
  union {
    SVDropTbReq* pReqs;
    SArray*      pArray;
  };
} SVDropTbBatchReq;

int32_t tEncodeSVDropTbBatchReq(SEncoder* pCoder, const SVDropTbBatchReq* pReq);
int32_t tDecodeSVDropTbBatchReq(SDecoder* pCoder, SVDropTbBatchReq* pReq);

typedef struct {
  int32_t nRsps;
  union {
    SVDropTbRsp* pRsps;
    SArray*      pArray;
  };
} SVDropTbBatchRsp;

int32_t tEncodeSVDropTbBatchRsp(SEncoder* pCoder, const SVDropTbBatchRsp* pRsp);
int32_t tDecodeSVDropTbBatchRsp(SDecoder* pCoder, SVDropTbBatchRsp* pRsp);

// TDMT_VND_ALTER_TABLE =====================
typedef struct SMultiTagUpateVal {
  char*    tagName;
  int32_t  colId;
  int8_t   tagType;
  int8_t   tagFree;
  uint32_t nTagVal;
  uint8_t* pTagVal;
  int8_t   isNull;
  SArray*  pTagArray;
} SMultiTagUpateVal;
typedef struct SVAlterTbReq {
  char*   tbName;
  int8_t  action;
  char*   colName;
  int32_t colId;
  // TSDB_ALTER_TABLE_ADD_COLUMN
  int8_t  type;
  int8_t  flags;
  int32_t bytes;
  // TSDB_ALTER_TABLE_DROP_COLUMN
  // TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES
  int8_t   colModType;
  int32_t  colModBytes;
  char*    colNewName;  // TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME
  char*    tagName;     // TSDB_ALTER_TABLE_UPDATE_TAG_VAL
  int8_t   isNull;
  int8_t   tagType;
  int8_t   tagFree;
  uint32_t nTagVal;
  uint8_t* pTagVal;
  SArray*  pTagArray;
  // TSDB_ALTER_TABLE_UPDATE_OPTIONS
  int8_t   updateTTL;
  int32_t  newTTL;
  int32_t  newCommentLen;
  char*    newComment;
  int64_t  ctimeMs;    // fill by vnode
  int8_t   source;     // TD_REQ_FROM_TAOX-taosX or TD_REQ_FROM_APP-taosClient
  uint32_t compress;   // TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS
  SArray*  pMultiTag;  // TSDB_ALTER_TABLE_ADD_MULTI_TAGS
  // for Add column
  STypeMod typeMod;
  // TSDB_ALTER_TABLE_ALTER_COLUMN_REF
  char* refDbName;
  char* refTbName;
  char* refColName;
  // TSDB_ALTER_TABLE_REMOVE_COLUMN_REF
} SVAlterTbReq;

int32_t tEncodeSVAlterTbReq(SEncoder* pEncoder, const SVAlterTbReq* pReq);
int32_t tDecodeSVAlterTbReq(SDecoder* pDecoder, SVAlterTbReq* pReq);
int32_t tDecodeSVAlterTbReqSetCtime(SDecoder* pDecoder, SVAlterTbReq* pReq, int64_t ctimeMs);
void    tfreeMultiTagUpateVal(void* pMultiTag);

typedef struct {
  int32_t        code;
  STableMetaRsp* pMeta;
} SVAlterTbRsp;

int32_t tEncodeSVAlterTbRsp(SEncoder* pEncoder, const SVAlterTbRsp* pRsp);
int32_t tDecodeSVAlterTbRsp(SDecoder* pDecoder, SVAlterTbRsp* pRsp);
// ======================

typedef struct {
  SMsgHead head;
  int64_t  uid;
  int32_t  tid;
  int16_t  tversion;
  int16_t  colId;
  int8_t   type;
  int16_t  bytes;
  int32_t  tagValLen;
  int16_t  numOfTags;
  int32_t  schemaLen;
  char     data[];
} SUpdateTagValReq;

typedef struct {
  SMsgHead head;
} SUpdateTagValRsp;

typedef struct {
  SMsgHead head;
} SVShowTablesReq;

typedef struct {
  SMsgHead head;
  int32_t  id;
} SVShowTablesFetchReq;

typedef struct {
  int64_t useconds;
  int8_t  completed;  // all results are returned to client
  int8_t  precision;
  int8_t  compressed;
  int32_t compLen;
  int32_t numOfRows;
  char    data[];
} SVShowTablesFetchRsp;

typedef struct {
  int64_t consumerId;
  int32_t epoch;
  char    cgroup[TSDB_CGROUP_LEN];
} SMqAskEpReq;

typedef struct {
  int32_t key;
  int32_t valueLen;
  void*   value;
} SKv;

typedef struct {
  int64_t tscRid;
  int8_t  connType;
} SClientHbKey;

typedef struct {
  int64_t tid;
  char    status[TSDB_JOB_STATUS_LEN];
} SQuerySubDesc;

typedef struct {
  char     sql[TSDB_SHOW_SQL_LEN];
  uint64_t queryId;
  int64_t  useconds;
  int64_t  stime;  // timestamp precision ms
  int64_t  reqRid;
  bool     stableQuery;
  bool     isSubQuery;
  char     fqdn[TSDB_FQDN_LEN];
  int32_t  subPlanNum;
  SArray*  subDesc;  // SArray<SQuerySubDesc>
} SQueryDesc;

typedef struct {
  uint32_t connId;
  SArray*  queryDesc;  // SArray<SQueryDesc>
} SQueryHbReqBasic;

typedef struct {
  uint32_t connId;
  uint64_t killRid;
  int32_t  totalDnodes;
  int32_t  onlineDnodes;
  int8_t   killConnection;
  int8_t   align[3];
  SEpSet   epSet;
  SArray*  pQnodeList;
} SQueryHbRspBasic;

typedef struct SAppClusterSummary {
  uint64_t numOfInsertsReq;
  uint64_t numOfInsertRows;
  uint64_t insertElapsedTime;
  uint64_t insertBytes;  // submit to tsdb since launched.

  uint64_t fetchBytes;
  uint64_t numOfQueryReq;
  uint64_t queryElapsedTime;
  uint64_t numOfSlowQueries;
  uint64_t totalRequests;
  uint64_t currentRequests;  // the number of SRequestObj
} SAppClusterSummary;

typedef struct {
  int64_t            appId;
  int32_t            pid;
  char               name[TSDB_APP_NAME_LEN];
  int64_t            startTime;
  SAppClusterSummary summary;
} SAppHbReq;

typedef struct {
  SClientHbKey      connKey;
  int64_t           clusterId;
  SAppHbReq         app;
  SQueryHbReqBasic* query;
  SHashObj*         info;  // hash<Skv.key, Skv>
  char              userApp[TSDB_APP_NAME_LEN];
  uint32_t          userIp;
  SIpRange          userDualIp;
} SClientHbReq;

typedef struct {
  int64_t reqId;
  SArray* reqs;  // SArray<SClientHbReq>
  int64_t ipWhiteListVer;
} SClientHbBatchReq;

typedef struct {
  SClientHbKey      connKey;
  int32_t           status;
  SQueryHbRspBasic* query;
  SArray*           info;  // Array<Skv>
} SClientHbRsp;

typedef struct {
  int64_t       reqId;
  int64_t       rspId;
  int32_t       svrTimestamp;
  SArray*       rsps;  // SArray<SClientHbRsp>
  SMonitorParas monitorParas;
  int8_t        enableAuditDelete;
  int8_t        enableStrongPass;
} SClientHbBatchRsp;

static FORCE_INLINE uint32_t hbKeyHashFunc(const char* key, uint32_t keyLen) { return taosIntHash_64(key, keyLen); }

static FORCE_INLINE void tFreeReqKvHash(SHashObj* info) {
  void* pIter = taosHashIterate(info, NULL);
  while (pIter != NULL) {
    SKv* kv = (SKv*)pIter;
    taosMemoryFreeClear(kv->value);
    pIter = taosHashIterate(info, pIter);
  }
}

static FORCE_INLINE void tFreeClientHbQueryDesc(void* pDesc) {
  SQueryDesc* desc = (SQueryDesc*)pDesc;
  if (desc->subDesc) {
    taosArrayDestroy(desc->subDesc);
    desc->subDesc = NULL;
  }
}

static FORCE_INLINE void tFreeClientHbReq(void* pReq) {
  SClientHbReq* req = (SClientHbReq*)pReq;
  if (req->query) {
    if (req->query->queryDesc) {
      taosArrayDestroyEx(req->query->queryDesc, tFreeClientHbQueryDesc);
    }
    taosMemoryFreeClear(req->query);
  }

  if (req->info) {
    tFreeReqKvHash(req->info);
    taosHashCleanup(req->info);
    req->info = NULL;
  }
}

int32_t tSerializeSClientHbBatchReq(void* buf, int32_t bufLen, const SClientHbBatchReq* pReq);
int32_t tDeserializeSClientHbBatchReq(void* buf, int32_t bufLen, SClientHbBatchReq* pReq);

static FORCE_INLINE void tFreeClientHbBatchReq(void* pReq) {
  if (pReq == NULL) return;
  SClientHbBatchReq* req = (SClientHbBatchReq*)pReq;
  taosArrayDestroyEx(req->reqs, tFreeClientHbReq);
  taosMemoryFree(pReq);
}

static FORCE_INLINE void tFreeClientKv(void* pKv) {
  SKv* kv = (SKv*)pKv;
  if (kv) {
    taosMemoryFreeClear(kv->value);
  }
}

static FORCE_INLINE void tFreeClientHbRsp(void* pRsp) {
  SClientHbRsp* rsp = (SClientHbRsp*)pRsp;
  if (rsp->query) {
    taosArrayDestroy(rsp->query->pQnodeList);
    taosMemoryFreeClear(rsp->query);
  }
  if (rsp->info) taosArrayDestroyEx(rsp->info, tFreeClientKv);
}

static FORCE_INLINE void tFreeClientHbBatchRsp(void* pRsp) {
  SClientHbBatchRsp* rsp = (SClientHbBatchRsp*)pRsp;
  taosArrayDestroyEx(rsp->rsps, tFreeClientHbRsp);
}

int32_t tSerializeSClientHbBatchRsp(void* buf, int32_t bufLen, const SClientHbBatchRsp* pBatchRsp);
int32_t tDeserializeSClientHbBatchRsp(void* buf, int32_t bufLen, SClientHbBatchRsp* pBatchRsp);
void    tFreeSClientHbBatchRsp(SClientHbBatchRsp* pBatchRsp);

static FORCE_INLINE int32_t tEncodeSKv(SEncoder* pEncoder, const SKv* pKv) {
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pKv->key));
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pKv->valueLen));
  TAOS_CHECK_RETURN(tEncodeBinary(pEncoder, (uint8_t*)pKv->value, pKv->valueLen));
  return 0;
}

static FORCE_INLINE int32_t tDecodeSKv(SDecoder* pDecoder, SKv* pKv) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pKv->key));
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pKv->valueLen));
  pKv->value = taosMemoryMalloc(pKv->valueLen + 1);
  if (pKv->value == NULL) {
    TAOS_CHECK_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  TAOS_CHECK_RETURN(tDecodeCStrTo(pDecoder, (char*)pKv->value));
  return 0;
}

static FORCE_INLINE int32_t tEncodeSClientHbKey(SEncoder* pEncoder, const SClientHbKey* pKey) {
  TAOS_CHECK_RETURN(tEncodeI64(pEncoder, pKey->tscRid));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pKey->connType));
  return 0;
}

static FORCE_INLINE int32_t tDecodeSClientHbKey(SDecoder* pDecoder, SClientHbKey* pKey) {
  TAOS_CHECK_RETURN(tDecodeI64(pDecoder, &pKey->tscRid));
  TAOS_CHECK_RETURN(tDecodeI8(pDecoder, &pKey->connType));
  return 0;
}

typedef struct {
  int32_t vgId;
  // TODO stas
} SMqReportVgInfo;

static FORCE_INLINE int32_t taosEncodeSMqVgInfo(void** buf, const SMqReportVgInfo* pVgInfo) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgInfo->vgId);
  return tlen;
}

static FORCE_INLINE void* taosDecodeSMqVgInfo(void* buf, SMqReportVgInfo* pVgInfo) {
  buf = taosDecodeFixedI32(buf, &pVgInfo->vgId);
  return buf;
}

typedef struct {
  int32_t epoch;
  int64_t topicUid;
  char    name[TSDB_TOPIC_FNAME_LEN];
  SArray* pVgInfo;  // SArray<SMqHbVgInfo>
} SMqTopicInfo;

static FORCE_INLINE int32_t taosEncodeSMqTopicInfoMsg(void** buf, const SMqTopicInfo* pTopicInfo) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pTopicInfo->epoch);
  tlen += taosEncodeFixedI64(buf, pTopicInfo->topicUid);
  tlen += taosEncodeString(buf, pTopicInfo->name);
  int32_t sz = taosArrayGetSize(pTopicInfo->pVgInfo);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqReportVgInfo* pVgInfo = (SMqReportVgInfo*)taosArrayGet(pTopicInfo->pVgInfo, i);
    tlen += taosEncodeSMqVgInfo(buf, pVgInfo);
  }
  return tlen;
}

static FORCE_INLINE void* taosDecodeSMqTopicInfoMsg(void* buf, SMqTopicInfo* pTopicInfo) {
  buf = taosDecodeFixedI32(buf, &pTopicInfo->epoch);
  buf = taosDecodeFixedI64(buf, &pTopicInfo->topicUid);
  buf = taosDecodeStringTo(buf, pTopicInfo->name);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  if ((pTopicInfo->pVgInfo = taosArrayInit(sz, sizeof(SMqReportVgInfo))) == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqReportVgInfo vgInfo;
    buf = taosDecodeSMqVgInfo(buf, &vgInfo);
    if (taosArrayPush(pTopicInfo->pVgInfo, &vgInfo) == NULL) {
      return NULL;
    }
  }
  return buf;
}

typedef struct {
  int32_t status;  // ask hb endpoint
  int32_t epoch;
  int64_t consumerId;
  SArray* pTopics;  // SArray<SMqHbTopicInfo>
} SMqReportReq;

static FORCE_INLINE int32_t taosEncodeSMqReportMsg(void** buf, const SMqReportReq* pMsg) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pMsg->status);
  tlen += taosEncodeFixedI32(buf, pMsg->epoch);
  tlen += taosEncodeFixedI64(buf, pMsg->consumerId);
  int32_t sz = taosArrayGetSize(pMsg->pTopics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqTopicInfo* topicInfo = (SMqTopicInfo*)taosArrayGet(pMsg->pTopics, i);
    tlen += taosEncodeSMqTopicInfoMsg(buf, topicInfo);
  }
  return tlen;
}

static FORCE_INLINE void* taosDecodeSMqReportMsg(void* buf, SMqReportReq* pMsg) {
  buf = taosDecodeFixedI32(buf, &pMsg->status);
  buf = taosDecodeFixedI32(buf, &pMsg->epoch);
  buf = taosDecodeFixedI64(buf, &pMsg->consumerId);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  if ((pMsg->pTopics = taosArrayInit(sz, sizeof(SMqTopicInfo))) == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqTopicInfo topicInfo;
    buf = taosDecodeSMqTopicInfoMsg(buf, &topicInfo);
    if (taosArrayPush(pMsg->pTopics, &topicInfo) == NULL) {
      return NULL;
    }
  }
  return buf;
}

typedef struct {
  SMsgHead head;
  int64_t  leftForVer;
  int32_t  vgId;
  int64_t  consumerId;
  char     subKey[TSDB_SUBSCRIBE_KEY_LEN];
} SMqVDeleteReq;

typedef struct {
  int8_t reserved;
} SMqVDeleteRsp;

typedef struct {
  char*   name;
  int8_t  igNotExists;
} SMDropStreamReq;

typedef struct {
  int8_t reserved;
} SMDropStreamRsp;

typedef struct {
  SMsgHead head;
  int64_t  resetRelHalt;  // reset related stream task halt status
  int64_t  streamId;
  int32_t  taskId;
} SVDropStreamTaskReq;

typedef struct {
  int8_t reserved;
} SVDropStreamTaskRsp;

int32_t tSerializeSMDropStreamReq(void* buf, int32_t bufLen, const SMDropStreamReq* pReq);
int32_t tDeserializeSMDropStreamReq(void* buf, int32_t bufLen, SMDropStreamReq* pReq);
void    tFreeMDropStreamReq(SMDropStreamReq* pReq);

typedef struct {
  char*  name;
  int8_t igNotExists;
} SMPauseStreamReq;

int32_t tSerializeSMPauseStreamReq(void* buf, int32_t bufLen, const SMPauseStreamReq* pReq);
int32_t tDeserializeSMPauseStreamReq(void* buf, int32_t bufLen, SMPauseStreamReq* pReq);
void    tFreeMPauseStreamReq(SMPauseStreamReq *pReq);

typedef struct {
  char*  name;
  int8_t igNotExists;
  int8_t igUntreated;
} SMResumeStreamReq;

int32_t tSerializeSMResumeStreamReq(void* buf, int32_t bufLen, const SMResumeStreamReq* pReq);
int32_t tDeserializeSMResumeStreamReq(void* buf, int32_t bufLen, SMResumeStreamReq* pReq);
void    tFreeMResumeStreamReq(SMResumeStreamReq *pReq);

typedef struct {
  char*       name;
  int8_t      calcAll;
  STimeWindow timeRange;
} SMRecalcStreamReq;

int32_t tSerializeSMRecalcStreamReq(void* buf, int32_t bufLen, const SMRecalcStreamReq* pReq);
int32_t tDeserializeSMRecalcStreamReq(void* buf, int32_t bufLen, SMRecalcStreamReq* pReq);
void    tFreeMRecalcStreamReq(SMRecalcStreamReq *pReq);

typedef struct SVUpdateCheckpointInfoReq {
  SMsgHead head;
  int64_t  streamId;
  int32_t  taskId;
  int64_t  checkpointId;
  int64_t  checkpointVer;
  int64_t  checkpointTs;
  int32_t  transId;
  int64_t  hStreamId;  // add encode/decode
  int64_t  hTaskId;
  int8_t   dropRelHTask;
} SVUpdateCheckpointInfoReq;

typedef struct {
  int64_t leftForVer;
  int32_t vgId;
  int64_t oldConsumerId;
  int64_t newConsumerId;
  char    subKey[TSDB_SUBSCRIBE_KEY_LEN];
  int8_t  subType;
  int8_t  withMeta;
  char*   qmsg;  // SubPlanToString
  int64_t suid;
} SMqRebVgReq;

int32_t tEncodeSMqRebVgReq(SEncoder* pCoder, const SMqRebVgReq* pReq);
int32_t tDecodeSMqRebVgReq(SDecoder* pCoder, SMqRebVgReq* pReq);

typedef struct {
  char    topic[TSDB_TOPIC_FNAME_LEN];
  int64_t ntbUid;
  SArray* colIdList;  // SArray<int16_t>
} STqCheckInfo;

int32_t tEncodeSTqCheckInfo(SEncoder* pEncoder, const STqCheckInfo* pInfo);
int32_t tDecodeSTqCheckInfo(SDecoder* pDecoder, STqCheckInfo* pInfo);
void    tDeleteSTqCheckInfo(STqCheckInfo* pInfo);

// tqOffset
enum {
  TMQ_OFFSET__RESET_NONE = -3,
  TMQ_OFFSET__RESET_EARLIEST = -2,
  TMQ_OFFSET__RESET_LATEST = -1,
  TMQ_OFFSET__LOG = 1,
  TMQ_OFFSET__SNAPSHOT_DATA = 2,
  TMQ_OFFSET__SNAPSHOT_META = 3,
};

enum {
  WITH_DATA = 0,
  WITH_META = 1,
  ONLY_META = 2,
};

#define TQ_OFFSET_VERSION 1

typedef struct {
  int8_t type;
  union {
    // snapshot
    struct {
      int64_t uid;
      int64_t ts;
      SValue  primaryKey;
    };
    // log
    struct {
      int64_t version;
    };
  };
} STqOffsetVal;

static FORCE_INLINE void tqOffsetResetToData(STqOffsetVal* pOffsetVal, int64_t uid, int64_t ts, SValue primaryKey) {
  pOffsetVal->type = TMQ_OFFSET__SNAPSHOT_DATA;
  pOffsetVal->uid = uid;
  pOffsetVal->ts = ts;
  if (IS_VAR_DATA_TYPE(pOffsetVal->primaryKey.type)) {
    taosMemoryFree(pOffsetVal->primaryKey.pData);
  }
  pOffsetVal->primaryKey = primaryKey;
}

static FORCE_INLINE void tqOffsetResetToMeta(STqOffsetVal* pOffsetVal, int64_t uid) {
  pOffsetVal->type = TMQ_OFFSET__SNAPSHOT_META;
  pOffsetVal->uid = uid;
}

static FORCE_INLINE void tqOffsetResetToLog(STqOffsetVal* pOffsetVal, int64_t ver) {
  pOffsetVal->type = TMQ_OFFSET__LOG;
  pOffsetVal->version = ver;
}

int32_t tEncodeSTqOffsetVal(SEncoder* pEncoder, const STqOffsetVal* pOffsetVal);
int32_t tDecodeSTqOffsetVal(SDecoder* pDecoder, STqOffsetVal* pOffsetVal);
void    tFormatOffset(char* buf, int32_t maxLen, const STqOffsetVal* pVal);
bool    tOffsetEqual(const STqOffsetVal* pLeft, const STqOffsetVal* pRight);
void    tOffsetCopy(STqOffsetVal* pLeft, const STqOffsetVal* pRight);
void    tOffsetDestroy(void* pVal);

typedef struct {
  STqOffsetVal val;
  char         subKey[TSDB_SUBSCRIBE_KEY_LEN];
} STqOffset;

int32_t tEncodeSTqOffset(SEncoder* pEncoder, const STqOffset* pOffset);
int32_t tDecodeSTqOffset(SDecoder* pDecoder, STqOffset* pOffset);
void    tDeleteSTqOffset(void* val);

typedef struct SMqVgOffset {
  int64_t   consumerId;
  STqOffset offset;
} SMqVgOffset;

int32_t tEncodeMqVgOffset(SEncoder* pEncoder, const SMqVgOffset* pOffset);
int32_t tDecodeMqVgOffset(SDecoder* pDecoder, SMqVgOffset* pOffset);

typedef struct {
  char    name[TSDB_TABLE_FNAME_LEN];
  char    stb[TSDB_TABLE_FNAME_LEN];
  int8_t  igExists;
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int8_t  timezone;  // int8_t is not enough, timezone is unit of second
  int32_t dstVgId;   // for stream
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int64_t maxDelay;
  int64_t watermark;
  int32_t exprLen;        // strlen + 1
  int32_t tagsFilterLen;  // strlen + 1
  int32_t sqlLen;         // strlen + 1
  int32_t astLen;         // strlen + 1
  char*   expr;
  char*   tagsFilter;
  char*   sql;
  char*   ast;
  int64_t deleteMark;
  int64_t lastTs;
  int64_t normSourceTbUid;  // the Uid of source tb if its a normal table, otherwise 0
  SArray* pVgroupVerList;
  int8_t  recursiveTsma;
  char    baseTsmaName[TSDB_TABLE_FNAME_LEN];  // base tsma name for recursively created tsma
  char*   createStreamReq;
  int32_t streamReqLen;
  char*   dropStreamReq;
  int32_t dropStreamReqLen;
  int64_t uid;
} SMCreateSmaReq;

int32_t tSerializeSMCreateSmaReq(void* buf, int32_t bufLen, SMCreateSmaReq* pReq);
int32_t tDeserializeSMCreateSmaReq(void* buf, int32_t bufLen, SMCreateSmaReq* pReq);
void    tFreeSMCreateSmaReq(SMCreateSmaReq* pReq);

typedef struct {
  char   name[TSDB_TABLE_FNAME_LEN];
  int8_t igNotExists;
  char*   dropStreamReq;
  int32_t dropStreamReqLen;
} SMDropSmaReq;

int32_t tSerializeSMDropSmaReq(void* buf, int32_t bufLen, SMDropSmaReq* pReq);
int32_t tDeserializeSMDropSmaReq(void* buf, int32_t bufLen, SMDropSmaReq* pReq);

typedef struct {
  char   dbFName[TSDB_DB_FNAME_LEN];
  char   stbName[TSDB_TABLE_NAME_LEN];
  char   colName[TSDB_COL_NAME_LEN];
  char   idxName[TSDB_INDEX_FNAME_LEN];
  int8_t idxType;
} SCreateTagIndexReq;

int32_t tSerializeSCreateTagIdxReq(void* buf, int32_t bufLen, SCreateTagIndexReq* pReq);
int32_t tDeserializeSCreateTagIdxReq(void* buf, int32_t bufLen, SCreateTagIndexReq* pReq);

typedef SMDropSmaReq SDropTagIndexReq;

// int32_t tSerializeSDropTagIdxReq(void* buf, int32_t bufLen, SDropTagIndexReq* pReq);
int32_t tDeserializeSDropTagIdxReq(void* buf, int32_t bufLen, SDropTagIndexReq* pReq);

typedef struct {
  int8_t         version;       // for compatibility(default 0)
  int8_t         intervalUnit;  // MACRO: TIME_UNIT_XXX
  int8_t         slidingUnit;   // MACRO: TIME_UNIT_XXX
  int8_t         timezoneInt;   // sma data expired if timezone changes.
  int32_t        dstVgId;
  char           indexName[TSDB_INDEX_NAME_LEN];
  int32_t        exprLen;
  int32_t        tagsFilterLen;
  int64_t        indexUid;
  tb_uid_t       tableUid;  // super/child/common table uid
  tb_uid_t       dstTbUid;  // for dstVgroup
  int64_t        interval;
  int64_t        offset;  // use unit by precision of DB
  int64_t        sliding;
  char*          dstTbName;  // for dstVgroup
  char*          expr;       // sma expression
  char*          tagsFilter;
  SSchemaWrapper schemaRow;  // for dstVgroup
  SSchemaWrapper schemaTag;  // for dstVgroup
} STSma;                     // Time-range-wise SMA

typedef STSma SVCreateTSmaReq;

typedef struct {
  int8_t  type;  // 0 status report, 1 update data
  int64_t indexUid;
  int64_t skey;  // start TS key of interval/sliding window
} STSmaMsg;

typedef struct {
  int64_t indexUid;
  char    indexName[TSDB_INDEX_NAME_LEN];
} SVDropTSmaReq;

typedef struct {
  int tmp;  // TODO: to avoid compile error
} SVCreateTSmaRsp, SVDropTSmaRsp;

#if 0
int32_t tSerializeSVCreateTSmaReq(void** buf, SVCreateTSmaReq* pReq);
void*   tDeserializeSVCreateTSmaReq(void* buf, SVCreateTSmaReq* pReq);
int32_t tSerializeSVDropTSmaReq(void** buf, SVDropTSmaReq* pReq);
void*   tDeserializeSVDropTSmaReq(void* buf, SVDropTSmaReq* pReq);
#endif

int32_t tEncodeSVCreateTSmaReq(SEncoder* pCoder, const SVCreateTSmaReq* pReq);
int32_t tDecodeSVCreateTSmaReq(SDecoder* pCoder, SVCreateTSmaReq* pReq);
int32_t tEncodeSVDropTSmaReq(SEncoder* pCoder, const SVDropTSmaReq* pReq);
// int32_t tDecodeSVDropTSmaReq(SDecoder* pCoder, SVDropTSmaReq* pReq);

typedef struct {
  int32_t number;
  STSma*  tSma;
} STSmaWrapper;

static FORCE_INLINE void tDestroyTSma(STSma* pSma) {
  if (pSma) {
    taosMemoryFreeClear(pSma->dstTbName);
    taosMemoryFreeClear(pSma->expr);
    taosMemoryFreeClear(pSma->tagsFilter);
  }
}

static FORCE_INLINE void tDestroyTSmaWrapper(STSmaWrapper* pSW, bool deepCopy) {
  if (pSW) {
    if (pSW->tSma) {
      if (deepCopy) {
        for (uint32_t i = 0; i < pSW->number; ++i) {
          tDestroyTSma(pSW->tSma + i);
        }
      }
      taosMemoryFreeClear(pSW->tSma);
    }
  }
}

static FORCE_INLINE void* tFreeTSmaWrapper(STSmaWrapper* pSW, bool deepCopy) {
  tDestroyTSmaWrapper(pSW, deepCopy);
  taosMemoryFreeClear(pSW);
  return NULL;
}

int32_t tEncodeSVCreateTSmaReq(SEncoder* pCoder, const SVCreateTSmaReq* pReq);
int32_t tDecodeSVCreateTSmaReq(SDecoder* pCoder, SVCreateTSmaReq* pReq);

int32_t tEncodeTSma(SEncoder* pCoder, const STSma* pSma);
int32_t tDecodeTSma(SDecoder* pCoder, STSma* pSma, bool deepCopy);

static int32_t tEncodeTSmaWrapper(SEncoder* pEncoder, const STSmaWrapper* pReq) {
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, pReq->number));
  for (int32_t i = 0; i < pReq->number; ++i) {
    TAOS_CHECK_RETURN(tEncodeTSma(pEncoder, pReq->tSma + i));
  }
  return 0;
}

static int32_t tDecodeTSmaWrapper(SDecoder* pDecoder, STSmaWrapper* pReq, bool deepCopy) {
  TAOS_CHECK_RETURN(tDecodeI32(pDecoder, &pReq->number));
  for (int32_t i = 0; i < pReq->number; ++i) {
    TAOS_CHECK_RETURN(tDecodeTSma(pDecoder, pReq->tSma + i, deepCopy));
  }
  return 0;
}

typedef struct {
  int idx;
} SMCreateFullTextReq;

int32_t tSerializeSMCreateFullTextReq(void* buf, int32_t bufLen, SMCreateFullTextReq* pReq);
int32_t tDeserializeSMCreateFullTextReq(void* buf, int32_t bufLen, SMCreateFullTextReq* pReq);
void    tFreeSMCreateFullTextReq(SMCreateFullTextReq* pReq);

typedef struct {
  char   name[TSDB_TABLE_FNAME_LEN];
  int8_t igNotExists;
} SMDropFullTextReq;

// int32_t tSerializeSMDropFullTextReq(void* buf, int32_t bufLen, SMDropFullTextReq* pReq);
// int32_t tDeserializeSMDropFullTextReq(void* buf, int32_t bufLen, SMDropFullTextReq* pReq);

typedef struct {
  char indexFName[TSDB_INDEX_FNAME_LEN];
} SUserIndexReq;

int32_t tSerializeSUserIndexReq(void* buf, int32_t bufLen, SUserIndexReq* pReq);
int32_t tDeserializeSUserIndexReq(void* buf, int32_t bufLen, SUserIndexReq* pReq);

typedef struct {
  char dbFName[TSDB_DB_FNAME_LEN];
  char tblFName[TSDB_TABLE_FNAME_LEN];
  char colName[TSDB_COL_NAME_LEN];
  char indexType[TSDB_INDEX_TYPE_LEN];
  char indexExts[TSDB_INDEX_EXTS_LEN];
} SUserIndexRsp;

int32_t tSerializeSUserIndexRsp(void* buf, int32_t bufLen, const SUserIndexRsp* pRsp);
int32_t tDeserializeSUserIndexRsp(void* buf, int32_t bufLen, SUserIndexRsp* pRsp);

typedef struct {
  char tbFName[TSDB_TABLE_FNAME_LEN];
} STableIndexReq;

int32_t tSerializeSTableIndexReq(void* buf, int32_t bufLen, STableIndexReq* pReq);
int32_t tDeserializeSTableIndexReq(void* buf, int32_t bufLen, STableIndexReq* pReq);

typedef struct {
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int64_t dstTbUid;
  int32_t dstVgId;
  SEpSet  epSet;
  char*   expr;
} STableIndexInfo;

typedef struct {
  char     tbName[TSDB_TABLE_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  uint64_t suid;
  int32_t  version;
  int32_t  indexSize;
  SArray*  pIndex;  // STableIndexInfo
} STableIndexRsp;

int32_t tSerializeSTableIndexRsp(void* buf, int32_t bufLen, const STableIndexRsp* pRsp);
int32_t tDeserializeSTableIndexRsp(void* buf, int32_t bufLen, STableIndexRsp* pRsp);
void    tFreeSerializeSTableIndexRsp(STableIndexRsp* pRsp);

void tFreeSTableIndexInfo(void* pInfo);

typedef struct {
  int8_t  mqMsgType;
  int32_t code;
  int32_t epoch;
  int64_t consumerId;
  int64_t walsver;
  int64_t walever;
} SMqRspHead;

typedef struct {
  SMsgHead     head;
  char         subKey[TSDB_SUBSCRIBE_KEY_LEN];
  int8_t       withTbName;
  int8_t       useSnapshot;
  int32_t      epoch;
  uint64_t     reqId;
  int64_t      consumerId;
  int64_t      timeout;
  STqOffsetVal reqOffset;
  int8_t       enableReplay;
  int8_t       sourceExcluded;
  int8_t       rawData;
  int32_t      minPollRows;
  int8_t       enableBatchMeta;
  SHashObj*    uidHash;  // to find if uid is duplicated
} SMqPollReq;

int32_t tSerializeSMqPollReq(void* buf, int32_t bufLen, SMqPollReq* pReq);
int32_t tDeserializeSMqPollReq(void* buf, int32_t bufLen, SMqPollReq* pReq);
void    tDestroySMqPollReq(SMqPollReq* pReq);

typedef struct {
  int32_t vgId;
  int64_t offset;
  SEpSet  epSet;
} SMqSubVgEp;

static FORCE_INLINE int32_t tEncodeSMqSubVgEp(void** buf, const SMqSubVgEp* pVgEp) {
  int32_t tlen = 0;
  tlen += taosEncodeFixedI32(buf, pVgEp->vgId);
  tlen += taosEncodeFixedI64(buf, pVgEp->offset);
  tlen += taosEncodeSEpSet(buf, &pVgEp->epSet);
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqSubVgEp(void* buf, SMqSubVgEp* pVgEp) {
  buf = taosDecodeFixedI32(buf, &pVgEp->vgId);
  buf = taosDecodeFixedI64(buf, &pVgEp->offset);
  buf = taosDecodeSEpSet(buf, &pVgEp->epSet);
  return buf;
}

typedef struct {
  char           topic[TSDB_TOPIC_FNAME_LEN];
  char           db[TSDB_DB_FNAME_LEN];
  SArray*        vgs;  // SArray<SMqSubVgEp>
  SSchemaWrapper schema;
} SMqSubTopicEp;

int32_t tEncodeMqSubTopicEp(void** buf, const SMqSubTopicEp* pTopicEp);
void*   tDecodeMqSubTopicEp(void* buf, SMqSubTopicEp* pTopicEp);
void    tDeleteMqSubTopicEp(SMqSubTopicEp* pSubTopicEp);

typedef struct {
  SMqRspHead   head;
  STqOffsetVal rspOffset;
  int16_t      resMsgType;
  int32_t      metaRspLen;
  void*        metaRsp;
} SMqMetaRsp;

int32_t tEncodeMqMetaRsp(SEncoder* pEncoder, const SMqMetaRsp* pRsp);
int32_t tDecodeMqMetaRsp(SDecoder* pDecoder, SMqMetaRsp* pRsp);
void    tDeleteMqMetaRsp(SMqMetaRsp* pRsp);

#define MQ_DATA_RSP_VERSION 100

typedef struct {
  SMqRspHead   head;
  STqOffsetVal rspOffset;
  STqOffsetVal reqOffset;
  int32_t      blockNum;
  int8_t       withTbName;
  int8_t       withSchema;
  SArray*      blockDataLen;
  SArray*      blockData;
  SArray*      blockTbName;
  SArray*      blockSchema;

  union {
    struct {
      int64_t sleepTime;
    };
    struct {
      int32_t createTableNum;
      SArray* createTableLen;
      SArray* createTableReq;
    };
    struct {
      int32_t len;
      void*   rawData;
    };
  };
  void* data;                  // for free in client, only effected if type is data or metadata. raw data not effected
  bool  blockDataElementFree;  // if true, free blockDataElement in blockData,(true in server, false in client)
} SMqDataRsp;

int32_t tEncodeMqDataRsp(SEncoder* pEncoder, const SMqDataRsp* pObj);
int32_t tDecodeMqDataRsp(SDecoder* pDecoder, SMqDataRsp* pRsp);
int32_t tDecodeMqRawDataRsp(SDecoder* pDecoder, SMqDataRsp* pRsp);
void    tDeleteMqDataRsp(SMqDataRsp* pRsp);
void    tDeleteMqRawDataRsp(SMqDataRsp* pRsp);

int32_t tEncodeSTaosxRsp(SEncoder* pEncoder, const SMqDataRsp* pRsp);
int32_t tDecodeSTaosxRsp(SDecoder* pDecoder, SMqDataRsp* pRsp);
void    tDeleteSTaosxRsp(SMqDataRsp* pRsp);

typedef struct SMqBatchMetaRsp {
  SMqRspHead   head;  // not serialize
  STqOffsetVal rspOffset;
  SArray*      batchMetaLen;
  SArray*      batchMetaReq;
  void*        pMetaBuff;    // not serialize
  uint32_t     metaBuffLen;  // not serialize
} SMqBatchMetaRsp;

int32_t tEncodeMqBatchMetaRsp(SEncoder* pEncoder, const SMqBatchMetaRsp* pRsp);
int32_t tDecodeMqBatchMetaRsp(SDecoder* pDecoder, SMqBatchMetaRsp* pRsp);
int32_t tSemiDecodeMqBatchMetaRsp(SDecoder* pDecoder, SMqBatchMetaRsp* pRsp);
void    tDeleteMqBatchMetaRsp(SMqBatchMetaRsp* pRsp);

typedef struct {
  SMqRspHead head;
  char       cgroup[TSDB_CGROUP_LEN];
  SArray*    topics;  // SArray<SMqSubTopicEp>
} SMqAskEpRsp;

static FORCE_INLINE int32_t tEncodeSMqAskEpRsp(void** buf, const SMqAskEpRsp* pRsp) {
  int32_t tlen = 0;
  // tlen += taosEncodeString(buf, pRsp->cgroup);
  int32_t sz = taosArrayGetSize(pRsp->topics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SMqSubTopicEp* pVgEp = (SMqSubTopicEp*)taosArrayGet(pRsp->topics, i);
    tlen += tEncodeMqSubTopicEp(buf, pVgEp);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqAskEpRsp(void* buf, SMqAskEpRsp* pRsp) {
  // buf = taosDecodeStringTo(buf, pRsp->cgroup);
  int32_t sz;
  buf = taosDecodeFixedI32(buf, &sz);
  pRsp->topics = taosArrayInit(sz, sizeof(SMqSubTopicEp));
  if (pRsp->topics == NULL) {
    return NULL;
  }
  for (int32_t i = 0; i < sz; i++) {
    SMqSubTopicEp topicEp;
    buf = tDecodeMqSubTopicEp(buf, &topicEp);
    if (buf == NULL) {
      return NULL;
    }
    if ((taosArrayPush(pRsp->topics, &topicEp) == NULL)) {
      return NULL;
    }
  }
  return buf;
}

static FORCE_INLINE void tDeleteSMqAskEpRsp(SMqAskEpRsp* pRsp) {
  taosArrayDestroyEx(pRsp->topics, (FDelete)tDeleteMqSubTopicEp);
}

typedef struct {
  int32_t      vgId;
  STqOffsetVal offset;
  int64_t      rows;
  int64_t      ever;
} OffsetRows;

typedef struct {
  char    topicName[TSDB_TOPIC_FNAME_LEN];
  SArray* offsetRows;
} TopicOffsetRows;

typedef struct {
  int64_t consumerId;
  int32_t epoch;
  SArray* topics;
  int8_t  pollFlag;
} SMqHbReq;

typedef struct {
  char   topic[TSDB_TOPIC_FNAME_LEN];
  int8_t noPrivilege;
} STopicPrivilege;

typedef struct {
  SArray* topicPrivileges;  // SArray<STopicPrivilege>
  int32_t debugFlag;
} SMqHbRsp;

typedef struct {
  SMsgHead head;
  int64_t  consumerId;
  char     subKey[TSDB_SUBSCRIBE_KEY_LEN];
} SMqSeekReq;

#define TD_AUTO_CREATE_TABLE 0x1
typedef struct {
  int64_t       suid;
  int64_t       uid;
  int32_t       sver;
  uint32_t      nData;
  uint8_t*      pData;
  SVCreateTbReq cTbReq;
} SVSubmitBlk;

typedef struct {
  SMsgHead header;
  uint64_t sId;
  uint64_t queryId;
  uint64_t clientId;
  uint64_t taskId;
  uint32_t sqlLen;
  uint32_t phyLen;
  char*    sql;
  char*    msg;
  int8_t   source;
} SVDeleteReq;

int32_t tSerializeSVDeleteReq(void* buf, int32_t bufLen, SVDeleteReq* pReq);
int32_t tDeserializeSVDeleteReq(void* buf, int32_t bufLen, SVDeleteReq* pReq);

typedef struct {
  int64_t affectedRows;
} SVDeleteRsp;

int32_t tEncodeSVDeleteRsp(SEncoder* pCoder, const SVDeleteRsp* pReq);
int32_t tDecodeSVDeleteRsp(SDecoder* pCoder, SVDeleteRsp* pReq);

typedef struct SDeleteRes {
  uint64_t suid;
  SArray*  uidList;
  int64_t  skey;
  int64_t  ekey;
  int64_t  affectedRows;
  char     tableFName[TSDB_TABLE_NAME_LEN];
  char     tsColName[TSDB_COL_NAME_LEN];
  int64_t  ctimeMs;  // fill by vnode
  int8_t   source;
} SDeleteRes;

int32_t tEncodeDeleteRes(SEncoder* pCoder, const SDeleteRes* pRes);
int32_t tDecodeDeleteRes(SDecoder* pCoder, SDeleteRes* pRes);

typedef struct {
  // int64_t uid;
  char    tbname[TSDB_TABLE_NAME_LEN];
  int64_t startTs;
  int64_t endTs;
} SSingleDeleteReq;

int32_t tEncodeSSingleDeleteReq(SEncoder* pCoder, const SSingleDeleteReq* pReq);
int32_t tDecodeSSingleDeleteReq(SDecoder* pCoder, SSingleDeleteReq* pReq);

typedef struct {
  int64_t suid;
  SArray* deleteReqs;  // SArray<SSingleDeleteReq>
  int64_t ctimeMs;     // fill by vnode
  int8_t  level;       // 0 tsdb(default), 1 rsma1 , 2 rsma2
} SBatchDeleteReq;

int32_t tEncodeSBatchDeleteReq(SEncoder* pCoder, const SBatchDeleteReq* pReq);
int32_t tDecodeSBatchDeleteReq(SDecoder* pCoder, SBatchDeleteReq* pReq);
int32_t tDecodeSBatchDeleteReqSetCtime(SDecoder* pDecoder, SBatchDeleteReq* pReq, int64_t ctimeMs);

typedef struct {
  int32_t msgIdx;
  int32_t msgType;
  int32_t msgLen;
  void*   msg;
} SBatchMsg;

typedef struct {
  SMsgHead header;
  SArray*  pMsgs;  // SArray<SBatchMsg>
} SBatchReq;

typedef struct {
  int32_t reqType;
  int32_t msgIdx;
  int32_t msgLen;
  int32_t rspCode;
  void*   msg;
} SBatchRspMsg;

typedef struct {
  SArray* pRsps;  // SArray<SBatchRspMsg>
} SBatchRsp;

int32_t                  tSerializeSBatchReq(void* buf, int32_t bufLen, SBatchReq* pReq);
int32_t                  tDeserializeSBatchReq(void* buf, int32_t bufLen, SBatchReq* pReq);
static FORCE_INLINE void tFreeSBatchReqMsg(void* msg) {
  if (NULL == msg) {
    return;
  }
  SBatchMsg* pMsg = (SBatchMsg*)msg;
  taosMemoryFree(pMsg->msg);
}

int32_t tSerializeSBatchRsp(void* buf, int32_t bufLen, SBatchRsp* pRsp);
int32_t tDeserializeSBatchRsp(void* buf, int32_t bufLen, SBatchRsp* pRsp);

static FORCE_INLINE void tFreeSBatchRspMsg(void* p) {
  if (NULL == p) {
    return;
  }

  SBatchRspMsg* pRsp = (SBatchRspMsg*)p;
  taosMemoryFree(pRsp->msg);
}

int32_t tSerializeSMqAskEpReq(void* buf, int32_t bufLen, SMqAskEpReq* pReq);
int32_t tDeserializeSMqAskEpReq(void* buf, int32_t bufLen, SMqAskEpReq* pReq);
int32_t tSerializeSMqHbReq(void* buf, int32_t bufLen, SMqHbReq* pReq);
int32_t tDeserializeSMqHbReq(void* buf, int32_t bufLen, SMqHbReq* pReq);
void    tDestroySMqHbReq(SMqHbReq* pReq);

int32_t tSerializeSMqHbRsp(void* buf, int32_t bufLen, SMqHbRsp* pRsp);
int32_t tDeserializeSMqHbRsp(void* buf, int32_t bufLen, SMqHbRsp* pRsp);
void    tDestroySMqHbRsp(SMqHbRsp* pRsp);

int32_t tSerializeSMqSeekReq(void* buf, int32_t bufLen, SMqSeekReq* pReq);
int32_t tDeserializeSMqSeekReq(void* buf, int32_t bufLen, SMqSeekReq* pReq);

#define TD_REQ_FROM_APP               0x0
#define SUBMIT_REQ_AUTO_CREATE_TABLE  0x1
#define SUBMIT_REQ_COLUMN_DATA_FORMAT 0x2
#define SUBMIT_REQ_FROM_FILE          0x4
#define TD_REQ_FROM_TAOX              0x8
#define TD_REQ_FROM_SML               0x10
#define SUBMIT_REQUEST_VERSION        (1)
#define SUBMIT_REQ_WITH_BLOB          0x10
#define SUBMIT_REQ_SCHEMA_RES         0x20

#define TD_REQ_FROM_TAOX_OLD 0x1  // for compatibility

typedef struct {
  int32_t        flags;
  SVCreateTbReq* pCreateTbReq;
  int64_t        suid;
  int64_t        uid;
  int32_t        sver;
  union {
    SArray* aRowP;
    SArray* aCol;
  };
  int64_t    ctimeMs;
  SBlobSet*  pBlobSet;
} SSubmitTbData;

typedef struct {
  SArray* aSubmitTbData;  // SArray<SSubmitTbData>
  SArray* aSubmitBlobData;
  bool    raw;
} SSubmitReq2;

typedef struct {
  SMsgHead header;
  int64_t  version;
  char     data[];  // SSubmitReq2
} SSubmitReq2Msg;

int32_t transformRawSSubmitTbData(void* data, int64_t suid, int64_t uid, int32_t sver);
int32_t tEncodeSubmitReq(SEncoder* pCoder, const SSubmitReq2* pReq);
int32_t tDecodeSubmitReq(SDecoder* pCoder, SSubmitReq2* pReq, SArray* rawList);
void    tDestroySubmitTbData(SSubmitTbData* pTbData, int32_t flag);
void    tDestroySubmitReq(SSubmitReq2* pReq, int32_t flag);

typedef struct {
  int32_t affectedRows;
  SArray* aCreateTbRsp;  // SArray<SVCreateTbRsp>
} SSubmitRsp2;

int32_t tEncodeSSubmitRsp2(SEncoder* pCoder, const SSubmitRsp2* pRsp);
int32_t tDecodeSSubmitRsp2(SDecoder* pCoder, SSubmitRsp2* pRsp);
void    tDestroySSubmitRsp2(SSubmitRsp2* pRsp, int32_t flag);

#define TSDB_MSG_FLG_ENCODE 0x1
#define TSDB_MSG_FLG_DECODE 0x2
#define TSDB_MSG_FLG_CMPT   0x3

typedef struct {
  union {
    struct {
      void*   msgStr;
      int32_t msgLen;
      int64_t ver;
    };
    void* pDataBlock;
  };
} SPackedData;

typedef struct {
  char     fullname[TSDB_VIEW_FNAME_LEN];
  char     name[TSDB_VIEW_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  char*    querySql;
  char*    sql;
  int8_t   orReplace;
  int8_t   precision;
  int32_t  numOfCols;
  SSchema* pSchema;
} SCMCreateViewReq;

int32_t tSerializeSCMCreateViewReq(void* buf, int32_t bufLen, const SCMCreateViewReq* pReq);
int32_t tDeserializeSCMCreateViewReq(void* buf, int32_t bufLen, SCMCreateViewReq* pReq);
void    tFreeSCMCreateViewReq(SCMCreateViewReq* pReq);

typedef struct {
  char   fullname[TSDB_VIEW_FNAME_LEN];
  char   name[TSDB_VIEW_NAME_LEN];
  char   dbFName[TSDB_DB_FNAME_LEN];
  char*  sql;
  int8_t igNotExists;
} SCMDropViewReq;

int32_t tSerializeSCMDropViewReq(void* buf, int32_t bufLen, const SCMDropViewReq* pReq);
int32_t tDeserializeSCMDropViewReq(void* buf, int32_t bufLen, SCMDropViewReq* pReq);
void    tFreeSCMDropViewReq(SCMDropViewReq* pReq);

typedef struct {
  char fullname[TSDB_VIEW_FNAME_LEN];
} SViewMetaReq;
int32_t tSerializeSViewMetaReq(void* buf, int32_t bufLen, const SViewMetaReq* pReq);
int32_t tDeserializeSViewMetaReq(void* buf, int32_t bufLen, SViewMetaReq* pReq);

typedef struct {
  char     name[TSDB_VIEW_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  char*    user;
  uint64_t dbId;
  uint64_t viewId;
  char*    querySql;
  int8_t   precision;
  int8_t   type;
  int32_t  version;
  int32_t  numOfCols;
  SSchema* pSchema;
} SViewMetaRsp;
int32_t tSerializeSViewMetaRsp(void* buf, int32_t bufLen, const SViewMetaRsp* pRsp);
int32_t tDeserializeSViewMetaRsp(void* buf, int32_t bufLen, SViewMetaRsp* pRsp);
void    tFreeSViewMetaRsp(SViewMetaRsp* pRsp);
typedef struct {
  char name[TSDB_TABLE_FNAME_LEN];  // table name or tsma name
  bool fetchingWithTsmaName;        // if we are fetching with tsma name
} STableTSMAInfoReq;

int32_t tSerializeTableTSMAInfoReq(void* buf, int32_t bufLen, const STableTSMAInfoReq* pReq);
int32_t tDeserializeTableTSMAInfoReq(void* buf, int32_t bufLen, STableTSMAInfoReq* pReq);

typedef struct {
  int32_t  funcId;
  col_id_t colId;
} STableTSMAFuncInfo;

typedef struct {
  char     name[TSDB_TABLE_NAME_LEN];
  uint64_t tsmaId;
  char     targetTb[TSDB_TABLE_NAME_LEN];
  char     targetDbFName[TSDB_DB_FNAME_LEN];
  char     tb[TSDB_TABLE_NAME_LEN];
  char     dbFName[TSDB_DB_FNAME_LEN];
  uint64_t suid;
  uint64_t destTbUid;
  uint64_t dbId;
  int32_t  version;
  int64_t  interval;
  int8_t   unit;
  SArray*  pFuncs;     // SArray<STableTSMAFuncInfo>
  SArray*  pTags;      // SArray<SSchema>
  SArray*  pUsedCols;  // SArray<SSchema>
  char*    ast;

  int64_t streamUid;
  int64_t reqTs;
  int64_t rspTs;
  int64_t delayDuration;  // ms
  bool    fillHistoryFinished;

  void*   streamAddr;  // for stream task, the address of the stream task
} STableTSMAInfo;

int32_t tSerializeTableTSMAInfoRsp(void* buf, int32_t bufLen, const STableTSMAInfoRsp* pRsp);
int32_t tDeserializeTableTSMAInfoRsp(void* buf, int32_t bufLen, STableTSMAInfoRsp* pRsp);
int32_t tCloneTbTSMAInfo(STableTSMAInfo* pInfo, STableTSMAInfo** pRes);
void    tFreeTableTSMAInfo(void* p);
void    tFreeAndClearTableTSMAInfo(void* p);
void    tFreeTableTSMAInfoRsp(STableTSMAInfoRsp* pRsp);

#define STSMAHbRsp            STableTSMAInfoRsp
#define tSerializeTSMAHbRsp   tSerializeTableTSMAInfoRsp
#define tDeserializeTSMAHbRsp tDeserializeTableTSMAInfoRsp
#define tFreeTSMAHbRsp        tFreeTableTSMAInfoRsp

typedef struct SDropCtbWithTsmaSingleVgReq {
  SVgroupInfo vgInfo;
  SArray*     pTbs;  // SVDropTbReq
} SMDropTbReqsOnSingleVg;

int32_t tEncodeSMDropTbReqOnSingleVg(SEncoder* pEncoder, const SMDropTbReqsOnSingleVg* pReq);
int32_t tDecodeSMDropTbReqOnSingleVg(SDecoder* pDecoder, SMDropTbReqsOnSingleVg* pReq);
void    tFreeSMDropTbReqOnSingleVg(void* p);

typedef struct SDropTbsReq {
  SArray* pVgReqs;  // SMDropTbReqsOnSingleVg
} SMDropTbsReq;

int32_t tSerializeSMDropTbsReq(void* buf, int32_t bufLen, const SMDropTbsReq* pReq);
int32_t tDeserializeSMDropTbsReq(void* buf, int32_t bufLen, SMDropTbsReq* pReq);
void    tFreeSMDropTbsReq(void*);

typedef struct SVFetchTtlExpiredTbsRsp {
  SArray* pExpiredTbs;  // SVDropTbReq
  int32_t vgId;
} SVFetchTtlExpiredTbsRsp;

int32_t tEncodeVFetchTtlExpiredTbsRsp(SEncoder* pCoder, const SVFetchTtlExpiredTbsRsp* pRsp);
int32_t tDecodeVFetchTtlExpiredTbsRsp(SDecoder* pCoder, SVFetchTtlExpiredTbsRsp* pRsp);

void tFreeFetchTtlExpiredTbsRsp(void* p);

void setDefaultOptionsForField(SFieldWithOptions* field);
void setFieldWithOptions(SFieldWithOptions* fieldWithOptions, SField* field);

int32_t tSerializeSVSubTablesRspImpl(SEncoder* pEncoder, SVSubTablesRsp *pRsp);
int32_t tDeserializeSVSubTablesRspImpl(SDecoder* pDecoder, SVSubTablesRsp *pRsp);

#ifdef USE_MOUNT
typedef struct {
  char     mountName[TSDB_MOUNT_NAME_LEN];
  int8_t   ignoreExist;
  int16_t  nMounts;
  int32_t* dnodeIds;
  char**   mountPaths;
  int32_t  sqlLen;
  char*    sql;
} SCreateMountReq;

int32_t tSerializeSCreateMountReq(void* buf, int32_t bufLen, SCreateMountReq* pReq);
int32_t tDeserializeSCreateMountReq(void* buf, int32_t bufLen, SCreateMountReq* pReq);
void    tFreeSCreateMountReq(SCreateMountReq* pReq);

typedef struct {
  char    mountName[TSDB_MOUNT_NAME_LEN];
  int8_t  ignoreNotExists;
  int32_t sqlLen;
  char*   sql;
} SDropMountReq;

int32_t tSerializeSDropMountReq(void* buf, int32_t bufLen, SDropMountReq* pReq);
int32_t tDeserializeSDropMountReq(void* buf, int32_t bufLen, SDropMountReq* pReq);
void    tFreeSDropMountReq(SDropMountReq* pReq);

typedef struct {
  char     mountName[TSDB_MOUNT_NAME_LEN];
  char     mountPath[TSDB_MOUNT_PATH_LEN];
  int64_t  mountUid;
  int32_t  dnodeId;
  uint32_t valLen;
  int8_t   ignoreExist;
  void*    pVal;
} SRetrieveMountPathReq;

int32_t tSerializeSRetrieveMountPathReq(void* buf, int32_t bufLen, SRetrieveMountPathReq* pReq);
int32_t tDeserializeSRetrieveMountPathReq(void* buf, int32_t bufLen, SRetrieveMountPathReq* pReq);

typedef struct {
  // path
  int32_t diskPrimary;
  // vgInfo
  int32_t  vgId;
  int32_t  cacheLastSize;
  int32_t  szPage;
  int32_t  szCache;
  uint64_t szBuf;
  int8_t   cacheLast;
  int8_t   standby;
  int8_t   hashMethod;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int16_t  hashPrefix;
  int16_t  hashSuffix;
  int16_t  sttTrigger;
  // syncInfo
  int32_t replications;
  // tsdbInfo
  int8_t  precision;
  int8_t  compression;
  int8_t  slLevel;
  int32_t daysPerFile;
  int32_t keep0;
  int32_t keep1;
  int32_t keep2;
  int32_t keepTimeOffset;
  int32_t minRows;
  int32_t maxRows;
  int32_t tsdbPageSize;
  int32_t ssChunkSize;
  int32_t ssKeepLocal;
  int8_t  ssCompact;
  // walInfo
  int32_t walFsyncPeriod;      // millisecond
  int32_t walRetentionPeriod;  // secs
  int32_t walRollPeriod;       // secs
  int64_t walRetentionSize;
  int64_t walSegSize;
  int32_t walLevel;
  // encryptInfo
  int32_t encryptAlgorithm;
  // SVState
  int64_t committed;
  int64_t commitID;
  int64_t commitTerm;
  // stats
  int64_t numOfSTables;
  int64_t numOfCTables;
  int64_t numOfNTables;
  // dbInfo
  uint64_t dbId;
} SMountVgInfo;

typedef struct {
  SMCreateStbReq req;
  SArray*        pColExts;  // element: column id
  SArray*        pTagExts;  // element: tag id
} SMountStbInfo;

int32_t tSerializeSMountStbInfo(void* buf, int32_t bufLen, int32_t* pFLen, SMountStbInfo* pInfo);
int32_t tDeserializeSMountStbInfo(void* buf, int32_t bufLen, int32_t flen, SMountStbInfo* pInfo);

typedef struct {
  char     dbName[TSDB_DB_FNAME_LEN];
  uint64_t dbId;
  SArray*  pVgs;      // SMountVgInfo
  SArray*  pStbs;     // 0 serialized binary of SMountStbInfo, 1 SMountStbInfo
} SMountDbInfo;

typedef struct {
  // common fields
  char     mountName[TSDB_MOUNT_NAME_LEN];
  char     mountPath[TSDB_MOUNT_PATH_LEN];
  int8_t   ignoreExist;
  int64_t  mountUid;
  int64_t  clusterId;
  int32_t  dnodeId;
  uint32_t valLen;
  void*    pVal;

  // response fields
  SArray* pDbs;  // SMountDbInfo

  // memory fields, no serialized
  SArray*   pDisks[TFS_MAX_TIERS];
  TdFilePtr pFile;
} SMountInfo;

int32_t tSerializeSMountInfo(void* buf, int32_t bufLen, SMountInfo* pReq);
int32_t tDeserializeSMountInfo(SDecoder* decoder, SMountInfo* pReq, bool extractStb);
void    tFreeMountInfo(SMountInfo* pReq, bool stbExtracted);

typedef struct {
  SCreateVnodeReq createReq;
  char            mountPath[TSDB_MOUNT_FPATH_LEN];
  char            mountName[TSDB_MOUNT_NAME_LEN];
  int64_t         mountId;
  int32_t         diskPrimary;
  int32_t         mountVgId;
  int64_t         committed;
  int64_t         commitID;
  int64_t         commitTerm;
  int64_t         numOfSTables;
  int64_t         numOfCTables;
  int64_t         numOfNTables;
} SMountVnodeReq;

int32_t tSerializeSMountVnodeReq(void *buf, int32_t *cBufLen, int32_t *tBufLen, SMountVnodeReq *pReq);
int32_t tDeserializeSMountVnodeReq(void* buf, int32_t bufLen, SMountVnodeReq* pReq);
int32_t tFreeSMountVnodeReq(SMountVnodeReq* pReq);

#endif  // USE_MOUNT

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TAOS_MSG_H_*/
