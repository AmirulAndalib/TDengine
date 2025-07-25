// sample code to verify all TDengine API
// to compile: gcc -o apitest apitest.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"
static int64_t count = 10000;

int64_t genReqid() {
  count += 100;
  return count;
}

void stmtAsyncQueryCb(void* param, TAOS_RES* pRes, int code) {
  int affected_rows = taos_affected_rows(pRes);
  return;
  /*
  SSP_CB_PARAM* qParam = (SSP_CB_PARAM*)param;
  if (code == 0 && pRes) {
    if (qParam->fetch) {
      taos_fetch_rows_a(pRes, sqAsyncFetchCb, param);
    } else {
      if (qParam->free) {
        taos_free_result(pRes);
      }
      *qParam->end = 1;
    }
  } else {
    sqError("select", taos_errstr(pRes));
    *qParam->end = 1;
    taos_free_result(pRes);
  }
  */
}

void veriry_stmt(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  /*
  const char* sql =
      "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin "
      "binary(40), blob nchar(10))";
  */
  const char* sql =
      "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, blob2 "
      "nchar(10), blobt nchar(10))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  // insert 10 records
  struct {
    int64_t ts[10];
    int8_t  b[10];
    int8_t  v1[10];
    int16_t v2[10];
    int32_t v4[10];
    int64_t v8[10];
    float   f4[10];
    double  f8[10];
    char    bin[10][40];
    char    blob[10][1];
    char    blob2[10][1];
  } v;

  int32_t* t8_len = malloc(sizeof(int32_t) * 10);
  int32_t* t16_len = malloc(sizeof(int32_t) * 10);
  int32_t* t32_len = malloc(sizeof(int32_t) * 10);
  int32_t* t64_len = malloc(sizeof(int32_t) * 10);
  int32_t* float_len = malloc(sizeof(int32_t) * 10);
  int32_t* double_len = malloc(sizeof(int32_t) * 10);
  int32_t* bin_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len2 = malloc(sizeof(int32_t) * 10);

#include "time.h"
  clock_t           start, end;
  TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, NULL};

  start = clock();
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  end = clock();
  printf("init time:%f\n", (double)(end - start) / CLOCKS_PER_SEC);
  // TAOS_MULTI_BIND params[10];
  TAOS_STMT2_BIND params[10];
  char            is_null[10] = {0};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  // params[0].buffer_length = sizeof(v.ts[0]);
  params[0].buffer = v.ts;
  params[0].length = t64_len;
  params[0].is_null = is_null;
  params[0].num = 10;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  // params[1].buffer_length = sizeof(v.b[0]);
  params[1].buffer = v.b;
  params[1].length = t8_len;
  params[1].is_null = is_null;
  params[1].num = 10;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  // params[2].buffer_length = sizeof(v.v1[0]);
  params[2].buffer = v.v1;
  params[2].length = t8_len;
  params[2].is_null = is_null;
  params[2].num = 10;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  // params[3].buffer_length = sizeof(v.v2[0]);
  params[3].buffer = v.v2;
  params[3].length = t16_len;
  params[3].is_null = is_null;
  params[3].num = 10;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  // params[4].buffer_length = sizeof(v.v4[0]);
  params[4].buffer = v.v4;
  params[4].length = t32_len;
  params[4].is_null = is_null;
  params[4].num = 10;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  // params[5].buffer_length = sizeof(v.v8[0]);
  params[5].buffer = v.v8;
  params[5].length = t64_len;
  params[5].is_null = is_null;
  params[5].num = 10;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  // params[6].buffer_length = sizeof(v.f4[0]);
  params[6].buffer = v.f4;
  params[6].length = float_len;
  params[6].is_null = is_null;
  params[6].num = 10;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  // params[7].buffer_length = sizeof(v.f8[0]);
  params[7].buffer = v.f8;
  params[7].length = double_len;
  params[7].is_null = is_null;
  params[7].num = 10;
  /*
  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  //params[8].buffer_length = sizeof(v.bin[0]);
  params[8].buffer = v.bin;
  params[8].length = bin_len;
  params[8].is_null = is_null;
  params[8].num = 10;
  */
  params[8].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[8].buffer_length = sizeof(v.blob2[0]);
  params[8].buffer = v.blob2;
  params[8].length = blob_len2;
  params[8].is_null = is_null;
  params[8].num = 10;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[9].buffer_length = sizeof(v.blob[0]);
  params[9].buffer = v.blob;
  params[9].length = blob_len;
  params[9].is_null = is_null;
  params[9].num = 10;

  sql = "insert into ? (ts, b, v1, v2, v4, v8, f4, f8, blob2, blob) values(?,?,?,?,?,?,?,?,?,?)";
  start = clock();
  code = taos_stmt2_prepare(stmt, sql, 0);
  end = clock();
  printf("prepare time:%f\n", (double)(end - start) / CLOCKS_PER_SEC);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  /*
  code = taos_stmt_set_tbname(stmt, "m1");
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  */

  int64_t ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    is_null[i] = 0;

    v.ts[i] = ts++;
    v.b[i] = (int8_t)i % 2;
    v.v1[i] = (int8_t)i;
    v.v2[i] = (int16_t)(i * 2);
    v.v4[i] = (int32_t)(i * 4);
    v.v8[i] = (int64_t)(i * 8);
    v.f4[i] = (float)(i * 40);
    v.f8[i] = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin[0]); ++j) {
      v.bin[i][j] = (char)(i + '0');
    }
    v.blob[i][0] = 'a' + i;
    v.blob2[i][0] = 'A' + i;

    // v.blob2[i] = malloc(strlen("一二三四五六七十九八"));
    // v.blob[i] = malloc(strlen("十九八七六五四三二一"));

    // strcpy(v.blob2[i], "一二三四五六七十九八");
    // strcpy(v.blob[i], "十九八七六五四三二一");

    t8_len[i] = sizeof(int8_t);
    t16_len[i] = sizeof(int16_t);
    t32_len[i] = sizeof(int32_t);
    t64_len[i] = sizeof(int64_t);
    float_len[i] = sizeof(float);
    double_len[i] = sizeof(double);
    bin_len[i] = sizeof(v.bin[0]);
    blob_len[i] = sizeof(char);
    blob_len2[i] = sizeof(char);
  }
  char*            tbname = "m1";
  TAOS_STMT2_BIND* bind_cols[1] = {&params[0]};
  TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &bind_cols[0]};
  start = clock();
  // taos_stmt2_bind_param(stmt, "m1", NULL, params, -1);
  taos_stmt2_bind_param(stmt, &bindv, -1);
  end = clock();
  printf("bind time:%f\n", (double)(end - start) / CLOCKS_PER_SEC);
  // taos_stmt_bind_param_batch(stmt, params);
  // taos_stmt_add_batch(stmt);
  /*
  int param_count = -1;
  code = taos_stmt2_param_count(stmt, &param_count);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_param_count. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  printf("param_count: %d\n", param_count);
  */
  TAOS_FIELD_ALL* fields = NULL;
  int           field_count = -1;
  start = clock();
  code = taos_stmt2_get_fields(stmt, &field_count, NULL);
  end = clock();
  printf("get fields time:%f\n", (double)(end - start) / CLOCKS_PER_SEC);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_param_count. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  printf("col field_count: %d\n", field_count);
  start = clock();
  taos_stmt2_free_fields(stmt, fields);
  end = clock();
  printf("free time:%f\n", (double)(end - start) / CLOCKS_PER_SEC);
  /*
  code = taos_stmt2_get_fields(stmt, TAOS_FIELD_TAG, &field_count, &fields);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_param_count. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    return;
  }
  printf("tag field_count: %d\n", field_count);
  taos_stmt2_free_fields(stmt, fields);
  */
  // if (taos_stmt_execute(stmt) != 0) {
  start = clock();
  // if (taos_stmt2_exec(stmt, NULL, stmtAsyncQueryCb, NULL) != 0) {
  if (taos_stmt2_exec(stmt, NULL) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  end = clock();
  printf("exec time:%f\n", (double)(end - start) / CLOCKS_PER_SEC);

  taos_stmt2_close(stmt);

  free(t8_len);
  free(t16_len);
  free(t32_len);
  free(t64_len);
  free(float_len);
  free(double_len);
  free(bin_len);
  free(blob_len);
  free(blob_len2);
}

int main(int argc, char* argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  printf("*********  verify stmt query  **********\n");
  veriry_stmt(taos);

  printf("done\n");
  taos_close(taos);
  taos_cleanup();
}
