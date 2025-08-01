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

#ifndef _TD_COMMON_TTYPE_H_
#define _TD_COMMON_TTYPE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"
#include "types.h"

// ----------------- For variable data types such as TSDB_DATA_TYPE_BINARY and TSDB_DATA_TYPE_NCHAR
typedef uint32_t TDRowLenT;
typedef uint8_t  TDRowValT;
typedef uint64_t TDRowVerT;
typedef int16_t  col_id_t;
typedef int8_t   col_type_t;
typedef int32_t  col_bytes_t;
typedef int32_t  schema_ver_t;
typedef int32_t  func_id_t;

#pragma pack(push, 1)
typedef struct {
  VarDataLenT len;
  uint8_t     data;
} SBinaryNullT;

typedef struct {
  VarDataLenT len;
  uint32_t    data;
} SNCharNullT;
#pragma pack(pop)

#define STypeMod int32_t
void extractTypeFromTypeMod(uint8_t type, STypeMod typeMod, uint8_t *prec, uint8_t *scale, int32_t *bytes);

#define DEFINE_TYPE_FROM_DECIMAL_FUNC(oType, decimalType) \
  oType oType##From##decimalType(const void *pDec, uint8_t prec, uint8_t scale)

#define DEFINE_TYPE_FROM_DECIMAL_FUNCS(prefix, decimalType)    \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(bool, decimalType);     \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(int8_t, decimalType);   \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(uint8_t, decimalType);  \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(int16_t, decimalType);  \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(uint16_t, decimalType); \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(int32_t, decimalType);  \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(uint32_t, decimalType); \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(int64_t, decimalType);  \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(uint64_t, decimalType); \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(float, decimalType);    \
  prefix DEFINE_TYPE_FROM_DECIMAL_FUNC(double, decimalType);

DEFINE_TYPE_FROM_DECIMAL_FUNCS(extern, Decimal64);
DEFINE_TYPE_FROM_DECIMAL_FUNCS(extern, Decimal128);

#define GET_TYPED_DATA(_v, _finalType, _type, _data, inputTypeMod)        \
  do {                                                                    \
    switch (_type) {                                                      \
      case TSDB_DATA_TYPE_BOOL:                                           \
      case TSDB_DATA_TYPE_TINYINT:                                        \
        (_v) = (_finalType)GET_INT8_VAL(_data);                           \
        break;                                                            \
      case TSDB_DATA_TYPE_UTINYINT:                                       \
        (_v) = (_finalType)GET_UINT8_VAL(_data);                          \
        break;                                                            \
      case TSDB_DATA_TYPE_SMALLINT:                                       \
        (_v) = (_finalType)GET_INT16_VAL(_data);                          \
        break;                                                            \
      case TSDB_DATA_TYPE_USMALLINT:                                      \
        (_v) = (_finalType)GET_UINT16_VAL(_data);                         \
        break;                                                            \
      case TSDB_DATA_TYPE_TIMESTAMP:                                      \
      case TSDB_DATA_TYPE_BIGINT:                                         \
        (_v) = (_finalType)(GET_INT64_VAL(_data));                        \
        break;                                                            \
      case TSDB_DATA_TYPE_UBIGINT:                                        \
        (_v) = (_finalType)(GET_UINT64_VAL(_data));                       \
        break;                                                            \
      case TSDB_DATA_TYPE_FLOAT:                                          \
        (_v) = (_finalType)GET_FLOAT_VAL(_data);                          \
        break;                                                            \
      case TSDB_DATA_TYPE_DOUBLE:                                         \
        (_v) = (_finalType)GET_DOUBLE_VAL(_data);                         \
        break;                                                            \
      case TSDB_DATA_TYPE_UINT:                                           \
        (_v) = (_finalType)GET_UINT32_VAL(_data);                         \
        break;                                                            \
      case TSDB_DATA_TYPE_INT:                                            \
        (_v) = (_finalType)GET_INT32_VAL(_data);                          \
        break;                                                            \
      case TSDB_DATA_TYPE_DECIMAL: {                                      \
        uint8_t prec = 0, scale = 0;                                      \
        extractTypeFromTypeMod(_type, inputTypeMod, &prec, &scale, NULL); \
        (_v) = _finalType##FromDecimal128(_data, prec, scale);            \
      } break;                                                            \
      case TSDB_DATA_TYPE_DECIMAL64: {                                    \
        uint8_t prec = 0, scale = 0;                                      \
        extractTypeFromTypeMod(_type, inputTypeMod, &prec, &scale, NULL); \
        (_v) = _finalType##FromDecimal64(_data, prec, scale);             \
      } break;                                                            \
      default:                                                            \
        (_v) = (_finalType)varDataLen(_data);                             \
        break;                                                            \
    }                                                                     \
  } while (0)

#define SET_TYPED_DATA(_v, _type, _data)       \
  do {                                         \
    switch (_type) {                           \
      case TSDB_DATA_TYPE_BOOL:                \
        *(bool *)(_v) = (bool)(_data);         \
        break;                                 \
      case TSDB_DATA_TYPE_TINYINT:             \
        *(int8_t *)(_v) = (int8_t)(_data);     \
        break;                                 \
      case TSDB_DATA_TYPE_UTINYINT:            \
        *(uint8_t *)(_v) = (uint8_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_SMALLINT:            \
        *(int16_t *)(_v) = (int16_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_USMALLINT:           \
        *(uint16_t *)(_v) = (uint16_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_BIGINT:              \
      case TSDB_DATA_TYPE_TIMESTAMP:           \
        *(int64_t *)(_v) = (int64_t)(_data);   \
        break;                                 \
      case TSDB_DATA_TYPE_UBIGINT:             \
        *(uint64_t *)(_v) = (uint64_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_FLOAT:               \
        *(float *)(_v) = (float)(_data);       \
        break;                                 \
      case TSDB_DATA_TYPE_DOUBLE:              \
        *(double *)(_v) = (double)(_data);     \
        break;                                 \
      case TSDB_DATA_TYPE_UINT:                \
        *(uint32_t *)(_v) = (uint32_t)(_data); \
        break;                                 \
      case TSDB_DATA_TYPE_INT:                 \
        *(int32_t *)(_v) = (int32_t)(_data);   \
        break;                                 \
      default:                                 \
        break;                                 \
    }                                          \
  } while (0)

#define SET_TYPED_DATA_MIN(_v, _type) \
  do {                                \
    switch (_type) {                  \
      case TSDB_DATA_TYPE_BOOL:       \
      case TSDB_DATA_TYPE_TINYINT:    \
        *(int8_t *)(_v) = INT8_MIN;   \
        break;                        \
      case TSDB_DATA_TYPE_SMALLINT:   \
        *(int16_t *)(_v) = INT16_MIN; \
        break;                        \
      case TSDB_DATA_TYPE_INT:        \
        *(int32_t *)(_v) = INT32_MIN; \
        break;                        \
      case TSDB_DATA_TYPE_BIGINT:     \
      case TSDB_DATA_TYPE_TIMESTAMP:  \
        *(int64_t *)(_v) = INT64_MIN; \
        break;                        \
      case TSDB_DATA_TYPE_FLOAT:      \
        *(float *)(_v) = FLT_MIN;     \
        break;                        \
      case TSDB_DATA_TYPE_DOUBLE:     \
        *(double *)(_v) = DBL_MIN;    \
        break;                        \
      case TSDB_DATA_TYPE_UTINYINT:   \
        *(uint8_t *)(_v) = 0;         \
        break;                        \
      case TSDB_DATA_TYPE_USMALLINT:  \
        *(uint16_t *)(_v) = 0;        \
        break;                        \
      case TSDB_DATA_TYPE_UBIGINT:    \
        *(uint64_t *)(_v) = 0;        \
        break;                        \
      case TSDB_DATA_TYPE_UINT:       \
        *(uint32_t *)(_v) = 0;        \
        break;                        \
      default:                        \
        break;                        \
    }                                 \
  } while (0)

#define SET_TYPED_DATA_MAX(_v, _type)   \
  do {                                  \
    switch (_type) {                    \
      case TSDB_DATA_TYPE_BOOL:         \
      case TSDB_DATA_TYPE_TINYINT:      \
        *(int8_t *)(_v) = INT8_MAX;     \
        break;                          \
      case TSDB_DATA_TYPE_SMALLINT:     \
        *(int16_t *)(_v) = INT16_MAX;   \
        break;                          \
      case TSDB_DATA_TYPE_INT:          \
        *(int32_t *)(_v) = INT32_MAX;   \
        break;                          \
      case TSDB_DATA_TYPE_BIGINT:       \
      case TSDB_DATA_TYPE_TIMESTAMP:    \
        *(int64_t *)(_v) = INT64_MAX;   \
        break;                          \
      case TSDB_DATA_TYPE_FLOAT:        \
        *(float *)(_v) = FLT_MAX;       \
        break;                          \
      case TSDB_DATA_TYPE_DOUBLE:       \
        *(double *)(_v) = DBL_MAX;      \
        break;                          \
      case TSDB_DATA_TYPE_UTINYINT:     \
        *(uint8_t *)(_v) = UINT8_MAX;   \
        break;                          \
      case TSDB_DATA_TYPE_USMALLINT:    \
        *(uint16_t *)(_v) = UINT16_MAX; \
        break;                          \
      case TSDB_DATA_TYPE_UINT:         \
        *(uint32_t *)(_v) = UINT32_MAX; \
        break;                          \
      case TSDB_DATA_TYPE_UBIGINT:      \
        *(uint64_t *)(_v) = UINT64_MAX; \
        break;                          \
      default:                          \
        break;                          \
    }                                   \
  } while (0)

#define NUM_TO_STRING(_inputType, _input, _outputBytes, _output)                          \
  do {                                                                                    \
    switch (_inputType) {                                                                 \
      case TSDB_DATA_TYPE_TINYINT:                                                        \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(int8_t *)(_input));            \
        break;                                                                            \
      case TSDB_DATA_TYPE_UTINYINT:                                                       \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(uint8_t *)(_input));           \
        break;                                                                            \
      case TSDB_DATA_TYPE_SMALLINT:                                                       \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(int16_t *)(_input));           \
        break;                                                                            \
      case TSDB_DATA_TYPE_USMALLINT:                                                      \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(uint16_t *)(_input));          \
        break;                                                                            \
      case TSDB_DATA_TYPE_TIMESTAMP:                                                      \
      case TSDB_DATA_TYPE_BIGINT:                                                         \
        snprintf(_output, (int32_t)(_outputBytes), "%" PRId64, *(int64_t *)(_input));     \
        break;                                                                            \
      case TSDB_DATA_TYPE_UBIGINT:                                                        \
        snprintf(_output, (int32_t)(_outputBytes), "%" PRIu64, *(uint64_t *)(_input));    \
        break;                                                                            \
      case TSDB_DATA_TYPE_FLOAT: {                                                        \
        snprintf(_output, (int32_t)(_outputBytes), "%.*g", FLT_DIG, *(float *)(_input));  \
        break;                                                                            \
      }                                                                                   \
      case TSDB_DATA_TYPE_DOUBLE: {                                                       \
        snprintf(_output, (int32_t)(_outputBytes), "%.*g", DBL_DIG, *(double *)(_input)); \
        break;                                                                            \
      }                                                                                   \
      case TSDB_DATA_TYPE_UINT:                                                           \
        snprintf(_output, (int32_t)(_outputBytes), "%u", *(uint32_t *)(_input));          \
        break;                                                                            \
      default:                                                                            \
        snprintf(_output, (int32_t)(_outputBytes), "%d", *(int32_t *)(_input));           \
        break;                                                                            \
    }                                                                                     \
  } while (0)

// TODO: use varchar(0) to represent NULL type
#define IS_VAR_NULL_TYPE(_t, _b) ((_t) == TSDB_DATA_TYPE_VARCHAR && (_b) == 0)
#define IS_NULL_TYPE(_t)         ((_t) == TSDB_DATA_TYPE_NULL)

#define IS_SIGNED_NUMERIC_TYPE(_t)   ((_t) >= TSDB_DATA_TYPE_TINYINT && (_t) <= TSDB_DATA_TYPE_BIGINT)
#define IS_UNSIGNED_NUMERIC_TYPE(_t) ((_t) >= TSDB_DATA_TYPE_UTINYINT && (_t) <= TSDB_DATA_TYPE_UBIGINT)
#define IS_FLOAT_TYPE(_t)            ((_t) == TSDB_DATA_TYPE_FLOAT || (_t) == TSDB_DATA_TYPE_DOUBLE)
#define IS_INTEGER_TYPE(_t)          ((IS_SIGNED_NUMERIC_TYPE(_t)) || (IS_UNSIGNED_NUMERIC_TYPE(_t)))
#define IS_TIMESTAMP_TYPE(_t)        ((_t) == TSDB_DATA_TYPE_TIMESTAMP)
#define IS_BOOLEAN_TYPE(_t)          ((_t) == TSDB_DATA_TYPE_BOOL)
#define IS_DECIMAL_TYPE(_t)          ((_t) == TSDB_DATA_TYPE_DECIMAL || (_t) == TSDB_DATA_TYPE_DECIMAL64)

#define IS_NUMERIC_TYPE(_t) \
  ((IS_SIGNED_NUMERIC_TYPE(_t)) || (IS_UNSIGNED_NUMERIC_TYPE(_t)) || (IS_FLOAT_TYPE(_t)) || (IS_DECIMAL_TYPE(_t)))
#define IS_MATHABLE_TYPE(_t) \
  (IS_NUMERIC_TYPE(_t) || (_t) == (TSDB_DATA_TYPE_BOOL) || (_t) == (TSDB_DATA_TYPE_TIMESTAMP))

#define IS_VAR_DATA_TYPE(t)                                                                                 \
  (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_VARBINARY) || ((t) == TSDB_DATA_TYPE_NCHAR) || \
   ((t) == TSDB_DATA_TYPE_JSON) || ((t) == TSDB_DATA_TYPE_GEOMETRY) || ((t) == TSDB_DATA_TYPE_BLOB) ||      \
   ((t) == TSDB_DATA_TYPE_MEDIUMBLOB))

#define IS_STR_DATA_TYPE(t) \
  (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_VARBINARY) || ((t) == TSDB_DATA_TYPE_NCHAR))

#define IS_STR_DATA_BLOB(t)         ((t) == TSDB_DATA_TYPE_BLOB || (t) == TSDB_DATA_TYPE_MEDIUMBLOB)
#define IS_COMPARE_STR_DATA_TYPE(t) (((t) == TSDB_DATA_TYPE_VARCHAR) || ((t) == TSDB_DATA_TYPE_NCHAR))

#define IS_VALID_TINYINT(_t)   ((_t) >= INT8_MIN && (_t) <= INT8_MAX)
#define IS_VALID_SMALLINT(_t)  ((_t) >= INT16_MIN && (_t) <= INT16_MAX)
#define IS_VALID_INT(_t)       ((_t) >= INT32_MIN && (_t) <= INT32_MAX)
#define IS_VALID_INT64(_t)     ((_t) >= INT64_MIN && (_t) <= INT64_MAX)
#define IS_VALID_UTINYINT(_t)  ((_t) >= 0 && (_t) <= UINT8_MAX)
#define IS_VALID_USMALLINT(_t) ((_t) >= 0 && (_t) <= UINT16_MAX)
#define IS_VALID_UINT(_t)      ((_t) >= 0 && (_t) <= UINT32_MAX)
#define IS_VALID_UINT64(_t)    ((_t) >= 0 && (_t) <= UINT64_MAX)
#define IS_VALID_FLOAT(_t)     ((_t) >= -FLT_MAX && (_t) <= FLT_MAX)
#define IS_VALID_DOUBLE(_t)    ((_t) >= -DBL_MAX && (_t) <= DBL_MAX)
#define IS_INVALID_TYPE(_t)    ((_t) < TSDB_DATA_TYPE_NULL || (_t) >= TSDB_DATA_TYPE_MAX)

#define IS_CONVERT_AS_SIGNED(_t) \
  (IS_SIGNED_NUMERIC_TYPE(_t) || (_t) == (TSDB_DATA_TYPE_BOOL) || (_t) == (TSDB_DATA_TYPE_TIMESTAMP))
#define IS_CONVERT_AS_UNSIGNED(_t) (IS_UNSIGNED_NUMERIC_TYPE(_t) || (_t) == (TSDB_DATA_TYPE_BOOL))

#if 0
// TODO remove this function
static FORCE_INLINE bool isNull(const void *val, int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return *(uint8_t *)val == TSDB_DATA_BOOL_NULL;
    case TSDB_DATA_TYPE_TINYINT:
      return *(uint8_t *)val == TSDB_DATA_TINYINT_NULL;
    case TSDB_DATA_TYPE_SMALLINT:
      return *(uint16_t *)val == TSDB_DATA_SMALLINT_NULL;
    case TSDB_DATA_TYPE_INT:
      return *(uint32_t *)val == TSDB_DATA_INT_NULL;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return *(uint64_t *)val == TSDB_DATA_BIGINT_NULL;
    case TSDB_DATA_TYPE_FLOAT:
      return *(uint32_t *)val == TSDB_DATA_FLOAT_NULL;
    case TSDB_DATA_TYPE_DOUBLE:
      return *(uint64_t *)val == TSDB_DATA_DOUBLE_NULL;
    case TSDB_DATA_TYPE_NCHAR:
      return varDataLen(val) == sizeof(int32_t) && *(uint32_t *)varDataVal(val) == TSDB_DATA_NCHAR_NULL;
    case TSDB_DATA_TYPE_BINARY:
      return varDataLen(val) == sizeof(int8_t) && *(uint8_t *)varDataVal(val) == TSDB_DATA_BINARY_NULL;
    case TSDB_DATA_TYPE_UTINYINT:
      return *(uint8_t *)val == TSDB_DATA_UTINYINT_NULL;
    case TSDB_DATA_TYPE_USMALLINT:
      return *(uint16_t *)val == TSDB_DATA_USMALLINT_NULL;
    case TSDB_DATA_TYPE_UINT:
      return *(uint32_t *)val == TSDB_DATA_UINT_NULL;
    case TSDB_DATA_TYPE_UBIGINT:
      return *(uint64_t *)val == TSDB_DATA_UBIGINT_NULL;
    case TSDB_DATA_TYPE_GEOMETRY:
      return varDataLen(val) == sizeof(int8_t) && *(uint8_t *)varDataVal(val) == TSDB_DATA_GEOMETRY_NULL;

    default:
      return false;
  };
}
#endif

typedef struct tDataTypeDescriptor {
  int16_t type;
  int16_t nameLen;
  int32_t bytes;
  char   *name;
  int64_t minValue;
  int64_t maxValue;
  int32_t (*compFunc)(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                      int32_t nBuf);
  int32_t (*decompFunc)(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf);
} tDataTypeDescriptor;

typedef struct tDataTypeCompress {
  int16_t type;
  int16_t nameLen;
  int32_t bytes;
  char   *name;
  int64_t minValue;
  int64_t maxValue;
  int32_t (*compFunc)(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                      int32_t nBuf);
  int32_t (*decompFunc)(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                        int32_t nBuf);
} tDataTypeCompress;

extern tDataTypeDescriptor tDataTypes[TSDB_DATA_TYPE_MAX];
extern tDataTypeCompress   tDataCompress[TSDB_DATA_TYPE_MAX];

typedef struct SDataType {
  uint8_t type;
  uint8_t precision;
  uint8_t scale;
  int32_t bytes;
} SDataType;

bool isValidDataType(int32_t type);

int32_t operateVal(void *dst, void *s1, void *s2, int32_t optr, int32_t type);
void    assignVal(char *val, const char *src, int32_t len, int32_t type);
void   *getDataMin(int32_t type, void *value);
void   *getDataMax(int32_t type, void *value);

STypeMod typeGetTypeMod(uint8_t type, uint8_t prec, uint8_t scale, int32_t bytes);
STypeMod typeGetTypeModFromDataType(const SDataType *pDataType);
uint8_t  decimalTypeFromPrecision(uint8_t precision);
STypeMod decimalCalcTypeMod(uint8_t prec, uint8_t scale);
void     decimalFromTypeMod(STypeMod typeMod, uint8_t *precision, uint8_t *scale);
// pType->type should has been set
void    fillTypeFromTypeMod(SDataType *pType, STypeMod mod);
uint8_t getScaleFromTypeMod(int32_t type, STypeMod mod);
// TODO fix me!! for compatibility issue, save precision in scale in bytes, move it to somewhere else
void fillBytesForDecimalType(int32_t *pBytes, int32_t type, uint8_t precision, uint8_t scale);
void extractDecimalTypeInfoFromBytes(int32_t *pBytes, uint8_t *precision, uint8_t *scale);

int32_t calcTypeBytesFromSchemaBytes(int32_t type, int32_t schemaBytes, bool isStmt);
int32_t calcSchemaBytesFromTypeBytes(int32_t type, int32_t varTypeBytes, bool isStmt);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TTYPE_H_*/
