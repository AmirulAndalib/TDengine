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

/* README.md   TAOS compression
 *
 * INTEGER Compression Algorithm:
 *   To compress integers (including char, short, int32_t, int64_t), the difference
 *   between two integers is calculated at first. Then the difference is
 *   transformed to positive by zig-zag encoding method
 *   (https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba). Then the value is
 *   encoded using simple 8B method. For more information about simple 8B,
 *   refer to https://en.wikipedia.org/wiki/8b/10b_encoding.
 *
 *   NOTE : For bigint, only 59 bits can be used, which means data from -(2**59) to (2**59)-1
 *   are allowed.
 *
 * BOOLEAN Compression Algorithm:
 *   We provide two methods for compress boolean types. Because boolean types in C
 *   code are char bytes with 0 and 1 values only, only one bit can used to discriminate
 *   the values.
 *   1. The first method is using only 1 bit to represent the boolean value with 1 for
 *   true and 0 for false. Then the compression rate is 1/8.
 *   2. The second method is using run length encoding (RLE) methods. This method works
 *   better when there are a lot of consecutive true values or false values.
 *
 * STRING Compression Algorithm:
 *   We us LZ4 method to compress the string type.
 *
 * FLOAT Compression Algorithm:
 *   We use the same method with Akumuli to compress float and double types. The compression
 *   algorithm assumes the float/double values change slightly. So we take the XOR between two
 *   adjacent values. Then compare the number of leading zeros and trailing zeros. If the number
 *   of leading zeros are larger than the trailing zeros, then record the last serveral bytes
 *   of the XORed value with informations. If not, record the first corresponding bytes.
 *
 */

#define _DEFAULT_SOURCE
#include "tcompression.h"
#include "lz4.h"
#include "tlog.h"
#include "ttypes.h"
// #include "tmsg.h"

#if defined(WINDOWS) || defined(_TD_DARWIN_64)
#else
#include "fast-lzma2.h"
#include "zlib.h"
#include "zstd.h"
#endif

#include "td_sz.h"

int32_t tsCompressPlain2(const char *const input, const int32_t nelements, char *const output, const char type);
int32_t tsDecompressPlain2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                           const char type);
// delta
int32_t tsCompressTimestampImp2(const char *const input, const int32_t nelements, char *const output, const char type);

int32_t tsDecompressTimestampImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                                  const char type);
// simple8b
int32_t tsCompressINTImp2(const char *const input, const int32_t nelements, char *const output, const char type);
int32_t tsDecompressINTImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                            const char type);

// bit
int32_t tsCompressBoolImp2(const char *const input, const int32_t nelements, char *const output, char const type);
int32_t tsDecompressBoolImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                             char const type);

// double specail

int32_t tsCompressDoubleImp2(const char *const input, const int32_t nelements, char *const output, char const type);
int32_t tsDecompressDoubleImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                               char const type);

int32_t tsCompressDoubleImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressDoubleImp(const char *const input, int32_t ninput, const int32_t nelements, char *const output);
int32_t tsCompressFloatImp(const char *const input, const int32_t nelements, char *const output);
int32_t tsDecompressFloatImp(const char *const input, int32_t ninput, const int32_t nelements, char *const output);

int32_t l2ComressInitImpl_disabled(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                                   uint32_t intervals, int32_t ifAdtFse, const char *compressor) {
  return 0;
}

int32_t l2CompressImpl_disabled(const char *const input, const int32_t inputSize, char *const output,
                                int32_t outputSize, const char type, int8_t lvl) {
  output[0] = 0;
  memcpy(output + 1, input, inputSize);
  return inputSize + 1;
}
int32_t l2DecompressImpl_disabled(const char *const input, const int32_t compressedSize, char *const output,
                                  int32_t outputSize, const char type) {
  memcpy(output, input + 1, compressedSize - 1);
  return compressedSize - 1;
}
int32_t l2ComressInitImpl_lz4(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                              uint32_t intervals, int32_t ifAdtFse, const char *compressor) {
  return 0;
}

int32_t l2CompressImpl_lz4(const char *const input, const int32_t inputSize, char *const output, int32_t outputSize,
                           const char type, int8_t lvl) {
  const int32_t compressed_data_size = LZ4_compress_default(input, output + 1, inputSize, outputSize - 1);

  // If cannot compress or after compression, data becomes larger.
  if (compressed_data_size <= 0 || compressed_data_size > inputSize) {
    /* First byte is for indicator */
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }
  output[0] = 1;
  return compressed_data_size + 1;
}
int32_t l2DecompressImpl_lz4(const char *const input, const int32_t compressedSize, char *const output,
                             int32_t outputSize, const char type) {
  if (input[0] == 1) {
    /* It is compressed by LZ4 algorithm */
    const int32_t decompressed_size = LZ4_decompress_safe(input + 1, output, compressedSize - 1, outputSize);
    if (decompressed_size < 0) {
      uError("Failed to decompress string with LZ4 algorithm, decompressed size:%d", decompressed_size);
      return TSDB_CODE_THIRDPARTY_ERROR;
    }

    return decompressed_size;
  } else if (input[0] == 0) {
    /* It is not compressed by LZ4 algorithm */
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  } else if (input[1] == 2) {
    uError("Invalid decompress string indicator:%d", input[0]);
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}
int32_t l2ComressInitImpl_tsz(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                              uint32_t intervals, int32_t ifAdtFse, const char *compressor) {
  return 0;
}
int32_t l2CompressImpl_tsz(const char *const input, const int32_t inputSize, char *const output, int32_t outputSize,
                           const char type, int8_t lvl) {
  if (type == TSDB_DATA_TYPE_FLOAT) {
    if (lossyFloat) {
      return tsCompressFloatLossyImp(input, inputSize, output);
    }
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    if (lossyDouble) {
      return tsCompressDoubleLossyImp(input, inputSize, output);
    }
  }

  return l2CompressImpl_lz4(input, inputSize, output, outputSize, type, lvl);
}

int32_t l2DecompressImpl_tsz(const char *const input, const int32_t inputSize, char *const output, int32_t outputSize,
                             const char type) {
  if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
    if (HEAD_ALGO(((uint8_t *)input)[0]) == ALGO_SZ_LOSSY) {
      return tsDecompressFloatLossyImp(input, inputSize, outputSize, output);
    }
  }

  return l2DecompressImpl_lz4(input, inputSize, output, outputSize, type);
}

#if defined(WINDOWS) || defined(_TD_DARWIN_64)
// do nothing
#else

int32_t l2ComressInitImpl_zlib(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                               uint32_t intervals, int32_t ifAdtFse, const char *compressor) {
  return 0;
}

int32_t l2CompressImpl_zlib(const char *const input, const int32_t inputSize, char *const output, int32_t outputSize,
                            const char type, int8_t lvl) {
  uLongf  dstLen = outputSize - 1;
  int32_t ret = compress2((Bytef *)(output + 1), (uLongf *)&dstLen, (Bytef *)input, (uLong)inputSize, lvl);
  if (ret == Z_OK) {
    output[0] = 1;
    return dstLen + 1;
  } else {
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}
int32_t l2DecompressImpl_zlib(const char *const input, const int32_t compressedSize, char *const output,
                              int32_t outputSize, const char type) {
  if (input[0] == 1) {
    uLongf len = outputSize;
    int    ret = uncompress((Bytef *)output, &len, (Bytef *)input + 1, compressedSize - 1);
    if (ret == Z_OK) {
      return len;
    } else {
      return TSDB_CODE_THIRDPARTY_ERROR;
    }

  } else if (input[0] == 0) {
    /* It is not compressed by LZ4 algorithm */
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  } else if (input[1] == 2) {
    uError("Invalid decompress string indicator:%d", input[0]);
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return 0;
}
int32_t l2ComressInitImpl_zstd(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                               uint32_t intervals, int32_t ifAdtFse, const char *compressor) {
  return 0;
}

int32_t l2CompressImpl_zstd(const char *const input, const int32_t inputSize, char *const output, int32_t outputSize,
                            const char type, int8_t lvl) {
  size_t len = ZSTD_compress(output + 1, outputSize - 1, input, inputSize, lvl);
  if (len > inputSize) {
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }
  output[0] = 1;

  return len + 1;
}
int32_t l2DecompressImpl_zstd(const char *const input, const int32_t compressedSize, char *const output,
                              int32_t outputSize, const char type) {
  if (input[0] == 1) {
    return ZSTD_decompress(output, outputSize, input + 1, compressedSize - 1);
  } else if (input[0] == 0) {
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}

int32_t l2ComressInitImpl_xz(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals,
                             uint32_t intervals, int32_t ifAdtFse, const char *compressor) {
  return 0;
}
int32_t l2CompressImpl_xz(const char *const input, const int32_t inputSize, char *const output, int32_t outputSize,
                          const char type, int8_t lvl) {
  size_t len = FL2_compress(output + 1, outputSize - 1, input, inputSize, lvl);
  if (len > inputSize) {
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }
  output[0] = 1;
  return len + 1;
}
int32_t l2DecompressImpl_xz(const char *const input, const int32_t compressedSize, char *const output,
                            int32_t outputSize, const char type) {
  if (input[0] == 1) {
    return FL2_decompress(output, outputSize, input + 1, compressedSize - 1);
  } else if (input[0] == 0) {
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}
#endif

TCmprL1FnSet compressL1Dict[] = {{"PLAIN", NULL, tsCompressPlain2, tsDecompressPlain2},
                                 {"SIMPLE-8B", NULL, tsCompressINTImp2, tsDecompressINTImp2},
                                 {"DELTAI", NULL, tsCompressTimestampImp2, tsDecompressTimestampImp2},
                                 {"BIT-PACKING", NULL, tsCompressBoolImp2, tsDecompressBoolImp2},
                                 {"DELTAD", NULL, tsCompressDoubleImp2, tsDecompressDoubleImp2}};

TCmprLvlSet compressL2LevelDict[] = {
    {"unknown", .lvl = {1, 2, 3}}, {"lz4", .lvl = {1, 2, 3}}, {"zlib", .lvl = {1, 6, 9}},
    {"zstd", .lvl = {1, 11, 22}},  {"tsz", .lvl = {1, 2, 3}}, {"xz", .lvl = {1, 6, 9}},
};

#if defined(WINDOWS) || defined(_TD_DARWIN_64)
TCmprL2FnSet compressL2Dict[] = {
    {"unknown", l2ComressInitImpl_disabled, l2CompressImpl_disabled, l2DecompressImpl_disabled},
    {"lz4", l2ComressInitImpl_lz4, l2CompressImpl_lz4, l2DecompressImpl_lz4},
    {"zlib", l2ComressInitImpl_lz4, l2CompressImpl_lz4, l2DecompressImpl_lz4},
    {"zstd", l2ComressInitImpl_lz4, l2CompressImpl_lz4, l2DecompressImpl_lz4},
    {"tsz", l2ComressInitImpl_tsz, l2CompressImpl_tsz, l2DecompressImpl_tsz},
    {"xz", l2ComressInitImpl_lz4, l2CompressImpl_lz4, l2DecompressImpl_lz4}};
#else
TCmprL2FnSet compressL2Dict[] = {
    {"unknown", l2ComressInitImpl_disabled, l2CompressImpl_disabled, l2DecompressImpl_disabled},
    {"lz4", l2ComressInitImpl_lz4, l2CompressImpl_lz4, l2DecompressImpl_lz4},
    {"zlib", l2ComressInitImpl_zlib, l2CompressImpl_zlib, l2DecompressImpl_zlib},
    {"zstd", l2ComressInitImpl_zstd, l2CompressImpl_zstd, l2DecompressImpl_zstd},
    {"tsz", l2ComressInitImpl_tsz, l2CompressImpl_tsz, l2DecompressImpl_tsz},
    {"xz", l2ComressInitImpl_xz, l2CompressImpl_xz, l2DecompressImpl_xz}};

#endif

int8_t tsGetCompressL2Level(uint8_t alg, uint8_t lvl) {
  if (lvl == L2_LVL_LOW) {
    return compressL2LevelDict[alg].lvl[0];
  } else if (lvl == L2_LVL_MEDIUM) {
    return compressL2LevelDict[alg].lvl[1];
  } else if (lvl == L2_LVL_HIGH) {
    return compressL2LevelDict[alg].lvl[2];
  }
  return 1;
}

static const int32_t TEST_NUMBER = 1;
#define is_bigendian()     ((*(char *)&TEST_NUMBER) == 0)
#define SIMPLE8B_MAX_INT64 ((uint64_t)1152921504606846974LL)

#define safeInt64Add(a, b) (((a >= 0) && (b <= INT64_MAX - a)) || ((a < 0) && (b >= INT64_MIN - a)))

bool lossyFloat = false;
bool lossyDouble = false;

// init call
void tsCompressInit(char *lossyColumns, float fPrecision, double dPrecision, uint32_t maxIntervals, uint32_t intervals,
                    int32_t ifAdtFse, const char *compressor) {
  // config
  lossyFloat = strstr(lossyColumns, "float") != NULL;
  lossyDouble = strstr(lossyColumns, "double") != NULL;

  tdszInit(fPrecision, dPrecision, maxIntervals, intervals, ifAdtFse, compressor);
  if (lossyFloat) uTrace("lossy compression float  is opened. ");
  if (lossyDouble) uTrace("lossy compression double is opened. ");
  return;
}
// exit call
void tsCompressExit() { tdszExit(); }

/*
 * Compress Integer (Simple8B).
 */
int32_t tsCompressINTImp(const char *const input, const int32_t nelements, char *const output, const char type) {
  // Selector value:              0    1   2   3   4   5   6   7   8  9  10  11
  // 12  13  14  15
  char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
  char    bit_to_selector[] = {0,  2,  3,  4,  5,  6,  7,  8,  9,  10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
                               14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
                               15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15};

  // get the byte limit.
  int32_t word_length = getWordLength(type);

  int32_t byte_limit = nelements * word_length + 1;
  int32_t opos = 1;
  int64_t prev_value = 0;

  for (int32_t i = 0; i < nelements;) {
    char    selector = 0;
    char    bit = 0;
    int32_t elems = 0;
    int64_t prev_value_tmp = prev_value;

    for (int32_t j = i; j < nelements; j++) {
      // Read data from the input stream and convert it to INT64 type.
      int64_t curr_value = 0;
      switch (type) {
        case TSDB_DATA_TYPE_TINYINT:
          curr_value = (int64_t)(*((int8_t *)input + j));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          curr_value = (int64_t)(*((int16_t *)input + j));
          break;
        case TSDB_DATA_TYPE_INT:
          curr_value = (int64_t)(*((int32_t *)input + j));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          curr_value = (int64_t)(*((int64_t *)input + j));
          break;
      }
      // Get difference.
      if (!safeInt64Add(curr_value, -prev_value_tmp)) goto _copy_and_exit;

      int64_t diff = curr_value - prev_value_tmp;
      // Zigzag encode the value.
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);

      if (zigzag_value >= SIMPLE8B_MAX_INT64) goto _copy_and_exit;

      int64_t tmp_bit;
      if (zigzag_value == 0) {
        // Take care here, __builtin_clzl give wrong anser for value 0;
        tmp_bit = 0;
      } else {
        tmp_bit = (LONG_BYTES * BITS_PER_BYTE) - BUILDIN_CLZL(zigzag_value);
      }

      if (elems + 1 <= selector_to_elems[(int32_t)selector] &&
          elems + 1 <= selector_to_elems[(int32_t)(bit_to_selector[(int32_t)tmp_bit])]) {
        // If can hold another one.
        selector = selector > bit_to_selector[(int32_t)tmp_bit] ? selector : bit_to_selector[(int32_t)tmp_bit];
        elems++;
        bit = bit_per_integer[(int32_t)selector];
      } else {
        // if cannot hold another one.
        while (elems < selector_to_elems[(int32_t)selector]) selector++;
        elems = selector_to_elems[(int32_t)selector];
        bit = bit_per_integer[(int32_t)selector];
        break;
      }
      prev_value_tmp = curr_value;
    }

    uint64_t buffer = 0;
    buffer |= (uint64_t)selector;
    for (int32_t k = 0; k < elems; k++) {
      int64_t curr_value = 0; /* get current values */
      switch (type) {
        case TSDB_DATA_TYPE_TINYINT:
          curr_value = (int64_t)(*((int8_t *)input + i));
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          curr_value = (int64_t)(*((int16_t *)input + i));
          break;
        case TSDB_DATA_TYPE_INT:
          curr_value = (int64_t)(*((int32_t *)input + i));
          break;
        case TSDB_DATA_TYPE_BIGINT:
          curr_value = (int64_t)(*((int64_t *)input + i));
          break;
      }
      int64_t  diff = curr_value - prev_value;
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);
      buffer |= ((zigzag_value & INT64MASK(bit)) << (bit * k + 4));
      i++;
      prev_value = curr_value;
    }

    // Output the encoded value to the output.
    if (opos + sizeof(buffer) <= byte_limit) {
      memcpy(output + opos, &buffer, sizeof(buffer));
      opos += sizeof(buffer);
    } else {
    _copy_and_exit:
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      return byte_limit;
    }
  }

  // set the indicator.
  output[0] = 0;
  return opos;
}

int32_t tsDecompressINTImp(const char *const input, const int32_t nelements, char *const output, const char type) {
  int32_t word_length = getWordLength(type);
  if (word_length < 0) {
    return -1;
  }

  // If not compressed.
  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * word_length);
    return nelements * word_length;
  }

  if (tsSIMDEnable && tsAVX512Enable && tsAVX512Supported) {
    int32_t cnt = tsDecompressIntImpl_Hw(input, nelements, output, type);
    if (cnt >= 0) {
      return cnt;
    }
  }

  // Selector value: 0    1   2   3   4   5   6   7   8  9  10  11 12  13  14  15
  char    bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int32_t selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

  const char *ip = input + 1;
  char       *op = output;
  int32_t     count = 0;
  int64_t     prev_value = 0;

  while (count < nelements) {
    uint64_t w = *(uint64_t *)ip;

    char    selector = (char)(w & INT64MASK(4));       // selector = 4
    char    bit = bit_per_integer[(int32_t)selector];  // bit = 3
    int32_t elems = selector_to_elems[(int32_t)selector];

    switch (type) {
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t *out = (int64_t *)op;
        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            *out = prev_value;
          }
        } else {
          uint64_t zigzag_value = 0;
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);
            *out = prev_value;
          }
        }
        op = (char *)out;
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t *out = (int32_t *)op;
        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            *out = (int32_t)prev_value;
          }
        } else {
          uint64_t zigzag_value = 0;
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);
            *out = (int32_t)prev_value;
          }
        }
        op = (char *)out;
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t *out = (int16_t *)op;
        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            *out = (int16_t)prev_value;
          }
        } else {
          uint64_t zigzag_value = 0;
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);
            *out = (int16_t)prev_value;
          }
        }
        op = (char *)out;
        break;
      }
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t *out = (int8_t *)op;
        if (selector == 0 || selector == 1) {
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            *out = (int8_t)prev_value;
          }
        } else {
          uint64_t zigzag_value = 0;
          for (int32_t i = 0; i < elems && count < nelements; ++i, ++count, ++out) {
            zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
            prev_value += ZIGZAG_DECODE(int64_t, zigzag_value);
            *out = (int8_t)prev_value;
          }
        }
        op = (char *)out;
        break;
      }
      default:
        perror("Wrong integer types.\n");
        return -1;
    }
    ip += LONG_BYTES;
  }

  return nelements * word_length;
}

/* ----------------------------------------------Bool Compression ---------------------------------------------- */
// TODO: You can also implement it using RLE method.
int32_t tsCompressBoolImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t pos = -1;
  int32_t ele_per_byte = BITS_PER_BYTE / 2;

  for (int32_t i = 0; i < nelements; i++) {
    if (i % ele_per_byte == 0) {
      pos++;
      output[pos] = 0;
    }

    uint8_t t = 0;
    if (input[i] == 1) {
      t = (((uint8_t)1) << (2 * (i % ele_per_byte)));
      output[pos] |= t;
    } else if (input[i] == 0) {
      t = ((uint8_t)1 << (2 * (i % ele_per_byte))) - 1;
      /* t = (~((( uint8_t)1) << (7-i%BITS_PER_BYTE))); */
      output[pos] &= t;
    } else if (input[i] == TSDB_DATA_BOOL_NULL) {
      t = ((uint8_t)2 << (2 * (i % ele_per_byte)));
      /* t = (~((( uint8_t)1) << (7-i%BITS_PER_BYTE))); */
      output[pos] |= t;
    } else {
      uError("Invalid compress bool value:%d", output[pos]);
      return TSDB_CODE_INVALID_PARA;
    }
  }

  return pos + 1;
}

int32_t tsDecompressBoolImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t ipos = -1, opos = 0;
  int32_t ele_per_byte = BITS_PER_BYTE / 2;

  for (int32_t i = 0; i < nelements; i++) {
    if (i % ele_per_byte == 0) {
      ipos++;
    }

    uint8_t ele = (input[ipos] >> (2 * (i % ele_per_byte))) & INT8MASK(2);
    if (ele == 1) {
      output[opos++] = 1;
    } else if (ele == 2) {
      output[opos++] = TSDB_DATA_BOOL_NULL;
    } else {
      output[opos++] = 0;
    }
  }

  return nelements;
}
int32_t tsCompressBoolImp2(const char *const input, const int32_t nelements, char *const output, char const type) {
  return tsCompressBoolImp(input, nelements, output);
}
int32_t tsDecompressBoolImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                             char const type) {
  return tsDecompressBoolImp(input, nelements, output);
}

int32_t tsCompressDoubleImp2(const char *const input, const int32_t nelements, char *const output, char const type) {
  if (type == TSDB_DATA_TYPE_FLOAT) {
    return tsCompressFloatImp(input, nelements, output);
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    return tsCompressDoubleImp(input, nelements, output);
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}
int32_t tsDecompressDoubleImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                               char const type) {
  if (type == TSDB_DATA_TYPE_FLOAT) {
    return tsDecompressFloatImp(input, ninput, nelements, output);
  } else if (type == TSDB_DATA_TYPE_DOUBLE) {
    return tsDecompressDoubleImp(input, ninput, nelements, output);
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}
int32_t tsCompressINTImp2(const char *const input, const int32_t nelements, char *const output, const char type) {
  return tsCompressINTImp(input, nelements, output, type);
}
int32_t tsDecompressINTImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                            const char type) {
  return tsDecompressINTImp(input, nelements, output, type);
}

#if 0
/* Run Length Encoding(RLE) Method */
int32_t tsCompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t _pos = 0;

  for (int32_t i = 0; i < nelements;) {
    unsigned char counter = 1;
    char          num = input[i];

    for (++i; i < nelements; i++) {
      if (input[i] == num) {
        counter++;
        if (counter == INT8MASK(7)) {
          i++;
          break;
        }
      } else {
        break;
      }
    }

    // Encode the data.
    if (num == 1) {
      output[_pos++] = INT8MASK(1) | (counter << 1);
    } else if (num == 0) {
      output[_pos++] = (counter << 1) | INT8MASK(0);
    } else {
      uError("Invalid compress bool value:%d", output[_pos]);
      return -1;
    }
  }

  return _pos;
}

int32_t tsDecompressBoolRLEImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t ipos = 0, opos = 0;
  while (1) {
    char     encode = input[ipos++];
    unsigned counter = (encode >> 1) & INT8MASK(7);
    char     value = encode & INT8MASK(1);

    memset(output + opos, value, counter);
    opos += counter;
    if (opos >= nelements) {
      return nelements;
    }
  }
}
#endif

/* ----------------------------------------------String Compression ---------------------------------------------- */
// Note: the size of the output must be larger than input_size + 1 and
// LZ4_compressBound(size) + 1;
// >= max(input_size, LZ4_compressBound(input_size)) + 1;
int32_t tsCompressStringImp(const char *const input, int32_t inputSize, char *const output, int32_t outputSize) {
  // Try to compress using LZ4 algorithm.
  const int32_t compressed_data_size = LZ4_compress_default(input, output + 1, inputSize, outputSize - 1);

  // If cannot compress or after compression, data becomes larger.
  if (compressed_data_size <= 0 || compressed_data_size > inputSize) {
    /* First byte is for indicator */
    output[0] = 0;
    memcpy(output + 1, input, inputSize);
    return inputSize + 1;
  }

  output[0] = 1;
  return compressed_data_size + 1;
}

int32_t tsDecompressStringImp(const char *const input, int32_t compressedSize, char *const output, int32_t outputSize) {
  // compressedSize is the size of data after compression.

  if (input[0] == 1) {
    /* It is compressed by LZ4 algorithm */
    const int32_t decompressed_size = LZ4_decompress_safe(input + 1, output, compressedSize - 1, outputSize);
    if (decompressed_size < 0) {
      uError("Failed to decompress string with LZ4 algorithm, decompressed size:%d", decompressed_size);
      return TSDB_CODE_THIRDPARTY_ERROR;
    }

    return decompressed_size;
  } else if (input[0] == 0) {
    /* It is not compressed by LZ4 algorithm */
    memcpy(output, input + 1, compressedSize - 1);
    return compressedSize - 1;
  } else if (input[1] == 2) {
    uError("Invalid decompress string indicator:%d", input[0]);
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return TSDB_CODE_THIRDPARTY_ERROR;
}

/* --------------------------------------------Timestamp Compression ---------------------------------------------- */
// TODO: Take care here, we assumes little endian encoding.
//
int32_t tsCompressTimestampImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t _pos = 1;
  int32_t longBytes = LONG_BYTES;

  if (nelements < 0) {
    return -1;
  }

  if (nelements == 0) return 0;

  int64_t *istream = (int64_t *)input;

  int64_t prev_value = istream[0];
  if (prev_value >= 0x8000000000000000) {
    uWarn("compression timestamp is over signed long long range. ts = 0x%" PRIx64 " \n", prev_value);
    goto _exit_over;
  }
  int64_t  prev_delta = -prev_value;
  uint8_t  flags = 0, flag1 = 0, flag2 = 0;
  uint64_t dd1 = 0, dd2 = 0;

  for (int32_t i = 0; i < nelements; i++) {
    int64_t curr_value = istream[i];
    if (!safeInt64Add(curr_value, -prev_value)) goto _exit_over;
    int64_t curr_delta = curr_value - prev_value;
    if (!safeInt64Add(curr_delta, -prev_delta)) goto _exit_over;
    int64_t delta_of_delta = curr_delta - prev_delta;
    // zigzag encode the value.
    uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, delta_of_delta);
    if (i % 2 == 0) {
      flags = 0;
      dd1 = zigzag_value;
      if (dd1 == 0) {
        flag1 = 0;
      } else {
        flag1 = (uint8_t)(LONG_BYTES - BUILDIN_CLZL(dd1) / BITS_PER_BYTE);
      }
    } else {
      dd2 = zigzag_value;
      if (dd2 == 0) {
        flag2 = 0;
      } else {
        flag2 = (uint8_t)(LONG_BYTES - BUILDIN_CLZL(dd2) / BITS_PER_BYTE);
      }
      flags = flag1 | (flag2 << 4);
      // Encode the flag.
      if ((_pos + CHAR_BYTES - 1) >= nelements * longBytes) goto _exit_over;
      memcpy(output + _pos, &flags, CHAR_BYTES);
      _pos += CHAR_BYTES;
      /* Here, we assume it is little endian encoding method. */
      // Encode dd1
      if (is_bigendian()) {
        if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd1) + longBytes - flag1, flag1);
      } else {
        if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd1), flag1);
      }
      _pos += flag1;
      // Encode dd2;
      if (is_bigendian()) {
        if ((_pos + flag2 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd2) + longBytes - flag2, flag2);
      } else {
        if ((_pos + flag2 - 1) >= nelements * longBytes) goto _exit_over;
        memcpy(output + _pos, (char *)(&dd2), flag2);
      }
      _pos += flag2;
    }
    prev_value = curr_value;
    prev_delta = curr_delta;
  }

  if (nelements % 2 == 1) {
    flag2 = 0;
    flags = flag1 | (flag2 << 4);
    // Encode the flag.
    if ((_pos + CHAR_BYTES - 1) >= nelements * longBytes) goto _exit_over;
    memcpy(output + _pos, &flags, CHAR_BYTES);
    _pos += CHAR_BYTES;
    // Encode dd1;
    if (is_bigendian()) {
      if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
      memcpy(output + _pos, (char *)(&dd1) + longBytes - flag1, flag1);
    } else {
      if ((_pos + flag1 - 1) >= nelements * longBytes) goto _exit_over;
      memcpy(output + _pos, (char *)(&dd1), flag1);
    }
    _pos += flag1;
  }

  output[0] = 1;  // Means the string is compressed
  return _pos;

_exit_over:
  output[0] = 0;  // Means the string is not compressed
  memcpy(output + 1, input, nelements * longBytes);
  return nelements * longBytes + 1;
}

int32_t tsDecompressTimestampImp(const char *const input, const int32_t nelements, char *const output) {
  int64_t longBytes = LONG_BYTES;

  if (nelements < 0) return -1;
  if (nelements == 0) return 0;

  if (input[0] == 0) {
    memcpy(output, input + 1, nelements * longBytes);
    return nelements * longBytes;
  } else if (input[0] == 1) {  // Decompress
    if (tsSIMDEnable && tsAVX512Enable && tsAVX512Supported) {
      int32_t cnt = tsDecompressTimestampAvx512(input, nelements, output, false);
      if (cnt >= 0) {
        return cnt;
      }
    }

    int64_t *ostream = (int64_t *)output;

    int32_t ipos = 1, opos = 0;
    int8_t  nbytes = 0;
    int64_t prev_value = 0;
    int64_t prev_delta = 0;
    int64_t delta_of_delta = 0;

    while (1) {
      uint8_t flags = input[ipos++];
      // Decode dd1
      uint64_t dd1 = 0;
      nbytes = flags & INT8MASK(4);
      if (nbytes == 0) {
        delta_of_delta = 0;
      } else {
        if (is_bigendian()) {
          memcpy(((char *)(&dd1)) + longBytes - nbytes, input + ipos, nbytes);
        } else {
          memcpy(&dd1, input + ipos, nbytes);
        }
        delta_of_delta = ZIGZAG_DECODE(int64_t, dd1);
      }

      ipos += nbytes;
      if (opos == 0) {
        prev_value = delta_of_delta;
        prev_delta = 0;
        ostream[opos++] = delta_of_delta;
      } else {
        prev_delta = delta_of_delta + prev_delta;
        prev_value = prev_value + prev_delta;
        ostream[opos++] = prev_value;
      }
      if (opos == nelements) return nelements * longBytes;

      // Decode dd2
      uint64_t dd2 = 0;
      nbytes = (flags >> 4) & INT8MASK(4);
      if (nbytes == 0) {
        delta_of_delta = 0;
      } else {
        if (is_bigendian()) {
          memcpy(((char *)(&dd2)) + longBytes - nbytes, input + ipos, nbytes);
        } else {
          memcpy(&dd2, input + ipos, nbytes);
        }
        // zigzag_decoding
        delta_of_delta = ZIGZAG_DECODE(int64_t, dd2);
      }
      ipos += nbytes;
      prev_delta = delta_of_delta + prev_delta;
      prev_value = prev_value + prev_delta;
      ostream[opos++] = prev_value;
      if (opos == nelements) return nelements * longBytes;
    }
  }

  return nelements * longBytes;
}

int32_t tsCompressPlain2(const char *const input, const int32_t nelements, char *const output, const char type) {
  int32_t bytes = tDataTypes[type].bytes * nelements;
  output[0] = 0;
  memcpy(output + 1, input, bytes);
  return bytes + 1;
}
int32_t tsDecompressPlain2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                           const char type) {
  int32_t bytes = tDataTypes[type].bytes * nelements;
  memcpy(output, input + 1, bytes);
  return bytes;
}
int32_t tsCompressTimestampImp2(const char *const input, const int32_t nelements, char *const output, const char type) {
  return tsCompressTimestampImp(input, nelements, output);
}
int32_t tsDecompressTimestampImp2(const char *const input, int32_t ninput, const int32_t nelements, char *const output,
                                  const char type) {
  return tsDecompressTimestampImp(input, nelements, output);
}

/* --------------------------------------------Double Compression ---------------------------------------------- */
void encodeDoubleValue(uint64_t diff, uint8_t flag, char *const output, int32_t *const pos) {
  int32_t longBytes = LONG_BYTES;

  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int32_t nshift = (longBytes * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t)(diff & INT64MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

int32_t tsCompressDoubleImp(const char *const input, const int32_t nelements, char *const output) {
  int32_t byte_limit = nelements * DOUBLE_BYTES + 1;
  int32_t opos = 1;

  uint64_t prev_value = 0;
  uint64_t prev_diff = 0;
  uint8_t  prev_flag = 0;

  double *istream = (double *)input;

  // Main loop
  for (int32_t i = 0; i < nelements; i++) {
    union {
      double   real;
      uint64_t bits;
    } curr;

    curr.real = istream[i];

    // Here we assume the next value is the same as previous one.
    uint64_t predicted = prev_value;
    uint64_t diff = curr.bits ^ predicted;

    int32_t leading_zeros = LONG_BYTES * BITS_PER_BYTE;
    int32_t trailing_zeros = leading_zeros;

    if (diff) {
      trailing_zeros = BUILDIN_CTZL(diff);
      leading_zeros = BUILDIN_CLZL(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (trailing_zeros > leading_zeros) {
      nbytes = (uint8_t)(LONG_BYTES - trailing_zeros / BITS_PER_BYTE);

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t)1 << 3) | nbytes;
    } else {
      nbytes = (uint8_t)(LONG_BYTES - leading_zeros / BITS_PER_BYTE);
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int32_t nbyte2 = (flag & INT8MASK(3)) + 1;
      if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
        uint8_t flags = prev_flag | (flag << 4);
        output[opos++] = flags;
        encodeDoubleValue(prev_diff, prev_flag, output, &opos);
        encodeDoubleValue(diff, flag, output, &opos);
      } else {
        output[0] = 1;
        memcpy(output + 1, input, byte_limit - 1);
        return byte_limit;
      }
    }
    prev_value = curr.bits;
  }

  if (nelements % 2) {
    int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int32_t nbyte2 = 1;
    if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
      uint8_t flags = prev_flag;
      output[opos++] = flags;
      encodeDoubleValue(prev_diff, prev_flag, output, &opos);
      encodeDoubleValue(0ul, 0, output, &opos);
    } else {
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      return byte_limit;
    }
  }

  output[0] = 0;
  return opos;
}

FORCE_INLINE uint64_t decodeDoubleValue(const char *const input, int32_t *const ipos, uint8_t flag) {
  int32_t longBytes = LONG_BYTES;

  uint64_t diff = 0ul;
  int32_t  nbytes = (flag & 0x7) + 1;
  for (int32_t i = 0; i < nbytes; i++) {
    diff |= (((uint64_t)0xff & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int32_t shift_width = (longBytes * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

static int32_t tsDecompressDoubleImpHelper(const char *input, int32_t nelements, char *output) {
  double  *ostream = (double *)output;
  uint8_t  flags = 0;
  int32_t  ipos = 0;
  int32_t  opos = 0;
  uint64_t diff = 0;
  union {
    uint64_t bits;
    double   real;
  } curr;

  curr.bits = 0;

  for (int32_t i = 0; i < nelements; i++) {
    if ((i & 0x01) == 0) {
      flags = input[ipos++];
    }

    diff = decodeDoubleValue(input, &ipos, flags & INT8MASK(4));
    flags >>= 4;
    curr.bits ^= diff;

    ostream[opos++] = curr.real;
  }

  return nelements * DOUBLE_BYTES;
}

int32_t tsDecompressDoubleImp(const char *const input, int32_t ninput, const int32_t nelements, char *const output) {
  // return the result directly if there is no compression
  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * DOUBLE_BYTES);
    return nelements * DOUBLE_BYTES;
  }

  // use AVX2 implementation when allowed and the compression ratio is not high
  double compressRatio = 1.0 * nelements * DOUBLE_BYTES / ninput;
  if (tsSIMDEnable && tsAVX2Supported && compressRatio < 2) {
    int32_t cnt = tsDecompressDoubleImpAvx2(input + 1, nelements, output);
    if (cnt >= 0) {
      return cnt;
    }
  }

  // use implementation without SIMD instructions by default
  return tsDecompressDoubleImpHelper(input + 1, nelements, output);
}

/* --------------------------------------------Float Compression ---------------------------------------------- */
void encodeFloatValue(uint32_t diff, uint8_t flag, char *const output, int32_t *const pos) {
  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int32_t nshift = (FLOAT_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t)(diff & INT32MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

int32_t tsCompressFloatImp(const char *const input, const int32_t nelements, char *const output) {
  float  *istream = (float *)input;
  int32_t byte_limit = nelements * FLOAT_BYTES + 1;
  int32_t opos = 1;

  uint32_t prev_value = 0;
  uint32_t prev_diff = 0;
  uint8_t  prev_flag = 0;

  // Main loop
  for (int32_t i = 0; i < nelements; i++) {
    union {
      float    real;
      uint32_t bits;
    } curr;

    curr.real = istream[i];

    // Here we assume the next value is the same as previous one.
    uint32_t predicted = prev_value;
    uint32_t diff = curr.bits ^ predicted;

    int32_t clz = FLOAT_BYTES * BITS_PER_BYTE;
    int32_t ctz = clz;

    if (diff) {
      ctz = BUILDIN_CTZ(diff);
      clz = BUILDIN_CLZ(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (ctz > clz) {
      nbytes = (uint8_t)(FLOAT_BYTES - ctz / BITS_PER_BYTE);

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t)1 << 3) | nbytes;
    } else {
      nbytes = (uint8_t)(FLOAT_BYTES - clz / BITS_PER_BYTE);
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int32_t nbyte2 = (flag & INT8MASK(3)) + 1;
      if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
        uint8_t flags = prev_flag | (flag << 4);
        output[opos++] = flags;
        encodeFloatValue(prev_diff, prev_flag, output, &opos);
        encodeFloatValue(diff, flag, output, &opos);
      } else {
        output[0] = 1;
        memcpy(output + 1, input, byte_limit - 1);
        return byte_limit;
      }
    }
    prev_value = curr.bits;
  }

  if (nelements % 2) {
    int32_t nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int32_t nbyte2 = 1;
    if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
      uint8_t flags = prev_flag;
      output[opos++] = flags;
      encodeFloatValue(prev_diff, prev_flag, output, &opos);
      encodeFloatValue(0, 0, output, &opos);
    } else {
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      return byte_limit;
    }
  }

  output[0] = 0;
  return opos;
}

uint32_t decodeFloatValue(const char *const input, int32_t *const ipos, uint8_t flag) {
  uint32_t diff = 0ul;
  int32_t  nbytes = (flag & INT8MASK(3)) + 1;
  for (int32_t i = 0; i < nbytes; i++) {
    diff = diff | ((INT32MASK(8) & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int32_t shift_width = (FLOAT_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

static int32_t tsDecompressFloatImpHelper(const char *input, int32_t nelements, char *output) {
  float   *ostream = (float *)output;
  uint8_t  flags = 0;
  int32_t  ipos = 0;
  int32_t  opos = 0;
  uint32_t diff = 0;
  union {
    uint32_t bits;
    float    real;
  } curr;

  curr.bits = 0;

  for (int32_t i = 0; i < nelements; i++) {
    if (i % 2 == 0) {
      flags = input[ipos++];
    }

    diff = decodeFloatValue(input, &ipos, flags & INT8MASK(4));
    flags >>= 4;
    curr.bits ^= diff;

    ostream[opos++] = curr.real;
  }

  return nelements * FLOAT_BYTES;
}

int32_t tsDecompressFloatImp(const char *const input, int32_t ninput, const int32_t nelements, char *const output) {
  if (input[0] == 1) {
    memcpy(output, input + 1, nelements * FLOAT_BYTES);
    return nelements * FLOAT_BYTES;
  }

  // use AVX2 implementation when allowed and the compression ratio is not high
  double compressRatio = 1.0 * nelements * FLOAT_BYTES / ninput;
  if (tsSIMDEnable && tsAVX2Supported && compressRatio < 2) {
    int32_t cnt = tsDecompressFloatImpAvx2(input + 1, nelements, output);
    if (cnt >= 0) {
      return cnt;
    }
  }

  // use implementation without SIMD instructions by default
  return tsDecompressFloatImpHelper(input + 1, nelements, output);
}

//
//   ----------  float double lossy  -----------
//
int32_t tsCompressFloatLossyImp(const char *input, const int32_t nelements, char *const output) {
  // compress with sz
  int32_t       compressedSize = tdszCompress(SZ_FLOAT, input, nelements, output + 1);
  unsigned char algo = ALGO_SZ_LOSSY << 1;
  if (compressedSize == 0 || compressedSize >= nelements * sizeof(float)) {
    // compressed error or large than original
    output[0] = MODE_NOCOMPRESS | algo;
    memcpy(output + 1, input, nelements * sizeof(float));
    compressedSize = 1 + nelements * sizeof(float);
  } else {
    // compressed successfully
    output[0] = MODE_COMPRESS | algo;
    compressedSize += 1;
  }

  return compressedSize;
}

int32_t tsDecompressFloatLossyImp(const char *input, int32_t compressedSize, const int32_t nelements,
                                  char *const output) {
  int32_t decompressedSize = 0;
  if (HEAD_MODE(input[0]) == MODE_NOCOMPRESS) {
    // orginal so memcpy directly
    decompressedSize = nelements * sizeof(float);
    memcpy(output, input + 1, decompressedSize);

    return decompressedSize;
  }

  // decompressed with sz
  return tdszDecompress(SZ_FLOAT, input + 1, compressedSize - 1, nelements, output);
}

int32_t tsCompressDoubleLossyImp(const char *input, const int32_t nelements, char *const output) {
  // compress with sz
  int32_t       compressedSize = tdszCompress(SZ_DOUBLE, input, nelements, output + 1);
  unsigned char algo = ALGO_SZ_LOSSY << 1;
  if (compressedSize == 0 || compressedSize >= nelements * sizeof(double)) {
    // compressed error or large than original
    output[0] = MODE_NOCOMPRESS | algo;
    memcpy(output + 1, input, nelements * sizeof(double));
    compressedSize = 1 + nelements * sizeof(double);
  } else {
    // compressed successfully
    output[0] = MODE_COMPRESS | algo;
    compressedSize += 1;
  }

  return compressedSize;
}

int32_t tsDecompressDoubleLossyImp(const char *input, int32_t compressedSize, const int32_t nelements,
                                   char *const output) {
  int32_t decompressedSize = 0;
  if (HEAD_MODE(input[0]) == MODE_NOCOMPRESS) {
    // orginal so memcpy directly
    decompressedSize = nelements * sizeof(double);
    memcpy(output, input + 1, decompressedSize);

    return decompressedSize;
  }

  // decompressed with sz
  return tdszDecompress(SZ_DOUBLE, input + 1, compressedSize - 1, nelements, output);
}

/*************************************************************************
 *                  REGULAR COMPRESSION
 *************************************************************************/
// Timestamp =====================================================
int32_t tsCompressTimestamp(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                            int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressTimestampImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressTimestampImp(pIn, nEle, pBuf);
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    // tDataTypeCompress[TSDB_DATA_TYPE_TIMESTAMP].compFunc(pIn, nIn, nEle, pOut, nOut, );
  }
  return 0;
}

int32_t tsDecompressTimestamp(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg,
                              void *pBuf, int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressTimestampImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if (tsDecompressStringImp(pIn, nIn, pBuf, nBuf) < 0) return -1;
    return tsDecompressTimestampImp(pBuf, nEle, pOut);
  } else {
    return -1;
  }
}

// Float =====================================================
int32_t tsCompressFloat(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf) {
  // lossy mode
  if (lossyFloat) {
    return tsCompressFloatLossyImp(pIn, nEle, pOut);
    // lossless mode
  } else {
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsCompressFloatImp(pIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      int32_t len = tsCompressFloatImp(pIn, nEle, pBuf);
      return tsCompressStringImp(pBuf, len, pOut, nOut);
    } else {
      return TSDB_CODE_INVALID_PARA;
    }
  }
}

int32_t tsDecompressFloat(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  if (HEAD_ALGO(((uint8_t *)pIn)[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressFloatLossyImp(pIn, nIn, nEle, pOut);
  } else {
    // decompress lossless
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsDecompressFloatImp(pIn, nIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      int32_t bufLen = tsDecompressStringImp(pIn, nIn, pBuf, nBuf);
      if (bufLen < 0) return -1;
      return tsDecompressFloatImp(pBuf, bufLen, nEle, pOut);
    } else {
      return TSDB_CODE_INVALID_PARA;
    }
  }
}

// Double =====================================================
int32_t tsCompressDouble(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  if (lossyDouble) {
    // lossy mode
    return tsCompressDoubleLossyImp(pIn, nEle, pOut);
  } else {
    // lossless mode
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsCompressDoubleImp(pIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      int32_t len = tsCompressDoubleImp(pIn, nEle, pBuf);
      return tsCompressStringImp(pBuf, len, pOut, nOut);
    } else {
      return TSDB_CODE_INVALID_PARA;
    }
  }
}

int32_t tsDecompressDouble(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  if (HEAD_ALGO(((uint8_t *)pIn)[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressDoubleLossyImp(pIn, nIn, nEle, pOut);
  } else {
    // decompress lossless
    if (cmprAlg == ONE_STAGE_COMP) {
      return tsDecompressDoubleImp(pIn, nIn, nEle, pOut);
    } else if (cmprAlg == TWO_STAGE_COMP) {
      int32_t bufLen = tsDecompressStringImp(pIn, nIn, pBuf, nBuf);
      if (bufLen < 0) return -1;
      return tsDecompressDoubleImp(pBuf, bufLen, nEle, pOut);
    } else {
      return TSDB_CODE_INVALID_PARA;
    }
  }
}

// Binary =====================================================
int32_t tsCompressString(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  return tsCompressStringImp(pIn, nIn, pOut, nOut);
}

int32_t tsDecompressString(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  return tsDecompressStringImp(pIn, nIn, pOut, nOut);
}

// Bool =====================================================
int32_t tsCompressBool(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                       int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressBoolImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressBoolImp(pIn, nEle, pBuf);
    if (len < 0) {
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
}

int32_t tsDecompressBool(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  int32_t code = 0;
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressBoolImp(pIn, nEle, pOut);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if ((code = tsDecompressStringImp(pIn, nIn, pBuf, nBuf)) < 0) return code;
    return tsDecompressBoolImp(pBuf, nEle, pOut);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

// Tinyint =====================================================
int32_t tsCompressTinyint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_TINYINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_TINYINT);
    if (len < 0) {
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t tsDecompressTinyint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                            int32_t nBuf) {
  int32_t code = 0;
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_TINYINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if ((code = tsDecompressStringImp(pIn, nIn, pBuf, nBuf)) < 0) return code;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_TINYINT);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

// Smallint =====================================================
int32_t tsCompressSmallint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_SMALLINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_SMALLINT);
    if (len < 0) {
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t tsDecompressSmallint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg,
                             void *pBuf, int32_t nBuf) {
  int32_t code = 0;
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_SMALLINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if ((code = tsDecompressStringImp(pIn, nIn, pBuf, nBuf)) < 0) return code;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_SMALLINT);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

// Int =====================================================
int32_t tsCompressInt(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                      int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_INT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_INT);
    if (len < 0) {
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t tsDecompressInt(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                        int32_t nBuf) {
  int32_t code = 0;
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_INT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if ((code = tsDecompressStringImp(pIn, nIn, pBuf, nBuf)) < 0) return code;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_INT);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

// Bigint =====================================================
int32_t tsCompressBigint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsCompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_BIGINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    int32_t len = tsCompressINTImp(pIn, nEle, pBuf, TSDB_DATA_TYPE_BIGINT);
    if (len < 0) {
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
    return tsCompressStringImp(pBuf, len, pOut, nOut);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

int32_t tsDecompressBigint(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint8_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  int32_t code = 0;
  if (cmprAlg == ONE_STAGE_COMP) {
    return tsDecompressINTImp(pIn, nEle, pOut, TSDB_DATA_TYPE_BIGINT);
  } else if (cmprAlg == TWO_STAGE_COMP) {
    if ((code = tsDecompressStringImp(pIn, nIn, pBuf, nBuf)) < 0) return code;
    return tsDecompressINTImp(pBuf, nEle, pOut, TSDB_DATA_TYPE_BIGINT);
  } else {
    return TSDB_CODE_INVALID_PARA;
  }
}

#define FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, alg, pBuf, nBuf, type, compress)                               \
  do {                                                                                                                \
    DEFINE_VAR(alg)                                                                                                   \
    if (l1 != L1_DISABLED && l2 == L2_DISABLED) {                                                                     \
      if (compress) {                                                                                                 \
        uTrace("encode:%s, compress:%s, level:%s, type:%s", compressL1Dict[l1].name, "disabled", "disabled",          \
               tDataTypes[type].name);                                                                                \
        return compressL1Dict[l1].comprFn(pIn, nEle, pOut, type);                                                     \
      } else {                                                                                                        \
        uTrace("dencode:%s, compress:%s, level:%s, type:%s", compressL1Dict[l1].name, "disabled", "disabled",         \
               tDataTypes[type].name);                                                                                \
        return compressL1Dict[l1].decomprFn(pIn, nIn, nEle, pOut, type);                                              \
      }                                                                                                               \
    } else if (l1 != L1_DISABLED && l2 != L2_DISABLED) {                                                              \
      if (compress) {                                                                                                 \
        uTrace("encode:%s, compress:%s, level:%d, type:%s, l1:%d", compressL1Dict[l1].name, compressL2Dict[l2].name,  \
               lvl, tDataTypes[type].name, l1);                                                                       \
        int32_t len = compressL1Dict[l1].comprFn(pIn, nEle, pBuf, type);                                              \
        if (len < 0) {                                                                                                \
          return len;                                                                                                 \
        }                                                                                                             \
        int8_t alvl = tsGetCompressL2Level(l2, lvl);                                                                  \
        return compressL2Dict[l2].comprFn(pBuf, len, pOut, nOut, type, alvl);                                         \
      } else {                                                                                                        \
        uTrace("dencode:%s, decompress:%s, level:%d, type:%s", compressL1Dict[l1].name, compressL2Dict[l2].name, lvl, \
               tDataTypes[type].name);                                                                                \
        int32_t bufLen = compressL2Dict[l2].decomprFn(pIn, nIn, pBuf, nBuf, type);                                    \
        if (bufLen < 0) return -1;                                                                                    \
        return compressL1Dict[l1].decomprFn(pBuf, bufLen, nEle, pOut, type);                                          \
      }                                                                                                               \
    } else if (l1 == L1_DISABLED && l2 != L2_DISABLED) {                                                              \
      if (compress) {                                                                                                 \
        uTrace("encode:%s, compress:%s, level:%d, type:%s", "disabled", "disable", lvl, tDataTypes[type].name);       \
        int8_t alvl = tsGetCompressL2Level(l2, lvl);                                                                  \
        return compressL2Dict[l2].comprFn(pIn, nIn, pOut, nOut, type, alvl);                                          \
      } else {                                                                                                        \
        uTrace("dencode:%s, decompress:%s, level:%d, type:%s", "disabled", compressL2Dict[l2].name, lvl,              \
               tDataTypes[type].name);                                                                                \
        return compressL2Dict[l2].decomprFn(pIn, nIn, pOut, nOut, type);                                              \
      }                                                                                                               \
    }                                                                                                                 \
    return TSDB_CODE_INVALID_PARA;                                                                                    \
  } while (1)

/*************************************************************************
 *                  REGULAR COMPRESSION 2
 *************************************************************************/
// Timestamp =====================================================
int32_t tsCompressTimestamp2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                             void *pBuf, int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_TIMESTAMP, 1);
}

int32_t tsDecompressTimestamp2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                               void *pBuf, int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_TIMESTAMP, 0);
}

// Float =====================================================
int32_t tsCompressFloat2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  DEFINE_VAR(cmprAlg)
  if (l2 == L2_TSZ && lvl != 0 && lossyFloat) {
    return tsCompressFloatLossyImp(pIn, nEle, pOut);
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_FLOAT, 1);
}

int32_t tsDecompressFloat2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  DEFINE_VAR(cmprAlg)
  if (lvl != 0 && HEAD_ALGO(((uint8_t *)pIn)[0]) == ALGO_SZ_LOSSY) {
    return tsDecompressFloatLossyImp(pIn, nIn, nEle, pOut);
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_FLOAT, 0);
}

// Double =====================================================
int32_t tsCompressDouble2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  DEFINE_VAR(cmprAlg)
  if (l2 == L2_TSZ && lvl != 0 && lossyDouble) {
    // lossy mode
    return tsCompressDoubleLossyImp(pIn, nEle, pOut);
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_DOUBLE, 1);
}

int32_t tsDecompressDouble2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf) {
  DEFINE_VAR(cmprAlg)
  if (lvl != 0 && HEAD_ALGO(((uint8_t *)pIn)[0]) == ALGO_SZ_LOSSY) {
    // decompress lossy
    return tsDecompressDoubleLossyImp(pIn, nIn, nEle, pOut);
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_DOUBLE, 0);
}

// Binary =====================================================
int32_t tsCompressString2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  DEFINE_VAR(cmprAlg)
  if (l2 == L2_DISABLED) {
    l2 = 0;
  }
  return compressL2Dict[l2].comprFn(pIn, nIn, pOut, nOut, TSDB_DATA_TYPE_BINARY, lvl);
}

int32_t tsDecompressString2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf) {
  // return 0;
  DEFINE_VAR(cmprAlg)
  if (l2 == L2_DISABLED) {
    l2 = 0;
  }
  return compressL2Dict[l2].decomprFn(pIn, nIn, pOut, nOut, TSDB_DATA_TYPE_BINARY);
}

// Bool =====================================================
int32_t tsCompressBool2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                        int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_RLE) {
    SET_COMPRESS(L1_RLE, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_BOOL, 1);
}

int32_t tsDecompressBool2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_RLE) {
    SET_COMPRESS(L1_RLE, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_BOOL, 0);
}

// Tinyint =====================================================
int32_t tsCompressTinyint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                           int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_SIMPLE_8B) {
    SET_COMPRESS(L1_SIMPLE_8B, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_TINYINT, 1);
}

int32_t tsDecompressTinyint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                             void *pBuf, int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_SIMPLE_8B) {
    SET_COMPRESS(L1_SIMPLE_8B, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_TINYINT, 0);
}

// Smallint =====================================================
int32_t tsCompressSmallint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_SIMPLE_8B) {
    SET_COMPRESS(L1_SIMPLE_8B, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_SMALLINT, 1);
}

int32_t tsDecompressSmallint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                              void *pBuf, int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_SIMPLE_8B) {
    SET_COMPRESS(L1_SIMPLE_8B, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_SMALLINT, 0);
}

// Int =====================================================
int32_t tsCompressInt2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                       int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_SIMPLE_8B) {
    SET_COMPRESS(L1_SIMPLE_8B, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }

  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_INT, 1);
}

int32_t tsDecompressInt2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                         int32_t nBuf) {
  uint32_t tCmprAlg = 0;
  DEFINE_VAR(cmprAlg)
  if (l1 != L1_SIMPLE_8B) {
    SET_COMPRESS(L1_SIMPLE_8B, l2, lvl, tCmprAlg);
  } else {
    tCmprAlg = cmprAlg;
  }
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, tCmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_INT, 0);
}

// Bigint =====================================================
int32_t tsCompressBigint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg, void *pBuf,
                          int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_BIGINT, 1);
}

int32_t tsDecompressBigint2(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_BIGINT, 0);
}

int32_t tsCompressDecimal64(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                            void *pBuf, int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_DECIMAL64, 1);
}
int32_t tsDecompressDecimal64(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                              void *pBuf, int32_t nBuf) {

  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_DECIMAL64, 0);
}

int32_t tsCompressDecimal128(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                             void *pBuf, int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_DECIMAL, 1);
}
int32_t tsDecompressDecimal128(void *pIn, int32_t nIn, int32_t nEle, void *pOut, int32_t nOut, uint32_t cmprAlg,
                               void *pBuf, int32_t nBuf) {
  FUNC_COMPRESS_IMPL(pIn, nIn, nEle, pOut, nOut, cmprAlg, pBuf, nBuf, TSDB_DATA_TYPE_DECIMAL, 0);
}

void tcompressDebug(uint32_t cmprAlg, uint8_t *l1Alg, uint8_t *l2Alg, uint8_t *level) {
  DEFINE_VAR(cmprAlg)
  *l1Alg = l1;
  *l2Alg = l2;
  *level = lvl;
  return;
}

int8_t tUpdateCompress(uint32_t oldCmpr, uint32_t newCmpr, uint8_t l2Disabled, uint8_t lvlDiabled, uint8_t lvlDefault,

                       uint32_t *dst) {
  int8_t  update = 0;
  uint8_t ol1 = COMPRESS_L1_TYPE_U32(oldCmpr);
  uint8_t ol2 = COMPRESS_L2_TYPE_U32(oldCmpr);
  uint8_t olvl = COMPRESS_L2_TYPE_LEVEL_U32(oldCmpr);

  uint8_t nl1 = COMPRESS_L1_TYPE_U32(newCmpr);
  uint8_t nl2 = COMPRESS_L2_TYPE_U32(newCmpr);
  uint8_t nlvl = COMPRESS_L2_TYPE_LEVEL_U32(newCmpr);

  // nl1 == 0, not update encode
  // nl2 == 0, not update compress
  // nl3 == 0, not update level
  if (nl1 != 0 && ol1 != nl1) {
    SET_COMPRESS(nl1, ol2, olvl, *dst);
    update = 1;
    ol1 = nl1;
  }

  if (nl2 != 0 && ol2 != nl2) {
    if (nl2 == l2Disabled) {
      SET_COMPRESS(ol1, nl2, lvlDiabled, *dst);
    } else {
      if (ol2 == l2Disabled) {
        SET_COMPRESS(ol1, nl2, lvlDefault, *dst);
      } else {
        SET_COMPRESS(ol1, nl2, olvl, *dst);
      }
    }
    update = 1;
    ol2 = nl2;
  }

  if (nlvl != 0 && olvl != nlvl) {
    if (update == 0) {
      if (ol2 == L2_DISABLED) {
        update = -1;
        return update;
      }
    }
    SET_COMPRESS(ol1, ol2, nlvl, *dst);
    update = 1;
  }

  return update;
}

int32_t getWordLength(char type) {
  int32_t wordLength = 0;
  switch (type) {
    case TSDB_DATA_TYPE_BIGINT:
      wordLength = LONG_BYTES;
      break;
    case TSDB_DATA_TYPE_INT:
      wordLength = INT_BYTES;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      wordLength = SHORT_BYTES;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      wordLength = CHAR_BYTES;
      break;
    case TSDB_DATA_TYPE_DECIMAL64:
      wordLength = DECIMAL64_BYTES;
      break;
    case TSDB_DATA_TYPE_DECIMAL:
      wordLength = DECIMAL128_BYTES;
      break;
    default:
      uError("Invalid decompress integer type:%d", type);
      return TSDB_CODE_INVALID_PARA;
  }

  return wordLength;
}

int32_t plainCompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t size = *dstSize;
  if (size < srcSize) {
    return -1;
  }
  memcpy(dst, src, srcSize);
  return srcSize;
}

int32_t plainDecompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t size = *dstSize;
  if (size < srcSize) {
    return -1;
  }
  memcpy(dst, src, srcSize);
  return 0;
}

int32_t lz4CompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t size = *dstSize;
  int32_t nWrite = LZ4_compress_default(src, dst, srcSize, size);
  if (nWrite <= 0) {
    return -1;
  }
  if (nWrite >= srcSize) {
    return -1;
  }

  *dstSize = nWrite;
  return 0;
}
int32_t lz4DecompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
  int32_t size = *dstSize;
  int32_t nread = LZ4_decompress_safe(src, dst, srcSize, size);
  if (nread <= 0) {
    return -1;
  }
  *dstSize = nread;
  return 0;
}

int32_t zlibCompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
#if defined(WINDOWS) || defined(DARWIN)
  return TSDB_CODE_INVALID_CFG;
#else
  uLongf  dstLen = *dstSize;
  int32_t ret = compress2(dst, &dstLen, src, srcSize, Z_BEST_COMPRESSION);
  if (ret != Z_OK) {
    return -1;
  }
  if (dstLen > srcSize) {
    return -1;
  }
  *dstSize = dstLen;
  return 0;
#endif
}
int32_t zlibDecompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
#if defined(WINDOWS) || defined(DARWIN)
  return TSDB_CODE_INVALID_CFG;
#else
  int32_t code = 0;

  uLongf  dstLen = *dstSize;
  int32_t ret = uncompress(dst, &dstLen, src, srcSize);
  if (ret != Z_OK) {
    return -1;
  }

  *dstSize = dstLen;
  return code;
#endif
}

int32_t zstdCompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
#if defined(WINDOWS) || defined(DARWIN)
  return TSDB_CODE_INVALID_CFG;
#else
  size_t len = ZSTD_compress(dst, *dstSize, src, srcSize, 9);
  if (ZSTD_isError(len)) {
    return -1;
  }

  if (len > srcSize) {
    return -1;
  }

  *dstSize = len;
  return 0;
#endif
}
int32_t zstdDecompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
#if defined(WINDOWS) || defined(DARWIN)
  return TSDB_CODE_INVALID_CFG;
#else
  size_t len = ZSTD_decompress(dst, *dstSize, src, srcSize);
  if (ZSTD_isError(len)) {
    return -1;
  }

  *dstSize = len;
  return 0;
#endif
}

int32_t xzCompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
#if defined(WINDOWS) || defined(DARWIN)
  return TSDB_CODE_INVALID_CFG;
#else
  size_t len = FL2_compress(dst, *dstSize, src, srcSize, 9);
  if (len == 0 || len > srcSize) {
    return -1;
  }
  *dstSize = len;
  return 0;
#endif
}
int32_t xzDecompressImpl(void *src, int32_t srcSize, void *dst, int32_t *dstSize) {
#if defined(WINDOWS) || defined(DARWIN)
  return TSDB_CODE_INVALID_CFG;
#else
  size_t len = FL2_decompress(dst, *dstSize, src, srcSize);
  if (len == 0) {
    return -1;
  }
  *dstSize = len;
  return 0;
#endif
}