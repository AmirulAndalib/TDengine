---
title: Schemaless 写入
description: 'Schemaless 写入方式，可以免于预先创建超级表/子表的步骤，随着数据写入接口能够自动创建与数据对应的存储结构'
---

在物联网应用中，常会采集比较多的数据项，用于实现智能控制、业务分析、设备监控等。由于应用逻辑的版本升级，或者设备自身的硬件调整等原因，数据采集项就有可能比较频繁地出现变动。为了在这种情况下方便地完成数据记录工作，TDengine 提供调用 Schemaless 写入方式，可以免于预先创建超级表/子表的步骤，随着数据写入接口能够自动创建与数据对应的存储结构。并且在必要时，Schemaless
将自动增加必要的数据列，保证用户写入的数据可以被正确存储。

无模式写入方式建立的超级表及其对应的子表与通过 SQL 直接建立的超级表和子表完全没有区别，你也可以通过，SQL 语句直接向其中写入数据。需要注意的是，通过无模式写入方式建立的表，其表名是基于标签值按照固定的映射规则生成，所以无法明确地进行表意，缺乏可读性。

注意：无模式写入会自动建表，不需要手动建表，手动建表的话可能会出现未知的错误。

## 无模式写入行协议

TDengine 的无模式写入的行协议兼容 InfluxDB 的 行协议（Line Protocol）、OpenTSDB 的 telnet 行协议、OpenTSDB 的 JSON 格式协议。但是使用这三种协议的时候，需要在 API 中指定输入内容使用解析协议的标准。

对于 InfluxDB、OpenTSDB 的标准写入协议请参考各自的文档。下面首先以 InfluxDB 的行协议为基础，介绍 TDengine 扩展的协议内容，允许用户采用更加精细的方式控制（超级表）模式。

Schemaless 采用一个字符串来表达一个数据行（可以向写入 API 中一次传入多行字符串来实现多个数据行的批量写入），其格式约定如下：

```json
measurement,tag_set field_set timestamp
```

其中:

- measurement 将作为数据表名。它与 tag_set 之间使用一个英文逗号来分隔。
- tag_set 将作为标签数据，其格式形如 `<tag_key>=<tag_value>,<tag_key>=<tag_value>`，也即可以使用英文逗号来分隔多个标签数据。它与 field_set 之间使用一个半角空格来分隔。
- field_set 将作为普通列数据，其格式形如 `<field_key>=<field_value>,<field_key>=<field_value>`，同样是使用英文逗号来分隔多个普通列的数据。它与 timestamp 之间使用一个半角空格来分隔。
- timestamp 即本行数据对应的主键时间戳。

tag_set 中的所有的数据自动转化为 nchar 数据类型，并不需要使用双引号（")。

在无模式写入数据行协议中，field_set 中的每个数据项都需要对自身的数据类型进行描述。具体来说：

- 如果两边有英文双引号，表示 VARCHAR 类型。例如 `"abc"`。
- 如果两边有英文双引号而且带有 L或l 前缀，表示 NCHAR 类型。例如 `L"报错信息"`。
- 如果两边有英文双引号而且带有 G或g 前缀，表示 GEOMETRY 类型。例如 `G"Point(4.343 89.342)"`。
- 如果两边有英文双引号而且带有 B或b 前缀，表示 VARBINARY 类型，双引号内可以为\x开头的16进制或者字符串。例如 `B"\x98f46e"` `B"hello"`。
- 对空格、等号（=）、逗号（,）、双引号（"）、反斜杠（\），前面需要使用反斜杠（\）进行转义。（都指的是英文半角符号）。具体转义规则如下：

| **序号**  | **域**      | **需转义字符**                  |
| -------- | ----------- | ----------------------------- |
| 1        | 超级表名      | 逗号，空格                      |
| 2        | 标签名       | 逗号，等号，空格                 |
| 3        | 标签值       | 逗号，等号，空格                 |
| 4        | 列名         | 逗号，等号，空格                 |
| 5        | 列值         | 双引号，反斜杠                  |

两个连续的反斜杠，第一个作为转义符，只有一个反斜杠则无需转义. 反斜杠转义规则举例如下:

| **序号**  | **反斜杠**     | **转义为**                     |
| -------- | -----------   | ----------------------------- |
| 1        | \             | \                             |
| 2        | \\\\          | \                             |
| 3        | \\\\\\        | \\\\                          |
| 4        | \\\\\\\\      | \\\\                          |
| 5        | \\\\\\\\\\    | \\\\\\                        |
| 6        | \\\\\\\\\\\\  | \\\\\\                        |

- 数值类型将通过后缀来区分数据类型：

| **序号**  | **后缀**    | **映射类型**                  | **大小(字节)** |
| -------- | ----------- | ----------------------------- | -------------- |
| 1        | 无或 f64     | double                        | 8              |
| 2        | f32         | float                         | 4              |
| 3        | i8/u8       | TinyInt/UTinyInt              | 1              |
| 4        | i16/u16     | SmallInt/USmallInt            | 2              |
| 5        | i32/u32     | Int/UInt                      | 4              |
| 6        | i64/i/u64/u | BigInt/BigInt/UBigInt/UBigInt | 8              |

- t, T, true, True, TRUE, f, F, false, False 将直接作为 BOOL 型来处理。

例如如下数据行表示：向名为 st 的超级表下的 t1 标签为 "3"（NCHAR）、t2 标签为 "4"（NCHAR）、t3
标签为 "t3"（NCHAR）的数据子表，写入 c1 列为 3（BIGINT）、c2 列为 false（BOOL）、c3
列为 "passit"（BINARY）、c4 列为 4（DOUBLE）、主键时间戳为 1626006833639000000 的一行数据。

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

需要注意的是，如果描述数据类型后缀时使用了错误的大小写，或者为数据指定的数据类型有误，均可能引发报错提示而导致数据写入失败。

## 无模式写入的主要处理逻辑

无模式写入按照如下原则来处理行数据：

1. 将使用如下规则来生成子表名：首先将 measurement 的名称和标签的 key 和 value 组合成为如下的字符串

```json
"measurement,tag_key1=tag_value1,tag_key2=tag_value2"
```

:::tip
需要注意的是，这里的 tag_key1, tag_key2 并不是用户输入的标签的原始顺序，而是使用了标签名称按照字符串升序排列后的结果。所以，tag_key1 并不是在行协议中输入的第一个标签。
排列完成以后计算该字符串的 MD5 散列值 "md5_val"。然后将计算的结果与字符串组合生成表名：“t_md5_val”。其中的 “t_” 是固定的前缀，每个通过该映射关系自动生成的表都具有该前缀。
:::tip 
为了让用户可以指定生成的表名，可以通过在taos.cfg里配置 smlChildTableName 参数来指定。
举例如下：配置 smlChildTableName=tname 插入数据为 st,tname=cpu1,t1=4 c1=3 1626006833639000000 则创建的表名为 cpu1，注意如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。

2. 如果解析行协议获得的超级表不存在，则会创建这个超级表（不建议手动创建超级表，不然插入数据可能异常）。
3. 如果解析行协议获得子表不存在，则 Schemaless 会按照步骤 1 或 2 确定的子表名来创建子表。
4. 如果数据行中指定的标签列或普通列不存在，则在超级表中增加对应的标签列或普通列（只增不减）。
5. 如果超级表中存在一些标签列或普通列未在一个数据行中被指定取值，那么这些列的值在这一行中会被置为
   NULL。
6. 对 BINARY 或 NCHAR 列，如果数据行中所提供值的长度超出了列类型的限制，自动增加该列允许存储的字符长度上限（只增不减），以保证数据的完整保存。
7. 整个处理过程中遇到的错误会中断写入过程，并返回错误代码。
8. 为了提高写入的效率，默认假设同一个超级表中 field_set 的顺序是一样的（第一条数据包含所有的 field，后面的数据按照这个顺序），如果顺序不一样，需要配置参数 smlDataFormat 为 false，否则，数据写入按照相同顺序写入，库中数据会异常，从3.0.3.0开始，自动检测顺序是否一致，该配置废弃。
9. 由于sql建表表名不支持点号（.），所以schemaless也对点号（.）做了处理，如果schemaless自动建表的表名如果有点号（.），会自动替换为下划线（\_）。如果手动指定子表名的话，子表名里有点号（.），同样转化为下划线（\_）。
10. taos.cfg 增加 smlTsDefaultName 配置（值为字符串），只在client端起作用，配置后，schemaless自动建表的时间列名字可以通过该配置设置。不配置的话，默认为 _ts
11. 无模式写入的数据超级表或子表名区分大小写
   
:::tip
无模式所有的处理逻辑，仍会遵循 TDengine 对数据结构的底层限制，例如每行数据的总长度不能超过
48KB（从 3.0.5.0 版本开始为 64KB），标签值的总长度不超过16KB。这方面的具体限制约束请参见 [TDengine SQL 边界限制](/taos-sql/limit)

:::

## 时间分辨率识别

无模式写入过程中支持三个指定的模式，具体如下

| **序号** | **值**              | **说明**                        |
| -------- | ------------------- | ------------------------------- |
| 1        | SML_LINE_PROTOCOL   | InfluxDB 行协议（Line Protocol) |
| 2        | SML_TELNET_PROTOCOL | OpenTSDB 文本行协议             |
| 3        | SML_JSON_PROTOCOL   | JSON 协议格式                   |

在 SML_LINE_PROTOCOL 解析模式下，需要用户指定输入的时间戳的时间分辨率。可用的时间分辨率如下表所示：

| **序号** | **时间分辨率定义**                | **含义**       |
| -------- | --------------------------------- | -------------- |
| 1        | TSDB_SML_TIMESTAMP_NOT_CONFIGURED | 未定义（无效） |
| 2        | TSDB_SML_TIMESTAMP_HOURS          | 小时           |
| 3        | TSDB_SML_TIMESTAMP_MINUTES        | 分钟           |
| 4        | TSDB_SML_TIMESTAMP_SECONDS        | 秒             |
| 5        | TSDB_SML_TIMESTAMP_MILLI_SECONDS  | 毫秒           |
| 6        | TSDB_SML_TIMESTAMP_MICRO_SECONDS  | 微秒           |
| 7        | TSDB_SML_TIMESTAMP_NANO_SECONDS   | 纳秒           |

在 SML_TELNET_PROTOCOL 和 SML_JSON_PROTOCOL 模式下，根据时间戳的长度来确定时间精度（与 OpenTSDB 标准操作方式相同），此时会忽略用户指定的时间分辨率。

## 数据模式映射规则

本节将说明 InfluxDB 行协议(Line Protocol)的数据如何映射成为具有模式的数据。每个行协议中数据 measurement 映射为
超级表名称。tag_set 中的 标签名称为 数据模式中的标签名，field_set 中的名称为列名称。以如下数据为例，说明映射规则：

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

该行数据映射生成一个超级表： st， 其包含了 3 个类型为 nchar 的标签，分别是：t1, t2, t3。五个数据列，分别是 ts（timestamp），c1 (bigint），c3(binary)，c2 (bool), c4 (bigint）。映射成为如下 SQL 语句：

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

## 数据模式变更处理

本节将说明不同行数据写入情况下，对于数据模式的影响。

在使用行协议写入一个明确的标识的字段类型的时候，后续更改该字段的类型定义，会出现明确的数据模式错误，即会触发写入 API 报告错误。如下所示，

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

第一行的数据类型映射将 c4 列定义为 Double， 但是第二行的数据又通过数值后缀方式声明该列为 BigInt， 由此会触发无模式写入的解析错误。

如果列前面的行协议将数据列声明为了 binary， 后续的要求长度更长的 binary 长度，此时会触发超级表模式的变更。

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

第一行中行协议解析会声明 c5 列是一个 binary(4)的字段，第二次行数据写入会提取列 c5 仍然是 binary 列，但是其宽度为 6，此时需要将 binary 的宽度增加到能够容纳 新字符串的宽度。

```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

第二行数据相对于第一行来说增加了一个列 c6，类型为 binary(6)。那么此时会自动增加一个列 c6， 类型为 binary(6)。

## 写入完整性

TDengine 提供数据写入的幂等性保证，即您可以反复调用 API 进行出错数据的写入操作。但是不提供多行数据写入的原子性保证。即在多行数据一批次写入过程中，会出现部分数据写入成功，部分数据写入失败的情况。

## 错误码

如果是无模式写入过程中的数据本身错误，应用会得到 TSDB_CODE_TSC_LINE_SYNTAX_ERROR
错误信息，该错误信息表明错误发生在写入文本中。其他的错误码与原系统一致，可以通过
taos_errstr 获取具体的错误原因。