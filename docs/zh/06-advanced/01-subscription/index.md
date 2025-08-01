---
sidebar_label: 数据订阅
title: 数据订阅
toc_max_heading_level: 4
---

为了满足应用程序实时获取 TDengine 写入的数据的需求，或以事件到达顺序处理数据，TDengine 提供了类似于消息队列产品的数据订阅和消费接口。在许多场景中，采用 TDengine 的时序大数据平台，无须再集成消息队列产品，从而简化应用程序设计并降低运维成本。

与 Kafka 类似，用户需要在 TDengine 中定义主题（topic）。然而，TDengine 的主题可以是一个数据库、一张超级表，或者基于现有超级表、子表或普通表的查询条件，即一条查询语句。用户可以利用 SQL 对标签、表名、列、表达式等条件进行过滤，并对数据进行标量函数与 UDF 计算（不包括数据聚合）。与其他消息队列工具相比，这是 TDengine 数据订阅功能的最大优势。它提供了更高的灵活性，数据的粒度由定义主题的 SQL 决定，而且数据的过滤与预处理由 TDengine 自动完成，从而减少传输的数据量并降低应用程序的复杂度。

消费者订阅主题后，可以实时接收最新的数据。多个消费者可以组成一个消费组，共享消费进度，实现多线程、分布式地消费数据，提高消费速度。不同消费组的消费者即使消费同一个主题，也不共享消费进度。一个消费者可以订阅多个主题。如果主题对应的是超级表或库，数据可能会分布在多个不同的节点或数据分片上。当一个消费组中有多个消费者时，可以提高消费效率。TDengine 的消息队列提供了消息的 ACK（Acknowledgment，确认，也译作收到）机制，确保在宕机、重启等复杂环境下实现至少一次（at least once）消费。

为实现上述功能，TDengine 会为预写数据日志（Write-Ahead Logging，WAL）文件自动创建索引，以支持快速随机访问，并提供了灵活可配置的文件切换与保留机制。用户可以根据需求指定 WAL 文件的保留时间和大小。通过这些方法，WAL 被改造成一个保留事件到达顺序的、可持久化的存储引擎。对于以主题形式创建的查询，TDengine 将从 WAL 读取数据。在消费过程中，TDengine 根据当前消费进度从 WAL 直接读取数据，并使用统一的查询引擎实现过滤、变换等操作，然后将数据推送给消费者。

从 3.2.0.0 版本开始，数据订阅支持 vnode 迁移和分裂。由于数据订阅依赖 wal 文件，而在 vnode 迁移和分裂的过程中，wal 文件并不会进行同步。因此，在迁移或分裂操作完成后，您将无法继续消费之前尚未消费完 wal 数据。请务必在执行 vnode 迁移或分裂之前，将所有 wal 数据消费完毕。

## 主题类型

TDengine 使用 SQL 创建的主题共有 3 种类型，下面分别介绍。

### 查询主题

订阅一条 SQL 查询的结果，本质上是连续查询，每次查询仅返回最新值，创建语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name as subquery
```

该 SQL 通过 SELECT 语句订阅（包括 SELECT *，或 SELECT ts, c1 等指定查询订阅，可以带条件过滤、标量函数计算，但不支持聚合函数、不支持时间窗口聚合）。需要注意的是：

1. 该类型 TOPIC 一旦创建则订阅数据的结构确定。
2. 被订阅或用于计算的列或标签不可被删除（ALTER table DROP）、修改（ALTER table MODIFY）。
3. 若发生表结构变更，新增的列不出现在结果中。
4. 对于 select *，则订阅展开为创建时所有的列（子表、普通表为数据列，超级表为数据列加标签列）

假设需要订阅所有智能电表中电压值大于 200 的数据，且仅仅返回时间戳、电流、电压 3 个采集量（不返回相位），那么可以通过下面的 SQL 创建 power_topic 这个主题。

```sql
CREATE TOPIC power_topic AS SELECT ts, current, voltage FROM power.meters WHERE voltage > 200;
```

### 超级表主题

订阅一个超级表中的所有数据，语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS STABLE stb_name [where_condition]
```

与使用 `SELECT * from stbName` 订阅的区别是：

1. 不会限制用户的表结构变更，即表结构变更以及变更后的新数据都能够订阅到。
2. 返回的是非结构化的数据，返回数据的结构会随着超级表的表结构变化而变化。
3. with meta 参数可选，选择时将返回创建超级表，子表等语句，主要用于 taosX 做超级表迁移。
4. where_condition 参数可选，选择时将用来过滤符合条件的子表，订阅这些子表。where 条件里不能有普通列，只能是 tag 或 tbname，where 条件里可以用函数，用来过滤 tag，但是不能是聚合函数，因为子表 tag 值无法做聚合。可以是常量表达式，比如 2 > 1（订阅全部子表），或者 false（订阅 0 个子表）。
5. 返回数据不包含标签。

### 数据库主题

订阅一个数据库里所有数据，其语法如下：

```sql
CREATE TOPIC [IF NOT EXISTS] topic_name [with meta] AS DATABASE db_name;
```

通过该语句可创建一个包含数据库所有表数据的订阅：

1. with meta 参数可选，选择时将返回数据库里所有超级表，子表、普通表的元数据创建、删除、修改语句，主要用于 taosX 做数据库迁移。
2. 超级表订阅和库订阅属于高级订阅模式，容易出错，如确实要使用，请咨询技术支持人员。

## 删除主题

如果不再需要订阅数据，可以删除 topic，如果当前 topic 被消费者订阅，通过 FORCE 语法可强制删除，强制删除后订阅的消费者会消费数据会出错（FORCE 语法从 v3.3.6.0 开始支持）。

```sql
DROP TOPIC [IF EXISTS] [FORCE] topic_name;
```

## 查看主题

```sql
SHOW TOPICS;
```

上面的 SQL 会显示当前数据库下的所有主题的信息。

## 消费者

### 创建消费者

消费者的创建只能通过 TDengine 客户端驱动或者连接器所提供的 API 创建，详情可以参考开发指南或参考手册。

### 查看消费者

```sql
SHOW CONSUMERS;
```

显示当前数据库下所有消费者的信息，会显示消费者的状态，创建时间等信息。

### 删除消费组

消费者创建的时候，会给消费者指定一个消费者组，消费者不能显式的删除，但是可以删除消费者组。如果当前消费者组里有消费者在消费，通过 FORCE 语法可强制删除，强制删除后订阅的消费者会消费数据会出错（FORCE 语法从 v3.3.6.0 开始支持）。

```sql
DROP CONSUMER GROUP [IF EXISTS] [FORCE] cgroup_name ON topic_name;
```

## 数据订阅

### 查看订阅信息

```sql
SHOW SUBSCRIPTIONS;
```

显示 topic 在不同 vgroup 上的消费信息，可用于查看消费进度。

### 订阅数据

TDengine 提供了全面且丰富的数据订阅 API，旨在满足不同编程语言和框架下的数据订阅需求。这些接口包括但不限于创建消费者、订阅主题、取消订阅、获取实时数据、提交消费进度以及获取和设置消费进度等功能。目前，TDengine 支持多种主流编程语言，包括 C、Java、Go、Rust、Python 和 C# 等，使得开发者能够轻松地在各种应用场景中使用 TDengine 的数据订阅功能。

值得一提的是，TDengine 的数据订阅 API 与业界流行的 Kafka 订阅 API 保持了高度的一致性，以便于开发者能够快速上手并利用现有的知识经验。为了方便用户了解和参考，TDengine 的官方文档详细介绍了各种 API 的使用方法和示例代码，具体内容可访问 TDengine 官方网站的连接器部分。通过这些 API，开发者可以高效地实现数据的实时订阅和处理，从而满足各种复杂场景下的数据处理需求。

TDengine v3.3.7.0 版本提供了 MQTT 订阅功能，可以通过 MQTT 客户端直接订阅数据，具体内容请参考 MQTT 数据订阅部分。

### 回放功能

TDengine 的数据订阅功能支持回放（replay）功能，允许用户按照数据的实际写入时间顺序重新播放数据流。这一功能基于 TDengine 的高效 WAL 机制实现，确保了数据的一致性和可靠性。

要使用数据订阅的回放功能，用户可以在查询语句中指定时间范围，从而精确控制回放的起始时间和结束时间。这使得用户能够轻松地重放特定时间段内的数据，无论是为了故障排查、数据分析还是其他目的。

如果写入了如下 3 条数据，那么回放时则先返回第 1 条数据，5s 后返回第 2 条数据，在获取第 2 条数据 3s 后返回第 3 条数据。

```text
2023/09/22 00:00:00.000
2023/09/22 00:00:05.000
2023/09/22 00:00:08.000
```

使用数据订阅的回放功能时需要注意如下几项：

- 通过配置消费参数 enable.replay 为 true 开启回放功能。

- 数据订阅的回放功能仅查询订阅支持数据回放，超级表和库订阅不支持回放。

- 回放不支持进度保存。

- 因为数据回放本身需要处理时间，所以回放的精度存在几十毫秒的误差。

  

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
