---
title: 数据订阅
sidebar_label: 数据订阅
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine 提供了类似于消息队列产品的数据订阅和消费接口。在许多场景中，采用 TDengine 的时序大数据平台，无须再集成消息队列产品，从而简化应用程序设计并降低运维成本。本章介绍各语言连接器数据订阅的相关 API 以及使用方法。 数据订阅的基础知识请参考 [数据订阅](../../advanced/subscription/)  

## 创建主题
请用 taos shell 或者 参考 [执行 SQL](../sql/) 章节用程序执行创建主题的 SQL：`CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters`  

上述 SQL 将创建一个名为 topic_meters 的订阅。使用该订阅所获取的消息中的每条记录都由此查询语句 `SELECT ts, current, voltage, phase, groupid, location FROM meters` 所选择的列组成。

**注意**
在 TDengine 连接器实现中，对于订阅查询，有以下限制。
- 查询语句限制：订阅查询只能使用 select 语句，不支持其他类型的SQL，如 insert、update 或 delete 等。
- 原始始数据查询：订阅查询只能查询原始数据，而不能查询聚合或计算结果。
- 时间顺序限制：订阅查询只能按照时间正序查询数据。

## 创建消费者

TDengine 消费者的概念跟 Kafka 类似，消费者通过订阅主题来接收数据流。消费者可以配置多种参数，如连接方式、服务器地址、自动提交 Offset 等以适应不同的数据处理需求。有的语言连接器的消费者还支持自动重连和数据传输压缩等高级功能，以确保数据的高效和稳定接收。


### 创建参数
创建消费者的参数较多，非常灵活的支持了各种连接类型、 Offset 提交方式、压缩、重连、反序列化等特性。各语言连接器都适用的通用基础配置项如下表所示：

|         参数名称          |  类型   | 参数说明                                                                                                                      | 备注                                                                                                                                                                |
| :-----------------------: | :-----: | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|      `td.connect.ip`      | string  | 服务端的 IP 地址                                                                                                              |                                                                                                                                                                     |
|     `td.connect.user`     | string  | 用户名                                                                                                                        |                                                                                                                                                                     |
|     `td.connect.pass`     | string  | 密码                                                                                                                          |                                                                                                                                                                     |
|     `td.connect.port`     | integer | 服务端的端口号                                                                                                                |                                                                                                                                                                     |
|        `group.id`         | string  | 消费组 ID，同一消费组共享消费进度                                                                                             | <br />**必填项**。最大长度：192。<br />每个topic最多可建立100个 consumer group                                                                                      |
|        `client.id`        | string  | 客户端 ID                                                                                                                     | 最大长度：192。                                                                                                                                                     |
|    `auto.offset.reset`    |  enum   | 消费组订阅的初始位置                                                                                                          | <br />`earliest`: default(version < 3.2.0.0);从头开始订阅; <br/>`latest`: default(version >= 3.2.0.0);仅从最新数据开始订阅; <br/>`none`: 没有提交的 offset 无法订阅 |
|   `enable.auto.commit`    | boolean | 是否启用消费位点自动提交，true: 自动提交，客户端应用无需commit；false：客户端应用需要自行commit                               | 默认值为 true                                                                                                                                                       |
| `auto.commit.interval.ms` | integer | 消费记录自动提交消费位点时间间隔，单位为毫秒                                                                                  | 默认值为 5000                                                                                                                                                       |
|   `msg.with.table.name`   | boolean | 是否允许从消息中解析表名, 不适用于列订阅（列订阅时可将 tbname 作为列写入 subquery 语句）（从3.2.0.0版本该参数废弃，恒为true） | 默认关闭                                                                                                                                                            |
|      `enable.replay`      | boolean | 是否开启数据回放功能                                                                                                          | 默认关闭                                                                                                                                                            |


下面是各语言连接器创建参数：
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
Java 连接器创建消费者的参数为 Properties， 可以设置的参数列表请参考 [消费者参数](../../reference/connector/java/#消费者)  
其他参数请参考上文通用基础配置项。


</TabItem>
<TabItem label="Python" value="python">

</TabItem>
<TabItem label="Go" value="go">

</TabItem>
<TabItem label="Rust" value="rust">
Rust 连接器创建消费者的参数为 DSN， 可以设置的参数列表请参考 [DSN](../../reference/connector/rust/#dsn)  
其他参数请参考上文通用基础配置项。

</TabItem>
<TabItem label="Node.js" value="node">

</TabItem>
<TabItem label="C#" value="csharp">

</TabItem>
<TabItem label="R" value="r">

</TabItem>
<TabItem label="C" value="c">

</TabItem>
<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

### Websocket 连接 
介绍各语言连接器使用 Websocket 连接方式创建消费者。指定连接的服务器地址，设置自动提交，从最新消息开始消费，指定 `group.id` 和 `client.id` 等信息。有的语言的连接器还支持反序列化参数。

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">


```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WsConsumerLoopFull.java:create_consumer}}
```
</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">


```rust
{{#include docs/examples/rust/nativeexample/examples/tmq.rs:create_consumer}}
```

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>


### 原生连接 
介绍各语言连接器使用原生连接方式创建消费者。指定连接的服务器地址，设置自动提交，从最新消息开始消费，指定 `group.id` 和 `client.id` 等信息。有的语言的连接器还支持反序列化参数。


<Tabs groupId="lang">
<TabItem value="java" label="Java">


```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/ConsumerLoopFull.java:create_consumer}}
```


</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

## 订阅消费数据
消费者订阅主题后，可以开始接收并处理这些主题中的消息。订阅消费数据的示例代码如下：
### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WsConsumerLoopFull.java:poll_data_code_piece}}
```

- `subscribe` 方法的参数含义为：订阅的主题列表（即名称），支持同时订阅多个主题。 
- `poll` 每次调用获取一个消息，一个消息中可能包含多个记录。
- `ResultBean` 是我们自定义的一个内部类，其字段名和数据类型与列的名称和数据类型一一对应，这样根据 `value.deserializer` 属性对应的反序列化类可以反序列化出 `ResultBean` 类型的对象。

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

### 原生连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

同 Websocket 代码样例。

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

## 指定订阅的 Offset
消费者可以指定从特定 Offset 开始读取分区中的消息，这允许消费者重读消息或跳过已处理的消息。下面展示各语言连接器如何指定订阅的 Offset。  

### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WsConsumerLoopFull.java:consumer_seek}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>

### 原生连接 
<Tabs groupId="lang">

<TabItem value="java" label="Java">
同 Websocket 代码样例。

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>
</Tabs>


## 提交 Offset
当消费者读取并处理完消息后，它可以提交 Offset，这表示消费者已经成功处理到这个 Offset 的消息。Offset 提交可以是自动的（根据配置定期提交）或手动的（应用程序控制何时提交）。   
当创建消费者时，属性 `enable.auto.commit` 为 false 时，可以手动提交 offset。  
### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">


```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WsConsumerLoopFull.java:commit_code_piece}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>

</Tabs>

### 原生连接 
<Tabs groupId="lang">

<TabItem value="java" label="Java">

同 Websocket 代码样例。

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>

</Tabs>


## 取消订阅和关闭消费
消费者可以取消对主题的订阅，停止接收消息。当消费者不再需要时，应该关闭消费者实例，以释放资源和断开与 TDengine 服务器的连接。  

### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WsConsumerLoopFull.java:unsubscribe_data_code_piece}}
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>

</Tabs>

### 原生连接 
<Tabs groupId="lang">
<TabItem value="java" label="Java">

同 Websocket 代码样例。

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>

</Tabs>


## 完整示例
### Websocket 连接 
<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">
<details>
<summary>完整 Websocket 连接代码示例</summary> 
```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/WsConsumerLoopFull.java:consumer_demo}}
```

**注意**：这里的 value.deserializer 配置参数值应该根据测试环境的包路径做相应的调整。  
</details>

</TabItem>
<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>

</Tabs>

### 原生连接 
<Tabs groupId="lang">
<TabItem value="java" label="Java">
<details>
<summary>完整原生连接代码示例</summary> 
```java
{{#include examples/JDBC/JDBCDemo/src/main/java/com/taosdata/example/ConsumerLoopFull.java:consumer_demo}}
```

**注意**：这里的 value.deserializer 配置参数值应该根据测试环境的包路径做相应的调整。  
</details>



</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```
</TabItem>

<TabItem label="Go" value="go">
   
</TabItem>

<TabItem label="Rust" value="rust">

</TabItem>

<TabItem label="Node.js" value="node">

</TabItem>

<TabItem label="C#" value="csharp">
    
</TabItem>

<TabItem label="R" value="r">
    
</TabItem>

<TabItem label="C" value="c">
    
</TabItem>

<TabItem label="PHP" value="php">

</TabItem>

</Tabs>
