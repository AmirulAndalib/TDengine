---
title: 建立连接
sidebar_label: 建立连接
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import ConnJava from "./_connect_java.mdx";
import ConnGo from "./_connect_go.mdx";
import ConnRust from "./_connect_rust.mdx";
import ConnNode from "./_connect_node.mdx";
import ConnPythonNative from "./_connect_python.mdx";
import ConnCSNative from "./_connect_cs.mdx";
import ConnC from "./_connect_c.mdx";
import ConnR from "./_connect_r.mdx";
import ConnPHP from "./_connect_php.mdx";
import InstallOnLinux from "../../14-reference/05-connector/_linux_install.mdx";
import InstallOnWindows from "../../14-reference/05-connector/_windows_install.mdx";
import InstallOnMacOS from "../../14-reference/05-connector/_macos_install.mdx";
import VerifyLinux from "../../14-reference/05-connector/_verify_linux.mdx";
import VerifyMacOS from "../../14-reference/05-connector/_verify_macos.mdx";
import VerifyWindows from "../../14-reference/05-connector/_verify_windows.mdx";

TDengine 提供了丰富的应用程序开发接口，为了便于用户快速开发自己的应用，TDengine 支持了多种编程语言的连接器，其中官方连接器包括支持 C/C++、Java、Python、Go、Node.js、C#、Rust、Lua（社区贡献）和 PHP （社区贡献）的连接器。这些连接器支持使用原生接口（taosc）和 REST 接口（部分语言暂不支持）连接 TDengine 集群。社区开发者也贡献了多个非官方连接器，例如 ADO.NET 连接器、Lua 连接器和 PHP 连接器。

## 连接方式

连接器建立连接的方式，TDengine 提供三种:

1. 通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接，这种连接方式下文中简称 “原生连接”。
2. 通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接，这种连接方式下文中简称 “REST 连接”
3. 通过 taosAdapter 组件提供的 Websocket API 建立与 taosd 的连接，这种连接方式下文中简称 “Websocket 连接”

![TDengine connection type](connection-type-zh.webp)

无论使用何种方式建立连接，连接器都提供了相同或相似的 API 操作数据库，都可以执行 SQL 语句，只是初始化连接的方式稍有不同，用户在使用上不会感到什么差别。

关键不同点在于：

1. 使用 原生连接，需要保证客户端的驱动程序 taosc 和服务端的 TDengine 版本配套。
2. 使用 REST 连接，用户无需安装客户端驱动程序 taosc，具有跨平台易用的优势，但是无法体验数据订阅和二进制数据类型等功能。另外与 原生连接 和 Websocket 连接相比，REST连接的性能最低。REST 接口是无状态的。在使用 REST 连接时，需要在 SQL 中指定表、超级表的数据库名称。  
3. 使用 Websocket 连接，用户也无需安装客户端驱动程序 taosc。
4. 连接云服务实例，必须使用 REST 连接 或 Websocket 连接。

一般我们建议使用 **Websocket 连接**。

## 安装客户端驱动 taosc

如果选择原生连接，而且应用程序不在 TDengine 同一台服务器上运行，你需要先安装客户端驱动，否则可以跳过此一步。为避免客户端驱动和服务端不兼容，请使用一致的版本。

### 安装步骤

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <InstallOnLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <InstallOnWindows />
  </TabItem>
  <TabItem value="macos" label="macOS">
    <InstallOnMacOS />
  </TabItem>
</Tabs>

### 安装验证

以上安装和配置完成后，并确认 TDengine 服务已经正常启动运行，此时可以执行安装包里带有的 TDengine 命令行程序 taos 进行登录。

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <VerifyLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <VerifyWindows />
  </TabItem>
  <TabItem value="macos" label="macOS">
    <VerifyMacOS />
  </TabItem>
</Tabs>

## 安装连接器

<Tabs groupId="lang">
<TabItem label="Java" value="java">

如果使用 Maven 管理项目，只需在 pom.xml 中加入以下依赖。

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.3.0</version>
</dependency>
```

</TabItem>
<TabItem label="Python" value="python">

使用 `pip` 从 PyPI 安装:

```
pip install taospy
```

从 Git URL 安装：

```
pip install git+https://github.com/taosdata/taos-connector-python.git
```

</TabItem>
<TabItem label="Go" value="go">

编辑 `go.mod` 添加 `driver-go` 依赖即可。

```go-mod title=go.mod
module goexample

go 1.17

require github.com/taosdata/driver-go/v3 latest
```

:::note
driver-go 使用 cgo 封装了 taosc 的 API。cgo 需要使用 GCC 编译 C 的源码。因此需要确保你的系统上有 GCC。

:::

</TabItem>
<TabItem label="Rust" value="rust">

编辑 `Cargo.toml` 添加 `taos` 依赖即可。

```toml title=Cargo.toml
[dependencies]
taos = { version = "*"}
```

:::info
Rust 连接器通过不同的特性区分不同的连接方式。默认同时支持原生连接和 Websocket 连接，如果仅需要建立 Websocket 连接，可设置 `ws` 特性：

```toml
taos = { version = "*", default-features = false, features = ["ws"] }
```

:::

</TabItem>
<TabItem label="Node.js" value="node">

Node.js 连接器通过不同的包提供不同的连接方式。

1. 安装 Node.js 原生连接器

```
npm install @tdengine/client
```

:::note
推荐 Node 版本大于等于 `node-v12.8.0` 小于 `node-v13.0.0`
:::

2. 安装 Node.js REST 连接器

```
npm install @tdengine/rest
```

</TabItem>
<TabItem label="C#" value="csharp">

编辑项目配置文件中添加 [TDengine.Connector](https://www.nuget.org/packages/TDengine.Connector/) 的引用即可：

```xml title=csharp.csproj {12}
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <StartupObject>TDengineExample.AsyncQueryExample</StartupObject>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.1.0" />
  </ItemGroup>

</Project>
```

也可通过 dotnet 命令添加：

```
dotnet add package TDengine.Connector
```

:::note
以下示例代码，均基于 dotnet6.0，如果使用其它版本，可能需要做适当调整。

:::

</TabItem>
<TabItem label="R" value="r">

1. 下载 [taos-jdbcdriver-version-dist.jar](https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/3.0.0/)。
2. 安装 R 的依赖包`RJDBC`：

```R
install.packages("RJDBC")
```

</TabItem>
<TabItem label="C" value="c">

如果已经安装了 TDengine 服务端软件或 TDengine 客户端驱动 taosc， 那么已经安装了 C 连接器，无需额外操作。
<br/>

</TabItem>
<TabItem label="PHP" value="php">

**下载代码并解压：**

```shell
curl -L -o php-tdengine.tar.gz https://github.com/Yurunsoft/php-tdengine/archive/refs/tags/v1.0.2.tar.gz \
&& mkdir php-tdengine \
&& tar -xzf php-tdengine.tar.gz -C php-tdengine --strip-components=1
```

> 版本 `v1.0.2` 只是示例，可替换为任意更新的版本，可在 [TDengine PHP Connector 发布历史](https://github.com/Yurunsoft/php-tdengine/releases) 中查看可用版本。

**非 Swoole 环境：**

```shell
phpize && ./configure && make -j && make install
```

**手动指定 TDengine 目录：**

```shell
phpize && ./configure --with-tdengine-dir=/usr/local/Cellar/tdengine/3.0.0.0 && make -j && make install
```

> `--with-tdengine-dir=` 后跟上 TDengine 目录。
> 适用于默认找不到的情况，或者 macOS 系统用户。

**Swoole 环境：**

```shell
phpize && ./configure --enable-swoole && make -j && make install
```

**启用扩展：**

方法一：在 `php.ini` 中加入 `extension=tdengine`

方法二：运行带参数 `php -d extension=tdengine test.php`

</TabItem>
</Tabs>

## 建立连接

在执行这一步之前，请确保有一个正在运行的，且可以访问到的 TDengine，而且服务端的 FQDN 配置正确。以下示例代码，都假设 TDengine 安装在本机，且 FQDN（默认 localhost） 和 serverPort（默认 6030） 都使用默认配置。
### 连接参数
连接的配置项较多，因此在建立连接之前，我们能先介绍一下各语言连接器建立连接使用的参数。

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">

Java 连接器建立连接的参数有 URL 和 properties，下面分别详细介绍。  
TDengine 的 JDBC URL 规范格式为：
`jdbc:[TAOS|TAOS-RS]://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`  
对于建立连接，原生连接与 REST 连接有细微不同。  
**注**：REST 连接中增加 `batchfetch` 参数并设置为 true，将开启 WebSocket 连接。   


**注意**：使用 JDBC 原生连接，taos-jdbcdriver 需要依赖客户端驱动（Linux 下是 libtaos.so；Windows 下是 taos.dll；macOS 下是 libtaos.dylib）。

url 中的配置参数如下：  
- user：登录 TDengine 用户名，默认值 'root'。
- password：用户登录密码，默认值 'taosdata'。
- batchfetch: true：在执行查询时批量拉取结果集；false：逐行拉取结果集。默认值为：false。逐行拉取结果集使用 HTTP 方式进行数据传输。JDBC REST 连接支持批量拉取数据功能。taos-jdbcdriver 与 TDengine 之间通过 WebSocket 连接进行数据传输。相较于 HTTP，WebSocket 可以使 JDBC REST 连接支持大数据量查询，并提升查询性能。
- charset: 当开启批量拉取数据时，指定解析字符串数据的字符集。
- batchErrorIgnore：true：在执行 Statement 的 executeBatch 时，如果中间有一条 SQL 执行失败，继续执行下面的 SQL 了。false：不再执行失败 SQL 后的任何语句。默认值为：false。
- httpConnectTimeout: 连接超时时间，单位 ms， 默认值为 60000。
- httpSocketTimeout: socket 超时时间，单位 ms，默认值为 60000。仅在 batchfetch 设置为 false 时生效。
- messageWaitTimeout: 消息超时时间, 单位 ms， 默认值为 60000。 仅在 batchfetch 设置为 true 时生效。
- useSSL: 连接中是否使用 SSL。
- httpPoolSize: REST 并发请求大小，默认 20。  


**注意**：部分配置项（比如：locale、timezone）在 REST 连接中不生效。  


除了通过指定的 URL 获取连接，还可以使用 Properties 指定建立连接时的参数。
properties 中的配置参数如下：
- TSDBDriver.PROPERTY_KEY_USER：登录 TDengine 用户名，默认值 'root'。
- TSDBDriver.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 'taosdata'。
- TSDBDriver.PROPERTY_KEY_BATCH_LOAD: true：在执行查询时批量拉取结果集；false：逐行拉取结果集。默认值为：false。
- TSDBDriver.PROPERTY_KEY_BATCH_ERROR_IGNORE：true：在执行 Statement 的 executeBatch 时，如果中间有一条 SQL 执行失败，继续执行下面的 sq 了。false：不再执行失败 SQL 后的任何语句。默认值为：false。
- TSDBDriver.PROPERTY_KEY_CONFIG_DIR：仅在使用 JDBC 原生连接时生效。客户端配置文件目录路径，Linux OS 上默认值 `/etc/taos`，Windows OS 上默认值 `C:/TDengine/cfg`。
- TSDBDriver.PROPERTY_KEY_CHARSET：客户端使用的字符集，默认值为系统字符集。
- TSDBDriver.PROPERTY_KEY_LOCALE：仅在使用 JDBC 原生连接时生效。 客户端语言环境，默认值系统当前 locale。
- TSDBDriver.PROPERTY_KEY_TIME_ZONE：仅在使用 JDBC 原生连接时生效。 客户端使用的时区，默认值为系统当前时区。因为历史的原因，我们只支持POSIX标准的部分规范，如UTC-8(代表中国上上海), GMT-8，Asia/Shanghai 这几种形式。
- TSDBDriver.HTTP_CONNECT_TIMEOUT: 连接超时时间，单位 ms， 默认值为 60000。仅在 REST 连接时生效。
- TSDBDriver.HTTP_SOCKET_TIMEOUT: socket 超时时间，单位 ms，默认值为 60000。仅在 REST 连接且 batchfetch 设置为 false 时生效。
- TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: 消息超时时间, 单位 ms， 默认值为 60000。 仅在 REST 连接且 batchfetch 设置为 true 时生效。
- TSDBDriver.PROPERTY_KEY_USE_SSL: 连接中是否使用 SSL。仅在 REST 连接时生效。
- TSDBDriver.HTTP_POOL_SIZE: REST 并发请求大小，默认 20。
- TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION: 传输过程是否启用压缩。仅在使用 REST/Websocket 连接时生效。true: 启用，false: 不启用。默认为 false。
- TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: 是否启用自动重连。仅在使用 Websocket 连接时生效。true: 启用，false: 不启用。默认为 false。
> **注意**：启用自动重连仅对简单执行 SQL 语句以及 无模式写入、数据订阅有效。对于参数绑定无效。自动重连仅对连接建立时通过参数指定数据库有效，对后面的 `use db` 语句切换数据库无效。

- TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS: 自动重连重试间隔，单位毫秒，默认值 2000。仅在 PROPERTY_KEY_ENABLE_AUTO_RECONNECT 为 true 时生效。
- TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT: 自动重连重试次数，默认值 3，仅在 PROPERTY_KEY_ENABLE_AUTO_RECONNECT 为 true 时生效。
- TSDBDriver.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION: 关闭 SSL 证书验证 。仅在使用 Websocket 连接时生效。true: 启用，false: 不启用。默认为 false。

**配置参数的优先级：**  

通过前面三种方式获取连接，如果配置参数在 url、Properties、客户端配置文件中有重复，则参数的**优先级由高到低**分别如下：

1. JDBC URL 参数，如上所述，可以在 JDBC URL 的参数中指定。
2. Properties connProps
3. 使用原生连接时，TDengine 客户端驱动的配置文件 taos.cfg

例如：在 url 中指定了 password 为 taosdata，在 Properties 中指定了 password 为 taosdemo，那么，JDBC 会使用 url 中的 password 建立连接。

    </TabItem>
    <TabItem label="Python" value="python">
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
使用客户端驱动访问 TDengine 集群的基本过程为：建立连接、查询和写入、关闭连接、清除资源。

下面为建立连接的示例代码，其中省略了查询和写入部分，展示了如何建立连接、关闭连接以及清除资源。

```c
  TAOS *taos = taos_connect("localhost:6030", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    exit(1);
  }

  /* put your code here for read and write */

  taos_close(taos);
  taos_cleanup();
```

在上面的示例代码中， `taos_connect()` 建立到客户端程序所在主机的 6030 端口的连接，`taos_close()`关闭当前连接，`taos_cleanup()`清除客户端驱动所申请和使用的资源。

:::note

- 如未特别说明，当 API 的返回值是整数时，_0_ 代表成功，其它是代表失败原因的错误码，当返回值是指针时， _NULL_ 表示失败。
- 所有的错误码以及对应的原因描述在 `taoserror.h` 文件中。

:::



    </TabItem>
    <TabItem label="PHP" value="php">
    </TabItem>

</Tabs>

### Websocket 连接
各语言连接器建立 Websocket 连接代码样例。 

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/WSConnectExample.java:main}}
```
    </TabItem>
    <TabItem label="Python" value="python">

        ```python
        {{#include docs/examples/python/connect_websocket_examples.py:connect}}
        ```
    </TabItem>
    <TabItem label="Go" value="go">
        <ConnGo />
    </TabItem>
    <TabItem label="Rust" value="rust">
        <ConnRust />
    </TabItem>
    <TabItem label="Node.js" value="node">
        ```js
            {{#include docs/examples/node/websocketexample/sql_example.js:createConnect}}
        ```
    </TabItem>
    <TabItem label="C#" value="csharp">
        <ConnCSNative />
    </TabItem>
    <TabItem label="R" value="r">
        <ConnR/>
    </TabItem>
    <TabItem label="C" value="c">
        <ConnC />
    </TabItem>
    <TabItem label="PHP" value="php">
        <ConnPHP />
    </TabItem>
</Tabs>

### 原生连接
各语言连接器建立原生连接代码样例。   
<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/JNIConnectExample.java:main}}
```
    </TabItem>
    <TabItem label="Python" value="python">
        <ConnPythonNative />
    </TabItem>
    <TabItem label="Go" value="go">
        <ConnGo />
    </TabItem>
    <TabItem label="Rust" value="rust">
        <ConnRust />
    </TabItem>
    <TabItem label="C#" value="csharp">
        <ConnCSNative />
    </TabItem>
    <TabItem label="R" value="r">
        <ConnR/>
    </TabItem>
    <TabItem label="C" value="c">
        <ConnC />
    </TabItem>
    <TabItem label="PHP" value="php">
        <ConnPHP />
    </TabItem>

</Tabs>

### REST 连接
各语言连接器建立 REST 连接代码样例。  
<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/RESTConnectExample.java:main}}
```
    </TabItem>
    <TabItem label="Python" value="python">
        <ConnPythonNative />
    </TabItem>
    <TabItem label="Go" value="go">
        <ConnGo />
    </TabItem>
    <TabItem label="Rust" value="rust">
        <ConnRust />
    </TabItem>
    <TabItem label="C#" value="csharp">
        <ConnCSNative />
    </TabItem>
    <TabItem label="R" value="r">
        <ConnR/>
    </TabItem>
    <TabItem label="C" value="c">
        <ConnC />
    </TabItem>
    <TabItem label="PHP" value="php">
        <ConnPHP />
    </TabItem>

</Tabs>


:::tip
如果建立连接失败，大部分情况下是 FQDN 或防火墙的配置不正确，详细的排查方法请看[《常见问题及反馈》](https://docs.taosdata.com/train-faq/faq)中的“遇到错误 Unable to establish connection, 我怎么办？”

:::


## 连接池
有些连接器提供了连接池，或者可以与已有的连接池组件配合使用。 使用连接池，应用程序可以快速地从连接池中获取可用连接，避免了每次操作时创建和销毁连接的开销。这不仅减少了资源消耗，还提高了响应速度。此外，连接池还支持对连接的管理，如最大连接数限制、连接的有效性检查，确保了连接的高效和可靠使用。我们**推荐使用连接池管理连接**。  
下面是各语言连接器的连接池支持代码样例。  

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">

**HikariCP**  

使用示例如下：

```java
{{#include examples/JDBC/connectionPools/src/main/java/com/taosdata/example/HikariDemo.java:connection_pool}}
```

> 通过 HikariDataSource.getConnection() 获取连接后，使用完成后需要调用 close() 方法，实际上它并不会关闭连接，只是放回连接池中。
> 更多 HikariCP 使用问题请查看[官方说明](https://github.com/brettwooldridge/HikariCP)。

**Druid**  

使用示例如下：

```java
{{#include examples/JDBC/connectionPools/src/main/java/com/taosdata/example/DruidDemo.java:connection_pool}}
```

> 更多 druid 使用问题请查看[官方说明](https://github.com/alibaba/druid)。

    </TabItem>
    <TabItem label="Python" value="python">
        <ConnPythonNative />
    </TabItem>
    <TabItem label="Go" value="go">
        <ConnGo />
    </TabItem>
    <TabItem label="Rust" value="rust">
        <ConnRust />
    </TabItem>
    <TabItem label="C#" value="csharp">
        <ConnCSNative />
    </TabItem>
    <TabItem label="R" value="r">
        <ConnR/>
    </TabItem>
    <TabItem label="C" value="c">
        <ConnC />
    </TabItem>
    <TabItem label="PHP" value="php">
        <ConnPHP />
    </TabItem>

</Tabs>
