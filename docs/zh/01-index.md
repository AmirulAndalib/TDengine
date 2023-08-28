---
title: TDengine Cloud 文档
sidebar_label: 主页
slug: /
---

TDengine Cloud 是全托管的时序数据处理云服务平台。它是基于开源的时序数据库 TDengine 而开发的。除高性能的时序数据库之外，它还具有缓存、订阅和流计算等系统功能，而且提供了便利而又安全的数据分享、以及众多的企业级服务功能。它可以让物联网、工业互联网、金融、IT 运维监控等领域企业在时序数据的管理上大幅降低人力成本和运营成本。

同时客户可以放心使用无处不在的第三方工具，比如 Prometheus，Telegraf，Grafana 和 MQTT 消息服务器等。天然地，TDengine Cloud 还支持 Python，Java，Go，Rust 和 Node.js等连接器。开发者可以选择自己习惯的语言来开发。通过支持 SQL，还有无模式的方式，TDengine Cloud 能够满足所有开发者的需求。TDengine Cloud 还提供了额外的特殊功能来进行时序数据的风险，使数据的分析和可视化变得极其简单。

下面是 TDengine Cloud 的文档结构：

1. [产品简介](./intro) 概述TDengine Cloud 的特点，能力和竞争优势。

2. [基本概念](./concept) 主要介绍 TDengine 如何有效利用时间序列数据的特点来提高计算性能，同时提高存储效率。

3. [数据写入](./data-in) 主要介绍 TDengine Cloud 提供了多种数据写入 TDengine 实例的方式。在数据源部分，您可以方便地从边缘云或者主机上面的 TDengine 把数据写入云上的任何实例。

4. [开发指南](./programming) 是使用 TDengine Cloud 上的时序数据开发 IoT 和大数据应用必须阅读的部分。在这一部分中，我们详细介绍了数据库连接，数据建模，数据抽取，数据查询，流式计算，缓存，数据订阅，用户自定义函数和其他功能。我们还提供了各种编程语言的示例代码。在大多数情况下，您只需简单地复制和粘贴这些示例代码，在您的应用程序中再做一些细微修改就能工作。
5. [流式计算](./stream) 这个部分也是 TDengine Cloud 的另外一个高级功能。通过这个功能，您无需无需部署任何流式处理系统，比如 Spark/Flink，就能创建连续查询或时间驱动的流计算。 TDengine Cloud 的流式计算可以很方便的让您实时处理进入的流式数据并把它们很轻松地按照您定义的规则导入到目的表里面。

6. [数据订阅](./data-subscription) 这个部分是 TDengine Cloud 的高级功能，类似于异步发布/订阅能力，即发布到一个主题的消息会被该主题的所有订阅者立即收到通知。 TDengine Cloud 的数据订阅让您无需部署任何的消息发布/订阅系统，比如 Kafka，就可以创建自己的事件驱动应用。而且我们提供了便捷而安全的方式，让您通过创建主题和分享主题给他人都变得极其容易。

7. [工具](./tools) TDengine Cloud 提供的数据输出，数据可视化以及数据访问等工具。通过这些工具，用户可以轻松和便捷地访问您在 TDengine Cloud 的 TDengine 实例的数据库数据并进行各种查询。另外还介绍了 taosBenchmark 这个工具。通过这个工具可以帮助您用简单的配置就能比较容易地产生大量的数据，并测试 TDengine Cloud 的性能。

8. [实例管理](./instance-mgmt) 这部分是 TDengine Cloud 提供的管理 TDengine 实例的各种资源。

9. [用户管理](./user-mgmt) 这部分是 TDengine Cloud 提供的管理 TDengine 实例的各种资源。

10. [组织管理](./orgs) 这部分是 TDengine Cloud 提供的管理 TDengine 实例的各种资源。

11. [数据库集市](./dbmarts) 这部分是 TDengine Cloud 提供的管理 TDengine 实例的各种资源。

12. [TDengine SQL](./taos-sql) 提供了标准 SQL 以及TDengine 扩展部分的详细介绍，通过这些 SQL 语句能方便地进行时序数据分析。

我们非常高兴您选择 TDengine Cloud 作为您的时序数据平台的一部分，并期待着听到您的反馈以及改进意见，并成为您成功的一个小部分。
