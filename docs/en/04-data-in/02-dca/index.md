---
sidebar_label: Data Collection Agents
title: Write Data Into TDengine Cloud Service
description: This document describes the methods by which you can write data into TDengine Cloud.
---

This chapter introduces a number of ways which can be used to write data into TDengine, users can use TDengine SQL to write data into TDengine cloud service, or use the [client libraries](../../programming/client-libraries/) provided by TDengine to program writing into TDengine. TDengine provides [taosBenchmark](../../tools/taosbenchmark), which is a performance testing tool to write into TDengine, and taosX, which is a tool provided by TDengine enterprise edition, to sync data from one TDengine cloud service to another. Furthermore, 3rd party tools, like telegraf and prometheus, can also be used to write data into TDengine.

:::note IMPORTANT
Because of privilege limitation on cloud, you need to firstly create database in the data explorer on cloud console before preparing to write data into TDengine cloud service. This limitation is applicable to any way of writing data.

:::
