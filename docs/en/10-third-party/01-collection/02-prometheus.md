---
title: Prometheus 
slug: /third-party-tools/data-collection/prometheus
---

import Prometheus from "../../assets/resources/_prometheus.mdx"

Prometheus is a popular open-source monitoring and alerting system. In 2016, Prometheus joined the Cloud Native Computing Foundation (CNCF), becoming the second hosted project after Kubernetes. The project has a very active developer and user community.

Prometheus provides `remote_write` and `remote_read` interfaces to utilize other database products as its storage engine. To enable users in the Prometheus ecosystem to leverage TDengine's efficient writing and querying capabilities, TDengine also supports these two interfaces.

With proper configuration, Prometheus data can be stored in TDengine via the `remote_write` interface and queried via the `remote_read` interface, fully utilizing TDengine's efficient storage and querying performance for time-series data and cluster processing capabilities.

## Prerequisites

The following preparations are needed to write Prometheus data into TDengine:

- TDengine cluster is deployed and running normally
- taosAdapter is installed and running normally. For details, please refer to [taosAdapter user manual](../../../tdengine-reference/components/taosadapter)
- Prometheus is installed. For installation of Prometheus, please refer to the [official documentation](https://prometheus.io/docs/prometheus/latest/installation/)

## Configuration Steps

<Prometheus />

## Verification Method

After restarting Prometheus, you can refer to the following example to verify that data is written from Prometheus to TDengine and can be correctly read.

### Querying Written Data Using TDengine CLI

```text
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 prometheus_data                |
Query OK, 3 row(s) in set (0.000585s)

taos> use prometheus_data;
Database changed.

taos> show stables;
              name              |
=================================
 metrics                        |
Query OK, 1 row(s) in set (0.000487s)

taos> select * from metrics limit 10;
              ts               |           value           |             labels             |
=============================================================================================
 2022-04-20 07:21:09.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:09.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
Query OK, 10 row(s) in set (0.011146s)
```

### Using promql-cli to Read Data from TDengine via remote_read

Install promql-cli

```shell
go install github.com/nalbury/promql-cli@latest
```

Query Prometheus data while TDengine and taosAdapter services are running

```text
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
JOB           VALUE    TIMESTAMP
prometheus    1        2022-04-20T08:05:26Z
node          1        2022-04-20T08:05:26Z
```

Query Prometheus data after pausing the taosAdapter service

```text
ubuntu@shuduo-1804 ~ $ sudo systemctl stop taosadapter.service
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
VALUE    TIMESTAMP
```

:::note

- The default subtable names generated by TDengine are unique IDs based on a set of rules.

:::