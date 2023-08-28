---
sidebar_label: 用户管理
title: 用户管理
description: 使用用户管理来管理组织、用户、用户组、角色、权限和资源。
---

TDengine Cloud 提供了一个简单而易用的用户管理，包括用户、用户组、角色、权限和资源。TDengine Cloud 默认提供了4个级别的权限和8种默认角色，其中包括组织级别、实例级别、数据库级别和权限管理级别，默认的角色是数据库管理员、账户、数据生产者、数据消费者、开发人员、实例管理员、超级管理员和组织所有者。 您还可以用 TDengine Cloud 已经定义的权限创建自定义角色。

组织所有者或超级管理员可以邀请任何其他用户进入组织。而且，他还可以给用户、用户组分配特定的角色，包括实例和数据库等指定资源。在组织级别、实例级别或数据库级别，TDengine 数据所有者通过分配角色（包括数据库读权限）可以很方便得与他人分享数据。并且，您还可以很容易地更新这些权限或删除它们。比如，您可以非常容易为整个实例授予开发人员的权限，以迅速给您的内部团队以充分的访问权，并授予特定相关人员对特定数据库资源的有限访问权。

本节的主要内容包括用户可以向选定的组织添加新的用户、用户组、角色，也可以给指定的用户分配一些资源的角色，如实例或数据库。您还可以编辑或删除指定的用户、用户组、角色或权限。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
