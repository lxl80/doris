# Doris 异步物化视图（MTMV）冷热分层存储修复方案

## 一、背景与现状
### 1.1 业务诉求

在 SSD + HDD 混合存储的 Doris 集群中，让**异步物化视图（MTMV）** 的新建分区跟随表级 `storage_medium` 属性分配到对应介质：
- `PROPERTIES ("storage_medium" = "SSD")` → 分区落 SSD 后端
- `PROPERTIES ("storage_medium" = "HDD")` → 分区落 HDD 后端

预期两个场景都生效：
1. **MV 创建时**：初始对齐基表的分区应继承表级 storage_medium
2. **MV REFRESH 时**：基表新增分区后，MV 对齐新建的分区应继承表级 storage_medium

### 1.2 实际现象（修复前）

| 场景 | 用户预期 | 实际结果 |
|------|---------|---------|
| CREATE MV 时初始分区 | SSD | 100% HDD |
| REFRESH COMPLETE 后新增分区 | SSD | 100% HDD |
| `SHOW CREATE` 显示表级属性 | SSD | SSD（无误） |
| `SHOW DATA` / `SHOW TABLETS` 的 `ReplicaStorageMedium` | SSD | 全部 HDD |

### 1.3 核心概念速查（给没读过 Doris 源码的读者）

本方案会反复提到以下 Doris FE（Java 后端）中的类、字段与方法。先用业务语言把它们定位清楚，后续分析就不会被类名/方法名卡住。**遇到看不懂的术语可随时回查本表。**

**表与分区相关类**：

| 术语 | 业务定位 | 在本方案中的角色 |
|------|---------|----------------|
| **MTMV（异步物化视图）** | Doris 的一种物化视图，底层实际就是一张 OlapTable，支持异步 REFRESH 刷新数据 | 本方案要修复的对象 |
| **OlapTable** | Doris 中所有 OLAP 引擎表（普通表、MV 都算）的基类，持有表级元数据 | MTMV 的父类，提供 `getStorageMedium()` 等接口 |
| **TableProperty** | OlapTable 内部存储"建表时用户传入的 PROPERTIES"的容器，保留原始 key-value | `tableProperty.getProperties()` 是本方案唯一可信的 `storage_medium` 数据源 |
| **DataProperty** | 分区级的存储属性对象，含 `storage_medium`、`cooldown_time`、`storage_policy` 等 | 每个分区独立持有一个 DataProperty |
| **SinglePartitionDesc** | 单个分区的描述对象，`analyze()` 负责把"分区属性 + 表级属性"合并成最终 DataProperty | 本方案注入点的下游消费者 |
| **InternalCatalog** | Doris FE 的"内部目录"，是建表/加分区等 DDL 的总入口 | `addPartition()` 是 REFRESH 新建分区的最终落地点 |
| **PropertyAnalyzer** | 把用户传入的 PROPERTIES 字符串 map 解析成结构化 DataProperty 的工具类 | `analyzeDataProperty()` 决定分区最终介质 |
| **SystemInfoService** | FE 中管理所有 BE 节点信息的组件，负责为新建副本挑选 BE | `selectBackendIdsForReplicaCreation()` 决定副本落到哪个介质 |

**关键字段/常量**：

| 术语 | 业务含义 |
|------|---------|
| **`storage_medium`** | 存储介质属性（HDD/SSD），用户在 `PROPERTIES(...)` 中指定 |
| **`storageMediumSpecified`** | 运行时标志位，记录"用户是否显式写了 `storage_medium`"。`true` = 强制介质（选不到就报错），`false` = 建议介质（选不到就静默回退到另一种） |
| **`mvProperties`** | MTMV 专有属性 map，只放 MV 特定 key（如 `grace_period`），**不含 `storage_medium`** |
| **`MV_PROPERTY_KEYS`** | 定义"哪些 key 属于 MTMV 专有属性"的白名单常量，`storage_medium` 不在其中 |
| **`PROPERTIES_STORAGE_MEDIUM`** | `PropertyAnalyzer` 中 `"storage_medium"` 字符串的常量名 |
| **继承白名单** | `InternalCatalog.addPartition()` 中硬编码的"表级属性 → 分区属性"自动继承列表（约 20 个 key），**`storage_medium` 不在其中** |

**关键方法**：

| 方法 | 业务功能 |
|------|---------|
| `OlapTable.getStorageMedium()` | 读取表级 storage_medium。建表时 `InternalCatalog.createOlapTable` 的两个分支都会调用 `setStorageMedium()`，`buildStorageMedium()` 从 `properties` map 重建字段 |
| `MTMVPartitionUtil.addPartition()` | REFRESH 时为 MV 新建对齐分区的工具方法，**方案中辅助修复的注入点** |
| `PropertyAnalyzer.analyzeDataProperty()` | 从 properties map 解析出 DataProperty，并设置 `storageMediumSpecified` 标志 |
| `SystemInfoService.selectBackendIdsForReplicaCreation()` | 为新建副本挑选 BE，根据"介质 + 是否强制指定"决定回退策略 |
| `InternalCatalog.addPartition()` | 加分区的 DDL 入口，含"表级属性 → 分区属性"的继承白名单 |

---

## 二、修复方案
### 2.1 根因概述

MTMV REFRESH 时新建分区始终走默认 HDD，根因是 **REFRESH 实际不走 `MTMVPartitionUtil.addPartition()`，而是走 `InsertOverwriteUtil.addTempPartitions()` → `addPartitionLike()` → `InternalCatalog.addPartition()` 路径**。`InternalCatalog.addPartition()` 中有 ~15 个表级→分区级属性继承白名单（`compaction_policy`、`storage_policy`、`tde_algorithm` 等），但 `storage_medium` **不在其中**（社区 PR #41192 曾存在后被删除），因此新分区始终拿到默认 HDD。

本方案包含两层修复：
1. **主修复**：在 `InternalCatalog.addPartition()` 的属性继承白名单中恢复 `storage_medium`，覆盖所有 ADD PARTITION 路径（含 INSERT OVERWRITE / 动态分区 / ALTER TABLE）
2. **辅助修复**：在 `MTMVPartitionUtil.addPartition()` 和 `getPartitionDescsByRelatedTable()` 中主动注入 `storage_medium`，覆盖 MTMV 分区对齐路径和 CREATE 路径，提供防御性纵深

> 关于 `DataProperty.storageMediumSpecified` 标志位不持久化的独立社区问题，见 [§四、扩展说明](#四扩展说明storagemediumspecified-持久化问题)。

### 2.2 根因分析

#### 2.2.1 直接根因：继承白名单缺失 `storage_medium`

Doris 的 `InternalCatalog.addPartition()`（所有 ADD PARTITION 的最终入口）在创建新分区时，有一个硬编码的属性继承循环，将表级 ~20 个属性自动继承到分区（`replication_allocation`、`storage_policy`、`compaction_policy`、`tde_algorithm` 等）。**但 `storage_medium` 不在这个继承白名单中**（社区 PR #41192 曾存在后被删除）。

因此 REFRESH 时无论走哪条路径创建分区，`properties` 中都没有 `storage_medium`，`analyzeDataProperty` 只能走默认值 HDD。

#### 2.2.2 路径根因：REFRESH 不走预期路径

MTMV REFRESH 的实际执行路径是：

```
MTMVTask.runTask()
  → alignMvPartition → MTMVPartitionUtil.addPartition()   ← 预期路径（辅助修复在此）
    【分区数匹配时跳过此段】
  → INSERT OVERWRITE                                        ← 实际主路径
    → InsertOverwriteUtil.addTempPartitions()
      → addPartitionLike(已有分区)                         ← 从已有分区拷贝属性
        → InternalCatalog.addPartition()                   ← 汇聚到同一入口
          → 继承白名单缺 storage_medium → HDD
```

`addPartitionLike` 通过 `toPartitionDesc()` 获取源分区属性，但 `toPartitionDesc()` **只输出 `STORAGE POLICY`**，从来不输出 `storage_medium`。因此无论 REFRESH 多少次，新临时分区始终没有 `storage_medium`，必须靠继承白名单补充。

#### 2.2.3 完整失败链路

```
REFRESH → INSERT OVERWRITE → addPartitionLike()
  → toPartitionDesc() → properties = {}  (不含 storage_medium)
  → InternalCatalog.addPartition()
    → 属性继承循环（~20 个属性，不含 storage_medium）
    → properties 仍无 storage_medium
    → analyzeDataProperty({}, HDD)
    → DataProperty(HDD, isStorageMediumSpecified=false)
    → isStorageMediumSpecified=false → SSD 不足可静默回退 HDD
    → 分区落 HDD
```

### 2.3 核心思路

**用业务语言说**：Doris 加分区时，会自动从表级继承大部分属性（如 `replication_num`、`storage_policy`），但 `storage_medium` 被排除在继承白名单之外。REFRESH 时 MV 通过 INSERT OVERWRITE 建临时分区，走的正是这条"有继承白名单但缺 storage_medium"的通用路径。修复方案就是在该继承白名单中加上 `storage_medium`，同时保留 MTMV 独有路径的防御性注入。

**技术实现**（两层）：
1. **`InternalCatalog.addPartition()` 继承块**：在属性继承白名单末尾新增 `storage_medium`，从 `olapTable.getStorageMedium()` 读取值。这是修复所有 ADD PARTITION 路径（包括 INSERT OVERWRITE）的根本方案。
2. **`MTMVPartitionUtil.addPartition()` 和 `getPartitionDescsByRelatedTable()` 主动注入**：从 `tableProperty.getProperties()` 读取 `storage_medium`，直接注入 `partitionProperties`。作为 MTMV 路径的防御性保护。

> 两层修复互不依赖、互不冲突：即使其中一层未生效，另一层也能兜底。

### 2.4 修复方案总览

本方案包含两处代码修改，分别对应不同路径：

| 修复层级 | 文件 | 路径覆盖 | 性质 |
|---------|------|---------|------|
| **主修复** | `InternalCatalog.addPartition()` | 所有 ADD PARTITION（INSERT OVERWRITE / 动态分区 / ALTER TABLE ADD PARTITION） | **恢复表级继承白名单** |
| **辅助修复** | `MTMVPartitionUtil.addPartition()` + `getPartitionDescsByRelatedTable()` | MTMV 分区对齐 + CREATE MTMV | **MTMV 路径主动注入** |

#### 2.4.1 主修复：`InternalCatalog.addPartition()` 继承白名单恢复

##### 2.4.1.1 改动清单

文件：[`fe/fe-core/src/main/java/org/apache/doris/datasource/InternalCatalog.java`](../../fe/fe-core/src/main/java/org/apache/doris/datasource/InternalCatalog.java)

| 位置 | 改动 |
|------|------|
| `addPartition` 方法，属性继承块末尾（约 L1737） | 新增 `storage_medium` 继承：如果分区未显式指定且表级有值，则从 `olapTable.getStorageMedium()` 继承 |

##### 2.4.1.2 关键代码

```java
// InternalCatalog.java: ~L1737
// 在现有 ~15 个表级→分区级属性继承之后添加：
if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)
        && olapTable.getStorageMedium() != null) {
    properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
            olapTable.getStorageMedium().name());
    LOG.warn("[storage_medium] addPartition inherit: partitionName={}, inherited storage_medium={}",
            partitionName, olapTable.getStorageMedium().name());
}
```

##### 2.4.1.3 设计要点

1. **putIfAbsent 语义**：`!containsKey` 确保分区显式指定的 `storage_medium` 优先级高于表级继承值
2. **null 安全**：`olapTable.getStorageMedium() != null` 防止表级未设介质时写入 null
3. **仅填充空位**：不覆盖已显式指定的值，不改变已有分区的介质（`addPartitionLike` 拷贝的源分区属性中的 `storage_medium` 会优先保留）
4. **isStorageMediumSpecified=true**：继承后的 `storage_medium` 会被 `analyzeDataProperty` 标记为"显式指定"（`isStorageMediumSpecified=true`），让 `selectBackendIdsForReplicaCreation` 按强制介质策略选 BE

> 关于 `isStorageMediumSpecified=true` 对 BE 选择的影响（SSD 不足时抛错 vs 静默回退），见 [§三、影响分析](#三影响分析)。简言之：这是 pre-#41192 的已有行为，社区 PR #41192 删除后本次恢复，属于**回归修复**而非新引入的行为变化。

#### 2.4.2 辅助修复：`MTMVPartitionUtil` 主动注入

##### 2.4.2.1 改动清单

文件：[`fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVPartitionUtil.java`](../../fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVPartitionUtil.java)

| 位置 | 改动 |
|------|------|
| `addPartition` 方法 | 新增注入逻辑：从 `mtmv.getTableProperty().getProperties()` 读取 `storage_medium`，若非空则注入 `partitionProperties` |
| `getPartitionDescsByRelatedTable` 方法 | 新增相同注入逻辑（覆盖 CREATE 路径） |
| import | 增加 `com.google.common.base.Strings`、移除未使用的 `DataProperty`、`TStorageMedium` |

##### 2.4.2.2 关键代码

在 `addPartition` 中，从 `tableProperty.getProperties()` 读取 `storage_medium`（这是建表时用户传入的原始 properties，`storage_medium` 不在 `MV_PROPERTY_KEYS` 中，从未被移出），注入到 `partitionProperties`：

```java
// addPartition
Map<String, String> partitionProperties = Maps.newHashMap();
if (mtmv.getTableProperty() != null && mtmv.getTableProperty().getProperties() != null) {
    String storageMedium = mtmv.getTableProperty().getProperties()
            .get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
    if (!Strings.isNullOrEmpty(storageMedium)) {
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium.toUpperCase());
        LOG.info("[storage_medium] addPartition mv={} injected storage_medium={}",
                mtmv.getName(), storageMedium.toUpperCase());
    } else {
        LOG.info("[storage_medium] addPartition mv={} skip, no storage_medium found", mtmv.getName());
    }
} else {
    LOG.info("[storage_medium] addPartition mv={} skip, tableProperty is null", mtmv.getName());
}
```
    ...
}
```


**`getPartitionDescsByRelatedTable`（修复后 CREATE 路径）**：

```java
public static List<AllPartitionDesc> getPartitionDescsByRelatedTable(
        Map<String, String> tableProperties, MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties)
        throws AnalysisException {
    List<AllPartitionDesc> res = Lists.newArrayList();
    HashMap<String, String> partitionProperties = Maps.newHashMap();
    
    if (tableProperties != null) {
        String storageMedium = tableProperties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
        if (!Strings.isNullOrEmpty(storageMedium)) {
            partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium.toUpperCase());
        }
    }

    Set<PartitionKeyDesc> relatedPartitionDescs = generateRelatedPartitionDescs(mvPartitionInfo, mvProperties)
            .keySet();
    ...
}
```


注：`getPartitionDescsByRelatedTable` 本身已接收 `tableProperties` 参数（含 `storage_medium`），`SinglePartitionDesc.analyze()` 会将其合并，所以在 CREATE 路径这个修复是防御性的——即使没有它，CREATE 路径的介质继承也能正常工作的。但加上的好处是：
- `partitionProperties` 中显式带了 `storage_medium`，即使未来 `SinglePartitionDesc.analyze` 的合并逻辑变化，也有冗余保护
- 代码逻辑与 `addPartition` 对称，降低维护成本

#### 2.4.2.3 设计要点（辅助修复）

1. **读取正确的数据源**：`tableProperty.getProperties()` 保留了 CREATE MV 时用户传入的完整 properties，`storage_medium` 从未被移除
2. **纯字符串传递**：直接取 map 中的字符串值，不依赖 `TStorageMedium` 枚举转换，避免 enum valueOf 异常风险
3. **大小写规范化**：`toUpperCase()` 确保值格式统一
4. **健壮性**：null map、空字符串、未知值都安全跳过
5. **isStorageMediumSpecified=true**：注入后的值被 `analyzeDataProperty` 标记为"显式指定"

#### 2.4.3 主修复设计要点

1. **putIfAbsent 语义**：`!containsKey` 确保分区显式指定的 `storage_medium` 优先级高于表级继承值
2. **null 安全**：`olapTable.getStorageMedium() != null` 防止表级未设介质时写入 null
3. **仅填充空位**：不覆盖已显式指定的值，不改变已有分区的介质
4. **恢复 pre-#41192 行为**：这段继承逻辑在 Doris 早期版本中存在，被 PR #41192 删除。本次恢复的是已有正确行为

#### 2.4.4 两层修复的覆盖范围对比

| 维度 | `InternalCatalog.addPartition()` 继承 | `MTMVPartitionUtil` 主动注入 |
|------|--------------------------------------|-----------------------------|
| **覆盖路径** | 所有 ADD PARTITION（INSERT OVERWRITE / 动态分区 / ALTER TABLE） | MTMV 分区对齐 + CREATE MTMV |
| **生效时机** | `analyze` 之前，继承白名单循环中 | `SinglePartitionDesc` 构造时 |
| **数据源** | `olapTable.getStorageMedium()` | `tableProperty.getProperties()` 原始 map |
| **优势** | 覆盖最广，一条修复解决所有路径 | 防御性纵深，MTMV 独有路径最前端注入 |
| **风险** | 影响所有 ADD PARTITION 操作（恢复 pre-#41192 行为） | 仅限 MTMV 路径，风险隔离 |

**精确时序验证**（基于 `InternalCatalog.java:1646-1738` 和 `SinglePartitionDesc.java:133-164` 源码）：

```
[MTMVPartitionUtil.addPartition]
  partitionProperties = {storage_medium=SSD}               ← 注入
  SinglePartitionDesc(true, ..., partitionProperties)
    → this.properties = {storage_medium=SSD}               ← 存入对象

[InternalCatalog.addPartition]
  1646: properties = singlePartitionDesc.getProperties()
    → {storage_medium=SSD}                                 ← 继承自 this.properties

  1667-1736: 属性继承循环
    properties.putIfAbsent(replication_allocation, ...)    ← 不涉及 storage_medium
    properties.putIfAbsent(storage_policy, ...)            ← 不涉及 storage_medium
    ... (~20 个 putIfAbsent，不含 storage_medium)
    → properties 仍含 {storage_medium=SSD, ...}            ← 注入值穿越继承循环 ✓

  1738: singlePartitionDesc.analyze(cols, properties)
    → [SinglePartitionDesc.analyze]
        mergedMap = new HashMap()
        mergedMap.putAll(otherProperties)                  ← otherProperties = {..., SSD}
        mergedMap.putAll(this.properties)                  ← this.properties 覆盖（与之前相同）
        this.properties = mergedMap                        ← {..., storage_medium=SSD}

      analyzeDataProperty(this.properties, HDD)
        → 读到 storage_medium=SSD
        → storageMedium=SSD
        → isStorageMediumSpecified=true                   ✓
```

两次穿越（继承循环、analyze merge）中 `storage_medium=SSD` 都正确保留，最终被 `analyzeDataProperty` 使用。

### 2.5 修复后端到端路径

先用业务语言把修复后 REFRESH 一次的完整流程走一遍：

1. 用户执行 `REFRESH MATERIALIZED VIEW`，FE 后台任务启动
2. `MTMVTask.runTask()` 先执行 `alignMvPartition` 对齐分区（如果基表有新增分区，此处创建缺失分区——辅助修复在这条路径生效）
3. **核心路径**：然后执行 INSERT OVERWRITE，为每个目标分区创建临时分区（`addTempPartitions` → `addPartitionLike`）
4. `addPartitionLike` 从已有分区拷贝属性到新的临时分区——如果已有分区没有 `storage_medium`，临时分区也没有
5. 进入 `InternalCatalog.addPartition()`，属性继承白名单循环**新增的 `storage_medium` 继承**生效：因为临时分区的 `properties` 中没有 `storage_medium`，自动从 `olapTable.getStorageMedium()` 继承 SSD
6. `SinglePartitionDesc.analyze()` 合并属性，`analyzeDataProperty()` 读到 `storage_medium=SSD`，标记 `isStorageMediumSpecified=true`
7. `createPartitionWithIndices(SSD, true)` → 临时分区落 SSD ✓
8. INSERT OVERWRITE 完成后，临时分区替换正式分区 → 所有分区变 SSD ✓

下面用源码级传递轨迹展示修复后的两层保障：

```
REFRESH 触发 → MTMVTask.runTask()

┌─ PATH X: alignMvPartition → 新增分区（基表有新增分区时触发）────┐
│                                                                  │
│  MTMVPartitionUtil.addPartition(mtmv, partitionKeyDesc)         │
│    → partitionProperties = {storage_medium=SSD}  ← 辅助修复     │
│    → SinglePartitionDesc(true, name, desc, {SSD})               │
│    → InternalCatalog.addPartition                                │
│      → 继承白名单 + analyze 后 → DataProperty(SSD) ✓            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

┌─ PATH Y: INSERT OVERWRITE → addTempPartitions（REFRESH 主路径）──┐
│                                                                  │
│  InsertOverwriteUtil.addTempPartitions(table, partNames, temp)  │
│    → for each partition:                                         │
│      addPartitionLike(db, table, AddPartitionLikeClause(temp,   │
│                                     existingPartition, true))   │
│        → SinglePartitionDesc(existingPartition.properties)       │
│          ← 已有分区 properties 可能不含 storage_medium            │
│                                                                  │
│  InternalCatalog.addPartition                                    │
│    → properties = singlePartitionDesc.getProperties()            │
│      ← {}(不含 storage_medium)                                   │
│                                                                  │
│    ┌─ 继承白名单循环 ──────────────────────────────────────────┐ │
│    │  properties.putIfAbsent(replication_allocation, ...)      │ │
│    │  properties.putIfAbsent(storage_policy, ...)              │ │
│    │  ...                                                      │ │
│    │  ✅ 新增: storage_medium 继承 ← 主修复                    │ │
│    │    properties.putIfAbsent("storage_medium", "SSD")        │ │
│    │    → properties = {..., storage_medium=SSD} ✓             │ │
│    └───────────────────────────────────────────────────────────┘ │
│                                                                  │
│    singlePartitionDesc.analyze(cols, {..., storage_medium=SSD}) │
│      → mergedMap = {..., storage_medium=SSD}                    │
│      → analyzeDataProperty({SSD,...}, HDD) → DataProperty(SSD) │
│      → isStorageMediumSpecified=true                             │
│                                                                  │
│    createPartitionWithIndices(..., SSD, true)                    │
│      → selectBackendIdsForReplicaCreation(SSD, true)             │
│      → 分配到 SSD 后端 ✓                                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

  InsertOverwriteUtil.replacePartition(table, partitions, tempPartitions)
    → 临时分区替换正式分区 → 所有分区 SSD ✓
```

---

## 三、影响分析

| 影响点 | 说明 |
|--------|------|
| **SSD 不足时的行为** | 修复前：`isStorageMediumSpecified=false` → SSD 不足时静默回退到 HDD。修复后：`isStorageMediumSpecified=true` → SSD 不足时报错（**恢复 pre-#41192 的正确行为**，用户显式要求 SSD，基础设施不足应暴露而非掩盖） |
| **非 MTMV 表** | `InternalCatalog.addPartition()` 继承新增 `storage_medium`，所有分区表 ADD PARTITION（含动态分区）都可自动继承表级介质。**回归修复** |
| **已有 MTMV** | 无需重建。部署后一次 REGULAR REFRESH 自动生效，临时分区通过继承拿到 SSD → 替换正式分区 |
| **回滚** | 无新增类/接口/Thrift 定义，替换 jar + 重启即可。已创建的 SSD 分区保持不降级 |
| **FE 重启后副本重建** | `storageMediumSpecified` 无 `@SerializedName`，重启后为 false（独立问题，见 §四）。不影响本方案修复效果 |

---

## 四、扩展说明：`storageMediumSpecified` 持久化问题

> 本节是独立于主方案的背景说明，不影响本方案修复。读者可按需阅读。

### 4.1 问题描述

**业务背景**：Doris FE 把表/分区的元数据存在一个叫"image"的文件里，重启时从 image 加载。image 的读写用 Gson 序列化，**只有标注了 `@SerializedName` 的字段才会被写入 image**。没标注的字段重启后会丢失，回到默认值。

`DataProperty.storageMediumSpecified` 是一个运行时标志位，记录"用户是否显式指定了 `storage_medium`"。它决定 `selectBackendIdsForReplicaCreation` 在选不到指定介质 BE 时的行为：

- `isStorageMediumSpecified=true`（用户显式指定）→ 选不到指定介质时抛 `DdlException`，不回退
- `isStorageMediumSpecified=false`（走默认值）→ 选不到指定介质时静默回退到另一种介质

该字段在 `DataProperty.java` 中**没有 `@SerializedName` 注解**，Gson 序列化时会丢弃。因此 FE 重启后，已有分区的 `storageMediumSpecified` 永远变回 `false`，"强制介质"契约被悄悄改成"建议介质"。

### 4.2 与本方案的关系

**此问题与本方案要修复的 MTMV 问题相互独立**：

- 本方案修复的是"REFRESH 新建分区不继承 `storage_medium`"，根因是数据源读取错误
- `storageMediumSpecified` 持久化问题影响的是"FE 重启后已有分区在副本重建时的介质回退行为"，只在 SSD 不足且触发副本重建时才显现

本方案修复后，REFRESH 新建分区在运行时会被设为 `storageMediumSpecified=true`，**不依赖** `storageMediumSpecified` 的持久化能力。因此本方案不包含此问题的修复。

### 4.3 社区进展

社区 PR #63011（`[feature](catalog) support medium allocation mode`，作者 `Ryan19929`）针对此问题提出了 `boolean` → `MediumAllocationMode` enum + `@SerializedName` 的改造方案。截至 2026-07-07，该 PR 为 **Open 状态、尚未合并**（目标 `apache:master`，3 位 CODEOWNERS 均未 approve）。

若你的集群存在"SSD 不足 + 频繁触发副本重建 + 关心重启后介质一致性"的场景，可关注该 PR 进展，待社区合入后评估是否 cherry-pick。否则可忽略此问题。

---

## 五、验证步骤与排查
### 5.1 编译验证

```bash
mvn -pl fe/fe-core -am compile -DskipTests \
    -Dcheckstyle.skip=true -Dmaven.javadoc.skip=true -Dlicense.skip=true
```

### 5.2 关键核查清单

- [ ] `MTMVPartitionUtil.java` 添加 `import com.google.common.base.Strings;`
- [ ] `applyStorageMediumFromTableProperties` 从 `tableProperty.getProperties()` 而非 `mvProperties` 读取
- [ ] `DataProperty.DEFAULT_STORAGE_MEDIUM` 为 `TStorageMedium.HDD`
- [ ] 设 `storage_medium=HDD` 时不注入（与默认值一致，避免冗余操作）
- [ ] `addPartition` 和 `getPartitionDescsByRelatedTable` 都调用了新工具方法

---

### 5.3 前置准备

1. 备份元数据
2. 确认集群 SSD 后端数量 ≥ `replication_num`（默认 3）

### 5.4 最小验证用例

```sql
-- 1. 创建基表
CREATE DATABASE IF NOT EXISTS test_mtmv;
CREATE TABLE test_mtmv.base_tb (
    id INT,
    dt DATE
)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

ALTER TABLE test_mtmv.base_tb ADD PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01'));
ALTER TABLE test_mtmv.base_tb ADD PARTITION p2 VALUES [('2026-02-01'), ('2026-03-01'));

-- 2. 创建指定 SSD 的 MTMV
CREATE MATERIALIZED VIEW test_mtmv.test_mv
BUILD IMMEDIATE REFRESH COMPLETE
PARTITION BY dt
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("storage_medium" = "SSD")
AS SELECT id, dt FROM test_mtmv.base_tb;

-- 验证初始分区介质
SELECT PARTITION_NAME, STORAGE_MEDIUM
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA='test_mtmv' AND TABLE_NAME='test_mv';
-- 期望：全部 SSD

-- 3. REFRESH 后新增分区
ALTER TABLE test_mtmv.base_tb ADD PARTITION p3 VALUES [('2026-03-01'), ('2026-04-01'));
REFRESH MATERIALIZED VIEW test_mtmv.test_mv COMPLETE;

-- 验证新增分区介质
SELECT PARTITION_NAME, STORAGE_MEDIUM
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA='test_mtmv' AND TABLE_NAME='test_mv';
-- 期望：p1, p2, p3 全部 SSD

-- 4. 验证 HDD 显式指定
CREATE MATERIALIZED VIEW test_mtmv.test_mv_hdd
BUILD IMMEDIATE REFRESH COMPLETE
PARTITION BY dt
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("storage_medium" = "HDD")
AS SELECT id, dt FROM test_mtmv.base_tb;
-- 期望：全部 HDD

-- 5. 验证不指定时默认 HDD
CREATE MATERIALIZED VIEW test_mtmv.test_mv_default
BUILD IMMEDIATE REFRESH COMPLETE
PARTITION BY dt
DISTRIBUTED BY HASH(id) BUCKETS 8
AS SELECT id, dt FROM test_mtmv.base_tb;
-- 期望：全部 HDD
```


### 5.5 回退预案

```bash
# 替换为 v1/原版 jar 即可
cp ${FE_HOME}/lib/doris-fe.jar.bak.origin ${FE_HOME}/lib/doris-fe.jar
# 重启 FE
```

---

### 5.6 日志参考

代码中保留了以下 `[storage_medium]` 日志用于线上巡检：

| 日志 | 位置 | 级别 | 含义 |
|------|------|------|------|
| `addPartition mv=xxx injected/skip` | `MTMVPartitionUtil.addPartition()` | INFO | 辅助修复命中/跳过 |
| `addPartition inherit: partitionName=xxx, ...` | `InternalCatalog.addPartition()` | WARN | 主修复继承生效 |

```bash
grep "\\[storage_medium\\]" ${FE_HOME}/log/fe.log | sort -k1,2
```

## 六、结论与代码改动

本方案采用**两层修复**：

1. **主修复**（`InternalCatalog.addPartition()` 继承白名单）：恢复 `storage_medium` 的表级→分区级继承，覆盖所有 ADD PARTITION 路径（INSERT OVERWRITE / 动态分区 / ALTER TABLE）。这是修复实际 REFRESH 路径（`addPartitionLike → addPartition`）的根本方案。
2. **辅助修复**（`MTMVPartitionUtil` 主动注入）：在 MTMV 独有路径（分区对齐 `addPartition` + CREATE `getPartitionDescsByRelatedTable`）中注入 `storage_medium`，提供防御性纵深。

两层互不依赖、互不冲突，任何一层生效即可保证分区落在正确介质。

**关键发现**：首次验证（仅 MTMVPartitionUtil 注入）未生效的原因是 REFRESH 实际不走 `alignMvPartition → addPartition`，而是走 `INSERT OVERWRITE → addPartitionLike → InternalCatalog.addPartition()`。加上继承白名单修复后，所有路径均被覆盖。

## 七、变更摘要

| 文件 | 主要改动 |
|------|---------|
| `fe/fe-core/src/main/java/org/apache/doris/datasource/InternalCatalog.java` | 在 `addPartition()` 继承白名单中恢复 `storage_medium` 表级→分区级继承（恢复 pre-#41192 行为），新增 `addPartition inherit` 调试日志 |
| `fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVPartitionUtil.java` | 在 `addPartition()` 和 `getPartitionDescsByRelatedTable()` 中从 `tableProperty.getProperties()` 读取并注入 `storage_medium`，新增注入/跳过调试日志 |

> 本文档对应 commit 包含两层修复 + 全链路 `[storage_medium]` 调试日志
> 文档变更日期：2026-07-07

---
