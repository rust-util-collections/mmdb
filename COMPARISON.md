# mmdb vs RocksDB / Pebble Feature Comparison

## Iterator / Range 方向

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| MergingIterator (heap-based) | Yes | Yes | Yes | O(N log K) 归并 |
| 方向切换优化 (in-heap tracking) | Yes | Yes | Yes | 仅 re-seek 离堆 source |
| TrySeekUsingNext | Yes | Yes | Yes | seek 目标 >= 当前位置时步进替代全量 seek |
| NextPrefix 跳转 | No | Yes | Yes | O(log N) 前缀间跳转 |
| SeekGEWithLimit / IterAtLimit | No | Yes | No | 软限制，分布式分片扫描用 |
| LazyValue / 延迟值加载 | Partial (BlobDB) | Yes | Yes | SST block 内零拷贝，skip 路径无 value 分配 |
| Iterator 对象池 | Yes | Yes | Partial | 提供 reset() 接口，未集成全局池 |
| SetBounds 传播到子迭代器 | Yes | Yes | Yes | upper_bound 传播到 TableIterator/LevelIterator |
| SkipPoint 回调 | No | Yes | Yes | ReadOptions.skip_point 回调过滤 |
| 迭代器错误传播 | Yes | Yes | Yes | DBIterator::error() 暴露 I/O 错误 |

## Range Tombstone

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| Range Deletion (RANGEDEL) | Yes | Yes | Yes | 独立 SST block 存储 |
| FragmentedRangeTombstone O(log T) | Yes | Yes | Yes | 预分片 + 二分查找 |
| 跨 Level Tombstone 剪枝 | Yes | Yes | Yes | 浅层 tombstone 不误删深层 key |
| Point Query tombstone O(log T) | Yes | Yes | Yes | 替代原 O(T) 线性扫描 |
| Range Key (RANGEKEYSET/UNSET) | No | Yes | No | CRDB 专用，通用 KV 不需要 |

## Memtable

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| SkipList (lock-free) | Yes | Yes | Yes | 单写多读，arena 分配 |
| backward O(1) (prev 指针) | Yes | Yes | Yes | level-0 双向链表 + 缓存 tail |
| 多种 MemTable 实现 | Yes (HashSkipList, Vector) | No | No | SkipList 足够 |

## SST / Block

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| BlockPropertyFilter | Yes | Yes | Yes | 用户自定义 block 级属性收集 + 过滤 |
| Bloom Filter (whole-key) | Yes | Yes | Yes | 可配置 bits_per_key |
| Prefix Bloom Filter | Yes | Yes | Yes | prefix_len 配置 |
| Block 压缩 (LZ4/Zstd) | Yes | Yes | Yes | 逐级可配 |
| BlockCache (LRU) | Yes | Yes | Yes | moka 并发缓存 + L0 pin |
| Value Block 分离 | Partial (BlobDB/Titan) | Yes | No | LazyValue 实现 block 内零拷贝 |

## Compaction

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| Leveled Compaction | Yes | Yes | Yes | 核心策略 |
| Sequence Number 清零 | Yes | Yes | Yes | bottommost + snapshot 感知 |
| Range Tombstone GC | Yes | Yes | Yes | bottommost 丢弃 tombstone |
| Compaction Filter | Yes | Yes | Yes | Keep/Remove/ChangeValue |
| Read-Triggered Compaction | Yes | Yes | Yes | 采样热点 key，hint 驱动 |
| Sub-compaction 并行 | Yes | Yes | No | 单线程 compaction |
| CompactionIter 快照边界感知 | Yes | Yes | Partial | 清零逻辑，无多版本保留 |
| Tiered/Universal Compaction | Yes | No | No | 仅 Leveled |

## Write Path

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| WAL (Write-Ahead Log) | Yes | Yes | Yes | group commit |
| WriteBatch | Yes | Yes | Yes | 原子批量写入 |
| WriteBatchWithIndex (batch 迭代) | Yes | Yes | Yes | 未提交写入可迭代 |
| Pipeline Write | Yes | No | No | group commit 替代 |
| Rate Limiter | Yes | Yes | Yes | compaction 写入限速 |

## Read Path

| Feature | RocksDB | Pebble | mmdb | 备注 |
|---------|---------|--------|------|------|
| Point Get (多级查找) | Yes | Yes | Yes | memtable → L0 → L1+ |
| SuperVersion (lock-free 读) | Yes | Yes | Yes | Arc<RwLock<SuperVersion>> |
| Prefix Iterator | Yes | Yes | Yes | bloom 过滤 + 前缀停止 |
| Bidirectional Iterator | Yes | Yes | Yes | 延迟流式，forward/backward |
| Parallel init_heap I/O | No | Partial (goroutine) | Yes | std::thread::scope 并行 peek |

## 主要差距总结

| 类别 | 缺失项 | 影响 |
|------|--------|------|
| 功能 | Range Key (RANGEKEYSET/RANGEUNSET) | CRDB 专用，通用场景无需 |
| 功能 | SeekGEWithLimit (软限制) | 分布式分片扫描需要 |
| 性能 | Value Block 完整分离 | 大值场景（>4KB）进一步优化 |
| 性能 | Sub-compaction 并行 | 大 compaction 任务可拆分 |
| 性能 | Tiered/Universal Compaction | 写密集场景替代策略 |
| 性能 | Iterator 全局对象池 | 高频短迭代避免分配 |
