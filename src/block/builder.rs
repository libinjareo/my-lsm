use crate::block::{Block, SIZEOF_U16};
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

/// BlockBuilder 负责将键值对按照前缀压缩格式写入内存缓冲区，同时记录偏移量。
/// 前缀压缩基于第一个 key，使得后续编码简单且支持随机访问。
/// 构建的 Block 需要进一步调用 encode() 才能得到最终可写入磁盘的字节序列。
pub struct BlockBuilder {
    /// 记录每个条目的起始位置（相对于 data 的字节偏移），类型为 u16 限制单个 Block 最大 64 Kib
    offsets: Vec<u16>,
    /// 依次存放所有条目的编码数据（与 Block::data 格式一致）
    data: Vec<u8>,
    /// 目标块大小上限。当估计的编码后大小超过该值时，不再添加新条目
    block_size: usize,
    /// 存储已添加的第一个完整Key（用于后续条目的前缀压缩计算）
    first_key: KeyVec,
}

/// 计算两个key的公共前缀长度（按照字节比较）
/// 逻辑：从第一个字节开始逐个比较，直到遇到不想等或任一Key结束，返回匹配的字节数
/// 示例：
/// ```
/// first_key = b"apple", key = b"application" → 公共前缀 "appl" → 返回 4
/// first_key = b"apple", key = b"banana" → 第一个字节就不同 → 返回 0
/// ```
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i: usize = 0;
    loop {
        if i >= first_key.key_len() || i >= key.key_len() {
            break;
        }
        if first_key.key_ref()[i] != key.key_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// 创建构建器
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// 估算值，正是最终 Block::encode()的大小（数据区 + 偏移量数组 + 2字节条目数）
    /// add 方法会使用此估计值判断是否超出 block_size
    fn estimated_size(&self) -> usize {
        SIZEOF_U16 // 最后要写入的条目数量（u16）
            + self.offsets.len() * SIZEOF_U16 // 偏移量数组总字节
            + self.data.len() // 数据区已写入字节
    }

    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        // 检查是否超过块大小限制（且当前块非空时允许至少有一条记录）
        // estimated_size 加上新条目产生的额外字节（key.raw_len 包括key内容+时间戳，value.len(),以及
        // 编码开销：3 * SIZEOF_U16 分别对应 overlap_len,key_len,value_len ）.
        // 注意这里与最终编码略有冗余(实际编码时还会写入value_len等)，但足以保守判断
        if self.estimated_size() + key.raw_len() + value.len() + SIZEOF_U16 * 3 > self.block_size && !self.is_empty() {
            return false; // 块已满，添加失败
        }

        //1. 偏移量记录：是当前条目开始写入 前 的位置，即该条目的起始偏移，注意此时self.data.还没有添加新的数据
        self.offsets.push(self.data.len() as u16);

        // 2. 前缀压缩：计算当前 key 与 first_key 的公共前缀长度
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);

        // 3. 编码 overlap_len(u16)
        self.data.put_u16(overlap as u16);

        // 4. 编码 key 的不共享部分长度（u16）
        self.data.put_u16((key.key_len() - overlap) as u16);

        // 5. 编码 key 的不共享部分内容
        self.data.put(&key.key_ref()[overlap..]);

        // 6. 编码时间戳（u64）
        self.data.put_u64(key.ts());

        // 7.编码value长度（u16）
        self.data.put_u16(value.len() as u16);

        // 8. 编码 value 内容
        self.data.put(value);

        // 9. 如果是第一个添加的条目，则记录 first_key，之后不再改变
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// 消费 BlockBuilder，返回包含 data 和 offsets 的 Block 结构体
    /// 注意：此时尚未添加条目数量和偏移量数组尾部，这些由 Block::encode() 完成
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block must not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}