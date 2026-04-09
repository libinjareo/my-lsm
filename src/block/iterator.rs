use crate::block::{Block, SIZEOF_U16};
use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use std::sync::Arc;

///  block 迭代器
/// 每个*条目*在 `Block.data` 中的布局如下（**相对于 Block 的第一个 key 进行前缀压缩**）：
/// ```
/// | 字段              | 类型  | 字节数     | 说明                                                    |
/// | ------------------- | ------- | ------------ | --------------------------------------------------------- |
/// | `overlap_len` | u16   | 2          | 当前 key 与 **Block 的第一个 key** 共享的前缀长度 |
/// | `key_len`     | u16   | 2          | 当前 key 的**不共享部分**的长度                   |
/// | `key`         | bytes | key_len   | 当前 key 的不共享部分（unique suffix）                  |
/// | `timestamp`   | u64   | 8          | 该 key 的时间戳（用于 MVCC）                            |
/// | `value_len`   | u16   | 2          | value 的长度                                            |
/// | `value`       | bytes | value_len | value 的实际内容
/// ```                                     |
/// 利用 “相对于第一个 key 的前缀压缩” 格式，高效恢复任意位置的完整 key。
/// 提供高效的顺序扫描和二分定位能力，是 LSM 树 SSTable 扫描的基础组件。
/// 前缀压缩：相对于第一个 key，使得随机访问时仅需 first_key 即可恢复完整 key。
pub struct BlockIterator {
    block: Arc<Block>,
    key: KeyVec,
    value_range: (usize, usize),
    idx: usize,
    fist_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16() as usize;
        let key = &buf[..key_len];
        buf.advance(key_len);
        KeyVec::from_vec_with_ts(key.to_vec(), buf.get_u64())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            fist_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// 创建迭代器并定位到第一个条目
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// 创建迭代器并定位到第一个 >= key 的条目
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// 获取当前条目的key
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    // 获取当前条目的value
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// 判断迭代器是否有效
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    // 跳转到第一个条目
    fn seek_to_first(&mut self) {
        self.seek_to(0);
    }
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// 移动到下一个条目
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];

        // 1. 读取  overlap_len ，表示当前 key 与 first_key 共享的字节数
        let overlap_len = entry.get_u16() as usize;
        // 读取不共享部分长度
        let key_len = entry.get_u16() as usize;

        // 2. 读取 key 的不共享部分
        let key = &entry[..key_len];

        // 3. 恢复完整 key
        self.key.clear();
        self.key.append(&self.fist_key.key_ref()[..overlap_len]);
        self.key.append(key);

        // 4. 移动游标越过 key 部分
        entry.advance(key_len);

        // 5. 读取时间戳
        let ts = entry.get_u64();
        self.key.set_ts(ts);

        // 6. 读取 value_len
        let value_len = entry.get_u16() as usize;

        // 7. 计算 value 在 block.data 中的绝对偏移范围
        let value_offset_begin = offset
            + SIZEOF_U16 // overlap_len
            + SIZEOF_U16  // key_len
            + std::mem::size_of::<u64>()  // timestap
            + key_len
            + SIZEOF_U16; // value_len
        // 由于 entry 的游标已经移动，直接使用偏移量计算更清晰
        // value起始 = offset + 所有头部字段总长度
        let value_offset_end = value_offset_begin + value_len;

        // 存储起止索引，供 value() 方法使用
        self.value_range = (value_offset_begin, value_offset_end);

        // 移动游标到下一个条目开头
        entry.advance(value_len);
    }

    /// 二分查找定位到第一个 >= key的条目
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low: usize = 0;
        let mut high: usize = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
}