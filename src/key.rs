use bytes::Bytes;
use std::fmt::Formatter;
use std::{cmp::Reverse, fmt::Debug};

/// 元组结构体，两个字段：T 类型（可视为字节序列）和一个 u64 时间戳。
/// 通过 AsRef<[u8]>约束，允许 T 是  Vec<u8>,&[u8]、Bytes、String 等可转换为字节切片的类型.
/// 版本化：通过 (key_bytes, timestamp) 对支持 MVCC。
pub struct Key<T: AsRef<[u8]>>(T, u64);

/// 所有权为借用，用于临时查询、写入时的参数，避免拷贝
pub type KeySlice<'a> = Key<&'a [u8]>;

/// 所有权为独占，可变构建键、测试、需要修改的场景
pub type KeyVec = Key<Vec<u8>>;

/// 所有权为共享，存储在跳表或SST中，支持克隆时共享内存
pub type KeyBytes = Key<Bytes>;

/// 用于条件编译或运行时开关
pub const TS_ENABLED: bool = true;

/// 默认时间戳 0，用于尚不支持 MVCC 的旧代码。
pub const TS_DEFAULT: u64 = 0;

// 无穷大
pub const TS_MAX: u64 = std::u64::MAX;

// 无穷小
pub const TS_MIN: u64 = std::u64::MIN;
pub const TS_RANGE_BEGIN: u64 = std::u64::MAX;
pub const TS_RANGE_END: u64 = std::u64::MIN;


/// 通用方法
impl<T: AsRef<[u8]>> Key<T> {
    //消费Key，返回内部 T
    pub fn into_inner(self) -> T {
        self.0
    }

    // 返回键内容长度
    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    // 返回键+时间戳总长度（用于编码时分配缓冲区）
    pub fn raw_len(&self) -> usize {
        self.0.as_ref().len() + std::mem::size_of::<u64>()
    }

    // 键内容是否为空
    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    // 仅测试用，获取时间戳
    pub fn for_testing_ts(self) -> u64 {
        self.1
    }
}

/// 让 KeyVec 成为一个可变的构建器：可以动态追加数据、修改时间戳、最后再转换为KeyBytes 存入持久化结构
impl Key<Vec<u8>> {
    ///创建空键，时间戳为0
    pub fn new() -> Self {
        Self(Vec::new(), TS_DEFAULT)
    }

    /// 通过时间戳构建
    pub fn from_vec_with_ts(key: Vec<u8>, ts: u64) -> Self {
        Self(key, ts)
    }

    /// 清空键内容，时间戳置 0
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// 追加字节到键末尾
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    /// 修改时间戳
    pub fn set_ts(&mut self, ts: u64) {
        self.1 = ts;
    }

    /// 从 KeySlice 复制内容（复用已分配内存）
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
        self.1 = key_slice.1;
    }

    /// 将 KeyVec 转为 KeySlice（借用）
    pub fn as_key_slice(&self) -> KeySlice {
        Key(self.0.as_slice(), self.1)
    }

    /// 转换为 KeyBytes（Vec<u8> → Bytes）
    pub fn into_key_bytes(self) -> KeyBytes {
        Key(self.0.into(), self.1)
    }

    /// 获取键内容的字节切片
    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// 获取时间戳
    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key, TS_DEFAULT)
    }
}

/// 是不可变、可共享的，通常用于存储再SkipMap 中，from_bytes_with_ts是生产 KeyBytes的主要方式
impl Key<Bytes> {
    pub fn new() -> Self {
        Self(Bytes::new(), TS_DEFAULT)
    }

    pub fn as_key_slice(&self) -> KeySlice {
        Key(&self.0, self.1)
    }

    pub fn from_bytes_with_ts(bytes: Bytes, ts: u64) -> KeyBytes {
        Key(bytes, ts)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        Key(bytes, TS_DEFAULT)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// 是零拷贝的借用视图，适合作为API参数（如:wal::put(key:KeySlice,value:&[u8])）避免在写入路径中克隆键数据
impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec(), self.1)
    }

    pub fn from_slice(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts(slice: &'a [u8]) -> Self {
        Self(slice, TS_DEFAULT)
    }
    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts)
    }
}

/// 只打印键内容（不打印时间戳），便于调试输出
impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// /创建空键 + TS_DEFAULT（0
impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default(), TS_DEFAULT)
    }
}

/// 比较 (键内容, 时间戳) 元组，两者都相等才相等。要求 T 本身实现了 PartialEq。
impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ref(), self.1).eq(&(other.0.as_ref(), other.1))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

// 如果 T 是 Clone，则 Key<T> 可以 clone（克隆内部 T 和时间戳）。
impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1)
    }
}

/// 如果 T 是 Copy（如 &[u8]），则 Key<T> 也是 Copy（元组结构体自动派生 Copy 条件）
impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}


/// 排序规则：
/// 1.首先按照键内容（字节序）升序
/// 2.键内容想等时，按时间戳降序 （Reverse(self.1) 使得大的时间戳被认为“更小”）。
/// 这样做的原因是在LSM树中，通常需要同一用户键的最新版本（最大时间戳）排在前面。
/// 例如扫描时，(b"user", 200) 会排在 (b"user", 100) 之前。时间戳降序确保最新版本最先被访问。
impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.0.as_ref(), Reverse(self.1)).partial_cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.as_ref(), Reverse(self.1)).cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}