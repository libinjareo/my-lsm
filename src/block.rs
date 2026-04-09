mod iterator;
mod builder;

use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Block 是一组排序后的键值对集合，是磁盘读写和缓存的最小单位。
/// 1.使用 u16 偏移量，限制了块最大为 64KB，这样做的好处是节省存储偏移量所需的空间（每个偏移量仅仅2个字节），适合大多数键值对
/// 较小的场景（例如用户数据活索引块）
/// 2.偏移量数组单独存储：使得可以再不解析所有键值对的情况下，快速进行二分查找定位某个key的起始位置
/// 3.数据区连续：减少磁盘I/O数量，一次读取整个快即可获取所有简直对数据
///
/// Block 整体布局
/// ```
/// +-------------------+------------------------+---------------------------+-----+
/// | entry 0           | entry 1                | entry 2                   | ... |
/// +-------------------+------------------------+---------------------------+-----+
/// ^                        ^
/// offset[0]               offset[1]               offset[2] ...
/// ```
///
/// * `Block.data` 顺序存储所有条目。
/// * `Block.offsets` 存储每个条目的起始偏移量。
pub struct Block {
    /// 连续存放所有键值对的数据区域,将所有键值对的编码(例如 key_len+key+value_len+value) 顺序拼接到一起的字节缓冲区
    pub(crate) data: Vec<u8>,
    /// 每个键值对在 data 中的起始偏移量，每个条目在data中的起始偏移位置（字节索引），类型为u16，
    /// 暗示单个Block的大小被限制在64KB以内。符合LSM树常见的设计（如 LevelDB 的 4KiB~32KiB 块）
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// 编码格式
    /// 最终磁盘布局（从文件头到文件尾）
    /// 数据区(data)->长度 self.data.len() 字节
    /// 偏移量数组-> 长度：2 * offsets.len() 字节
    /// 偏移量数量（元素数）-> 长度：2 （u16）
    /// 这种格式支持二分查找：首先读取最后的 u16 获得条目数量 N,然后通过索引倒推出偏移量数组的位置，
    /// 进而读取任意偏移量，再定位到 data 中国年的对应键值对。
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone(); // 先拷贝数据区
        let offsets_len = self.offsets.len();

        for offset in &self.offsets {
            buf.put_u16(*offset);  // 依次写入每个偏移量(2字节)
        }

        buf.put_u16(offsets_len as u16); // 最后写入偏移量数量
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        // 从输入字节流的最后2字节读出条目数量 N
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        // 计算数据区结束位置：总长度 - 2 - N * 2
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        // 提取偏移量数组（从 data_end 到倒数第 3 字节），每 2 字节一个 u16。
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // 将一个字节切片 offsets_raw 中连续的两个字节为一组，每组解释为一个 u16 小端整数，并收集成一个Vec<u16>偏移量数组
        let offsets = offsets_raw
            // offsets_ras的类型是&[u8],它包含了之前编码时写入的多个 u16 偏移量(每个占2个字节)
            // .chunks(2) 会返回一个迭代器，每次产生一个 正好包含 2 个字节的切片（最后一个切片可能不足 2 字节
            // 但这里偏移量数量是整数，所以长度必定是偶数）
            .chunks(SIZEOF_U16)
            // 对2个 字节的切片调用map
            // x 是一个 &[u8]，但因为 mut x 被声明为 mut，所以它可以被当作实现了 Buf trait 的类型。
            // get_u16() 是 bytes::Buf 提供的方法：它会从字节切片的前 2 个字节中读取一个 u16（小端序），
            // 并消费这 2 个字节（即移动内部游标）。由于 x 恰好是 2 字节，读取后切片变为空
            //  实际上，这里x.get_u16() 等同于 u16::from_le_bytes(x[0],x[1]),但更简洁且利用了 bytes 库的约定
            .map(|mut x| x.get_u16())
            // 将迭代器产生的所有 u16 值收集到一个 Vec<u16>中
            // 最终结果 offsets 就是解码出的偏移量数组，每个元素表示一个键值对在 data 区域中的起始位置
            // 假设 offsets_raw 的字节内容为 [0x10, 0x00, 0x20, 0x00]（小端序，对应十进制 16 和 32）
            // chunks(2) 产生两个切片：[0x10, 0x00] 和 [0x20, 0x00]
            // 第一个切片调用 get_u16() 得到 0x0010 = 16；第二个得到 0x0020 = 32
            // collect 得到 vec![16, 32]
            // 这种写法简洁且高效：避免了手动循环和字节拼接，充分利用了 bytes 库的 Buf 抽象。
            // 同时，chunks + map + collect 是 Rust 中处理字节数组到整数数组的惯用模式
            .collect();
        // 构建 Block
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}