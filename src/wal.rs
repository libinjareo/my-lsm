use crate::key::{KeyBytes, KeySlice};
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

pub struct Wal {
    /*
    File:
    标准库中表示一个操作系统文件句柄，支持读写操作。但 File 本身不保证多线程下的安全共享（它没有实现 Sync，且直接共享会导致数据竞争）
    BufWriter<File>:
    为 File 添加一个内存缓冲区。写入数据时先写进缓冲区，积累到一定量或手动 flush 时才真正调用系统调用写入文件。
    好处：显著减少系统调用次数，提升写入性能（尤其适合 WAL（Write-Ahead Log）这种频繁追加写入的场景）。
    Mutex<BufWriter<File>>:
    互斥锁，确保同一时刻只有一个线程能访问内部的 BufWriter<File>。
    原因：BufWriter 内部有可变状态（缓冲区），多线程同时写会破坏数据；而且 File 也要求独占写入（或需要同步）。
    好处：提供线程安全的内部可变性，允许 Wal 结构体在多线程环境中共享并安全地写入日志。
    Arc<Mutex<BufWriter<File>>>:
    原子引用计数（Atomic Reference Counted），允许多个所有者共享同一个 Mutex。
    原因：WAL 通常会在多个地方（例如不同的任务、线程）被引用，而 Wal 本身可能需要被克隆或在线程间传递。Arc 使得多个 Wal 实例可以指向同一份底层文件资源。
    好处：无需手动管理生命周期，自动在最后一个引用释放时清理资源。
    */
    //经典模式：线程安全 + 共享所有权 + 缓冲写入
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    /*
     impl AsRef<Path> 是Rust中的参数多态（静态分发）语法，表示参数类型T必须实现 AsRef<Path> trait
     AsRef<Path> trait:
     AsRef 提供一种廉价的引用转换，例如 &str、String、PathBuf 都实现了 AsRef<Path>。这意味着你可以传入多种“可被视作路径”的类型，而无需手动转换。

     让 create 方法接受更广泛的参数类型，而不需要调用者显式转换.
     等价于手动写泛型：
     pub fn create<T:AsRef<Path>>(path:T) -> Result<Self>{
        let path = path.as_ref(); // 得到&Path
        // 使用 path...
     }
     便利性：调用者无需写 .as_ref() 或 .into()。
     零成本抽象：impl Trait 在参数位置是静态分发，无运行时开销。
     表达意图：明确表示只需要“能转换为路径”的能力，而不是任意类型
    */
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    // 从已有的WAL文件中读取所有记录，逐条验证校验和，并将有效的键值对插入到内存跳表 skiplist 中。
    // 最后返回一个新的WAL示例，用于后续继续追加写入
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true) // 以追加的模式打开现有文件
            .open(path)
            .context("failed to recover WAL")?;
        // 一次性读取所有数据到Vec<u8>，然后使用&[u8]作为游标进行解析
        // 但是对于巨大文件可能消耗较多的内存，此处为了简洁，采用全量读取；
        // 可以使用BufReader流式解析进行优化
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf = buf.as_slice(); // 将整个文件读入内存
        while rbuf.has_remaining() { // 只要还有未读字节就继续
            let mut hasher = crc32fast::Hasher::new();

            let key_len = rbuf.get_u16() as usize; // 从缓冲区读取2字节并前进游标
            hasher.write_u16(key_len as u16); // 开始计算校验和，每次解析一个字段前，先更新 hasher

            // 会分配新的内存，将key字节复制出来，然后更新哈西并移动游标
            let key = Bytes::copy_from_slice(&rbuf[..key_len]);
            hasher.write(&key);
            rbuf.advance(key_len);

            // 读取ts
            let ts = rbuf.get_u64();
            hasher.write_u64(ts);

            // 读取 value_len
            let value_len = rbuf.get_u16() as usize;
            hasher.write_u16(value_len as u16);

            // 读取 value 内容
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            hasher.write(&value);
            rbuf.advance(value_len);

            // 读取存储的  checkSum
            let checksum = rbuf.get_u32();

            // 检验
            if hasher.finalize() != checksum {
                bail!("checksum mismatch");  //防止使用损坏的或者不完整的 WAL 文件
            }

            // 插入调表，会把时间戳编码进 key 的内部表示（通常用于 MVCC 或版本管理）
            skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
        }

        Ok(Self {
            // 此处复用了之前的file对象（它已经以 append 模式打开，且文件位置已经在末尾），然后创建BufWriter
            // 之后调用 put 时，新记录会追加到原有数据之后
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    // 将一条健值对记录（附带key的时间戳）追加写入WAL文件，同时计算并写入CRC32校验和，用于后续恢复时检测数据完整性
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        // 需要先上锁，获得独占访问权。锁会再函数返回时自动释放
        let mut file = self.file.lock();
        // 预分配内存缓冲区
        // 预分配足够的容量，避免写入过程中反复扩容。
        // key内容长度+时间戳(u64) + value长度 + 2字节（size_of::<u16>）
        let mut buf: Vec<u8> = Vec::with_capacity(key.raw_len() + value.len() + std::mem::size_of::<u16>());
        // 用于计算记录中所有字段的校验和
        let mut hasher = crc32fast::Hasher::new();
        //按固定格式写入数据并更新哈希

        // Key内容的字节长度（不含时间戳），字节数：2
        hasher.write_u16(key.key_len() as u16); // 更新校验和
        buf.put_u16(key.key_len() as u16); // 将数据写入缓冲区

        // key的实际内容，字节数:key_len
        hasher.write(key.key_ref()); // 更新校验和
        buf.put_slice(key.key_ref()); // 将数据写入缓冲区

        // 时间戳（用于版本/排序），字节数：8
        hasher.write_u64(key.ts()); // 更新校验和
        buf.put_u64(key.ts()); // 将数据写入缓冲区

        // value内容的字节长度，字节数:2
        hasher.write_u16(value.len() as u16); // 更新校验和
        buf.put_u16(value.len() as u16); // 将数据写入缓冲区

        // value的实际数据，字节数：value_len
        buf.put_slice(value); // 更新校验和
        hasher.write(value); // 将数据写入缓冲区

        // todo add checksum

        // 从哈希器中得到最终的CRC32校验和，追加到缓冲区末尾,字节数:4
        buf.put_u32(hasher.finalize());

        // 因file 是BufWriter<File>，数据会先进入BufWriter的内部缓冲区，未必立即落盘
        file.write_all(&buf)?;

        // 方法没有调用flush 或 sync，因此写入的数据可能还在内存缓冲区中。
        // 显式的持久化由sync()方法完成，这符合WAL的常见设计：允许批量吸入后统一同步，提高性能
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
