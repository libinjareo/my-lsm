use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::sync::Arc;
use std::path::Path;

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

    // pub fn put(&self,key:KeySlice,value:&[u8]) -> Result<()> {
    //     todo!()
    // }
}
