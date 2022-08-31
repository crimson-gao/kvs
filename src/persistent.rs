use crate::errors::KvStoreError;

use super::errors::{DiskError, Result};
use super::serial::{Command, Log};
use log::{info, warn};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{BufWriter, Cursor, Read, Write};
use std::path::Path;
pub trait Persistenter {
    fn store_log(self: &mut Self, log: Log) -> Result<()>;
    fn get_all_logs(self: &mut Self) -> Result<Vec<Log>>;
    fn flush(self: &mut Self) -> Result<()>;
}

pub struct Disk {
    logs_in_mem: Vec<Log>,
    path: Box<Path>,
    max_size: usize,
    compact_counter: i32,
    file_read_counter: ref_mut(i32),
    file_write_count: ref_mut(i32),
}

static DISK_BATCH_SIZE: usize = 100;

impl Default for Disk {
    fn default() -> Self {
        Disk {
            logs_in_mem: vec![],
            path: Box::from(Path::new("logs.log")),
            max_size: DISK_BATCH_SIZE,
            compact_counter: 0,
            file_read_counter: 0,
            file_write_count: 0,
        }
    }
}

impl Disk {
    fn check_compact(&mut self) {
        if self.should_compact() {
            info!("compact starts!");
            _ = self.compact_all();
        }
    }

    fn should_compact(&self) -> bool {
        (self.compact_counter + 1) % 400 == 0
    }

    pub fn new(path: Box<Path>) -> Self {
        Disk {
            logs_in_mem: vec![],
            path: path,
            max_size: DISK_BATCH_SIZE,
            compact_counter: 0,
            file_read_counter: 0,
            file_write_count: 0,
        }
    }
    /// write logs to disk file
    /// if append is set to false, the log file will be overwritted
    fn write_logs_to_file(&self, logs: &Vec<Log>, append: bool) -> Result<()> {
        self.file_write_count += 1;
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(append)
            .open(self.path.as_ref())
            .map_err(|err| -> KvStoreError {
                DiskError::FileOpen(self.path.to_string_lossy().to_string(), err.to_string()).into()
            })?;
        let mut buf = BufWriter::new(file);

        for log in logs {
            buf.write_all(&log.serialize()?)?;
        }
        buf.flush()?;
        Ok(())
    }

    fn read_logs_from_file(&mut self) -> Result<Vec<Log>> {
        self.file_read_counter += 1;
        let mut file = OpenOptions::new().read(true).open(self.path.as_ref())?;
        let mut content = vec![];
        file.read_to_end(&mut content)?;
        let size = content.len() as u64;
        let mut cursor = Cursor::new(content);
        let mut res: Vec<Log> = vec![];

        while cursor.position() < size {
            res.push(Log::read_from_cursor(&mut cursor)?);
        }
        Ok(res)
    }

    // mark useless log to deleted
    fn compact_logs_in_place(logs: &mut Vec<Log>) {
        let mut marks: HashSet<String> = HashSet::new();
        for log in logs.iter_mut().rev() {
            match &log.command {
                Command::Get(_) => {}
                Command::Set(ref key, _) => {
                    if !marks.contains(key) {
                        marks.insert(key.clone());
                    } else {
                        log.deleted = true;
                    }
                }
                Command::Remove(ref key) => {
                    if !marks.contains(key) {
                        marks.insert(key.clone());
                    }
                }
            }
        }
    }
    fn compact_in_mem(&mut self) {
        Self::compact_logs_in_place(&mut self.logs_in_mem);
    }

    fn compact_all(&mut self) -> Result<()> {
        let mut file_logs = self.read_logs_from_file()?;
        let file_log_size = file_logs.len();
        file_logs.append(&mut self.logs_in_mem);
        Self::compact_logs_in_place(&mut file_logs);

        self.logs_in_mem = file_logs;

        if self.logs_in_mem.len() > 0 {
            for _ in 0..3 {
                if let Ok(_) = self.write_logs_to_file(&self.logs_in_mem, false) {
                    self.logs_in_mem.clear();
                    return Ok(());
                }
            }
            warn!("compact write file failed, restore it!");
            // restore
            self.logs_in_mem = self.logs_in_mem[file_log_size..].to_vec();
        }
        Ok(())
    }
}

impl Drop for Disk {
    fn drop(&mut self) {
        self.flush().unwrap();
        info!(
            "read file count:{}, write file count:{}",
            self.file_read_counter, self.file_write_count
        );
    }
}

impl Persistenter for Disk {
    fn store_log(&mut self, log: Log) -> Result<()> {
        self.compact_counter += 1;
        self.check_compact();
        if self.logs_in_mem.len() >= self.max_size {
            self.flush()?;
        }
        self.logs_in_mem.push(log);
        Ok(())
    }
    /// read all logs
    fn get_all_logs(&mut self) -> Result<Vec<Log>> {
        let mut logs = self.read_logs_from_file()?;
        logs.append(&mut self.logs_in_mem.clone());
        Ok(logs)
    }

    /// flush logs to file append
    fn flush(self: &mut Self) -> Result<()> {
        if self.logs_in_mem.len() > 0 {
            self.write_logs_to_file(&self.logs_in_mem, true)?;
            self.logs_in_mem.clear();
        }
        Ok(())
    }
}
