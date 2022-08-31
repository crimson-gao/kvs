#[macro_use(Serialize, Deserialize)]
extern crate serde;
extern crate serde_json;
use debug_ignore::DebugIgnore;

mod errors;
mod persistent;
mod serial;

use errors::KeyError;
use persistent::{Disk, Persistenter};
use serial::{Command, Log};
use std::{collections::HashMap, path::PathBuf};

pub use errors::Result;

#[derive(Debug)]
pub struct KvStore {
    map: HashMap<String, String>,
    persistent: DebugIgnore<Box<dyn Persistenter>>,
}

impl KvStore {
    pub fn replace_logs(&mut self, logs: Vec<Log>) {
        for log in logs.into_iter() {
            match log.command {
                Command::Set(key, value) => {
                    self.map.insert(key, value);
                }
                Command::Remove(ref key) => {
                    self.map.remove(key);
                }
                Command::Get(_) => (),
            };
        }
    }
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.clone(), value.clone());
        self.persistent.store_log(Command::Set(key, value).into())?;
        Ok(())
    }
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map
            .remove(&key.clone())
            .ok_or(KeyError::KeyNotFound(key.clone()))?;
        self.persistent.store_log(Command::Remove(key).into())?;
        Ok(())
    }
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key.clone()).cloned())
    }
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut dest_path = path.into();
        if dest_path.clone().is_dir() {
            dest_path.push("logs.log");
        };
        let mut store = KvStore {
            map: HashMap::new(),
            persistent: DebugIgnore(Box::from(Disk::new(Box::from(dest_path.clone())))),
        };
        if dest_path.exists() {
            let logs = store.persistent.get_all_logs()?;
            store.replace_logs(logs);
        }
        Ok(store)
    }
}
