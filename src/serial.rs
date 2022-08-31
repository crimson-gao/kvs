use std::{io::Cursor, io::Read, str::FromStr};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::errors::KvStoreError;

use super::errors::Result;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", content = "content")]
pub enum Command {
    Get(String),
    Set(String, String),
    Remove(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Log {
    pub command: Command,
    pub deleted: bool,
}

impl Log {
    /// u32 + body
    pub fn serialize(&self) -> Result<Vec<u8>> {
        if self.deleted {
            return Ok(vec![]);
        }
        let mut res: Vec<u8> = vec![];
        let mut cursor = Cursor::new(&mut res);
        cursor.set_position(std::mem::size_of::<u32>() as u64);
        serde_json::to_writer(&mut cursor, self)?;

        {
            let total_size = res.len();
            let mut cursor = Cursor::new(&mut res);
            let body_size: u32 = (total_size - std::mem::size_of::<u32>()) as u32;
            cursor.write_u32::<LittleEndian>(body_size)?;
        }

        Ok(res)
    }

    fn deserialize(buf: &Vec<u8>) -> Result<Log> {
        serde_json::from_slice(&buf).map_err(|err| KvStoreError::SerialError(err))
        // serde_json::from_reader(Cursor::new(buf)).map_err(|err| KvStoreError::SerialError(err))
    }

    // read from cursor, parse size + body => Log
    pub fn read_from_cursor(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<Log> {
        let log_size = cursor.read_u32::<LittleEndian>().unwrap() as usize;
        let mut log_bytes: Vec<u8> = vec![0; log_size];
        cursor.read_exact(&mut log_bytes)?;
        Self::deserialize(&log_bytes)
    }
}

impl FromStr for Log {
    type Err = KvStoreError;
    fn from_str(s: &str) -> Result<Self> {
        serde_json::from_str(s).map_err(|err| KvStoreError::SerialError(err))
    }
}

impl From<Command> for Log {
    fn from(cmd: Command) -> Self {
        Log {
            command: cmd,
            deleted: false,
        }
    }
}

#[test]
fn test_serialize() {
    let log: Log = Command::Get("hello".to_owned()).into();
    let v = log.serialize().unwrap();
    println!(
        "{:?}",
        serde_json::from_str::<Log>(&String::from_utf8(v[4..].to_vec()).unwrap())
    );
    println!("{}", v.len());
    let log2 = Log::read_from_cursor(&mut Cursor::new(v));
    println!("second: {:?} {:?}", log, log2);
}
