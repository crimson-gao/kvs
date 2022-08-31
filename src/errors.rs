use std::io;
use thiserror::Error;

/// network failures
#[derive(Error, Debug)]
pub enum NetError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
}

/// disk failures
#[derive(Error, Debug)]
pub enum DiskError {
    #[error("unknown `{0}`")]
    UnKnown(String),
    #[error("store file path is a dir")]
    PathIsDir,
    #[error("file path `{0}` open: `{1}")]
    FileOpen(String, String),
}

/// store failures
#[derive(Error, Debug)]
pub enum KeyError {
    #[error("key `{0}` not found in store")]
    KeyNotFound(String),
}

/// binary opt failures
#[derive(Error, Debug)]
pub enum BinaryError {
    #[error("parse log size: `{0}`")]
    ParseLogSize(String),
}

/// KvsStore all types failures
#[derive(Error, Debug)]
pub enum KvStoreError {
    #[error("io error: `{0}`")]
    IOError(#[from] std::io::Error),
    #[error("io error: `{0}`")]
    DiskError(#[from] DiskError),
    #[error("store error: `{0}`")]
    KeyError(#[from] KeyError),
    #[error("net error: `{0}`")]
    NetError(#[from] NetError),
    #[error("serialize error: `{0}`")]
    SerialError(#[from] serde_json::Error),
    #[error("binary serialize error: `{0}`")]
    BinaryError(#[from] BinaryError),
}

/// KvStore Result, returns type `KvStoreError` if error occurs
pub type Result<T> = std::result::Result<T, KvStoreError>;
