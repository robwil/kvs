use std::io::Read;
use std::io::Write;
use serde::{Deserialize, Serialize};
use bincode::{serialize_into, deserialize_from};
use super::Result;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    pub fn to_writer<W>(&self, writer: W) -> Result<()>
    where
        W: Write,
    {
        serialize_into(writer, &self)?;
        Ok(())
    }

    pub fn from_reader<R>(reader: R) -> Result<Self> where
    R: Read,
     {
        let cmd: Command = deserialize_from(reader)?;
        Ok(cmd)
    }
}
