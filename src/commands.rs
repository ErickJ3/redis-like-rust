use std::time::Duration;

use crate::{resp::Resp, Error, Result, Storage};

#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        expiry: Option<Duration>,
    },
    Get(String),
}

impl Command {
    pub fn from_resp(resp: Resp) -> Result<Self> {
        match resp {
            Resp::Array(mut items) => {
                if items.is_empty() {
                    return Err(Error::Command("Empty command".into()));
                }

                let command = match items.remove(0) {
                    Resp::BulkString(cmd) => cmd.to_uppercase(),
                    _ => return Err(Error::Command("Invalid command format".into())),
                };

                match command.as_str() {
                    "PING" => Ok(Command::Ping),
                    "ECHO" => Self::echo(items),
                    "SET" => match items.len() {
                        2 => Self::set(items),
                        4 => Self::set_with_expiry(items),
                        _ => Err(Error::Command("Wrong number of SET arguments".into())),
                    },
                    "GET" => Self::get(items),
                    _ => Err(Error::Command(format!("Unknown command: {}", command))),
                }
            }
            _ => Err(Error::Command("Invalid command format".into())),
        }
    }

    pub async fn execute(&self, storage: &Storage) -> Resp {
        match self {
            Command::Ping => Resp::SimpleString("PONG".into()),
            Command::Echo(message) => Resp::SimpleString(message.clone()),
            Command::Set { key, value, expiry } => {
                match storage.set(key.clone(), value.clone(), *expiry) {
                    Ok(()) => Resp::SimpleString("OK".into()),
                    Err(_) => Resp::Error("ERR failed to set value".into()),
                }
            }
            Command::Get(key) => match storage.get(key) {
                Ok(Some(value)) => Resp::BulkString(value),
                Ok(None) => Resp::Null,
                Err(_) => Resp::Error("ERR failed to get value".into()),
            },
        }
    }

    fn get(mut items: Vec<Resp>) -> Result<Command> {
        if items.len() != 1 {
            return Err(Error::Command("GET requires exactly one argument".into()));
        }
        if let Resp::BulkString(key) = items.remove(0) {
            Ok(Command::Get(key))
        } else {
            Err(Error::Command("Invalid GET argument".into()))
        }
    }

    fn echo(mut items: Vec<Resp>) -> Result<Command> {
        if items.len() != 1 {
            return Err(Error::Command("ECHO requires exactly one argument".into()));
        }
        if let Resp::BulkString(message) = items.remove(0) {
            Ok(Command::Echo(message))
        } else {
            Err(Error::Command("Invalid ECHO argument".into()))
        }
    }

    fn set(mut items: Vec<Resp>) -> Result<Self> {
        if let (Resp::BulkString(key), Resp::BulkString(value)) = (items.remove(0), items.remove(0))
        {
            Ok(Self::Set {
                key,
                value,
                expiry: None,
            })
        } else {
            Err(Error::Command("Invalid SET arguments".into()))
        }
    }

    fn set_with_expiry(mut items: Vec<Resp>) -> Result<Self> {
        let (key, value, opt, px) = match (
            items.remove(0),
            items.remove(0),
            items.remove(0),
            items.remove(0),
        ) {
            (
                Resp::BulkString(k),
                Resp::BulkString(v),
                Resp::BulkString(o),
                Resp::BulkString(p),
            ) => (k, v, o, p),
            _ => return Err(Error::Command("Invalid SET arguments".into())),
        };

        if opt.to_uppercase() != "PX" {
            return Err(Error::Command("Invalid SET option".into()));
        }

        let ms = px
            .parse::<u64>()
            .map_err(|_| Error::Command("Invalid PX value".into()))?;

        Ok(Self::Set {
            key,
            value,
            expiry: Some(Duration::from_millis(ms)),
        })
    }
}
