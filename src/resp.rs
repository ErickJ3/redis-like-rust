use crate::{Error, Result};

#[derive(Debug, Clone)]
pub enum Resp {
    SimpleString(String),
    Error(String),
    BulkString(String),
    Array(Vec<Resp>),
    Null,
}

impl Resp {
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Resp::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Resp::Error(s) => format!("-{}\r\n", s).into_bytes(),
            Resp::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            Resp::Array(arr) => {
                let mut bytes = format!("*{}\r\n", arr.len()).into_bytes();
                for item in arr {
                    bytes.extend(item.into_bytes());
                }
                bytes
            }
            Resp::Null => "$-1\r\n".as_bytes().to_vec(),
        }
    }

    pub fn parse(input: &[u8]) -> Result<Option<Resp>> {
        if input.is_empty() {
            return Ok(None);
        }

        match input[0] as char {
            '*' => {
                let input_str = std::str::from_utf8(input)
                    .map_err(|_| Error::Protocol("Invalid UTF-8".into()))?;
                let lines: Vec<&str> = input_str.split("\r\n").collect();

                let count = lines[0][1..]
                    .parse::<usize>()
                    .map_err(|_| Error::Protocol("Invalid array length".into()))?;

                let mut array = Vec::with_capacity(count);
                let mut current_line = 1;

                for _ in 0..count {
                    if current_line >= lines.len() {
                        return Err(Error::Protocol("Incomplete array".into()));
                    }

                    match lines[current_line].chars().next() {
                        Some('$') => {
                            current_line += 1;
                            if current_line >= lines.len() {
                                return Err(Error::Protocol("Incomplete bulk string".into()));
                            }

                            array.push(Resp::BulkString(lines[current_line].to_string()));
                            current_line += 1;
                        }
                        _ => return Err(Error::Protocol("Invalid array element".into())),
                    }
                }

                Ok(Some(Resp::Array(array)))
            }
            _ => Err(Error::Protocol("Unsupported RESP type".into())),
        }
    }
}
