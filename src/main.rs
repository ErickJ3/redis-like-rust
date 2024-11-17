use std::sync::Arc;
use persistence::storage::Storage;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{error, info, Level};

mod commands;
mod resp;
mod persistence;

use commands::Command;
use resp::Resp;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Command error: {0}")]
    Command(String),
}

pub type Result<T> = std::result::Result<T, Error>;

async fn handle_client(mut stream: TcpStream, storage: Arc<Storage>) {
    let mut buffer = vec![0; 1024];

    loop {
        match stream.read(&mut buffer).await {
            Ok(n) if n == 0 => break,
            Ok(n) => {
                let response = match Resp::parse(&buffer[..n]) {
                    Ok(Some(resp)) => match Command::from_resp(resp) {
                        Ok(cmd) => cmd.execute(&storage).await,
                        Err(e) => Resp::Error(e.to_string()),
                    },
                    Ok(None) => Resp::Error("Empty request".into()),
                    Err(e) => Resp::Error(e.to_string()),
                };

                if let Err(e) = stream.write_all(&response.into_bytes()).await {
                    error!("Failed to write response: {}", e);
                    break;
                }
            }
            Err(e) => {
                error!("Failed to read from socket: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    let storage = Arc::new(Storage::new()?);

    info!("Server listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("New connection from {}", addr);
                let storage = storage.clone();

                tokio::spawn(async move {
                    handle_client(stream, storage).await;
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        thread,
        time::Duration,
    };

    #[test]
    fn test_set_and_get() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        let mut read_buffer = [0; 1024];

        let set_command = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        stream.write(set_command.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let set_response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(set_response, "+OK\r\n");

        let get_command = "*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n";
        stream.write(get_command.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let get_response = String::from_utf8_lossy(&read_buffer[..n]);

        assert_eq!(get_response, "$5\r\nworld\r\n");

        let get_missing = "*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n";
        stream.write(get_missing.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let missing_response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(missing_response, "$-1\r\n");
    }

    #[test]
    fn test_ping() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        let mut read_buffer = [0; 1024];

        let message = "*1\r\n$4\r\nPING\r\n";
        stream.write(message.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(response, "+PONG\r\n");
    }

    #[test]
    fn test_echo() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        let mut read_buffer = [0; 1024];

        let message = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        stream.write(message.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(response, "+hey\r\n");
    }

    #[test]
    fn test_set_with_px() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        let mut read_buffer = [0; 1024];

        let set_command =
            "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$4\r\n1000\r\n";
        stream.write(set_command.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let set_response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(set_response, "+OK\r\n");

        let get_command = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        stream.write(get_command.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let get_response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(get_response, "$5\r\nvalue\r\n");

        thread::sleep(Duration::from_millis(1100));

        stream.write(get_command.as_bytes()).unwrap();
        stream.flush().unwrap();

        let n = stream.read(&mut read_buffer).unwrap();
        let get_response = String::from_utf8_lossy(&read_buffer[..n]);
        assert_eq!(get_response, "$-1\r\n");

        stream.shutdown(std::net::Shutdown::Both).unwrap();

        thread::sleep(Duration::from_millis(100));
    }
}
