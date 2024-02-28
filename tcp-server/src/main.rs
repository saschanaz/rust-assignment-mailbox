use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    self, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex, time::timeout
};

const DEFAULT_TIMEOUT: Option<Duration> = Some(Duration::from_millis(1000));

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:7878").await?;

    let storage = Arc::new(Mutex::new(VecDeque::new()));

    loop {
        // accept connections and process them one at a time
        let (stream, _) = listener.accept().await?;
        println!("Got client {:?}", stream.peer_addr());
        let storage = storage.clone();
        tokio::task::spawn(async move {
            if let Err(e) = handle_client(stream, &storage).await {
                println!("Error handling client: {:?}", e);
            }
        });
    }
}

/// Process a single connection from a single client.
///
/// Drops the stream when it has finished.
async fn handle_client(mut stream: TcpStream, storage: &Mutex<VecDeque<String>>) -> io::Result<()> {
    // stream.set_read_timeout(DEFAULT_TIMEOUT)?;
    // stream.set_write_timeout(DEFAULT_TIMEOUT)?;

    let mut buffer = String::new();
    stream.read_to_string(&mut buffer).await?;
    println!("Received: {:?}", buffer);

    let command = match simple_db::parse(&buffer) {
        Ok(s) => s,
        Err(e) => {
            println!("Error parsing command: {:?}", e);
            let str = format!("Error: {}!", e);
            stream.write_all(str.as_bytes()).await?;
            return Ok(());
        }
    };

    println!("Got command {:?}", command);

    match command {
        simple_db::Command::Publish(message) => {
            storage.lock().await.push_back(message);
            stream.write_all(b"OK").await?;
        }
        simple_db::Command::Retrieve => match storage.lock().await.pop_front() {
            Some(message) => {
                let str = format!("Got: {:?}\n", message);
                stream.write_all(str.as_bytes()).await?;
            },
            None => stream.write_all(b"Error: Queue empty!").await?,
        },
    }
    Ok(())
}
