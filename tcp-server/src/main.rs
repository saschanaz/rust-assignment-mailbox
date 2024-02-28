use std::collections::VecDeque;
use std::io::{self, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use tokio::{self, sync::Mutex};

const DEFAULT_TIMEOUT: Option<Duration> = Some(Duration::from_millis(1000));

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener: TcpListener = TcpListener::bind("127.0.0.1:7878")?;

    let storage = Arc::new(Mutex::new(VecDeque::new()));

    // accept connections and process them one at a time
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Got client {:?}", stream.peer_addr());
                let storage = storage.clone();
                tokio::task::spawn(async move {
                    if let Err(e) = handle_client(stream, &storage).await {
                        println!("Error handling client: {:?}", e);
                    }
                });
            }
            Err(e) => {
                println!("Error connecting: {:?}", e);
            }
        }
    }

    Ok(())
}

/// Process a single connection from a single client.
///
/// Drops the stream when it has finished.
async fn handle_client(mut stream: TcpStream, storage: &Mutex<VecDeque<String>>) -> io::Result<()> {
    stream.set_read_timeout(DEFAULT_TIMEOUT)?;
    stream.set_write_timeout(DEFAULT_TIMEOUT)?;

    let mut buffer = String::new();
    stream.read_to_string(&mut buffer)?;
    println!("Received: {:?}", buffer);

    let command = match simple_db::parse(&buffer) {
        Ok(s) => s,
        Err(e) => {
            println!("Error parsing command: {:?}", e);
            writeln!(stream, "Error: {}!", e)?;
            return Ok(());
        }
    };

    println!("Got command {:?}", command);

    match command {
        simple_db::Command::Publish(message) => {
            storage.lock().await.push_back(message);
            writeln!(stream, "OK")?;
        }
        simple_db::Command::Retrieve => match storage.lock().await.pop_front() {
            Some(message) => writeln!(stream, "Got: {:?}", message)?,
            None => writeln!(stream, "Error: Queue empty!")?,
        },
    }
    Ok(())
}
