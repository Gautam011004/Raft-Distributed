use std::sync::Arc;

use tokio::{net::TcpStream, sync::Mutex};

use crate::types::ThisNode;

pub async fn connect(me: Arc<Mutex<ThisNode>>) {
    let mut node = me.lock().await;
    let id = node.id.clone();
    println!("Reached here");
    for i in node.peers.iter_mut() {
        if i.id == id {
            continue;
        };
        loop {
            match TcpStream::connect(&i.addr).await {
                Ok(stream) => {
                    i.conn = Some(stream);
                    print!("Connected {}", i.id);
                    break;
                }
                Err(_) => {continue;}
            }
        }
    }
}