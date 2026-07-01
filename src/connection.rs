use std::{sync::Arc,time::Duration};

use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}, sync::Mutex, time::{Instant, sleep}};

use crate::{distributed::request_reader, types::{Peer, ThisNode}};

pub async fn connect_to_peers(me: Arc<Mutex<ThisNode>>) {
    let mut node = me.lock().await;
    let id = node.id.clone();
    for i in node.peers.iter_mut() {
        if i.id == id {
            continue;
        };
        loop {
            match TcpStream::connect(&i.addr).await {
                Ok(stream) => {
                    i.conn = Some(stream);
                    break;
                }
                Err(a) => {
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
        }
    }
}

pub async fn retry_conn(me: Arc<Mutex<ThisNode>>) {
    let mut node = me.lock().await;
    for i in node.peers.iter_mut() {
        if i.conn.is_none() {
            let conn = match TcpStream::connect(&i.addr).await {
                Ok(c) => {
                    c
                },
                Err(_) => {
                    return
                }
            };
            i.conn = Some(conn);
        }
    }
}

pub async fn handle_connection(listener: TcpListener, me: Arc<Mutex<ThisNode>>, heartbeat: Arc<Mutex<Instant>>) {
    loop{
        let (socket, a) = listener.accept().await.unwrap();
        tokio::spawn(request_reader(socket, me.clone(), heartbeat.clone()));
    }
}

pub async fn send_msg(mut msg: Vec<u8>, to: &mut Peer) {
    msg.push(b'\n');
    match to.conn.as_mut().unwrap().write_all(&msg).await {
        Ok(_) => {},
        Err(_) => {
            to.conn = None
        }   
    }
}