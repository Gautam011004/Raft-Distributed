use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex, time::{Instant, sleep}};

use crate::types::{Role, Rpc, ThisNode};

pub async fn request_reader(mut conn: TcpStream, me: Arc<Mutex<ThisNode>>, heartbeat_guard: Arc<Mutex<Instant>>) {
    let mut buf = BytesMut::new();
    loop {
        if let Some(s) = buf.iter().position(|x| *x == b'\n') {
            vote_caster(&mut buf, s, me.clone(), heartbeat_guard.clone()).await;
        }
        conn.read_buf(&mut buf).await.unwrap();
        sleep(Duration::from_secs(5)).await;
        if let Some(s) = buf.iter().position(|x| *x == b'\n') {
            vote_caster(&mut buf, s, me.clone(), heartbeat_guard.clone()).await;
        }
    }
}

pub async fn vote_caster(buf: &mut BytesMut, pos: usize, me: Arc<Mutex<ThisNode>>, heartbeat_guard: Arc<Mutex<Instant>>) {
    let node = me.lock().await;
    let bytes = buf.split_to(pos + 1);
    println!("{}", String::from_utf8(bytes.to_vec()).unwrap());
    let msg: Rpc = serde_json::from_slice(&bytes).unwrap();
    match msg {
        Rpc::RequestVote { term, candidate_id } => {
            let address = &node.peers.get((candidate_id - 1) as usize).unwrap().addr;
            let vote = serde_json::to_vec(&Rpc::VoteResponse { term, granted: true }).unwrap();
            println!("{}",address);
            let mut conn = TcpStream::connect(address).await.unwrap();
            println!("Connected");
            let _ = conn.write_all(&vote).await;
        }, 
        Rpc::Hearbeat { term, leader_id } => {
            let mut heartbeat = heartbeat_guard.lock().await;
            *heartbeat = Instant::now();
        }
        _ => {
        }
    }
}   