use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    time::{Instant, sleep},
};

use crate::types::{Role, Rpc, ThisNode};

pub async fn request_reader(
    mut conn: TcpStream,
    me: Arc<Mutex<ThisNode>>,
    heartbeat_guard: Arc<Mutex<Instant>>,
) {
    let mut buf = BytesMut::new();
    loop {
        if let Some(s) = buf.iter().position(|x| *x == b'\n') {
            vote_caster(&mut buf, s, me.clone(), heartbeat_guard.clone(), &mut conn).await;
        }
        let n = conn.read_buf(&mut buf).await.unwrap();

        if n == 0 {
            break;
        }
        if let Some(s) = buf.iter().position(|x| *x == b'\n') {
            vote_caster(&mut buf, s, me.clone(), heartbeat_guard.clone(), &mut conn).await;
        }
    }
}

pub async fn vote_caster(
    buf: &mut BytesMut,
    pos: usize,
    me: Arc<Mutex<ThisNode>>,
    heartbeat_guard: Arc<Mutex<Instant>>,
    stream: &mut TcpStream
) {
    let node = me.lock().await;
    let bytes = buf.split_to(pos + 1);
    println!("{}", String::from_utf8(bytes.to_vec()).unwrap());
    let msg: Rpc = serde_json::from_slice(&bytes).unwrap();
    match msg {
        Rpc::RequestVote { term, candidate_id } => {
            let mut vote = serde_json::to_vec(&Rpc::VoteResponse {
                term,
                granted: true,
            })
            .unwrap();
            vote.push(b'\n');
            stream.write_all(&vote).await.unwrap()
        }
        Rpc::Hearbeat { term, leader_id } => {
            let mut heartbeat = heartbeat_guard.lock().await;
            *heartbeat = Instant::now();
        }
        _ => {}
    }
}
