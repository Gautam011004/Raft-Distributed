use std::sync::{Arc};

use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::types::{Rpc, ThisNode};

pub async fn request_reader(mut socket: TcpStream, me: Arc<Mutex<ThisNode>>) {
    let mut buf = BytesMut::new();
    loop {
        if let Some(s) = buf.iter().position(|x| *x == b'n') {
            vote_caster(&mut buf, s, me.clone()).await;
        }
        socket.read(&mut buf).await.unwrap();
        if let Some(s) = buf.iter().position(|x| *x == b'n') {
            vote_caster(&mut buf, s, me.clone()).await;
        }
    }
}

pub async fn vote_caster(buf: &mut BytesMut, pos: usize, me: Arc<Mutex<ThisNode>>) {
    let node = me.lock().await;
    let bytes = buf.split_to(pos + 1);
    let msg: Rpc = serde_json::from_slice(&bytes).unwrap();
    match msg {
        Rpc::RequestVote { term, candidate_id } => {
            let address = &node.peers.get((candidate_id - 1) as usize).unwrap().addr;
            let vote = serde_json::to_vec(&Rpc::VoteResponse { term, granted: true }).unwrap();
            let mut conn = TcpStream::connect(address).await.unwrap();
            let _ = conn.write_all(&vote).await;
        }, 
        _ => {
        }
    }
}   