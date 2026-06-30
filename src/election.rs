use std::{sync::Arc};

use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::types::{Role::{self, Candidate}, Rpc, ThisNode};

pub async fn election(me: Arc<Mutex<ThisNode>>) {
    println!("Starting election");
    let mut buf = BytesMut::new();
    let mut me_guard = me.lock().await;
    me_guard.current_term += 1;
    me_guard.role = Candidate;
    let mut total_votes = 0;
    let vote = Rpc::RequestVote {
        term: me_guard.current_term,
        candidate_id: me_guard.id,
    };
    for i in me_guard.peers.iter() {
        if i.id == me_guard.id || i.conn.is_none() {
            continue;
        }
        send_request(&i.addr, &vote, &mut buf).await;
        check_vote(&mut buf, &mut total_votes).await;
        println!("{}", total_votes);
        if total_votes >= 2 {
            announce_leadership(me.clone()).await;
            break;
        }
    }
    me_guard.role = Role::Leader;
}

pub async fn send_request(address: &String, vote: &Rpc, buf: &mut BytesMut) {
    let mut connection = TcpStream::connect(address).await.unwrap();
    let mut bytes = serde_json::to_vec(vote).unwrap();
    bytes.push(b'\n');
    println!("{}", String::from_utf8(bytes.to_vec()).unwrap());
    let _ = connection.write_all(&bytes).await;
    println!("Wrote to {}", address);
    loop {
        let _ = connection.read_buf(buf).await;
        if let Some(_) = buf.iter().position(|x| *x == b'\n') {
            return;
        }
    }
}

pub async fn check_vote(buf: &mut BytesMut, total_votes: &mut u64) {
    let bytes = buf.split();
    println!("{}", String::from_utf8(bytes.to_vec()).unwrap());
    let vote: Rpc = serde_json::from_slice(&bytes).unwrap();
    match vote {
        Rpc::VoteResponse { term, granted } => {
            if granted == true {
                *total_votes += 1
            }
        }
        _ => {}
    }
}

pub async fn announce_leadership(me: Arc<Mutex<ThisNode>>) {
    let mut node = me.lock().await;
    node.current_leader = Some(node.id);
    let my_id = node.id;
    let msg = serde_json::to_vec(&Rpc::LeaderAnnounce { leader_id: my_id }).unwrap();
    for i in node.peers.iter_mut() {
        if i.id == my_id || i.conn.is_none() {
            continue;
        };
        if let Some(conn) = &mut i.conn {
            let _ = conn.write_all(&msg).await;
        }
    }
}