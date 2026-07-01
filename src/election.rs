use std::{sync::Arc};

use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::{connection::send_msg, types::{Peer, Role::{self, Candidate}, Rpc, ThisNode}};

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
    let my_id = me_guard.id;
    for mut i in me_guard.peers.iter_mut() {
        if i.id == my_id|| i.conn.is_none() {
            continue;
        }
        send_request(&mut i, &vote, &mut buf).await;
        check_vote(&mut buf, &mut total_votes).await;
        println!("{}", total_votes);
        if total_votes >= 2 {
            announce_leadership(&mut me_guard.peers, my_id).await;
            break;
        }
    }
    me_guard.role = Role::Leader;
}

pub async fn send_request(mut peer: &mut Peer, vote: &Rpc, buf: &mut BytesMut) {
    let msg = serde_json::to_vec(vote).unwrap();
    send_msg(msg, &mut peer ).await;
    loop {
        let _ = peer.conn.as_mut().unwrap().read_buf(buf).await;
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

pub async fn announce_leadership(peers: &mut Vec<Peer>, id: u64) {
    let msg = serde_json::to_vec(&Rpc::LeaderAnnounce { leader_id: id }).unwrap();
    for i in peers.iter_mut() {
        if i.id == id || i.conn.is_none() {
            continue;
        };
        send_msg(msg.clone(), i).await;
    }
}