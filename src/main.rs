use std::{clone, ptr::read, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use rand::RngExt;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex, time::{Instant, sleep}};

use crate::{connection::connect, distributed::request_reader, types::{Peer, Role::{self, Candidate, Follower}, Rpc, ThisNode}};


pub mod types;
pub mod distributed;
pub mod connection;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:7002").await.unwrap();
    let (socket, a) = listener.accept().await.unwrap();
    
    let stream = Arc::new(Mutex::new(socket));

    let heartbeat = Arc::new(Mutex::new(Instant::now()));

    let peers = vec![
        Peer { id: 1, addr: "127.0.0.1:7000".into(), conn: None },
        Peer { id: 2, addr: "127.0.0.1:7001".into(), conn: None },
        Peer { id: 3, addr: "127.0.0.1:7002".into(), conn: None },
    ];

    let me = Arc::new(Mutex::new(ThisNode {
        current_term: 0,
        id: 3,
        role: Follower,
        peers: peers,
        last_heartbeat: Instant::now(),
        voted_for: Some(3),
        current_leader: 1
    }));
    let node = me.clone();

    connect(me.clone()).await;
    
    
    let watchdog = tokio::spawn(workdogfn(heartbeat.clone(), me.clone()));
    let vote_reader = tokio::spawn(request_reader(stream.clone(), me.clone(), heartbeat.clone()));

    let heartbeat = tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            send_heartbeat(node.clone()).await;
        }
    });

    let _ = tokio::join!(watchdog, vote_reader, heartbeat);
}


pub async fn send_heartbeat(me: Arc<Mutex<ThisNode>>) {
    let mut node = me.lock().await;
    let leader_id = node.id.clone();
    let msg = Rpc::Hearbeat { term: node.current_term, leader_id: leader_id };
    let bytes = serde_json::to_vec(&msg).unwrap();
    if node.role == Role::Leader {
        for i in node.peers.iter_mut() {
            if i.id == leader_id {
                continue;
            }
            let _ = i.conn.as_mut().unwrap().write_all(&bytes).await;
        }
    }
}


async fn workdogfn(heartbeat_guard: Arc<Mutex<Instant>>, me: Arc<Mutex<ThisNode>>) {
    let mut x;
    loop {
        sleep(Duration::from_secs(1)).await;
        let heartbeat = heartbeat_guard.lock().await;
        if Instant::now().duration_since(*heartbeat) > Duration::from_secs(5) {
            println!("Leader died");
            {
                let mut rng = rand::rng();
                x = rng.random_range(1..5);
            }
            sleep(Duration::from_secs(x)).await;
            if Instant::now().duration_since(*heartbeat) < Duration::from_secs(5) {
                continue;
            }
            election(me).await;
            break;
        }
    }
}

pub async fn election(me: Arc<Mutex<ThisNode>>) {
    let mut buf = BytesMut::new();
    let mut me_guard = me.lock().await;
    me_guard.current_term += 1;
    me_guard.role = Candidate;
    let mut total_votes = 1;
    let vote = Rpc::RequestVote { term: me_guard.current_term, candidate_id: me_guard.id };
    for i in me_guard.peers.iter() {
        if i.id == me_guard.id || i.id == me_guard.current_leader {
            continue;
        }
        send_request(&i.addr, vote.clone(), &mut buf).await;
        check_vote(&mut buf, &mut total_votes).await;
        if total_votes >= 2 {
            break;
        }
    }
    me_guard.role = Role::Leader;
}

pub async fn send_request(address: &String, vote: Rpc, buf: &mut BytesMut) {
    let mut connection = TcpStream::connect(address).await.unwrap();
    let bytes = serde_json::to_vec(&vote).unwrap();
    let _ = connection.write_all(&bytes).await;
    loop {
        let _ = connection.read(buf).await;
        if let Some(_) = buf.iter().position(|x| *x == b'n') {
            return 
        }
    }
}

pub async fn check_vote(buf: &mut BytesMut, total_votes: &mut u64) {
    let bytes = buf.split();
    let vote: Rpc =  serde_json::from_slice(&bytes).unwrap();
    match vote {
        Rpc::VoteResponse { term, granted } => {
            if granted == true {
                *total_votes += 1
            }
        },
        _ => {}
    }

}
