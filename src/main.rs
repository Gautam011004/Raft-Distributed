use std::{ptr::read, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use rand::RngExt;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex, time::{Instant, sleep}};

use crate::{distributed::request_reader, types::{Peer, Role::{Candidate, Follower}, Rpc, ThisNode}};


pub mod types;
pub mod distributed;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:7002").await.unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    
    let heartbeat = Arc::new(Mutex::new(Instant::now()));

    let peers = vec![
        Peer { id: 1, addr: "127.0.0.1:7000".into() },
        Peer { id: 2, addr: "127.0.0.1:7001".into() },
        Peer { id: 3, addr: "127.0.0.1:7002".into() },
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

    let stream = Arc::new(Mutex::new(TcpStream::connect("127.0.0.1:7000").await.unwrap()));
    
    let watchdog = tokio::spawn(workdogfn(heartbeat.clone(), me.clone()));
    let heartbeat_reader = tokio::spawn(reader(stream.clone(), heartbeat.clone()));
    let vote_reader = tokio::spawn(request_reader(socket, me));

    let _ = tokio::join!(watchdog, heartbeat_reader, vote_reader);
}

async fn workdogfn(heartbeat_guard: Arc<Mutex<Instant>>, me: Arc<Mutex<ThisNode>>) {
    let x;
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
            election(me).await;
            break;
        }
    }
}
async fn reader(socket: Arc<Mutex<TcpStream>>, hearbeat_guard: Arc<Mutex<Instant>>) {
    let mut buf = BytesMut::new();
    let mut stream = socket.lock().await;
    loop {
        if let Some(s) = buf.iter().position(|x|*x == b'\n') {
            deserialize(&mut buf, s);
        }
        stream.read_buf(&mut buf).await.unwrap();
        if let Some(s) = buf.iter().position(|x|*x == b'\n') {
            deserialize(&mut buf, s);
        } else {
            continue;
        }
        let mut last_heartbeat = hearbeat_guard.lock().await;
        *last_heartbeat = Instant::now();

    }
}
pub fn deserialize(buf: &mut BytesMut, pos: usize) -> Rpc {
    let new = buf.split_to(pos + 1);
    println!("{}", String::from_utf8(new.to_vec()).unwrap());
    let msg: Rpc = serde_json::from_slice(&new).unwrap();
    msg
}

pub async fn election(me: Arc<Mutex<ThisNode>>) {
    let mut buf = BytesMut::new();
    let mut me_guard = me.lock().await;
    me_guard.current_term += 1;
    me_guard.role = Candidate;
    let total_votes = 1;
    let vote = Rpc::RequestVote { term: me_guard.current_term, candidate_id: me_guard.id };
    for i in me_guard.peers.iter() {
        if i.id == me_guard.id || i.id == me_guard.current_leader {
            continue;
        }
        send_request(&i.addr, vote.clone(), &mut buf).await;
        if total_votes >= 2 {
            announce_leadership(&mut buf);
        }
        
    }
}

pub async fn send_request(address: &String, vote: Rpc, buf: &mut BytesMut) {
    let mut connection = TcpStream::connect(address).await.unwrap();
    let bytes = serde_json::to_vec(&vote).unwrap();
    let _ = connection.write_all(&bytes).await;
    loop {
        if let Some(_) = buf.iter().position(|x| *x == b'n') {
            return 
        }
    }
}

pub async fn announce_leadership(buf: &mut BytesMut) {
    
}

