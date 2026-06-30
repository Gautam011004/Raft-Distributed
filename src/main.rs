use std::{sync::Arc, time::Duration};
use rand::RngExt;
use tokio::{
    io::{AsyncWriteExt},
    net::{TcpListener},
    sync::Mutex,
    time::{Instant, sleep},
};

use crate::{
    connection::{connect_to_peers, handle_connection, retry_conn}, election::election, types::{
        Peer,
        Role::{self, Follower},
        Rpc, ThisNode,
    },
};

pub mod connection;
pub mod distributed;
pub mod types;
pub mod election;

#[tokio::main]
async fn main() {
    let heartbeat = Arc::new(Mutex::new(Instant::now()));

    let peers = vec![
        Peer {
            id: 0,
            addr: "127.0.0.1:7000".into(),
            conn: None,
        },
        Peer {
            id: 1,
            addr: "127.0.0.1:7001".into(),
            conn: None,
        },
        Peer {
            id: 2,
            addr: "127.0.0.1:7002".into(),
            conn: None,
        },
    ];

    let me = Arc::new(Mutex::new(ThisNode {
        current_term: 0,
        id: 3,
        role: Follower,
        peers: peers,
        last_heartbeat: Instant::now(),
        voted_for: Some(3),
        current_leader: None,
    }));
    let node = me.clone();

    let listener = TcpListener::bind("127.0.0.1:7002").await.unwrap();

    tokio::spawn(handle_connection(listener, me.clone(), heartbeat.clone()));

    let connector = tokio::spawn(connect_to_peers(me.clone()));

    connector.await.unwrap();


    let watchdog = tokio::spawn(workdogfn(heartbeat.clone(), me.clone()));

    let heartbeat = tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            send_heartbeat(node.clone()).await;
        }
    });

    let retry_node = me.clone();

    tokio::spawn(async move {
        loop {
            retry_conn(retry_node.clone()).await;
            sleep(Duration::from_secs(2)).await;
        }
    });

    let _ = tokio::join!(watchdog, heartbeat);
}

pub async fn send_heartbeat(me: Arc<Mutex<ThisNode>>) {
    let mut node = me.lock().await;
    let leader_id = node.id.clone();
    let msg = Rpc::Hearbeat {
        term: node.current_term,
        leader_id: leader_id,
    };
    let mut bytes = serde_json::to_vec(&msg).unwrap();
    bytes.push(b'\n');
    if node.role == Role::Leader {
        for i in node.peers.iter_mut() {
            if i.id == leader_id || i.conn.is_none() {
                continue;
            }
            println!("Writin to {}, - {:?}", i.addr, String::from_utf8(bytes.to_vec()).unwrap());
            let _ = i.conn.as_mut().unwrap().write_all(&bytes).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn workdogfn(heartbeat_guard: Arc<Mutex<Instant>>, me: Arc<Mutex<ThisNode>>) {
    let mut x;
    let mut beat: Instant;
    loop {
        sleep(Duration::from_secs(1)).await;
        {
            let heartbeat = heartbeat_guard.lock().await;
            beat = *heartbeat;
        }
        if Instant::now().duration_since(beat) > Duration::from_secs(5) {
            println!("Leader died");
            {
                let mut rng = rand::rng();
                x = rng.random_range(1..5);
            }
            sleep(Duration::from_secs(10)).await;
            {
                let heartbeat = heartbeat_guard.lock().await;
                beat = *heartbeat;
            }
            if Instant::now().duration_since(beat) < Duration::from_secs(5) {
                continue;
            }
            {
                let mut node = me.lock().await;
                if let Some(s) = node.current_leader {
                    let leader = node.peers.get_mut(s as usize).unwrap();
                    leader.conn = None;
                }
            }
            election(me).await;
            break;
        }
    }
}