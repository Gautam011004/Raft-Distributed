use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, time::Instant};

pub struct Peer {
    pub id: u64,
    pub addr: String,
    pub conn: Option<TcpStream>
}
pub struct Node {
    pub id: u64,
    pub peers: Vec<Peer>
}

#[derive(PartialEq)]
pub enum Role {
    Leader,
    Follower,
    Candidate
}

pub struct ThisNode {
    pub id: u64,
    pub role: Role,
    pub current_term: u64,
    pub peers: Vec<Peer>,
    pub last_heartbeat: Instant,
    pub voted_for: Option<u64>,
    pub current_leader: Option<u64>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Rpc {
    Hearbeat {
        term: u64,
        leader_id: u64
    },
    RequestVote {
        term: u64,
        candidate_id: u64
    },
    VoteResponse {
        term: u64,
        granted: bool
    },
    LeaderAnnounce {
        leader_id: u64
    }
}