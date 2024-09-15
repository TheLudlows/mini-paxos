use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use log::info;
use tonic::{IntoRequest, Request, Response, Status};
use tonic::transport::Server;
use crate::paxos_api::{Acceptor, BallotNum, PaxosInstanceId, Proposer};

pub mod paxos_api {
    include!("../protos/paxos_api.rs");
}
impl PartialOrd<Self> for BallotNum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let r = self.n.partial_cmp(&other.n);
        match r {
            None => { None }
            Some(ord) => {
                if ord.is_eq() {
                    return self.proposer_id.partial_cmp(&other.proposer_id);
                }
                Some(ord)
            }
        }
    }
}

impl Eq for BallotNum {
}

impl Ord for BallotNum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
type Versions = HashMap<i64, Version>;
#[derive(Default, Clone)]
struct Version {
    val: Acceptor,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    std::env::set_var("RUST_LOG", "trace");
    env_logger::init();

    let address = "127.0.0.1:8080".parse().unwrap();
    let paxos_service = PaxosService::default();
    Server::builder()
        .add_service(paxos_api::paxos_kv_server::PaxosKvServer::new(paxos_service))
        .serve(address)
        .await?;
    Ok(())
}

#[derive(Default, Clone)]
pub struct PaxosService {
    kv: Arc<Mutex<HashMap<String, Versions>>>,
}
impl PaxosService {
    pub fn get_version(&self, id: PaxosInstanceId) -> Version {
        let mut kv = self.kv.lock().unwrap();
        let key = &id.key;
        let version = id.ver;

        let vs = kv.entry(key.clone()).or_insert(Versions::default());
        let v = vs.entry(version).or_insert(Version::default());
        v.clone()
    }
}

#[tonic::async_trait]
impl paxos_api::paxos_kv_server::PaxosKv for PaxosService {
    async fn prepare(&self, request: Request<Proposer>) -> Result<Response<Acceptor>, Status> {
        info!("receive prepared req {:?}", request);
        let p = request.into_inner();
        let v = self.get_version(p.id.unwrap());
        let val = &v.val;
        if p.bal.unwrap().ge(&val.last_bal.unwrap())
        {

        }
        todo!()
    }

    async fn accept(&self, request: Request<Proposer>) -> Result<Response<Acceptor>, Status> {
        todo!()
    }
}