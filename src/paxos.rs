use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::format;
use thiserror::Error;

use std::sync::{Arc};
use anyhow::anyhow;
use log::info;
use parking_lot::{Mutex, MutexGuard};
use tonic::{IntoRequest, Request, Response, Status};
use tonic::transport::Server;
use crate::paxos::paxos_api::{Acceptor, BallotNum, PaxosInstanceId, Proposer, Value};
use crate::paxos::paxos_api::paxos_kv_client::PaxosKvClient;
use crate::paxos::PaxosError::NotEnoughQuorum;
use crate::paxos::RpcType::{ph1, ph2};

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
impl Eq for BallotNum {}
impl Ord for BallotNum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
type Versions = HashMap<i64, ArcVersion>;
type ArcVersion = Arc<Mutex<Version>>;
#[derive(Default, Clone)]
struct Version {
    val: Acceptor,
}



pub async fn  start_servers(ids: &[i64]) -> anyhow::Result<Vec<Server>> {
    std::env::set_var("RUST_LOG", "trace");
    env_logger::init();
    let mut servers = vec![];
    for i in ids {
        let address = format!("127.0.0.1:{}", 8000 + *i).parse()?;
        let paxos_service = PaxosService::default();
        let mut server = Server::builder();
        server.add_service(paxos_api::paxos_kv_server::PaxosKvServer::new(paxos_service))
            .serve(address)
            .await?;
        servers.push(server);
    }

    Ok(servers)
}

#[derive(Default, Clone)]
pub struct PaxosService {
    kv: Arc<Mutex<HashMap<String, Versions>>>,
}
impl PaxosService {
    pub fn get_version(&self, id: PaxosInstanceId) -> ArcVersion {
        let mut kv = self.kv.lock();
        let key = &id.key;
        let version = id.ver;

        let vs = kv.entry(key.clone()).or_insert(Versions::default());
        let v = vs.entry(version).or_insert(ArcVersion::default());
        v.clone()
    }
}

#[tonic::async_trait]
impl paxos_api::paxos_kv_server::PaxosKv for PaxosService {
    async fn prepare(&self, request: Request<Proposer>) -> Result<Response<Acceptor>, Status> {
        info!("receive prepared req {:?}", request);
        let p = request.into_inner();
        let lock = self.get_version(p.id.unwrap());
        let mut v = lock.lock();
        let mut val = &mut v.val;
        if p.bal >= val.last_bal
        {
            val.last_bal = p.bal;
        }
        Ok(Response::new(val.clone()))
    }

    async fn accept(&self, request: Request<Proposer>) -> Result<Response<Acceptor>, Status> {
        info!("receive accept req {:?}", request);
        let p = request.into_inner();
        let lock = self.get_version(p.id.unwrap());
        let mut v = lock.lock();
        let val = &mut v.val;
        let reply = val.clone();
        if p.bal.unwrap().ge(&val.last_bal.unwrap()) {
            val.last_bal = p.bal;
            val.v_bal = p.bal;
            val.val = p.val;
        }
        Ok(Response::new(reply))
    }
}
impl Proposer {
    pub async fn run_paxos(&mut self, acceptorIds: &[i64], val: Value) -> Value {
        let quorum = (acceptorIds.len() / 2 + 1) as i32;
        loop {
            let r =  self.ph1(acceptorIds, quorum).await;
            if r.is_err() {
                self.bal.unwrap().n += 1;
                continue;
            }
        }

    }
    pub async fn ph1(&self, acceptors:&[i64], quorum: i32) -> anyhow::Result<(Option<Value>, Option<BallotNum>, Option<PaxosError>)> {
        let accs = self.rpc_to_all(&acceptors, ph1).await?;
        let mut max_bal = &accs[0];
        let mut ok = 0;
        let mut higher_bal = BallotNum::default();

        for acc in accs.iter() {
            info!("Proposer: handling Prepare reply: {:?}", acc);
            if self.bal.lt(&acc.last_bal)
            {
                if acc.last_bal.unwrap().ge(&higher_bal)
                {
                    higher_bal = acc.last_bal.unwrap();
                }
                continue;
            }
            ok += 1;
            if acc.v_bal >= max_bal.v_bal {
                max_bal = acc;
            }
            if ok == quorum {
                return Ok((max_bal.val, None, None));
            }
        }
        Ok((None, Some(higher_bal), Some(NotEnoughQuorum)))
    }

    pub async fn ph2(&self, acceptors: Vec<i64>, quorum: i32) -> anyhow::Result<(Option<BallotNum>, Option<PaxosError>)> {
        let accs = self.rpc_to_all(&acceptors, ph2).await?;
        let mut ok = 0;
        let mut higher_bal = BallotNum::default();
        for acc in accs.iter() {
            info!("Proposer: handling Accept reply:{:?}", acc);
            if self.bal.lt(&acc.last_bal) {
                if (acc.last_bal.unwrap().ge(&higher_bal))
                {
                    higher_bal = acc.last_bal.unwrap();
                }
                continue;
            }
            ok += 1;
            if ok == quorum {
                return Ok((None, None));
            }
        }
        Ok((Some(higher_bal), Some(NotEnoughQuorum)))
    }


    pub async fn rpc_to_all(&self, acceptors: &[i64], rpc_type: RpcType) -> anyhow::Result<Vec<Acceptor>> {
        let mut acceptor_vec = Vec::new();
        for id in acceptors.iter() {
            let address = format!("127.0.0.1:{}", id);
            let mut client = PaxosKvClient::connect(address).await?;
            let res = if rpc_type == ph1 {
                client.prepare(Request::new(self.clone())).await
            } else {
                client.accept(Request::new(self.clone())).await
            };
            if res.is_ok() {
                acceptor_vec.push(res?.into_inner());
            } else {
                info!("ph1 can't send to {}", id)
            }
        }
        Ok(acceptor_vec)
    }
}
#[derive(Eq, PartialEq)]
enum RpcType {
    ph1,
    ph2,
}

#[derive(Error, Debug)]
enum PaxosError {
    #[error("NotEnoughQuorum")]
    NotEnoughQuorum
}
