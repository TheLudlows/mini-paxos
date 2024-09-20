use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::format;
use thiserror::Error;

use std::sync::{Arc};
use anyhow::{anyhow, Error};
use futures::{join, try_join};
use futures::future::join_all;
use log::{error, info};
use parking_lot::{Mutex, MutexGuard};
use tokio::io::join;
use tokio::runtime::Runtime;
use tonic::{IntoRequest, Request, Response, Status};
use tonic::transport::Server;
use crate::paxos::paxos_api::{Acceptor, BallotNum, PaxosInstanceId, Proposer, Value};
use crate::paxos::paxos_api::paxos_kv_client::PaxosKvClient;
use crate::paxos::paxos_api::paxos_kv_server::PaxosKvServer;
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


pub async fn start_server(id: i64) -> anyhow::Result<()> {
    let address = format!("127.0.0.1:{}", 8000 + id).parse()?;
    let paxos_service = PaxosService::new(id);
    info!("start server {}!", id);
    Server::builder().add_service(PaxosKvServer::new(paxos_service))
        .serve(address).await?;
    Ok(())
}

#[derive(Default, Clone)]
pub struct PaxosService {
    server_id: i64,
    kv: Arc<Mutex<HashMap<String, Versions>>>,
}
impl PaxosService {
    pub fn new(id: i64) -> Self {
        Self {
            server_id: id,
            kv: Arc::new(Default::default()),
        }
    }
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
        info!("Server {} receive prepared {:?}", self.server_id, request.get_ref());
        let p = request.into_inner();
        let lock = self.get_version(p.id.unwrap());
        let mut v = lock.lock();
        let mut val = &mut v.val;
        info!("old val is {:?}", val);
        if p.bal >= val.last_bal
        {
            val.last_bal = p.bal;
        }

        Ok(Response::new(val.clone()))
    }

    async fn accept(&self, request: Request<Proposer>) -> Result<Response<Acceptor>, Status> {
        info!("Server {} receive accept {:?}", self.server_id, request.get_ref());
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
    pub async fn run_paxos(&mut self, acceptor_ids: &[i64], val: Option<Value>) -> Option<Value> {
        let quorum = (acceptor_ids.len() / 2 + 1) as i32;
        self.val = val;
        loop {
            let r = self.ph1(acceptor_ids, quorum).await;
            if r.is_err() {
                error!("Proposer: ph1 error {:?}", r);
                self.bal?.n += 1;
                continue;
            }
            let (max_val, high_bal) = r.unwrap();
            if max_val.is_none() {
                info!("Proposer: no voted value seen, propose my value: {:?}", val);
            } else {
                self.val = max_val;
            }
            info!("Proposer: proposer chose value to propose: {:?}", val);
            if self.val.is_none() {
                return None;
            }

            let r = self.ph2(&acceptor_ids, quorum).await;
            if r.is_err() {
                info!("ph2 error ");
                self.bal?.n += 1;
                continue;
            }
            info!("Proposer: value is voted by a quorum and has been safe: {:?}",  self.val);
            return self.val;
        }
    }
    pub async fn ph1(&self, acceptors: &[i64], quorum: i32) -> anyhow::Result<(Option<Value>, Option<BallotNum>)> {
        let accs = self.rpc_to_all(&acceptors, ph1).await?;
        let mut max_bal = &accs[0];
        let mut ok = 0;
        let mut higher_bal = self.bal;

        for acc in accs.iter() {
            info!("Proposer: handling prepare reply: {:?}", acc);
            if higher_bal.lt(&acc.last_bal)
            {
                higher_bal = acc.last_bal;
                continue;
            }
            ok += 1;
            if acc.v_bal >= max_bal.v_bal {
                max_bal = acc;
            }
            if ok == quorum {
                return Ok((max_bal.val, None));
            }
        }
        Err(Error::from(NotEnoughQuorum))
    }

    pub async fn ph2(&self, acceptors: &[i64], quorum: i32) -> anyhow::Result<Option<BallotNum>> {
        let accs = self.rpc_to_all(&acceptors, ph2).await?;
        let mut ok = 0;
        let mut higher_bal = self.bal;
        for acc in accs.iter() {
            info!("Proposer: handling Accept reply:{:?}", acc);
            if higher_bal.lt(&acc.last_bal) {
                higher_bal = acc.last_bal;
                continue;
            }
            ok += 1;
            if ok == quorum {
                return Ok(None);
            }
        }
        Err(Error::from(NotEnoughQuorum))
    }


    pub async fn rpc_to_all(&self, acceptors: &[i64], rpc_type: RpcType) -> anyhow::Result<Vec<Acceptor>> {
        let mut acceptor_vec = Vec::new();
        for id in acceptors.iter() {
            let address = format!("http://127.0.0.1:{}", 8000 + *id);
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
