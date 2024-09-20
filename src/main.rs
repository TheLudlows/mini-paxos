use std::time::Duration;
use tokio::join;
use tokio::time::sleep;
use crate::paxos::paxos_api::{BallotNum, PaxosInstanceId, Proposer, Value};
use crate::paxos::start_server;

mod paxos;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "trace");
    env_logger::init();
    join!(
        tokio::spawn(start_server(1)),
        tokio::spawn(start_server(2)),
        tokio::spawn(start_server(3)),
        tokio::spawn(client())
    );
    Ok(())
}

async fn client() {
    sleep(Duration::from_secs(10)).await;
    let ids = vec![1, 2, 3];

    let mut p1 = Proposer {
        id: Some(PaxosInstanceId {
            key: "foo".to_string(),
            ver: 0,
        }),
        bal: Some(BallotNum {
            n: 0,
            proposer_id: 1,
        }),
        val: None,
    };
    p1.run_paxos(&ids, Some(Value { vi64: 100 })).await;

    let mut p2 = Proposer {
        id: Some(PaxosInstanceId {
            key: "foo".to_string(),
            ver: 0,
        }),
        bal: Some(BallotNum {
            n: 0,
            proposer_id: 2,
        }),
        val: None,
    };
    let r = p2.run_paxos(&ids, None).await;
    println!("get res {:?}", r);

    let r = p2.run_paxos(&ids, None).await;
    println!("get res {:?}", r);

}