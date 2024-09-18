use crate::paxos::paxos_api::{BallotNum, PaxosInstanceId, Proposer, Value};
use crate::paxos::start_servers;

mod paxos;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "trace");
    env_logger::init();
    let ids = vec![1, 2, 3];
     start_servers(&ids).await?;

    {
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
        p1.run_paxos(&ids, Value { vi64: 100 }).await;
    }


    Ok(())
}