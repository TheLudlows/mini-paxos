use crate::paxos::paxos_api::{BallotNum, PaxosInstanceId, Proposer};
use crate::paxos::start_servers;

mod paxos;
#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let ids = vec![1,2,3];
    let servers = start_servers(&ids).await?;

    {
       let p1 =  Proposer {
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

        
    }


    Ok(())

}