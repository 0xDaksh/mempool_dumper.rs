use dotenv::dotenv;

use ethers::prelude::*;
use ethers::providers::{Provider, Ws};
use std::sync::Arc;

mod df;
mod tx_entry;

use df::{process_batch, serialize_df};
use tx_entry::TxEntry;

const BATCH_SIZE: usize = 32;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let rpc_uri = std::env::var("RPC_URI").expect("RPC_URI must be set.");
    let filename = std::env::var("FILENAME").expect("RPC_URI must be set.");

    let client = get_client(rpc_uri.as_str())
        .await
        .expect("Failed to create client!");

    let mut stream = client
        .subscribe_full_pending_txs()
        .await
        .expect("Failed to subscribe blocks!");

    let mut entries = Vec::<TxEntry>::new();
    let mut main_df = serialize_df(&entries).unwrap();

    while let Some(tx) = stream.next().await {
        println!("{:?}", tx.hash);
        entries.push(TxEntry::new(tx));

        if entries.len() >= BATCH_SIZE {
            println!("Dumping {} transactions!", BATCH_SIZE);
            process_batch(&mut entries, &mut main_df, filename.as_str());
            println!("Dumped to csv!");
        }
    }
}

async fn get_client(rpc_uri: &str) -> eyre::Result<Arc<Provider<Ws>>> {
    let provider = Provider::<Ws>::connect(rpc_uri).await?;

    Ok(Arc::new(provider))
}
