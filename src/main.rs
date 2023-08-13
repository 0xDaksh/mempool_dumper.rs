use dotenv::dotenv;

use ethers::prelude::*;
use ethers::providers::{Provider, Ws};
use polars::prelude::*;
use std::sync::Arc;

struct TxSummaryEntry {
    from: H160,
    to: H160,
    gas: U256,
    gas_price: U256,
    hash: H256,
    input: Bytes,
    nonce: U256,
    tx_type: U64,
    max_fee_per_gas: U256,
    max_priority_fee_per_gas: U256,
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let client = get_client().await.expect("Failed to create client!");
    let filename = std::env::var("FILENAME").expect("RPC_URI must be set.");

    let mut stream = client
        .subscribe_full_pending_txs()
        .await
        .expect("Failed to subscribe blocks!");

    let mut summary_entries = Vec::<TxSummaryEntry>::new();
    let mut main_df = entries_to_dataframe(&summary_entries).unwrap();

    while let Some(tx) = stream.next().await {
        println!("{:?}", tx.hash);
        let entry = TxSummaryEntry {
            from: tx.from,
            to: tx.to.unwrap(),
            gas: tx.gas,
            gas_price: tx.gas_price.unwrap(),
            hash: tx.hash,
            input: tx.input,
            nonce: tx.nonce,
            tx_type: tx.transaction_type.unwrap(),
            max_fee_per_gas: match tx.max_fee_per_gas {
                Some(x) => x,
                None => U256::from(0),
            },
            max_priority_fee_per_gas: match tx.max_priority_fee_per_gas {
                Some(x) => x,
                None => U256::from(0),
            },
        };

        summary_entries.push(entry);

        if summary_entries.len() > 20 {
            println!("Dumping 20 transactions!");
            let df = entries_to_dataframe(&summary_entries).unwrap();
            main_df = main_df.vstack(&df).unwrap();
            summary_entries.clear();
            df_to_csv(&mut main_df, filename.as_str()).unwrap();
            println!("Dumped to csv!");
        }
    }
}

async fn get_client() -> eyre::Result<Arc<Provider<Ws>>> {
    let rpc_uri = std::env::var("RPC_URI").expect("RPC_URI must be set.");
    let provider = Provider::<Ws>::connect(rpc_uri).await.unwrap();

    Ok(Arc::new(provider))
}

// TODO: need to improve, thx perplexity
fn entries_to_dataframe(entries: &[TxSummaryEntry]) -> PolarsResult<DataFrame> {
    let froms: Vec<String> = entries.iter().map(|t| format!("{:?}", t.from)).collect();
    let tos: Vec<String> = entries.iter().map(|t| format!("{:?}", t.to)).collect();
    let gas: Vec<u64> = entries.iter().map(|t| t.gas.as_u64()).collect();
    let gas_prices: Vec<u64> = entries.iter().map(|t| t.gas_price.as_u64()).collect();
    let hashes: Vec<String> = entries.iter().map(|t| format!("{:?}", t.hash)).collect();
    let inputs: Vec<String> = entries.iter().map(|t| format!("{:?}", t.input)).collect();
    let nonces: Vec<u64> = entries.iter().map(|t| t.nonce.as_u64()).collect();
    let tx_types: Vec<u64> = entries.iter().map(|t| t.tx_type.as_u64()).collect();
    let max_fee_per_gas: Vec<u64> = entries.iter().map(|t| t.max_fee_per_gas.as_u64()).collect();
    let max_priority_fee_per_gas: Vec<u64> = entries
        .iter()
        .map(|t| t.max_priority_fee_per_gas.as_u64())
        .collect();

    let from_series = Series::new("from", froms);
    let to_series = Series::new("to", tos);
    let gas_series = Series::new("gas", gas);
    let gas_price_series = Series::new("gas_price", gas_prices);
    let hash_series = Series::new("hash", hashes);
    let input_series = Series::new("input", inputs);
    let nonce_series = Series::new("nonce", nonces);
    let tx_type_series = Series::new("tx_type", tx_types);
    let max_fee_per_gas_series = Series::new("max_fee_per_gas", max_fee_per_gas);
    let max_priority_fee_per_gas_series =
        Series::new("max_priority_fee_per_gas", max_priority_fee_per_gas);

    DataFrame::new(vec![
        from_series,
        to_series,
        gas_series,
        gas_price_series,
        hash_series,
        input_series,
        nonce_series,
        tx_type_series,
        max_fee_per_gas_series,
        max_priority_fee_per_gas_series,
    ])
}

fn df_to_csv(df: &mut DataFrame, filename: &str) -> PolarsResult<()> {
    let file = std::fs::File::create(filename).unwrap();
    CsvWriter::new(file).finish(df)
}
