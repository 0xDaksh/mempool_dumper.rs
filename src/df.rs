use polars::prelude::{CsvWriter, DataFrame, NamedFrom, PolarsResult, SerWriter, Series};

use crate::tx_entry::TxEntry;

pub fn process_batch(entries: &mut Vec<TxEntry>, main_df: &mut DataFrame, filename: &str) {
    let new_df = serialize_df(entries).unwrap();

    *main_df = main_df.vstack(&new_df).unwrap();

    println!("{:?}", entries.len());

    df_to_csv(main_df, filename).unwrap();
}

// TODO: need to improve, thx perplexity
pub fn serialize_df(entries: &[TxEntry]) -> PolarsResult<DataFrame> {
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
