use ethers::prelude::{Bytes, Transaction, H160, H256, U256, U64};

pub struct TxEntry {
    pub from: H160,
    pub to: H160,
    pub gas: U256,
    pub gas_price: U256,
    pub hash: H256,
    pub input: Bytes,
    pub nonce: U256,
    pub tx_type: U64,
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
}

impl TxEntry {
    pub fn new(tx: Transaction) -> TxEntry {
        TxEntry {
            from: tx.from,
            to: tx.to.unwrap_or_else(|| H160::zero()),
            gas: tx.gas,
            gas_price: tx.gas_price.unwrap_or_else(|| U256::from(0)),
            hash: tx.hash,
            input: tx.input,
            nonce: tx.nonce,
            tx_type: tx.transaction_type.unwrap_or_else(|| U64::from(0)),
            max_fee_per_gas: tx.max_fee_per_gas.unwrap_or_else(|| U256::from(0)),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.unwrap_or_else(|| U256::from(0)),
        }
    }
}
