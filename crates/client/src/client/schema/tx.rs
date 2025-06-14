use crate::client::{
    PageDirection,
    PaginatedResult,
    PaginationRequest,
    schema::{
        Address,
        AssetId,
        ConnectionArgsFields,
        ConversionError,
        HexString,
        PageInfo,
        Tai64Timestamp,
        TransactionId,
        TxPointer,
        U16,
        U32,
        U64,
        UtxoId,
        coins::ExcludeInput,
        schema,
        tx::{
            transparent_receipt::Receipt,
            transparent_tx::Output,
        },
    },
    types::TransactionResponse,
};
use fuel_core_types::{
    fuel_tx,
    fuel_types::{
        Bytes32,
        canonical::Deserialize,
    },
    fuel_vm,
    services::executor::{
        TransactionExecutionResult,
        TransactionExecutionStatus,
    },
};
use std::convert::{
    TryFrom,
    TryInto,
};
use std::ops::Deref;

pub mod transparent_receipt;
pub mod transparent_tx;

#[derive(cynic::QueryVariables, Debug)]
pub struct TxIdArgs {
    pub id: TransactionId,
}

/// Retrieves the transaction in opaque form
#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "TxIdArgs"
)]
pub struct TransactionQuery {
    #[arguments(id: $id)]
    pub transaction: Option<OpaqueTransactionWithStatus>,
}

/// Retrieves the transaction in opaque form
#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "TxIdArgs"
)]
pub struct TransactionStatusQuery {
    #[arguments(id: $id)]
    pub transaction: Option<OpaqueTransactionStatus>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "ConnectionArgs"
)]
pub struct TransactionsQuery {
    #[arguments(after: $after, before: $before, first: $first, last: $last)]
    pub transactions: TransactionConnection,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct TransactionConnection {
    pub edges: Vec<TransactionEdge>,
    pub page_info: PageInfo,
}

impl TryFrom<TransactionConnection> for PaginatedResult<TransactionResponse, String> {
    type Error = ConversionError;

    fn try_from(conn: TransactionConnection) -> Result<Self, Self::Error> {
        let results: Result<Vec<TransactionResponse>, Self::Error> =
            conn.edges.into_iter().map(|e| e.node.try_into()).collect();

        Ok(PaginatedResult {
            cursor: conn.page_info.end_cursor,
            has_next_page: conn.page_info.has_next_page,
            has_previous_page: conn.page_info.has_previous_page,
            results: results?,
        })
    }
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct TransactionEdge {
    pub cursor: String,
    pub node: OpaqueTransactionWithStatus,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(graphql_type = "Transaction", schema_path = "./assets/schema.sdl")]
pub struct OpaqueTransaction {
    pub raw_payload: HexString,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(graphql_type = "Transaction", schema_path = "./assets/schema.sdl")]
pub struct OpaqueTransactionWithStatus {
    pub raw_payload: HexString,
    pub status: Option<TransactionStatus>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(graphql_type = "Transaction", schema_path = "./assets/schema.sdl")]
pub struct OpaqueTransactionStatus {
    pub status: Option<TransactionStatus>,
}

impl TryFrom<OpaqueTransaction> for fuel_tx::Transaction {
    type Error = ConversionError;

    fn try_from(value: OpaqueTransaction) -> Result<Self, Self::Error> {
        let bytes = &*value.raw_payload.0.0;
        fuel_tx::Transaction::from_bytes(bytes)
            .map_err(ConversionError::TransactionFromBytesError)
    }
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl", graphql_type = "Transaction")]
pub struct TransactionIdFragment {
    pub id: TransactionId,
}

#[derive(cynic::Enum, Copy, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub enum ReturnType {
    Return,
    ReturnData,
    Revert,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(graphql_type = "ProgramState", schema_path = "./assets/schema.sdl")]
pub struct ProgramState {
    pub return_type: ReturnType,
    pub data: HexString,
}

impl TryFrom<ProgramState> for fuel_vm::ProgramState {
    type Error = ConversionError;

    fn try_from(state: ProgramState) -> Result<Self, Self::Error> {
        Ok(match state.return_type {
            ReturnType::Return => fuel_vm::ProgramState::Return({
                let b = state.data.0.deref();
                let b: [u8; 8] =
                    b.try_into().map_err(|_| ConversionError::BytesLength)?;
                u64::from_be_bytes(b)
            }),
            ReturnType::ReturnData => fuel_vm::ProgramState::ReturnData({
                Bytes32::try_from(state.data.0.0.deref())?
            }),
            ReturnType::Revert => fuel_vm::ProgramState::Revert({
                let b = state.data.0.deref();
                let b: [u8; 8] =
                    b.try_into().map_err(|_| ConversionError::BytesLength)?;
                u64::from_be_bytes(b)
            }),
        })
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(cynic::InlineFragments, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub enum TransactionStatus {
    SubmittedStatus(SubmittedStatus),
    SuccessStatus(SuccessStatus),
    PreconfirmationSuccessStatus(PreconfirmationSuccessStatus),
    SqueezedOutStatus(SqueezedOutStatus),
    FailureStatus(FailureStatus),
    PreconfirmationFailureStatus(PreconfirmationFailureStatus),
    #[cynic(fallback)]
    Unknown,
}

#[allow(clippy::enum_variant_names)]
#[derive(cynic::InlineFragments, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "TransactionStatus"
)]
pub enum StatusWithTransaction {
    SubmittedStatus(SubmittedStatus),
    SuccessStatus(SuccessStatusWithTransaction),
    PreconfirmationSuccessStatus(PreconfirmationSuccessStatusWithTransaction),
    SqueezedOutStatus(SqueezedOutStatus),
    FailureStatus(FailureStatusWithTransaction),
    PreconfirmationFailureStatus(PreconfirmationFailureStatusWithTransaction),
    #[cynic(fallback)]
    Unknown,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct SubmittedStatus {
    pub time: Tai64Timestamp,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct SuccessStatus {
    pub block_height: U32,
    pub time: Tai64Timestamp,
    pub program_state: Option<ProgramState>,
    pub receipts: Vec<Receipt>,
    pub total_gas: U64,
    pub total_fee: U64,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl", graphql_type = "SuccessStatus")]
pub struct SuccessStatusWithTransaction {
    pub transaction: OpaqueTransaction,
    pub block_height: U32,
    pub time: Tai64Timestamp,
    pub program_state: Option<ProgramState>,
    pub receipts: Vec<Receipt>,
    pub total_gas: U64,
    pub total_fee: U64,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct ResolvedOutput {
    pub utxo_id: UtxoId,
    pub output: Output,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct PreconfirmationSuccessStatus {
    pub tx_pointer: TxPointer,
    pub transaction_id: TransactionId,
    pub total_fee: U64,
    pub total_gas: U64,
    pub receipts: Option<Vec<Receipt>>,
    pub resolved_outputs: Option<Vec<ResolvedOutput>>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "PreconfirmationSuccessStatus"
)]
pub struct PreconfirmationSuccessStatusWithTransaction {
    pub tx_pointer: TxPointer,
    pub transaction_id: TransactionId,
    pub total_fee: U64,
    pub total_gas: U64,
    pub transaction: Option<OpaqueTransaction>,
    pub receipts: Option<Vec<Receipt>>,
    pub resolved_outputs: Option<Vec<ResolvedOutput>>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct FailureStatus {
    pub block_height: U32,
    pub time: Tai64Timestamp,
    pub reason: String,
    pub program_state: Option<ProgramState>,
    pub receipts: Vec<Receipt>,
    pub total_gas: U64,
    pub total_fee: U64,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl", graphql_type = "FailureStatus")]
pub struct FailureStatusWithTransaction {
    pub transaction: OpaqueTransaction,
    pub block_height: U32,
    pub time: Tai64Timestamp,
    pub reason: String,
    pub program_state: Option<ProgramState>,
    pub receipts: Vec<Receipt>,
    pub total_gas: U64,
    pub total_fee: U64,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct PreconfirmationFailureStatus {
    pub tx_pointer: TxPointer,
    pub transaction_id: TransactionId,
    pub total_fee: U64,
    pub total_gas: U64,
    pub receipts: Option<Vec<Receipt>>,
    pub resolved_outputs: Option<Vec<ResolvedOutput>>,
    pub reason: String,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "PreconfirmationFailureStatus"
)]
pub struct PreconfirmationFailureStatusWithTransaction {
    pub tx_pointer: TxPointer,
    pub transaction_id: TransactionId,
    pub total_fee: U64,
    pub total_gas: U64,
    pub transaction: Option<OpaqueTransaction>,
    pub receipts: Option<Vec<Receipt>>,
    pub resolved_outputs: Option<Vec<ResolvedOutput>>,
    pub reason: String,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct SqueezedOutStatus {
    pub reason: String,
}

#[allow(clippy::enum_variant_names)]
#[derive(cynic::InlineFragments, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub enum DryRunTransactionStatus {
    SuccessStatus(DryRunSuccessStatus),
    FailureStatus(DryRunFailureStatus),
    #[cynic(fallback)]
    Unknown,
}

impl TryFrom<DryRunTransactionStatus> for TransactionExecutionResult {
    type Error = ConversionError;

    fn try_from(status: DryRunTransactionStatus) -> Result<Self, Self::Error> {
        Ok(match status {
            DryRunTransactionStatus::SuccessStatus(s) => {
                let receipts = s
                    .receipts
                    .into_iter()
                    .map(|receipt| receipt.try_into())
                    .collect::<Result<Vec<fuel_tx::Receipt>, _>>()?;
                TransactionExecutionResult::Success {
                    result: s.program_state.map(TryInto::try_into).transpose()?,
                    receipts,
                    total_gas: s.total_gas.0,
                    total_fee: s.total_fee.0,
                }
            }
            DryRunTransactionStatus::FailureStatus(s) => {
                let receipts = s
                    .receipts
                    .into_iter()
                    .map(|receipt| receipt.try_into())
                    .collect::<Result<Vec<fuel_tx::Receipt>, _>>()?;
                TransactionExecutionResult::Failed {
                    result: s.program_state.map(TryInto::try_into).transpose()?,
                    receipts,
                    total_gas: s.total_gas.0,
                    total_fee: s.total_fee.0,
                }
            }
            DryRunTransactionStatus::Unknown => {
                return Err(Self::Error::UnknownVariant("DryRunTxStatus"))
            }
        })
    }
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct DryRunSuccessStatus {
    pub program_state: Option<ProgramState>,
    pub receipts: Vec<Receipt>,
    pub total_gas: U64,
    pub total_fee: U64,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct DryRunFailureStatus {
    pub program_state: Option<ProgramState>,
    pub receipts: Vec<Receipt>,
    pub total_gas: U64,
    pub total_fee: U64,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct DryRunTransactionExecutionStatus {
    pub id: TransactionId,
    pub status: DryRunTransactionStatus,
}

impl TryFrom<DryRunTransactionExecutionStatus> for TransactionExecutionStatus {
    type Error = ConversionError;

    fn try_from(schema: DryRunTransactionExecutionStatus) -> Result<Self, Self::Error> {
        let id = schema.id.into();
        let status = schema.status.try_into()?;

        Ok(TransactionExecutionStatus { id, result: status })
    }
}

#[derive(cynic::QueryVariables, Debug)]
pub struct TransactionsByOwnerConnectionArgs {
    /// Select transactions based on related `owner`s
    pub owner: Address,
    /// Skip until cursor (forward pagination)
    pub after: Option<String>,
    /// Skip until cursor (backward pagination)
    pub before: Option<String>,
    /// Retrieve the first n transactions in order (forward pagination)
    pub first: Option<i32>,
    /// Retrieve the last n transactions in order (backward pagination).
    /// Can't be used at the same time as `first`.
    pub last: Option<i32>,
}

impl From<(Address, PaginationRequest<String>)> for TransactionsByOwnerConnectionArgs {
    fn from(r: (Address, PaginationRequest<String>)) -> Self {
        match r.1.direction {
            PageDirection::Forward => TransactionsByOwnerConnectionArgs {
                owner: r.0,
                after: r.1.cursor,
                before: None,
                first: Some(r.1.results),
                last: None,
            },
            PageDirection::Backward => TransactionsByOwnerConnectionArgs {
                owner: r.0,
                after: None,
                before: r.1.cursor,
                first: None,
                last: Some(r.1.results),
            },
        }
    }
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "TransactionsByOwnerConnectionArgs"
)]
pub struct TransactionsByOwnerQuery {
    #[arguments(owner: $owner, after: $after, before: $before, first: $first, last: $last)]
    pub transactions_by_owner: TransactionConnection,
}

#[derive(cynic::QueryVariables, Debug)]
pub struct StatusChangeSubscriptionArgs {
    pub id: TransactionId,
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub include_preconfirmation: Option<bool>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Subscription",
    variables = "StatusChangeSubscriptionArgs"
)]
pub struct StatusChangeSubscription {
    #[arguments(id: $id, includePreconfirmation: $include_preconfirmation)]
    pub status_change: TransactionStatus,
}

// mutations

#[derive(cynic::QueryVariables)]
pub struct TxArg {
    pub tx: HexString,
}

#[derive(cynic::QueryVariables)]
pub struct TxWithEstimatedPredicatesArg {
    pub tx: HexString,
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub estimate_predicates: Option<bool>,
}

#[derive(cynic::QueryVariables)]
pub struct SubmitAndAwaitStatusArg {
    pub tx: HexString,
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub estimate_predicates: Option<bool>,
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub include_preconfirmation: Option<bool>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "TxArg"
)]
pub struct EstimatePredicates {
    #[arguments(tx: $tx)]
    pub estimate_predicates: OpaqueTransaction,
}

#[derive(cynic::InputObject, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct Predicate {
    pub predicate_address: Address,
    pub predicate: HexString,
    pub predicate_data: HexString,
}

#[derive(cynic::InputObject, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct Account {
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub address: Option<Address>,
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<Predicate>,
}

#[derive(cynic::InputObject, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct ChangePolicy {
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub change: Option<Address>,
    #[cynic(skip_serializing_if = "Option::is_none")]
    pub destroy: Option<Destroy>,
}

#[derive(cynic::Enum, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub enum Destroy {
    Destroy,
}

#[derive(cynic::InputObject, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct RequiredBalance {
    pub asset_id: AssetId,
    pub amount: U64,
    pub account: Account,
    pub change_policy: ChangePolicy,
}

#[derive(cynic::QueryVariables)]
pub struct AssembleTxArg {
    pub tx: HexString,
    pub block_horizon: U32,
    pub required_balances: Vec<RequiredBalance>,
    pub fee_address_index: U16,
    pub exclude_input: Option<ExcludeInput>,
    pub estimate_predicates: bool,
    pub reserve_gas: Option<U64>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "AssembleTxArg"
)]
pub struct AssembleTx {
    #[arguments(
        tx: $tx,
        blockHorizon: $block_horizon,
        requiredBalances: $required_balances,
        feeAddressIndex: $fee_address_index,
        excludeInput: $exclude_input,
        estimatePredicates: $estimate_predicates,
        reserveGas: $reserve_gas,
    )]
    pub assemble_tx: AssembleTransactionResult,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct AssembleTransactionResult {
    pub transaction: OpaqueTransaction,
    pub status: DryRunTransactionStatus,
    pub gas_price: U64,
}

#[derive(cynic::QueryVariables)]
pub struct DryRunArg {
    pub txs: Vec<HexString>,
    pub utxo_validation: Option<bool>,
    pub gas_price: Option<U64>,
    pub block_height: Option<U32>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Mutation",
    variables = "DryRunArg"
)]
pub struct DryRun {
    #[arguments(txs: $txs, utxoValidation: $utxo_validation, gasPrice: $gas_price, blockHeight: $block_height)]
    pub dry_run: Vec<DryRunTransactionExecutionStatus>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl")]
pub struct DryRunStorageReads {
    pub tx_statuses: Vec<DryRunTransactionExecutionStatus>,
    pub storage_reads: Vec<super::storage_read_replay::StorageReadReplayEvent>,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Query",
    variables = "DryRunArg"
)]
pub struct DryRunRecordStorageReads {
    #[arguments(txs: $txs, utxoValidation: $utxo_validation, gasPrice: $gas_price, blockHeight: $block_height)]
    pub dry_run_record_storage_reads: DryRunStorageReads,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Mutation",
    variables = "TxWithEstimatedPredicatesArg"
)]
pub struct Submit {
    #[arguments(tx: $tx, estimatePredicates: $estimate_predicates)]
    pub submit: TransactionIdFragment,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Subscription",
    variables = "TxWithEstimatedPredicatesArg"
)]
pub struct SubmitAndAwaitSubscription {
    #[arguments(tx: $tx, estimatePredicates: $estimate_predicates)]
    pub submit_and_await: TransactionStatus,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Subscription",
    variables = "TxWithEstimatedPredicatesArg"
)]
pub struct SubmitAndAwaitSubscriptionWithTransaction {
    #[arguments(tx: $tx, estimatePredicates: $estimate_predicates)]
    pub submit_and_await: StatusWithTransaction,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(
    schema_path = "./assets/schema.sdl",
    graphql_type = "Subscription",
    variables = "SubmitAndAwaitStatusArg"
)]
pub struct SubmitAndAwaitStatusSubscription {
    #[arguments(tx: $tx, estimatePredicates: $estimate_predicates, includePreconfirmation: $include_preconfirmation)]
    pub submit_and_await_status: TransactionStatus,
}

#[derive(cynic::QueryFragment, Clone, Debug)]
#[cynic(schema_path = "./assets/schema.sdl", graphql_type = "Query")]
pub struct AllReceipts {
    pub all_receipts: Vec<Receipt>,
}

#[cfg(test)]
pub mod tests {
    use std::borrow::Cow;
    use super::*;
    use crate::client::schema::Bytes;
    use fuel_core_types::fuel_types::canonical::Serialize;

    #[cfg(not(feature = "test-helpers"))]
    #[test]
    fn transparent_transaction_by_id_query_gql_output() {
        use cynic::QueryBuilder;
        let operation = transparent_tx::TransactionQuery::build(TxIdArgs {
            id: TransactionId::default(),
        });
        insta::assert_snapshot!(operation.query)
    }

    #[cfg(not(feature = "test-helpers"))]
    #[test]
    fn opaque_transaction_by_id_query_gql_output() {
        use cynic::QueryBuilder;
        let operation = TransactionQuery::build(TxIdArgs {
            id: TransactionId::default(),
        });
        insta::assert_snapshot!(operation.query)
    }

    #[cfg(not(feature = "test-helpers"))]
    #[test]
    fn transactions_connection_query_gql_output() {
        use cynic::QueryBuilder;

        let args = crate::client::schema::ConnectionArgs {
            after: None,
            before: None,
            first: None,
            last: None,
        };
        let operation = TransactionsQuery::build(args);
        insta::assert_snapshot!(operation.query)
    }

    #[cfg(not(feature = "test-helpers"))]
    #[test]
    fn transactions_by_owner_gql_output() {
        use cynic::QueryBuilder;
        let operation =
            TransactionsByOwnerQuery::build(TransactionsByOwnerConnectionArgs {
                owner: Default::default(),
                after: None,
                before: None,
                first: None,
                last: None,
            });
        insta::assert_snapshot!(operation.query)
    }

    #[test]
    fn dry_run_tx_gql_output() {
        use cynic::MutationBuilder;
        let tx = fuel_tx::Transaction::default_test_tx();
        let query = DryRun::build(DryRunArg {
            txs: vec![HexString(Bytes(Cow::Owned(tx.to_bytes())))],
            utxo_validation: Some(true),
            gas_price: Some(123u64.into()),
            block_height: Some(456u32.into()),
        });
        insta::assert_snapshot!(query.query)
    }

    #[test]
    fn submit_tx_gql_output() {
        use cynic::MutationBuilder;
        let tx = fuel_tx::Transaction::default_test_tx();
        let query = Submit::build(TxWithEstimatedPredicatesArg {
            tx: HexString(Bytes(Cow::Owned(tx.to_bytes()))),
            estimate_predicates: Some(true),
        });
        insta::assert_snapshot!(query.query)
    }
}
