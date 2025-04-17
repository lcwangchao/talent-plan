use percolator_proto::message::{
    CommitRequest, CommitResponse, DebugRequest, DebugResponse, GetRequest, GetResponse,
    PrewriteRequest, PrewriteResponse, TimestampRequest, TimestampResponse,
};

labrpc::service! {
    service timestamp {
        rpc get_timestamp(TimestampRequest) returns (TimestampResponse);
    }
}

pub use timestamp::{add_service as add_tso_service, Client as TSOClient, Service};

labrpc::service! {
    service transaction {
        rpc get(GetRequest) returns (GetResponse);
        rpc prewrite(PrewriteRequest) returns (PrewriteResponse);
        rpc commit(CommitRequest) returns (CommitResponse);
        rpc debug(DebugRequest) returns (DebugResponse);
    }
}

pub use transaction::{add_service as add_transaction_service, Client as TransactionClient};
