//! Raft RPCs between peers, over a pluggable [`Transport`].
//!
//! Requests are postcard-encoded openraft messages addressed by (group, target
//! node). The peer supplies the transport (gRPC between machines); tests use an
//! in-process one. [`handle_rpc`] is the receiving side.

use crate::log_store::{dec, enc, io_err};
use crate::{Group, NodeId, TypeConfig};
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::network::v2::RaftNetworkV2;
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, SnapshotResponse, TransferLeaderRequest, TransferLeaderResponse,
    VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{BasicNode, OptionalSend, RaftNetworkFactory};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RpcKind {
    Append,
    Vote,
    PreVote,
    TransferLeader,
}

impl RpcKind {
    pub fn code(self) -> u8 {
        self as u8
    }
    pub fn from_code(c: u8) -> Option<RpcKind> {
        [RpcKind::Append, RpcKind::Vote, RpcKind::PreVote, RpcKind::TransferLeader].get(c as usize).copied()
    }
}

pub type RpcFuture = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send>>;

/// Delivers raft RPCs to `target` (reachable at `addr`) for `group`.
pub trait Transport: Send + Sync + 'static {
    fn call(&self, target: NodeId, addr: &str, group: u32, kind: RpcKind, body: Vec<u8>) -> RpcFuture;
    /// Send a snapshot: `header` (the vote and the snapshot's meta), then
    /// `file` streamed in pieces, never as one message. The reply is what
    /// [`handle_snapshot`] returns on the receiving peer.
    fn send_snapshot(&self, target: NodeId, addr: &str, group: u32, header: Vec<u8>, file: std::fs::File) -> RpcFuture;
}

pub struct NetworkFactory {
    group: u32,
    transport: Arc<dyn Transport>,
}

impl NetworkFactory {
    pub fn new(group: u32, transport: Arc<dyn Transport>) -> NetworkFactory {
        NetworkFactory { group, transport }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Network {
        Network { target, addr: node.addr.clone(), group: self.group, transport: self.transport.clone() }
    }
}

pub struct Network {
    target: NodeId,
    addr: String,
    group: u32,
    transport: Arc<dyn Transport>,
}

fn unreachable(e: impl std::fmt::Display) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&io_err(e)))
}

impl Network {
    async fn rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        kind: RpcKind,
        req: &Req,
    ) -> Result<Resp, RPCError<TypeConfig>> {
        let body = enc(req).map_err(unreachable)?;
        let bytes = self.transport.call(self.target, &self.addr, self.group, kind, body).await.map_err(unreachable)?;
        let resp: Result<Resp, String> = dec(&bytes).map_err(unreachable)?;
        resp.map_err(unreachable)
    }
}

/// Wire form of [`AppendResult`]. openraft leaves the encoding to transports;
/// this one does not carry the optional conflict hint.
#[derive(serde::Serialize, serde::Deserialize)]
enum AppendWire {
    Ok(Option<LogIdOf<TypeConfig>>),
    Conflict(LogIdOf<TypeConfig>),
    HigherVote(VoteOf<TypeConfig>),
}

impl From<AppendResult<TypeConfig>> for AppendWire {
    fn from(r: AppendResult<TypeConfig>) -> Self {
        match r {
            Ok(l) => AppendWire::Ok(l),
            Err(AppendError::Conflict { log_id, .. }) => AppendWire::Conflict(log_id),
            Err(AppendError::HigherVote(v)) => AppendWire::HigherVote(v),
        }
    }
}

impl From<AppendWire> for AppendResult<TypeConfig> {
    fn from(w: AppendWire) -> Self {
        match w {
            AppendWire::Ok(l) => Ok(l),
            AppendWire::Conflict(log_id) => Err(AppendError::Conflict { log_id, hint: None }),
            AppendWire::HigherVote(v) => Err(AppendError::HigherVote(v)),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for Network {
    type SnapshotData = std::fs::File;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let wire: AppendWire = self.rpc(RpcKind::Append, &rpc).await?;
        Ok(wire.into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.rpc(RpcKind::Vote, &rpc).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.rpc(RpcKind::PreVote, &rpc).await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.rpc(RpcKind::TransferLeader, &req).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let failed = |e: io::Error| StreamingError::Unreachable(Unreachable::new(&e));
        let header = enc(&(vote, snapshot.meta)).map_err(failed)?;
        let send = self.transport.send_snapshot(self.target, &self.addr, self.group, header, snapshot.snapshot);
        let bytes = tokio::select! {
            r = send => r.map_err(failed)?,
            closed = cancel => return Err(StreamingError::Closed(closed)),
        };
        let resp: Result<SnapshotResponse<TypeConfig>, String> = dec(&bytes).map_err(failed)?;
        resp.map_err(|e| failed(io_err(e)))
    }
}

/// Install a snapshot that arrived as `header` plus the file at `path`. The
/// reply is a postcard `Result<SnapshotResponse, String>`.
pub async fn handle_snapshot(group: &Group, header: &[u8], path: &std::path::Path) -> io::Result<Vec<u8>> {
    let (vote, meta): (VoteOf<TypeConfig>, SnapshotMetaOf<TypeConfig>) = dec(header)?;
    let snapshot = openraft::storage::Snapshot { meta, snapshot: std::fs::File::open(path)? };
    let r = group.raft.install_full_snapshot(vote, snapshot).await;
    enc(&r.map_err(|e| e.to_string()))
}

/// Serve one raft RPC addressed to `group` on this peer. The reply is always a
/// postcard `Result<Response, String>`, so remote failures stay visible.
pub async fn handle_rpc(group: &Group, kind: RpcKind, body: &[u8]) -> io::Result<Vec<u8>> {
    fn wrap<T: serde::Serialize, E: std::fmt::Display>(r: Result<T, E>) -> io::Result<Vec<u8>> {
        enc(&r.map_err(|e| e.to_string()))
    }
    match kind {
        RpcKind::Append => wrap(group.raft.append_entries(dec(body)?).await.map(AppendWire::from)),
        RpcKind::Vote => wrap(group.raft.vote(dec(body)?).await),
        RpcKind::PreVote => wrap(group.raft.pre_vote(dec(body)?).await),
        RpcKind::TransferLeader => wrap(group.raft.handle_transfer_leader(dec(body)?).await),
    }
}

/// A transport for single-voter groups: there is never anyone to call.
pub struct NoTransport;

impl Transport for NoTransport {
    fn call(&self, target: NodeId, _addr: &str, group: u32, _kind: RpcKind, _body: Vec<u8>) -> RpcFuture {
        Box::pin(async move { Err(io_err(format!("no transport to node {target} for group {group}"))) })
    }

    fn send_snapshot(
        &self,
        target: NodeId,
        _addr: &str,
        group: u32,
        _header: Vec<u8>,
        _file: std::fs::File,
    ) -> RpcFuture {
        Box::pin(async move { Err(io_err(format!("no transport to node {target} for group {group}"))) })
    }
}
