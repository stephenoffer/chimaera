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
use openraft::raft::{AppendEntriesRequest, AppendError, AppendResult, SnapshotResponse, VoteRequest, VoteResponse};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{BasicNode, OptionalSend, RaftNetworkFactory};
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RpcKind {
    Append,
    Vote,
    PreVote,
    Snapshot,
}

impl RpcKind {
    pub fn code(self) -> u8 {
        self as u8
    }
    pub fn from_code(c: u8) -> Option<RpcKind> {
        [RpcKind::Append, RpcKind::Vote, RpcKind::PreVote, RpcKind::Snapshot].get(c as usize).copied()
    }
}

pub type RpcFuture = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send>>;

/// Delivers one raft RPC to `target` (reachable at `addr`) for `group`.
pub trait Transport: Send + Sync + 'static {
    fn call(&self, target: NodeId, addr: &str, group: u32, kind: RpcKind, body: Vec<u8>) -> RpcFuture;
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

#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotRpc {
    vote: VoteOf<TypeConfig>,
    meta: SnapshotMetaOf<TypeConfig>,
    data: Vec<u8>,
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

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let mut data = Vec::new();
        let mut f = snapshot.snapshot;
        f.read_to_end(&mut data).map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        let req = SnapshotRpc { vote, meta: snapshot.meta, data };
        self.rpc(RpcKind::Snapshot, &req).await.map_err(|e| match e {
            RPCError::Unreachable(u) => StreamingError::Unreachable(u),
            other => StreamingError::Unreachable(Unreachable::new(&io_err(other))),
        })
    }
}

/// Serve one raft RPC addressed to `group` on this peer. The reply is always a
/// postcard `Result<Response, String>`, so remote failures stay visible.
pub async fn handle_rpc(group: &Group, kind: RpcKind, body: &[u8], scratch: &std::path::Path) -> io::Result<Vec<u8>> {
    fn wrap<T: serde::Serialize, E: std::fmt::Display>(r: Result<T, E>) -> io::Result<Vec<u8>> {
        enc(&r.map_err(|e| e.to_string()))
    }
    match kind {
        RpcKind::Append => wrap(group.raft.append_entries(dec(body)?).await.map(AppendWire::from)),
        RpcKind::Vote => wrap(group.raft.vote(dec(body)?).await),
        RpcKind::PreVote => wrap(group.raft.pre_vote(dec(body)?).await),
        RpcKind::Snapshot => {
            let req: SnapshotRpc = dec(body)?;
            let path = scratch.join(format!("incoming-{}-{}.snap", group.id, std::process::id()));
            std::fs::File::create(&path)?.write_all(&req.data)?;
            let file = std::fs::File::open(&path)?;
            let snap = openraft::storage::Snapshot { meta: req.meta, snapshot: file };
            let r = group.raft.install_full_snapshot(req.vote, snap).await;
            let _ = std::fs::remove_file(&path);
            wrap(r)
        }
    }
}

/// A transport for single-voter groups: there is never anyone to call.
pub struct NoTransport;

impl Transport for NoTransport {
    fn call(&self, target: NodeId, _addr: &str, group: u32, _kind: RpcKind, _body: Vec<u8>) -> RpcFuture {
        Box::pin(async move { Err(io_err(format!("no transport to node {target} for group {group}"))) })
    }
}
