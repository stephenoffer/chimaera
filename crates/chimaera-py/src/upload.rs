//! Blob uploads from a driver that cannot write its peer's store (a remote
//! client). The payload is never copied whole: each chunk is copied out of
//! the caller's buffer only once the transport has room for it, so an upload
//! of any size stages a few chunks here, and every upload in the process
//! together at most STAGED of them.

use chimaera_core::pb;
use chimaera_proto::client::PeerClient;
use tokio::sync::Semaphore;

/// Bytes per chunk of an upload.
const CHUNK: usize = 1 << 20;
/// Chunks copied out of callers' buffers that the transport has not taken
/// yet, across every upload in this process (32 MiB).
static STAGED: Semaphore = Semaphore::const_new(32);
/// Of those, the chunks one upload may have waiting.
const DEPTH: usize = 4;

/// Upload `data` as the blob `head` names (its run, id, size and flags), in
/// CHUNK pieces. `data` is read only while this future runs: the transport
/// gets owned chunks, and the copying ends (and is dropped) once the reply
/// arrives, even when the peer answers before reading everything.
pub async fn put_blob(peer: &mut PeerClient, mut head: pb::PutBlobChunk, data: &[u8]) -> Result<(), tonic::Status> {
    head.size = data.len() as u64;
    let (tx, rx) = tokio::sync::mpsc::channel(DEPTH);
    let copy = async move {
        let mut pieces = data.chunks(CHUNK);
        let mut next = Some(head); // the first chunk names the blob, even an empty one
        while let Some(mut chunk) = next.take() {
            let Ok(staged) = STAGED.acquire().await else { break };
            chunk.data = pieces.next().unwrap_or_default().to_vec();
            if tx.send((chunk, staged)).await.is_err() {
                break; // the upload ended (refused, or the connection was lost)
            }
            if pieces.len() > 0 {
                next = Some(pb::PutBlobChunk::default());
            }
        }
        drop(tx); // the stream ends after the last chunk
        std::future::pending::<()>().await
    };
    // A chunk's share of STAGED is given back as the transport takes it.
    let chunks = tokio_stream::StreamExt::map(tokio_stream::wrappers::ReceiverStream::new(rx), |(c, _staged)| c);
    tokio::select! {
        reply = peer.put_blob(chunks) => reply.map(|_| ()),
        () = copy => unreachable!("copying never finishes on its own"),
    }
}
