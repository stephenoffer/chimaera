//! Spilling to object storage: `CHIMAERA_SPILL_URL` (`s3://bucket/prefix`,
//! `gs://...`, `az://...`, or `file:///path`), like Ray's smart_open
//! spilling. Built with the `object-store-spill` feature; a peer built
//! without it says so at start and spills to its local directories only.
//!
//! A spilled object is uploaded to `<url>/chimaera-<peer>/<run>/<object>`
//! and its path in the store becomes a link to a cache file on local disk
//! that does not exist yet. The first reader, or a peer fetching it, restores
//! it there by downloading it (`ObjectStore::ensure_local`); from then on it
//! is read like any object spilled to disk.

use std::io;
use std::path::{Path, PathBuf};

pub struct RemoteSpill {
    pub url: String,
    /// Local directory that restored objects are downloaded into.
    pub cache: PathBuf,
    #[cfg(feature = "object-store-spill")]
    inner: imp::Inner,
}

impl RemoteSpill {
    /// Open the store at `url` for this peer (`peer_hex`), restoring into `cache`.
    pub fn open(url: &str, peer_hex: &str, cache: PathBuf) -> io::Result<RemoteSpill> {
        #[cfg(feature = "object-store-spill")]
        {
            std::fs::create_dir_all(&cache)?;
            let inner = imp::Inner::open(url, peer_hex)?;
            Ok(RemoteSpill { url: url.to_string(), cache, inner })
        }
        #[cfg(not(feature = "object-store-spill"))]
        {
            let _ = (peer_hex, cache);
            Err(io::Error::other(format!(
                "CHIMAERA_SPILL_URL={url} needs a peer built with the object-store-spill feature"
            )))
        }
    }

    /// Upload the file at `src` as object `object` of run `run` (hex names).
    pub fn upload(&self, src: &Path, run: &str, object: &str) -> io::Result<()> {
        #[cfg(feature = "object-store-spill")]
        return self.inner.upload(src, run, object);
        #[cfg(not(feature = "object-store-spill"))]
        {
            let _ = (src, run, object);
            unreachable!("opened only with the feature")
        }
    }

    /// Download object `object` of run `run` to `dest` (atomically); its size.
    pub fn download(&self, run: &str, object: &str, dest: &Path) -> io::Result<u64> {
        #[cfg(feature = "object-store-spill")]
        return self.inner.download(run, object, dest);
        #[cfg(not(feature = "object-store-spill"))]
        {
            let _ = (run, object, dest);
            unreachable!("opened only with the feature")
        }
    }

    /// Download `len` bytes from `offset` of object `name` of run `run` (an
    /// object packed with others, see ObjectStore::spill_pack) to `dest`,
    /// atomically; its size.
    pub fn download_range(&self, run: &str, name: &str, offset: u64, len: u64, dest: &Path) -> io::Result<u64> {
        #[cfg(feature = "object-store-spill")]
        return self.inner.download_range(run, name, offset, len, dest);
        #[cfg(not(feature = "object-store-spill"))]
        {
            let _ = (run, name, offset, len, dest);
            unreachable!("opened only with the feature")
        }
    }

    /// Delete the remote copies of `objects` of `run`, or the run's whole
    /// prefix when `objects` is None. In the background: never blocks a caller.
    pub fn delete(&self, run: &str, objects: Option<Vec<String>>) {
        #[cfg(feature = "object-store-spill")]
        self.inner.delete(run, objects);
        #[cfg(not(feature = "object-store-spill"))]
        let _ = (run, objects);
    }
}

#[cfg(feature = "object-store-spill")]
mod imp {
    use futures_util::{StreamExt, TryStreamExt};
    use object_store::path::Path as OPath;
    use object_store::{ObjectStore, PutPayload, WriteMultipart};
    use std::io::{self, Read, Write};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Objects at least this large are uploaded in parts, several at once.
    const PART: usize = 16 << 20;
    static SEQ: AtomicU64 = AtomicU64::new(0);

    pub struct Inner {
        store: Arc<dyn ObjectStore>,
        prefix: OPath,
        /// Always Some until dropped (see `Drop`).
        rt: Option<tokio::runtime::Runtime>,
    }

    impl Drop for Inner {
        /// The store may be dropped on a peer's async worker, where dropping a
        /// runtime would block and panic: shut it down without waiting.
        fn drop(&mut self) {
            if let Some(rt) = self.rt.take() {
                rt.shutdown_background();
            }
        }
    }

    /// Block on `f` from a thread of its own: callers include a peer's async
    /// threads, where blocking on a runtime directly would panic.
    fn block_on<T: Send>(handle: &tokio::runtime::Handle, f: impl Future<Output = T> + Send) -> T {
        std::thread::scope(|s| s.spawn(|| handle.block_on(f)).join().expect("object store thread"))
    }

    fn err(e: object_store::Error) -> io::Error {
        match e {
            object_store::Error::NotFound { .. } => io::Error::new(io::ErrorKind::NotFound, e.to_string()),
            _ => io::Error::other(e.to_string()),
        }
    }

    impl Inner {
        pub fn open(url: &str, peer_hex: &str) -> io::Result<Inner> {
            let parsed =
                url::Url::parse(url).map_err(|e| io::Error::other(format!("CHIMAERA_SPILL_URL={url}: {e}")))?;
            // Credentials and region come from the usual environment (AWS_*,
            // GOOGLE_*, AZURE_*), else the machine's own role.
            let options: Vec<(String, String)> = std::env::vars()
                .filter(|(k, _)| k.starts_with("AWS_") || k.starts_with("GOOGLE_") || k.starts_with("AZURE_"))
                .map(|(k, v)| (k.to_ascii_lowercase(), v))
                .collect();
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
            let mut options = options;
            let has_region = options.iter().any(|(k, _)| k == "aws_region" || k == "aws_default_region");
            if parsed.scheme() == "s3"
                && !has_region
                && let Some(bucket) = parsed.host_str()
            {
                // Without a region the client signs for us-east-1, which other
                // regions' buckets refuse: ask S3 where the bucket is.
                let client = object_store::ClientOptions::default();
                let region = block_on(rt.handle(), object_store::aws::resolve_bucket_region(bucket, &client))
                    .map_err(|e| io::Error::other(format!("finding the region of bucket {bucket}: {e}")))?;
                options.push(("aws_region".into(), region));
            }
            let (store, base) = object_store::parse_url_opts(&parsed, options).map_err(err)?;
            let prefix = base.child(format!("chimaera-{}", &peer_hex[..16.min(peer_hex.len())]));
            Ok(Inner { store: store.into(), prefix, rt: Some(rt) })
        }

        fn path(&self, run: &str, object: &str) -> OPath {
            self.prefix.child(run).child(object)
        }

        fn rt(&self) -> &tokio::runtime::Runtime {
            self.rt.as_ref().expect("present until dropped")
        }

        /// Run `f` on this store's own runtime, from any thread.
        fn run<T: Send>(&self, f: impl Future<Output = T> + Send) -> T {
            block_on(self.rt().handle(), f)
        }

        pub fn upload(&self, src: &Path, run: &str, object: &str) -> io::Result<()> {
            let (store, path, src) = (self.store.clone(), self.path(run, object), src.to_path_buf());
            self.run(async move {
                let mut f = std::fs::File::open(&src)?;
                let len = f.metadata()?.len() as usize;
                if len < PART {
                    let mut data = Vec::with_capacity(len);
                    f.read_to_end(&mut data)?;
                    store.put(&path, PutPayload::from(data)).await.map_err(err)?;
                    return Ok(());
                }
                let mut w = WriteMultipart::new_with_chunk_size(store.put_multipart(&path).await.map_err(err)?, PART);
                let mut buf = vec![0u8; PART];
                loop {
                    let n = f.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    w.wait_for_capacity(8).await.map_err(err)?;
                    w.write(&buf[..n]);
                }
                w.finish().await.map_err(err)?;
                Ok(())
            })
        }

        pub fn download(&self, run: &str, object: &str, dest: &Path) -> io::Result<u64> {
            let (store, path, dest) = (self.store.clone(), self.path(run, object), dest.to_path_buf());
            self.run(async move {
                let mut stream = store.get(&path).await.map_err(err)?.into_stream();
                if let Some(dir) = dest.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let tmp = dest.with_extension(format!(
                    "part-restore-{}-{}",
                    std::process::id(),
                    SEQ.fetch_add(1, Ordering::Relaxed)
                ));
                let mut f = std::fs::File::create(&tmp)?;
                let mut n = 0u64;
                let got: io::Result<()> = async {
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.map_err(err)?;
                        f.write_all(&chunk)?;
                        n += chunk.len() as u64;
                    }
                    Ok(())
                }
                .await;
                match got.and_then(|()| std::fs::rename(&tmp, &dest)) {
                    Ok(()) => Ok(n),
                    Err(e) => {
                        let _ = std::fs::remove_file(&tmp);
                        Err(e)
                    }
                }
            })
        }

        pub fn download_range(&self, run: &str, name: &str, offset: u64, len: u64, dest: &Path) -> io::Result<u64> {
            let (store, path, dest) = (self.store.clone(), self.path(run, name), dest.to_path_buf());
            self.run(async move {
                let bytes = store.get_range(&path, offset..offset + len).await.map_err(err)?;
                if bytes.len() as u64 != len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the pack is shorter than its index says",
                    ));
                }
                if let Some(dir) = dest.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                let tmp = dest.with_extension(format!(
                    "part-restore-{}-{}",
                    std::process::id(),
                    SEQ.fetch_add(1, Ordering::Relaxed)
                ));
                match std::fs::write(&tmp, &bytes).and_then(|()| std::fs::rename(&tmp, &dest)) {
                    Ok(()) => Ok(len),
                    Err(e) => {
                        let _ = std::fs::remove_file(&tmp);
                        Err(e)
                    }
                }
            })
        }

        pub fn delete(&self, run: &str, objects: Option<Vec<String>>) {
            let (store, prefix) = (self.store.clone(), self.prefix.child(run));
            self.rt().spawn(async move {
                let paths: Vec<OPath> = match objects {
                    Some(objects) => objects.iter().map(|o| prefix.child(o.as_str())).collect(),
                    None => store.list(Some(&prefix)).map_ok(|m| m.location).try_collect().await.unwrap_or_default(),
                };
                for p in paths {
                    if let Err(e) = store.delete(&p).await
                        && !matches!(e, object_store::Error::NotFound { .. })
                    {
                        tracing::warn!("spill store: cannot delete {p}: {e}");
                    }
                }
            });
        }
    }
}
