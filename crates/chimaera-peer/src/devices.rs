//! Accelerators on this host, which calls hold them, and what a call that
//! holds some sees.
//!
//! Discovery runs the pure parsers in `chimaera_core::devices` over the real
//! filesystem (`/`, or a captured tree in tests) and adds the Apple GPU from
//! `sysctl`. [`Allocator`] grants whole devices. [`grant_env`] is the
//! environment of an executor that may use exactly its grant: the vendor's
//! visibility variables, plus `CHIMAERA_DEVICES` for `ch.devices()`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chimaera_core::devices::{
    Device, Inventory, Kind, Selector, SysFs, discover_cuda, discover_hpu, discover_neuron, discover_rocm,
    discover_tpu, discover_xpu, mps_device, visibility_env,
};

/// The host filesystem under `root`, read as absolute paths.
pub struct HostFs {
    root: PathBuf,
}

impl HostFs {
    pub fn new(root: &Path) -> HostFs {
        HostFs { root: root.to_path_buf() }
    }

    fn at(&self, path: &str) -> PathBuf {
        self.root.join(path.trim_start_matches('/'))
    }
}

impl SysFs for HostFs {
    fn read(&self, path: &str) -> Option<String> {
        std::fs::read_to_string(self.at(path)).ok()
    }

    fn list(&self, dir: &str) -> Vec<String> {
        let entries = std::fs::read_dir(self.at(dir)).into_iter().flatten().flatten();
        let mut names: Vec<String> = entries.map(|e| e.file_name().to_string_lossy().into_owned()).collect();
        names.sort();
        names
    }

    fn is_dir(&self, path: &str) -> bool {
        self.at(path).is_dir()
    }

    fn exists(&self, path: &str) -> bool {
        self.at(path).exists()
    }
}

/// Every accelerator this peer may use: the files under `root` and the
/// visibility variables in `env` (the peer's own, which may be a Slurm or
/// Kubernetes restriction). The Apple GPU is read from `sysctl` when `root`
/// is `/` on macOS.
pub fn discover(root: &Path, env: &BTreeMap<String, String>) -> Inventory {
    let fs = HostFs::new(root);
    let mut inv = Inventory::default();
    let parts = [discover_cuda, discover_rocm, discover_tpu, discover_neuron, discover_hpu, discover_xpu];
    for part in parts.map(|discover| discover(&fs, env)) {
        inv.devices.extend(part.devices);
        inv.links.extend(part.links);
        inv.domain = inv.domain.or(part.domain);
    }
    if cfg!(target_os = "macos") && root == Path::new("/") {
        inv.devices.extend(apple_gpu());
    }
    inv
}

fn apple_gpu() -> Option<Device> {
    let out =
        std::process::Command::new("sysctl").args(["-n", "machdep.cpu.brand_string", "hw.memsize"]).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let mut lines = text.lines();
    let brand = lines.next()?.trim().to_string();
    let memsize = lines.next()?.trim().parse().ok()?;
    let arch = if std::env::consts::ARCH == "aarch64" { "arm64" } else { std::env::consts::ARCH };
    mps_device(arch, &brand, memsize)
}

/// This host's devices as the peer advertises them (`PeerInfo.gpus`, `Member.gpus`).
pub fn shape_gpus() -> Vec<chimaera_core::topology::Gpu> {
    let env: BTreeMap<String, String> = std::env::vars().collect();
    discover(Path::new("/"), &env)
        .devices
        .into_iter()
        .map(|d| chimaera_core::topology::Gpu {
            uuid: d.id,
            name: d.name,
            memory_bytes: d.memory_bytes,
            pci_bus_id: d.labels.get("pci").cloned().unwrap_or_default(),
            numa: d.numa,
        })
        .collect()
}

/// Whole devices granted to calls. A call takes `n` devices of one kind that
/// match its selector, and gives them back when its attempt ends.
pub struct Allocator {
    devices: Vec<Device>,
    busy: Vec<bool>,
}

impl Allocator {
    pub fn new(inv: &Inventory) -> Allocator {
        Allocator { devices: inv.devices.clone(), busy: vec![false; inv.devices.len()] }
    }

    fn candidates<'a>(&'a self, selector: Option<&'a Selector>) -> impl Iterator<Item = usize> + 'a {
        (0..self.devices.len()).filter(move |&i| selector.is_none_or(|s| s.matches(&self.devices[i])))
    }

    /// Devices matching `selector` (any kind without one), free or not: whether
    /// a call can ever run here.
    pub fn total(&self, selector: Option<&Selector>) -> usize {
        self.candidates(selector).count()
    }

    pub fn free(&self, selector: Option<&Selector>) -> usize {
        self.candidates(selector).filter(|&i| !self.busy[i]).count()
    }

    /// `n` devices matching `selector` (free ones only, unless `busy_too`), all
    /// of one kind (the first kind, in discovery order, with enough), lowest
    /// positions first. shortcut: lowest positions, not the best-linked set;
    /// choose by `Inventory::links` once gangs pack by NVLink (P5 gangs).
    fn choose(&self, n: usize, selector: Option<&Selector>, busy_too: bool) -> Option<Vec<usize>> {
        let pool: Vec<usize> = self.candidates(selector).filter(|&i| busy_too || !self.busy[i]).collect();
        let kind = Kind::ALL.into_iter().find(|&k| pool.iter().filter(|&&i| self.devices[i].kind == k).count() >= n)?;
        Some(pool.into_iter().filter(|&i| self.devices[i].kind == kind).take(n).collect())
    }

    /// `n` free devices matching `selector`, now held; `None` if not enough are free.
    pub fn take(&mut self, n: usize, selector: Option<&Selector>) -> Option<Vec<Device>> {
        if n == 0 {
            return Some(Vec::new());
        }
        let chosen = self.choose(n, selector, false)?;
        for &i in &chosen {
            self.busy[i] = true;
        }
        Some(chosen.into_iter().map(|i| self.devices[i].clone()).collect())
    }

    /// Return a grant (devices by id); unknown ids are ignored.
    pub fn give(&mut self, granted: &[Device]) {
        for d in granted {
            if let Some(i) = self.devices.iter().position(|x| x.id == d.id) {
                self.busy[i] = false;
            }
        }
    }
}

/// The core's view: one inventory, an allocator, and the devices each
/// executor holds. An executor's pool key names its grant (`BASE|dev=ID,ID`),
/// because a process reads its visibility variables once.
pub struct Devices {
    inv: Inventory,
    alloc: Allocator,
    held: std::collections::HashMap<u32, Vec<Device>>,
}

/// What a call's `gpus`/`device` options need from this peer.
#[derive(Debug, PartialEq, Eq)]
pub enum Want {
    /// No accelerator: the base pool key.
    None,
    /// This pool key: the call's grant, free now.
    Key(String),
    /// Feasible here, but not enough devices are free yet.
    Wait,
    /// Never on this peer, and why.
    Infeasible(String),
}

const DEV: &str = "|dev=";

impl Devices {
    /// This host's devices (`/` and the peer's own environment).
    pub fn discover() -> Devices {
        let env: BTreeMap<String, String> = std::env::vars().collect();
        let inv = discover(Path::new("/"), &env);
        if !inv.devices.is_empty() {
            let names: Vec<String> =
                inv.devices.iter().map(|d| format!("{}:{} {}", d.kind.as_str(), d.index, d.model)).collect();
            tracing::info!(devices = %names.join(", "), "accelerators");
        }
        Devices::new(inv)
    }

    pub fn new(inv: Inventory) -> Devices {
        Devices { alloc: Allocator::new(&inv), inv, held: Default::default() }
    }

    pub fn inventory(&self) -> &Inventory {
        &self.inv
    }

    fn want(&self, gpus: f64, device: &str) -> Result<Option<(usize, Option<Selector>)>, String> {
        if gpus <= 0.0 && device.is_empty() {
            return Ok(None);
        }
        // shortcut: a fraction takes a whole device until sharing is measured (plan P5).
        let n = if gpus > 0.0 { gpus.ceil() as usize } else { 1 };
        let selector = if device.is_empty() { None } else { Some(Selector::parse(device)?) };
        Ok(Some((n, selector)))
    }

    /// The executor pool key for a call needing `gpus` devices matching `device`.
    pub fn key_for(&self, gpus: f64, device: &str, base: &str) -> Want {
        let (n, selector) = match self.want(gpus, device) {
            Ok(None) => return Want::None,
            Ok(Some(w)) => w,
            Err(e) => return Want::Infeasible(e),
        };
        let total = self.alloc.total(selector.as_ref());
        if total < n {
            let what = if device.is_empty() { "accelerator(s)".to_string() } else { format!("{device} device(s)") };
            return Want::Infeasible(format!("needs {n} {what}; this peer has {total}"));
        }
        match self.alloc.choose(n, selector.as_ref(), false) {
            Some(chosen) => Want::Key(self.key(base, &chosen)),
            None => Want::Wait,
        }
    }

    /// As `key_for`, but when the devices are held, the lowest matching ones
    /// anyway: an actor shares them rather than wait (as before P5).
    /// shortcut: GPU actors queue for free devices once reservations land.
    pub fn key_for_actor(&self, gpus: f64, device: &str, base: &str) -> Want {
        match self.key_for(gpus, device, base) {
            Want::Wait => {
                let (n, selector) = self.want(gpus, device).ok().flatten().expect("key_for checked");
                let chosen = self.alloc.choose(n, selector.as_ref(), true).expect("key_for checked the total");
                tracing::warn!(device, gpus, "every matching device is held: the actor shares them");
                Want::Key(self.key(base, &chosen))
            }
            other => other,
        }
    }

    fn key(&self, base: &str, chosen: &[usize]) -> String {
        let ids: Vec<&str> = chosen.iter().map(|&i| self.alloc.devices[i].id.as_str()).collect();
        format!("{base}{DEV}{}", ids.join(","))
    }

    fn granted(&self, key: &str) -> Vec<Device> {
        let Some((_, ids)) = key.rsplit_once(DEV) else { return Vec::new() };
        ids.split(',').filter_map(|id| self.inv.devices.iter().find(|d| d.id == id).cloned()).collect()
    }

    /// `exec` (whose pool key is `key`) starts using its devices: the free
    /// ones are held until [`Devices::release`].
    pub fn hold(&mut self, exec: u32, key: &str) {
        let mut mine = Vec::new();
        for d in self.granted(key) {
            if let Some(i) = self.alloc.devices.iter().position(|x| x.id == d.id)
                && !self.alloc.busy[i]
            {
                self.alloc.busy[i] = true;
                mine.push(d);
            }
        }
        if !mine.is_empty() {
            self.held.entry(exec).or_default().extend(mine);
        }
    }

    /// `exec` is idle or gone: what it held is free.
    pub fn release(&mut self, exec: u32) {
        if let Some(granted) = self.held.remove(&exec) {
            self.alloc.give(&granted);
        }
    }

    /// The executor environment for pool `key`: its grant, or every
    /// accelerator hidden.
    pub fn env_for_key(&self, key: &str) -> Vec<(&'static str, Option<String>)> {
        grant_env(&self.inv, &self.granted(key))
    }
}

/// The environment of an executor that may use exactly `granted`: for every
/// kind on the host, its visibility variables (an empty grant hides the kind,
/// so a CPU call never grabs a GPU), and `CHIMAERA_DEVICES`, a JSON list of
/// the granted devices as the executor numbers them.
pub fn grant_env(inv: &Inventory, granted: &[Device]) -> Vec<(&'static str, Option<String>)> {
    let mut env = Vec::new();
    for kind in Kind::ALL {
        let on_host = inv.devices.iter().filter(|d| d.kind == kind).count();
        if on_host == 0 {
            continue;
        }
        let mine: Vec<&Device> = granted.iter().filter(|d| d.kind == kind).collect();
        env.extend(visibility_env(kind, &mine, on_host));
    }
    let records: Vec<serde_json::Value> = granted
        .iter()
        .enumerate()
        .map(|(i, d)| {
            // Visibility renumbers the granted devices from 0 in the executor.
            let torch = match d.kind {
                Kind::Hpu | Kind::Mps => d.kind.torch_device().to_string(),
                _ => format!("{}:{i}", d.kind.torch_device()),
            };
            serde_json::json!({
                "kind": d.kind.as_str(),
                "id": d.id,
                "index": i,
                "torch": torch,
                "backend": d.kind.dist_backend(),
                "model": d.model,
                "memory_bytes": d.memory_bytes,
            })
        })
        .collect();
    env.push(("CHIMAERA_DEVICES", Some(serde_json::Value::Array(records).to_string())));
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(machine: &str) -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/topology")).join(machine)
    }

    fn env_of(root: &Path) -> BTreeMap<String, String> {
        let text = std::fs::read_to_string(root.join("env")).unwrap_or_default();
        text.lines().filter_map(|l| l.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn discovers_a_dgx_from_its_tree() {
        let root = fixture("dgx-h100");
        let inv = discover(&root, &env_of(&root));
        assert_eq!(inv.devices.len(), 8);
        assert!(inv.devices.iter().all(|d| d.kind == Kind::Cuda && d.model.starts_with("H100")));
    }

    #[test]
    fn grants_whole_devices_by_selector_and_takes_them_back() {
        let root = fixture("dgx-h100");
        let inv = discover(&root, &env_of(&root));
        let mut alloc = Allocator::new(&inv);
        let h100 = Selector::parse("cuda:H100").unwrap();
        let a100 = Selector::parse("cuda:A100").unwrap();
        assert_eq!(alloc.total(Some(&a100)), 0, "no A100 here: such a call is infeasible");
        let first = alloc.take(3, Some(&h100)).unwrap();
        let second = alloc.take(5, None).unwrap();
        assert!(alloc.take(1, None).is_none(), "all 8 are held");
        let mut ids: Vec<&str> = first.iter().chain(&second).map(|d| d.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 8, "no device is granted twice");
        alloc.give(&first);
        assert_eq!(alloc.free(None), 3);
        assert_eq!(alloc.take(0, None), Some(Vec::new()));
    }

    #[test]
    fn a_grant_sees_only_its_devices_and_a_cpu_call_sees_none() {
        let root = fixture("dgx-h100");
        let inv = discover(&root, &env_of(&root));
        let granted = vec![inv.devices[5].clone(), inv.devices[2].clone()];
        let env: BTreeMap<_, _> = grant_env(&inv, &granted).into_iter().collect();
        assert_eq!(
            env["CUDA_VISIBLE_DEVICES"].as_deref(),
            Some(format!("{},{}", granted[0].id, granted[1].id).as_str())
        );
        let records: serde_json::Value = serde_json::from_str(env["CHIMAERA_DEVICES"].as_deref().unwrap()).unwrap();
        assert_eq!(records[0]["torch"], "cuda:0");
        assert_eq!(records[1]["torch"], "cuda:1");
        assert_eq!(records[1]["id"], granted[1].id.as_str());
        assert_eq!(records[0]["backend"], "nccl");
        let cpu: BTreeMap<_, _> = grant_env(&inv, &[]).into_iter().collect();
        assert_eq!(cpu["CUDA_VISIBLE_DEVICES"].as_deref(), Some(""), "a CPU call sees no GPU");
        assert_eq!(cpu["CHIMAERA_DEVICES"].as_deref(), Some("[]"));
    }

    #[test]
    fn an_intel_and_a_gaudi_grant_name_their_cards_the_way_their_runtimes_count() {
        let root = fixture("intel-max1550-igpu");
        let mut devs = Devices::new(discover(&root, &BTreeMap::new()));
        // Two Max 1550s; the integrated GPU is never granted.
        assert!(matches!(devs.key_for(3.0, "xpu", "py|1"), Want::Infeasible(_)));
        let Want::Key(key) = devs.key_for(1.0, "", "py|1") else { panic!() };
        devs.hold(1, &key);
        let env: BTreeMap<_, _> = devs.env_for_key(&key).into_iter().collect();
        assert_eq!(env["ZE_AFFINITY_MASK"].as_deref(), Some("1"), "Level Zero's ordinal, counting the integrated GPU");
        assert_eq!(env["ZE_FLAT_DEVICE_HIERARCHY"].as_deref(), Some("COMPOSITE"));
        assert!(env["CHIMAERA_DEVICES"].as_deref().unwrap().contains("\"torch\":\"xpu:0\""));

        let root = fixture("gaudi2");
        let inv = discover(&root, &BTreeMap::new());
        let env: BTreeMap<_, _> = grant_env(&inv, &inv.devices[1..]).into_iter().collect();
        assert_eq!(env["HABANA_VISIBLE_MODULES"].as_deref(), Some("5"));
        assert!(env["CHIMAERA_DEVICES"].as_deref().unwrap().contains("\"torch\":\"hpu\""));
    }

    #[test]
    fn this_mac_has_its_gpu_when_it_is_apple_silicon() {
        let inv = discover(Path::new("/"), &BTreeMap::new());
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            let mps: Vec<_> = inv.devices.iter().filter(|d| d.kind == Kind::Mps).collect();
            assert_eq!(mps.len(), 1, "{inv:?}");
            assert!(mps[0].memory_bytes > 0 && !mps[0].model.is_empty());
            let env: BTreeMap<_, _> = grant_env(&inv, &[mps[0].clone()]).into_iter().collect();
            assert!(env["CHIMAERA_DEVICES"].as_deref().unwrap().contains("\"torch\":\"mps\""));
        }
    }

    #[test]
    fn pool_keys_name_the_grant_and_holds_last_until_release() {
        let root = fixture("dgx-h100");
        let mut devs = Devices::new(discover(&root, &env_of(&root)));
        assert_eq!(devs.key_for(0.0, "", "py|1"), Want::None);
        assert!(matches!(devs.key_for(1.0, "rocm", "py|1"), Want::Infeasible(w) if w.contains("this peer has 0")));
        assert!(matches!(devs.key_for(1.0, "tpu:", "py|1"), Want::Infeasible(_)), "a malformed selector");
        let Want::Key(k6) = devs.key_for(6.0, "cuda:h100", "py|1") else { panic!() };
        devs.hold(7, &k6);
        assert_eq!(devs.env_for_key(&k6)[0].1.as_deref().unwrap().split(',').count(), 6);
        // Two are left: a call for 3 waits, a call for 2 gets the other two.
        assert_eq!(devs.key_for(3.0, "", "py|1"), Want::Wait);
        let Want::Key(k2) = devs.key_for(2.0, "", "py|1") else { panic!() };
        assert!(k2.ends_with(&format!("{},{}", devs.inv.devices[6].id, devs.inv.devices[7].id)));
        // A fraction takes a whole device.
        assert_eq!(devs.key_for(0.5, "", "py|1"), devs.key_for(1.0, "", "py|1"));
        devs.release(7);
        assert!(matches!(devs.key_for(8.0, "", "py|1"), Want::Key(_)));
        // An actor that finds its devices held shares them, and releasing it frees nothing it did not hold.
        devs.hold(1, &k6);
        let Want::Key(shared) = devs.key_for_actor(8.0, "", "py|1") else { panic!() };
        devs.hold(2, &shared);
        devs.release(2);
        assert_eq!(devs.alloc.free(None), 2, "the 6 held by exec 1 stay held");
        assert_eq!(devs.env_for_key("py|1")[0], ("CUDA_VISIBLE_DEVICES", Some(String::new())));
    }
}
