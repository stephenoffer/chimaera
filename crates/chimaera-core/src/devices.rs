//! Accelerators: device records, the `device="KIND[:MODEL]"` selector, the
//! variables that make an executor see exactly its grant, and discovery from
//! procfs/sysfs/`/dev` contents.
//!
//! Discovery reads through [`SysFs`], so it stays pure and is tested against
//! fixture trees; `chimaera-peer` supplies the real file system. Each kind
//! follows the same steps: procfs/sysfs enumerates devices, the `/dev` node
//! filters them (a Kubernetes device plugin mounts only the granted nodes),
//! and a visibility variable the peer inherited narrows them further.
//!
//! Every vendor fact here except CUDA's documented `CUDA_VISIBLE_DEVICES=""`
//! (hides every GPU) is unverified on hardware here. The TPU and Intel
//! environment rules follow Ray 2.60's accelerator managers. Neuron, Gaudi
//! (`hpu`) and Intel (`xpu`) discovery is not in this phase; Ray reads those
//! from vendor tools.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

/// Accelerator family, named by the software stack that drives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Cuda,
    Rocm,
    Tpu,
    Neuron,
    Hpu,
    Xpu,
    Mps,
}

impl Kind {
    pub const ALL: [Kind; 7] = [Kind::Cuda, Kind::Rocm, Kind::Tpu, Kind::Neuron, Kind::Hpu, Kind::Xpu, Kind::Mps];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Cuda => "cuda",
            Kind::Rocm => "rocm",
            Kind::Tpu => "tpu",
            Kind::Neuron => "neuron",
            Kind::Hpu => "hpu",
            Kind::Xpu => "xpu",
            Kind::Mps => "mps",
        }
    }

    /// PyTorch device type: ROCm builds use `cuda`; TPU and Neuron go through torch-xla.
    pub fn torch_device(self) -> &'static str {
        match self {
            Kind::Cuda | Kind::Rocm => "cuda",
            Kind::Tpu | Kind::Neuron => "xla",
            Kind::Hpu => "hpu",
            Kind::Xpu => "xpu",
            Kind::Mps => "mps",
        }
    }

    /// `torch.distributed` backend (RCCL registers as `nccl`).
    pub fn dist_backend(self) -> &'static str {
        match self {
            Kind::Cuda | Kind::Rocm => "nccl",
            Kind::Tpu | Kind::Neuron => "xla",
            Kind::Hpu => "hccl",
            Kind::Xpu => "xccl",
            Kind::Mps => "gloo",
        }
    }
}

impl FromStr for Kind {
    type Err = String;

    /// Case-insensitive. The error lists every kind and suggests the closest
    /// one when a single kind is within two edits.
    fn from_str(s: &str) -> Result<Kind, String> {
        let want = s.to_ascii_lowercase();
        if let Some(kind) = Kind::ALL.into_iter().find(|k| k.as_str() == want) {
            return Ok(kind);
        }
        let mut near: Vec<(usize, Kind)> =
            Kind::ALL.into_iter().map(|k| (edit_distance(&want, k.as_str()), k)).filter(|(d, _)| *d <= 2).collect();
        near.sort();
        let hint = match near.as_slice() {
            [(d, k), rest @ ..] if rest.first().is_none_or(|(d2, _)| d2 > d) => {
                format!(" (did you mean {:?}?)", k.as_str())
            }
            _ => String::new(),
        };
        let all: Vec<&str> = Kind::ALL.iter().map(|k| k.as_str()).collect();
        Err(format!("unknown device kind {s:?}{hint}; expected one of {}", all.join(", ")))
    }
}

/// Levenshtein distance, for the "did you mean" hint.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut row = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            row.push((prev[j] + usize::from(ca != *cb)).min(prev[j + 1] + 1).min(row[j] + 1));
        }
        prev = row;
    }
    prev[b.len()]
}

/// One accelerator unit as a peer sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device {
    pub kind: Kind,
    /// Stable id: the CUDA UUID, AMD `GPU-<hex>`, `tpu:<n>`, `mps:0`.
    pub id: String,
    /// Position as this peer's processes see it.
    pub index: u32,
    /// Normalized short model, matched by [`Selector`] (`H100 80GB HBM3`, `MI300X-OAM`, `v5e`).
    pub model: String,
    /// Raw vendor name.
    pub name: String,
    /// 0 when unknown.
    pub memory_bytes: u64,
    pub numa: Option<u32>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkKind {
    NvLink,
    Xgmi,
    Ici,
    NeuronLink,
    Pcie,
}

/// A direct link between two devices, by device id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    pub a: String,
    pub b: String,
    pub kind: LinkKind,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Inventory {
    pub devices: Vec<Device>,
    pub links: Vec<Link>,
    /// The interconnect domain the devices belong to, such as a TPU slice name.
    pub domain: Option<String>,
}

/// `device="KIND[:MODEL]"`: a kind plus an optional case-insensitive model prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selector {
    pub kind: Kind,
    pub model: Option<String>,
}

impl Selector {
    /// Splits once on ':', so a model may itself contain one (`cuda:A100:1g.10gb`).
    pub fn parse(s: &str) -> Result<Selector, String> {
        let (kind, model) = match s.split_once(':') {
            Some((_, "")) => return Err(format!("device selector {s:?} has an empty model; expected KIND[:MODEL]")),
            Some((kind, model)) => (kind, Some(model.to_string())),
            None => (s, None),
        };
        Ok(Selector { kind: kind.parse()?, model })
    }

    /// Same kind, and the model is a case-insensitive prefix of the device's.
    pub fn matches(&self, d: &Device) -> bool {
        d.kind == self.kind
            && self.model.as_ref().is_none_or(|m| d.model.to_ascii_lowercase().starts_with(&m.to_ascii_lowercase()))
    }
}

fn strip<'a>(s: &'a str, prefix: &str) -> &'a str {
    s.strip_prefix(prefix).unwrap_or(s)
}

/// `NVIDIA H100 80GB HBM3` → `H100 80GB HBM3`; `Tesla V100-SXM2-16GB` → `V100-SXM2-16GB`.
pub fn nvidia_model(name: &str) -> String {
    strip(strip(name.trim(), "NVIDIA "), "Tesla ").to_string()
}

/// PCI device id → model, from Ray 2.60's `amd_gpu.py` with `AMD-Instinct-` removed.
const AMD_MODELS: [(u64, &str); 12] = [
    (0x66a1, "MI50"),
    (0x738c, "MI100"),
    (0x7408, "MI250X"),
    (0x740c, "MI250X-MI250"),
    (0x740f, "MI210"),
    (0x74a0, "MI300A"),
    (0x74a1, "MI300X-OAM"),
    (0x74a2, "MI308X-OAM"),
    (0x74a9, "MI300X-HF"),
    (0x74a5, "MI325X-OAM"),
    (0x75a0, "MI350X-OAM"),
    (0x75a3, "MI355X-OAM"),
];

/// AMD model: the `product_name` sysfs file without `AMD `/`Instinct `, else
/// the PCI device id (KFD reports it in decimal) from the table above, else
/// the LLVM target: `gfx_target_version` 90402 is major 9, minor 4, stepping 2,
/// which LLVM writes `gfx942` (minor and stepping in hex: 90010 is `gfx90a`).
pub fn amd_model(product_name: &str, device_id: u64, gfx_target_version: u64) -> String {
    let product = strip(strip(product_name.trim(), "AMD "), "Instinct ");
    if !product.is_empty() {
        return product.to_string();
    }
    let v = gfx_target_version;
    AMD_MODELS
        .iter()
        .find(|(id, _)| *id == device_id)
        .map_or_else(|| format!("gfx{}{:x}{:x}", v / 10000, (v / 100) % 100, v % 100), |(_, model)| model.to_string())
}

/// TPU generation from an accelerator type: `v5litepod-16` → `v5e`, `v6e-8` → `v6e`.
pub fn tpu_model(accelerator_type: &str) -> String {
    match accelerator_type.trim().split('-').next().unwrap_or_default() {
        "v5litepod" => "v5e".to_string(),
        generation => generation.to_string(),
    }
}

/// `Apple M3 Max` → `M3 Max`.
pub fn apple_model(brand: &str) -> String {
    strip(brand.trim(), "Apple ").to_string()
}

/// Variables that make an executor see exactly `granted`, in a fixed order;
/// `None` removes the variable. `on_host` is how many units of `kind` the
/// peer has (only TPU uses it). An empty grant hides the kind from the task.
pub fn visibility_env(kind: Kind, granted: &[&Device], on_host: usize) -> Vec<(&'static str, Option<String>)> {
    let list = |unit: &dyn Fn(&Device) -> String| Some(granted.iter().map(|d| unit(d)).collect::<Vec<_>>().join(","));
    let indices = list(&|d| d.index.to_string());
    match kind {
        // UUIDs, so the child's ordinals never matter.
        Kind::Cuda => vec![("CUDA_VISIBLE_DEVICES", list(&|d| d.id.clone()))],
        // Indices relative to what the peer sees. The peer's own
        // ROCR_VISIBLE_DEVICES stays (it may be the platform's restriction);
        // the CUDA-style variables would renumber on top of HIP's, so they go.
        Kind::Rocm => {
            vec![("HIP_VISIBLE_DEVICES", indices), ("CUDA_VISIBLE_DEVICES", None), ("GPU_DEVICE_ORDINAL", None)]
        }
        // Ray 2.60's TPUAcceleratorManager for a fresh process: the whole host
        // keeps the framework defaults; one or two chips also need sub-host
        // bounds; any other count sets only the chip mask.
        Kind::Tpu if granted.len() == on_host => vec![("TPU_VISIBLE_CHIPS", None)],
        Kind::Tpu => {
            let mut chips: Vec<u32> = granted.iter().map(|d| d.index).collect();
            chips.sort_unstable();
            let mask = Some(chips.iter().map(u32::to_string).collect::<Vec<_>>().join(","));
            let per_host = match chips.len() {
                1 => "1,1,1",
                2 => "1,2,1",
                _ => return vec![("TPU_VISIBLE_CHIPS", mask)],
            };
            vec![
                ("TPU_VISIBLE_CHIPS", mask),
                ("TPU_CHIPS_PER_HOST_BOUNDS", Some(per_host.to_string())),
                ("TPU_HOST_BOUNDS", Some("1,1,1".to_string())),
            ]
        }
        // Unit = NeuronCore.
        Kind::Neuron => vec![("NEURON_RT_VISIBLE_CORES", indices)],
        Kind::Hpu => vec![(
            "HABANA_VISIBLE_MODULES",
            list(&|d| d.labels.get("module_id").cloned().unwrap_or_else(|| d.index.to_string())),
        )],
        Kind::Xpu => vec![("ZE_AFFINITY_MASK", indices)],
        // One device that cannot be hidden.
        Kind::Mps => Vec::new(),
    }
}

/// Read-only view of the host's files, so discovery is pure: the peer passes
/// the real file system, tests a fixture tree. Paths are absolute.
pub trait SysFs {
    /// The file's contents; `None` if it is missing or unreadable.
    fn read(&self, path: &str) -> Option<String>;
    /// Entry names in `dir`, sorted; empty if it is missing.
    fn list(&self, dir: &str) -> Vec<String>;
    fn is_dir(&self, path: &str) -> bool;
    fn exists(&self, path: &str) -> bool;
}

/// A sysfs `numa_node` file; `-1` (no affinity) and absence are `None`.
fn numa_node(fs: &dyn SysFs, path: &str) -> Option<u32> {
    fs.read(path)?.trim().parse().ok()
}

/// Narrows `devices` (in discovery order) to an inherited visibility list.
/// Unset keeps all. An integer entry indexes `devices`; any other entry names
/// the one device whose id it prefixes. As with CUDA, the list ends at the
/// first entry that names no device (or several), so `""` keeps none.
fn inherited(devices: Vec<Device>, list: Option<&String>) -> Vec<Device> {
    let Some(list) = list else { return devices };
    let mut keep = vec![false; devices.len()];
    for entry in list.split(',').map(|e| e.trim().to_ascii_lowercase()) {
        let hit = match entry.parse::<usize>() {
            Ok(i) => (i < devices.len()).then_some(i),
            Err(_) => {
                let mut named = (0..devices.len())
                    .filter(|&i| !entry.is_empty() && devices[i].id.to_ascii_lowercase().starts_with(&entry));
                match (named.next(), named.next()) {
                    (Some(i), None) => Some(i),
                    _ => None,
                }
            }
        };
        match hit {
            Some(i) => keep[i] = true,
            None => break,
        }
    }
    devices.into_iter().zip(keep).filter(|(_, k)| *k).map(|(d, _)| d).collect()
}

/// Sets each device's `index` to its position.
fn numbered(mut devices: Vec<Device>) -> Vec<Device> {
    for (i, d) in devices.iter_mut().enumerate() {
        d.index = i as u32;
    }
    devices
}

/// NVIDIA GPUs from `/proc/driver/nvidia/gpus/*/information`, kept only if
/// `/dev/nvidia<minor>` exists (procfs lists every GPU on the host), then
/// narrowed by an inherited `CUDA_VISIBLE_DEVICES`: UUIDs, unique UUID
/// prefixes (`GPU-3f5b`), or indices into PCI-bus order of the GPUs left,
/// which assumes `CUDA_DEVICE_ORDER=PCI_BUS_ID`. `index` is the position in
/// PCI-bus order after filtering. Memory is unknown (0) until NVML is read.
pub fn discover_cuda(fs: &dyn SysFs, env: &BTreeMap<String, String>) -> Inventory {
    const GPUS: &str = "/proc/driver/nvidia/gpus";
    let mut gpus: Vec<Device> = fs
        .list(GPUS)
        .iter()
        .filter_map(|dir| {
            let text = fs.read(&format!("{GPUS}/{dir}/information"))?;
            let info: BTreeMap<&str, &str> =
                text.lines().filter_map(|l| l.split_once(':')).map(|(k, v)| (k.trim(), v.trim())).collect();
            let (name, uuid, minor) = (*info.get("Model")?, *info.get("GPU UUID")?, *info.get("Device Minor")?);
            let bus = info.get("Bus Location")?.to_ascii_lowercase();
            fs.exists(&format!("/dev/nvidia{minor}")).then(|| Device {
                kind: Kind::Cuda,
                id: uuid.to_string(),
                index: 0,
                model: nvidia_model(name),
                name: name.to_string(),
                memory_bytes: 0,
                numa: numa_node(fs, &format!("/sys/bus/pci/devices/{bus}/numa_node")),
                labels: BTreeMap::from([("pci".to_string(), bus), ("minor".to_string(), minor.to_string())]),
            })
        })
        .collect();
    gpus.sort_by(|a, b| a.labels["pci"].cmp(&b.labels["pci"]));
    Inventory { devices: numbered(inherited(gpus, env.get("CUDA_VISIBLE_DEVICES"))), ..Inventory::default() }
}

const KFD: &str = "/sys/class/kfd/kfd/topology/nodes";
const CRAT_IOLINK_TYPE_XGMI: u64 = 11;

/// A KFD `properties` file: `key value` lines; non-numeric values are dropped.
fn kfd_properties(fs: &dyn SysFs, path: &str) -> Option<BTreeMap<String, u64>> {
    let text = fs.read(path)?;
    Some(
        text.lines()
            .filter_map(|l| {
                let (key, value) = l.split_once(' ')?;
                Some((key.to_string(), value.trim().parse().ok()?))
            })
            .collect(),
    )
}

/// AMD GPUs from the KFD topology: nodes with SIMDs (CPU nodes report none)
/// and vendor 0x1002, in node order, kept only if `/dev/dri/renderD<minor>`
/// exists, then narrowed by an inherited `ROCR_VISIBLE_DEVICES` (indices into
/// the GPUs left, or `GPU-<16 hex>` UUIDs). The id is `GPU-<unique_id hex>`,
/// suffixed with `.<kfd node>` when the unique id is 0 or shared (CPX
/// partitions of one GPU can share it). xGMI links between kept GPUs become
/// links; they are read from both `io_links` and `p2p_links`, because which
/// one a kernel uses for GPU-to-GPU xGMI is unverified here.
pub fn discover_rocm(fs: &dyn SysFs, env: &BTreeMap<String, String>) -> Inventory {
    let mut nodes: Vec<u64> = fs.list(KFD).iter().filter_map(|n| n.parse().ok()).collect();
    nodes.sort_unstable();
    let num = |p: &BTreeMap<String, u64>, key: &str| p.get(key).copied().unwrap_or(0);
    let gpus: Vec<(u64, BTreeMap<String, u64>)> = nodes
        .into_iter()
        .filter_map(|node| Some((node, kfd_properties(fs, &format!("{KFD}/{node}/properties"))?)))
        .filter(|(_, p)| num(p, "simd_count") > 0 && num(p, "vendor_id") == 0x1002)
        .collect();
    let mut sharing: BTreeMap<u64, usize> = BTreeMap::new();
    for (_, p) in &gpus {
        *sharing.entry(num(p, "unique_id")).or_default() += 1;
    }

    let mut node_of: BTreeMap<String, u64> = BTreeMap::new();
    let mut devices = Vec::new();
    for (node, p) in &gpus {
        let minor = num(p, "drm_render_minor");
        if !fs.exists(&format!("/dev/dri/renderD{minor}")) {
            continue;
        }
        let dir = format!("/sys/class/drm/renderD{minor}/device");
        let read = |file: &str| fs.read(&format!("{dir}/{file}")).map(|s| s.trim().to_string());
        let unique = num(p, "unique_id");
        let mut id = format!("GPU-{unique:016x}");
        if unique == 0 || sharing[&unique] > 1 {
            id = format!("{id}.{node}");
        }
        node_of.insert(id.clone(), *node);
        let name = read("product_name").unwrap_or_default();
        devices.push(Device {
            kind: Kind::Rocm,
            id,
            index: 0,
            model: amd_model(&name, num(p, "device_id"), num(p, "gfx_target_version")),
            name,
            memory_bytes: read("mem_info_vram_total").and_then(|m| m.parse().ok()).unwrap_or(0),
            numa: numa_node(fs, &format!("{dir}/numa_node")),
            labels: read("current_compute_partition").map(|part| ("partition".to_string(), part)).into_iter().collect(),
        });
    }
    let devices = numbered(inherited(devices, env.get("ROCR_VISIBLE_DEVICES")));

    let kept: BTreeMap<u64, &str> = devices.iter().map(|d| (node_of[&d.id], d.id.as_str())).collect();
    let mut pairs = BTreeSet::new();
    for (node, sub) in kept.keys().flat_map(|n| [(n, "io_links"), (n, "p2p_links")]) {
        let dir = format!("{KFD}/{node}/{sub}");
        for link in fs.list(&dir) {
            let Some(p) = kfd_properties(fs, &format!("{dir}/{link}/properties")) else { continue };
            let (a, b) = (num(&p, "node_from"), num(&p, "node_to"));
            if p.get("type") == Some(&CRAT_IOLINK_TYPE_XGMI) && a != b && kept.contains_key(&a) && kept.contains_key(&b)
            {
                pairs.insert((a.min(b), a.max(b)));
            }
        }
    }
    let links = pairs
        .into_iter()
        .map(|(a, b)| Link { a: kept[&a].to_string(), b: kept[&b].to_string(), kind: LinkKind::Xgmi })
        .collect();
    Inventory { devices, links, domain: None }
}

/// TPU chips: `/dev/accel<N>` entries that are not directories (NVIDIA
/// drivers ≥ 570 create a `/dev/accel/` directory), numbered N; otherwise the
/// numeric `/dev/vfio` groups backed by a Google (`0x1ae0`) PCI device,
/// numbered in group order. Model, labels and the slice name come from the
/// GKE `TPU_*` variables.
pub fn discover_tpu(fs: &dyn SysFs, env: &BTreeMap<String, String>) -> Inventory {
    let mut chips: Vec<u32> = fs
        .list("/dev")
        .iter()
        .filter(|e| !fs.is_dir(&format!("/dev/{e}")))
        .filter_map(|e| e.strip_prefix("accel")?.parse().ok())
        .collect();
    if chips.is_empty() {
        let google = |group: &String| {
            let dir = format!("/sys/kernel/iommu_groups/{group}/devices");
            fs.list(&dir)
                .iter()
                .any(|d| fs.read(&format!("{dir}/{d}/vendor")).is_some_and(|v| v.trim().eq_ignore_ascii_case("0x1ae0")))
        };
        let groups = fs.list("/dev/vfio").into_iter().filter(|g| g.parse::<u32>().is_ok() && google(g)).count();
        chips = (0..groups as u32).collect();
    }
    chips.sort_unstable();
    let accelerator_type = env.get("TPU_ACCELERATOR_TYPE");
    let labels: BTreeMap<String, String> = [("worker_id", "TPU_WORKER_ID"), ("topology", "TPU_TOPOLOGY")]
        .into_iter()
        .filter_map(|(label, var)| Some((label.to_string(), env.get(var)?.clone())))
        .collect();
    let devices = chips
        .into_iter()
        .map(|n| Device {
            kind: Kind::Tpu,
            id: format!("tpu:{n}"),
            index: n,
            model: accelerator_type.map_or_else(|| "unknown".to_string(), |t| tpu_model(t)),
            name: accelerator_type.cloned().unwrap_or_default(),
            memory_bytes: 0,
            numa: None,
            labels: labels.clone(),
        })
        .collect();
    Inventory { devices, links: Vec::new(), domain: env.get("TPU_NAME").cloned() }
}

/// The Apple GPU, given `uname -m`, the CPU brand string and `hw.memsize`
/// (unified memory). `None` unless the host is an arm64 Apple machine.
pub fn mps_device(arch: &str, brand: &str, memsize: u64) -> Option<Device> {
    (arch == "arm64" && brand.starts_with("Apple")).then(|| Device {
        kind: Kind::Mps,
        id: "mps:0".to_string(),
        index: 0,
        model: apple_model(brand),
        name: brand.trim().to_string(),
        memory_bytes: memsize,
        numa: None,
        labels: BTreeMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn device(kind: Kind, model: &str) -> Device {
        Device {
            kind,
            id: String::new(),
            index: 0,
            model: model.to_string(),
            name: String::new(),
            memory_bytes: 0,
            numa: None,
            labels: BTreeMap::new(),
        }
    }

    #[test]
    fn kinds_parse_case_insensitively_and_suggest_the_closest() {
        for kind in Kind::ALL {
            assert_eq!(kind.as_str().to_uppercase().parse::<Kind>(), Ok(kind));
        }
        let err = "cdua".parse::<Kind>().unwrap_err();
        assert!(
            err.contains("did you mean \"cuda\"") && err.contains("cuda, rocm, tpu, neuron, hpu, xpu, mps"),
            "{err}"
        );
        // tpu, hpu and xpu are all one edit from "gpu": no single suggestion.
        assert!(!"gpu".parse::<Kind>().unwrap_err().contains("did you mean"));
        assert!(!"nvidia".parse::<Kind>().unwrap_err().contains("did you mean"));
    }

    #[test]
    fn framework_names() {
        let torch: Vec<&str> = Kind::ALL.iter().map(|k| k.torch_device()).collect();
        assert_eq!(torch, ["cuda", "cuda", "xla", "xla", "hpu", "xpu", "mps"]);
        let dist: Vec<&str> = Kind::ALL.iter().map(|k| k.dist_backend()).collect();
        assert_eq!(dist, ["nccl", "nccl", "xla", "xla", "hccl", "xccl", "gloo"]);
    }

    #[test]
    fn selectors_match_kind_and_model_prefix() {
        let cases = [
            ("cuda:H100", Kind::Cuda, "H100 80GB HBM3"),
            ("rocm:MI300X", Kind::Rocm, "MI300X-OAM"),
            ("tpu:v5e", Kind::Tpu, "v5e"),
            ("mps:M3", Kind::Mps, "M3 Max"),
            ("CUDA:h100", Kind::Cuda, "H100 80GB HBM3"),
            ("cuda", Kind::Cuda, "anything"),
            ("cuda:A100:1g.10gb", Kind::Cuda, "A100:1g.10gb"),
        ];
        for (sel, kind, model) in cases {
            assert!(Selector::parse(sel).unwrap().matches(&device(kind, model)), "{sel} vs {model}");
        }
        let h100 = Selector::parse("cuda:H100").unwrap();
        assert!(!h100.matches(&device(Kind::Rocm, "H100")));
        assert!(!h100.matches(&device(Kind::Cuda, "A100")));
        assert!(!Selector::parse("cuda:80GB").unwrap().matches(&device(Kind::Cuda, "H100 80GB HBM3")));
        assert!(Selector::parse("").is_err());
        assert!(Selector::parse("cuda:").unwrap_err().contains("empty model"));
        assert!(Selector::parse("gpu:H100").is_err());
    }

    #[test]
    fn model_normalization() {
        assert_eq!(nvidia_model("NVIDIA H100 80GB HBM3"), "H100 80GB HBM3");
        assert_eq!(nvidia_model("Tesla V100-SXM2-16GB"), "V100-SXM2-16GB");
        assert_eq!(amd_model("AMD Instinct MI300X\n", 29857, 90402), "MI300X");
        assert_eq!(amd_model("", 29857, 90402), "MI300X-OAM");
        assert_eq!(amd_model("", 0x740f, 90010), "MI210");
        assert_eq!(amd_model("", 1, 90402), "gfx942");
        assert_eq!(amd_model("", 1, 90010), "gfx90a");
        assert_eq!(amd_model("", 1, 110000), "gfx1100");
        let tpus = [("v5litepod-16", "v5e"), ("v6e-8", "v6e"), ("v4-16", "v4"), ("v5p-8", "v5p"), ("v2-8", "v2")];
        for (accelerator_type, model) in tpus {
            assert_eq!(tpu_model(accelerator_type), model);
        }
        assert_eq!(tpu_model("v7x"), "v7x");
        assert_eq!(apple_model("Apple M3 Max"), "M3 Max");
    }

    #[test]
    fn mps_only_on_apple_silicon() {
        let d = mps_device("arm64", "Apple M3 Max", 64 << 30).unwrap();
        assert_eq!((d.id.as_str(), d.model.as_str(), d.memory_bytes), ("mps:0", "M3 Max", 64 << 30));
        assert!(mps_device("x86_64", "Apple M3 Max", 1).is_none());
        assert!(mps_device("arm64", "Ampere Altra", 1).is_none());
    }
}
