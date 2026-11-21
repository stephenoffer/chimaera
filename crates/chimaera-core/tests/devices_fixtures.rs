//! Device discovery against captured host trees in `tests/fixtures/topology/`.

use chimaera_core::devices::*;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A fixture tree mirroring absolute paths, read with std::fs.
struct Fixture(PathBuf);

impl Fixture {
    fn new(machine: &str) -> Fixture {
        Fixture(PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/fixtures/topology")).join(machine))
    }

    fn at(&self, path: &str) -> PathBuf {
        self.0.join(path.trim_start_matches('/'))
    }

    /// The fixture's `env` file of `KEY=VALUE` lines.
    fn env(&self) -> BTreeMap<String, String> {
        let text = std::fs::read_to_string(self.at("/env")).unwrap_or_default();
        text.lines().filter_map(|l| l.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }
}

impl SysFs for Fixture {
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

const H100_UUIDS: [&str; 8] = [
    "GPU-3f5b1c9e-8d2a-4e17-9b6c-0a1d2e3f4a5b",
    "GPU-91c4e2d7-1b3a-4c58-8e6f-2b3c4d5e6f70",
    "GPU-a7e0f3b2-5c6d-4e8f-9a0b-1c2d3e4f5a6b",
    "GPU-2d8b6a1f-7e9c-4b3d-a5f6-7a8b9c0d1e2f",
    "GPU-c5f17e3a-9b2d-4f6e-8c1a-3d4e5f6a7b8c",
    "GPU-6e3a9d4b-2f1c-4a7e-b8d9-0e1f2a3b4c5d",
    "GPU-e4b2c8f1-3a5d-4c9e-9f0a-5b6c7d8e9f01",
    "GPU-58d1a6c3-4e7b-4d2f-a9c8-6f7a8b9c0d1e",
];

fn ids(inv: &Inventory) -> Vec<&str> {
    inv.devices.iter().map(|d| d.id.as_str()).collect()
}

fn indices(inv: &Inventory) -> Vec<u32> {
    inv.devices.iter().map(|d| d.index).collect()
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn dgx_h100_lists_eight_gpus_in_pci_order() {
    let inv = discover_cuda(&Fixture::new("dgx-h100"), &BTreeMap::new());
    assert_eq!(ids(&inv), H100_UUIDS);
    assert_eq!(indices(&inv), (0..8).collect::<Vec<_>>());
    let numa: Vec<Option<u32>> = inv.devices.iter().map(|d| d.numa).collect();
    assert_eq!(numa, [Some(0), Some(0), Some(0), Some(0), Some(1), Some(1), Some(1), Some(1)]);
    let d = &inv.devices[3];
    assert_eq!(
        (d.kind, d.model.as_str(), d.name.as_str(), d.memory_bytes),
        (Kind::Cuda, "H100 80GB HBM3", "NVIDIA H100 80GB HBM3", 0)
    );
    assert_eq!(d.labels, env(&[("pci", "0000:5d:00.0"), ("minor", "3")]));
    assert!(inv.links.is_empty() && inv.domain.is_none());
    assert!(inv.devices.iter().all(|d| Selector::parse("cuda:H100").unwrap().matches(d)));
}

#[test]
fn k8s_device_nodes_hide_unallocated_gpus() {
    let inv = discover_cuda(&Fixture::new("dgx-h100-k8s"), &BTreeMap::new());
    assert_eq!(ids(&inv), [H100_UUIDS[3], H100_UUIDS[5]]);
    assert_eq!(indices(&inv), [0, 1]);
    // Inherited indices count the GPUs this peer can open.
    let inv = discover_cuda(&Fixture::new("dgx-h100-k8s"), &env(&[("CUDA_VISIBLE_DEVICES", "1")]));
    assert_eq!((ids(&inv), indices(&inv)), (vec![H100_UUIDS[5]], vec![0]));
}

#[test]
fn inherited_cuda_visible_devices_narrows_discovery() {
    let fs = Fixture::new("dgx-h100");
    let visible = |value: &str| discover_cuda(&fs, &env(&[("CUDA_VISIBLE_DEVICES", value)]));
    assert_eq!(ids(&visible(H100_UUIDS[6])), [H100_UUIDS[6]]);
    assert_eq!(ids(&visible("GPU-3f5b")), [H100_UUIDS[0]]);
    let by_index = visible("4,1");
    assert_eq!((ids(&by_index), indices(&by_index)), (vec![H100_UUIDS[1], H100_UUIDS[4]], vec![0, 1]));
    assert!(visible("").devices.is_empty());
    // Like CUDA, an entry naming no GPU (or several) ends the list.
    assert_eq!(ids(&visible("2,-1,3")), [H100_UUIDS[2]]);
    assert_eq!(ids(&visible("5,GPU-")), [H100_UUIDS[5]]);
    assert_eq!(ids(&visible("8")), Vec::<&str>::new());
}

#[test]
fn mi300x_spx_has_eight_gpus_linked_all_to_all() {
    let inv = discover_rocm(&Fixture::new("mi300x-spx"), &BTreeMap::new());
    assert_eq!(inv.devices.len(), 8);
    assert_eq!(
        (ids(&inv)[0], ids(&inv)[1], ids(&inv)[7]),
        ("GPU-81df2366eb0b6c8d", "GPU-17e0abae018378f7", "GPU-316b515bb91d871a")
    );
    assert_eq!(indices(&inv), (0..8).collect::<Vec<_>>());
    for (i, d) in inv.devices.iter().enumerate() {
        assert_eq!((d.kind, d.model.as_str(), d.name.as_str()), (Kind::Rocm, "MI300X-OAM", ""));
        assert_eq!((d.memory_bytes, d.numa), (206158430208, Some(if i < 4 { 0 } else { 1 })));
        assert_eq!(d.labels, env(&[("partition", "SPX")]));
    }
    assert!(inv.devices.iter().all(|d| Selector::parse("rocm:MI300X").unwrap().matches(d)));
    assert_eq!(inv.links.len(), 28);
    assert!(inv.links.iter().all(|l| l.kind == LinkKind::Xgmi && l.a != l.b));

    let fs = Fixture::new("mi300x-spx");
    let visible = |value: &str| discover_rocm(&fs, &env(&[("ROCR_VISIBLE_DEVICES", value)]));
    let two = visible("0,2");
    assert_eq!(ids(&two), [inv.devices[0].id.as_str(), inv.devices[2].id.as_str()]);
    assert_eq!((indices(&two), two.links.len()), (vec![0, 1], 1));
    assert_eq!(ids(&visible("GPU-316b515bb91d871a")), ["GPU-316b515bb91d871a"]);
    assert!(visible("").devices.is_empty());
}

#[test]
fn mi300x_cpx_partitions_share_a_unique_id() {
    let inv = discover_rocm(&Fixture::new("mi300x-cpx-1gpu"), &BTreeMap::new());
    let want: Vec<String> = (1..=8).map(|node| format!("GPU-81df2366eb0b6c8d.{node}")).collect();
    assert_eq!(ids(&inv), want);
    assert_eq!(indices(&inv), (0..8).collect::<Vec<_>>());
    for d in &inv.devices {
        assert_eq!((d.model.as_str(), d.name.as_str()), ("MI300X", "AMD Instinct MI300X"));
        assert_eq!((d.memory_bytes, d.numa), (25769803776, Some(0)));
        assert_eq!(d.labels, env(&[("partition", "CPX")]));
    }
    assert!(inv.links.is_empty());
}

#[test]
fn tpu_v5e_host_from_accel_nodes() {
    let fs = Fixture::new("tpu-v5e-host");
    let inv = discover_tpu(&fs, &fs.env());
    assert_eq!(ids(&inv), ["tpu:0", "tpu:1", "tpu:2", "tpu:3"]);
    assert_eq!(indices(&inv), [0, 1, 2, 3]);
    let d = &inv.devices[0];
    assert_eq!((d.kind, d.model.as_str(), d.name.as_str()), (Kind::Tpu, "v5e", "v5litepod-16"));
    assert_eq!(d.labels, env(&[("worker_id", "1"), ("topology", "4x4")]));
    assert_eq!(inv.domain.as_deref(), Some("slice-a"));
    assert!(Selector::parse("tpu:v5e").unwrap().matches(d));
}

#[test]
fn tpu_v6e_from_vfio_ignores_the_nvidia_accel_directory() {
    let fs = Fixture::new("tpu-v6e-vfio");
    let inv = discover_tpu(&fs, &fs.env());
    assert_eq!(ids(&inv), ["tpu:0", "tpu:1", "tpu:2", "tpu:3"]);
    assert!(inv.devices.iter().all(|d| d.model == "v6e" && d.labels.is_empty()));
    assert!(inv.domain.is_none());
    assert_eq!(discover_tpu(&fs, &BTreeMap::new()).devices[0].model, "unknown");
}

type Env = Vec<(&'static str, Option<String>)>;

fn set(pairs: &[(&'static str, Option<&str>)]) -> Env {
    pairs.iter().map(|(k, v)| (*k, v.map(String::from))).collect()
}

#[test]
fn visibility_env_for_cuda_and_rocm_grants() {
    let cuda = discover_cuda(&Fixture::new("dgx-h100"), &BTreeMap::new()).devices;
    let grant = [&cuda[1], &cuda[4]];
    let both = format!("{},{}", H100_UUIDS[1], H100_UUIDS[4]);
    assert_eq!(visibility_env(Kind::Cuda, &grant, 8), set(&[("CUDA_VISIBLE_DEVICES", Some(&both))]));
    assert_eq!(visibility_env(Kind::Cuda, &[], 8), set(&[("CUDA_VISIBLE_DEVICES", Some(""))]));

    let rocm = discover_rocm(&Fixture::new("mi300x-spx"), &BTreeMap::new()).devices;
    let removed = [("CUDA_VISIBLE_DEVICES", None), ("GPU_DEVICE_ORDINAL", None)];
    let want = |hip| [&[("HIP_VISIBLE_DEVICES", Some(hip))][..], &removed].concat();
    assert_eq!(visibility_env(Kind::Rocm, &[&rocm[0], &rocm[3]], 8), set(&want("0,3")));
    assert_eq!(visibility_env(Kind::Rocm, &[], 8), set(&want("")));
}

#[test]
fn visibility_env_for_tpu_grants_follows_ray() {
    let fs = Fixture::new("tpu-v5e-host");
    let chips = discover_tpu(&fs, &fs.env()).devices;
    let tpu = |grant: &[&Device]| visibility_env(Kind::Tpu, grant, chips.len());
    let bounds = |mask, per_host| {
        set(&[
            ("TPU_VISIBLE_CHIPS", Some(mask)),
            ("TPU_CHIPS_PER_HOST_BOUNDS", Some(per_host)),
            ("TPU_HOST_BOUNDS", Some("1,1,1")),
        ])
    };
    assert_eq!(tpu(&[&chips[2]]), bounds("2", "1,1,1"));
    assert_eq!(tpu(&[&chips[3], &chips[1]]), bounds("1,3", "1,2,1"));
    assert_eq!(tpu(&chips.iter().collect::<Vec<_>>()), set(&[("TPU_VISIBLE_CHIPS", None)]));
    assert_eq!(tpu(&[&chips[0], &chips[1], &chips[2]]), set(&[("TPU_VISIBLE_CHIPS", Some("0,1,2"))]));
    assert_eq!(tpu(&[]), set(&[("TPU_VISIBLE_CHIPS", Some(""))]));
}

#[test]
fn visibility_env_for_the_other_kinds() {
    let unit = |kind, index, module: Option<&str>| Device {
        kind,
        id: format!("{index}"),
        index,
        model: String::new(),
        name: String::new(),
        memory_bytes: 0,
        numa: None,
        labels: module.map(|m| ("module_id".to_string(), m.to_string())).into_iter().collect(),
    };
    let (a, b) = (unit(Kind::Neuron, 0, None), unit(Kind::Neuron, 5, None));
    assert_eq!(visibility_env(Kind::Neuron, &[&a, &b], 32), set(&[("NEURON_RT_VISIBLE_CORES", Some("0,5"))]));
    assert_eq!(visibility_env(Kind::Neuron, &[], 32), set(&[("NEURON_RT_VISIBLE_CORES", Some(""))]));
    let (a, b) = (unit(Kind::Hpu, 0, Some("3")), unit(Kind::Hpu, 1, None));
    assert_eq!(visibility_env(Kind::Hpu, &[&a, &b], 8), set(&[("HABANA_VISIBLE_MODULES", Some("3,1"))]));
    assert_eq!(visibility_env(Kind::Hpu, &[], 8), set(&[("HABANA_VISIBLE_MODULES", Some(""))]));
    let a = unit(Kind::Xpu, 2, None);
    let xpu = |mask| {
        set(&[
            ("ZE_AFFINITY_MASK", Some(mask)),
            ("ZE_ENABLE_PCI_ID_DEVICE_ORDER", Some("1")),
            ("ZE_FLAT_DEVICE_HIERARCHY", Some("COMPOSITE")),
            ("ONEAPI_DEVICE_SELECTOR", None),
        ])
    };
    assert_eq!(visibility_env(Kind::Xpu, &[&a], 4), xpu("2"));
    assert_eq!(visibility_env(Kind::Xpu, &[], 4), xpu(""));
    let mps = mps_device("arm64", "Apple M3 Max", 1 << 30).unwrap();
    assert!(visibility_env(Kind::Mps, &[&mps], 1).is_empty());
}

#[test]
fn neuron_cores_of_the_mounted_devices_pair_up_on_trn2() {
    let fs = Fixture::new("neuron-trn2-pod");
    let inv = discover_neuron(&fs, &BTreeMap::new());
    // neuron1 is on the host but not mounted; 8 physical cores pair into 4 logical ones.
    assert_eq!(
        ids(&inv),
        ["neuron0:0", "neuron0:1", "neuron0:2", "neuron0:3", "neuron2:0", "neuron2:1", "neuron2:2", "neuron2:3"]
    );
    assert_eq!(indices(&inv), (0..8).collect::<Vec<_>>());
    let d = &inv.devices[5];
    assert_eq!((d.kind, d.model.as_str(), d.name.as_str()), (Kind::Neuron, "trn2", "Trainium2"));
    assert_eq!((d.memory_bytes, d.labels["device"].as_str()), (24 << 30, "2"));
    assert_eq!(discover_neuron(&fs, &env(&[("NEURON_LOGICAL_NC_CONFIG", "1")])).devices.len(), 16);
    // An inherited core list (ranges too) narrows without renumbering: a grant's list replaces it.
    let narrowed = discover_neuron(&fs, &env(&[("NEURON_RT_VISIBLE_CORES", "1-2,6")]));
    assert_eq!((ids(&narrowed), indices(&narrowed)), (vec!["neuron0:1", "neuron0:2", "neuron2:2"], vec![1, 2, 6]));
    assert!(discover_neuron(&fs, &env(&[("NEURON_RT_VISIBLE_CORES", "")])).devices.is_empty());
}

#[test]
fn gaudi_cards_are_usable_mounted_habana_ones_in_module_order() {
    let fs = Fixture::new("gaudi2");
    let inv = discover_hpu(&fs, &BTreeMap::new());
    // accel2 needs a reset, accel3 is not mounted, accel4 is an Intel NPU.
    assert_eq!(ids(&inv), ["hpu:4", "hpu:5"]);
    assert_eq!(indices(&inv), [0, 1]);
    let d = &inv.devices[0];
    assert_eq!((d.model.as_str(), d.name.as_str(), d.numa), ("gaudi2", "GAUDI2", Some(0)));
    assert_eq!(d.labels["pci"], "0000:1a:00.0");
    let one = discover_hpu(&fs, &env(&[("HABANA_VISIBLE_MODULES", "5,7")]));
    assert_eq!(ids(&one), ["hpu:5"]);
    let grant: Vec<&Device> = one.devices.iter().collect();
    assert_eq!(visibility_env(Kind::Hpu, &grant, 2), set(&[("HABANA_VISIBLE_MODULES", Some("5"))]));
    assert!(Selector::parse("hpu:gaudi2").unwrap().matches(d));
}

#[test]
fn intel_cards_are_numbered_in_pci_order_and_the_integrated_gpu_is_skipped() {
    let fs = Fixture::new("intel-max1550-igpu");
    let inv = discover_xpu(&fs, &BTreeMap::new());
    // Level Zero numbers the integrated GPU (00:02.0) 0; renderD132 is not mounted; renderD131 is AMD.
    assert_eq!(ids(&inv), ["xpu:0000:29:00.0", "xpu:0000:9a:00.0"]);
    assert_eq!(indices(&inv), [1, 2]);
    assert_eq!((inv.devices[0].model.as_str(), inv.devices[0].numa), ("Max 1550", Some(0)));
    assert!(Selector::parse("xpu:max").unwrap().matches(&inv.devices[1]));
    // An inherited mask by card or by tile keeps the card and its ordinal.
    for mask in ["2", "2.0,2.1"] {
        let narrowed = discover_xpu(&fs, &env(&[("ZE_AFFINITY_MASK", mask)]));
        assert_eq!((ids(&narrowed), indices(&narrowed)), (vec!["xpu:0000:9a:00.0"], vec![2]));
    }
}
