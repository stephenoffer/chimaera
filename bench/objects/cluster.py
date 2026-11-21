"""One Chimaera peer per node of a Ray (Anyscale) cluster, for the
multi-node object scenarios; Ray is only the launcher here.

    python bench/objects/cluster.py up --dist /tmp/chimaera-devdist [--env K=V ...]
    python bench/objects/cluster.py status | logs | down
    python bench/objects/cluster.py kill --ip 10.0.0.5     # SIGKILL that node's peer and its executors
    python bench/objects/cluster.py exec --ip 10.0.0.5 -- df -h /dev/shm

Each node gets the dist directory's binary and wheel, a virtualenv at VENV
with the wheel installed (executors run there), and a peer under PEER_DIR.
The driver then connects through the head node's peer: CHIMAERA_HOME=PEER_DIR.
"""

from __future__ import annotations

import argparse
import os
import sys

PORT = 7300
PEER_DIR = "/tmp/ch-objects"
VENV = "/mnt/local_storage/devenv"
NAMESPACE = "chimaera-objects"


def _ray():
    import ray

    ray.init(address="auto", namespace=NAMESPACE, log_to_driver=False)
    return ray


def _host_class(ray):
    @ray.remote(num_cpus=0)
    class PeerHost:
        def __init__(self, files: dict[str, bytes], seeds: str, env: dict[str, str]) -> None:
            import glob
            import socket
            import subprocess

            here = "/tmp/chimaera-objects-dist"
            os.makedirs(here, exist_ok=True)
            for name, data in files.items():
                with open(os.path.join(here, name + ".new"), "wb") as f:
                    f.write(data)
                os.replace(os.path.join(here, name + ".new"), os.path.join(here, name))
            os.chmod(os.path.join(here, "chimaera"), 0o755)
            if not os.path.exists(os.path.join(VENV, "bin", "python")):
                subprocess.run([sys.executable, "-m", "venv", "--system-site-packages", VENV], check=True)
            wheel = glob.glob(os.path.join(here, "*.whl"))[0]
            subprocess.run([os.path.join(VENV, "bin", "python"), "-m", "pip", "install", "-q", "--force-reinstall",
                            "--no-deps", wheel], check=True)
            ip = ray.util.get_node_ip_address()
            self.ip = ip
            self.log_path = "/tmp/chimaera-objects-peer.log"
            penv = dict(os.environ, CHIMAERA_PYTHON=os.path.join(VENV, "bin", "python"), **env)
            log = open(self.log_path, "ab")
            self.proc = subprocess.Popen(
                [os.path.join(here, "chimaera"), "--dir", PEER_DIR, "peer", "start", "--listen", f"{ip}:{PORT}",
                 "--seeds", seeds, "--cluster", "objects", "--cpus", str(os.cpu_count()),
                 "--token-file", os.path.join(here, "token")],
                stdout=log, stderr=log, start_new_session=True, env=penv,
            )
            self.desc = f"{ip} ({socket.gethostname()})"

        def describe(self) -> str:
            return self.desc

        def alive(self) -> bool:
            return self.proc.poll() is None

        def log_tail(self, n: int = 3000) -> str:
            return open(self.log_path, errors="replace").read()[-n:]

        def kill(self) -> str:
            """SIGKILL the peer's whole process group (the peer and its executors)."""
            import signal

            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc.wait()
            return f"killed peer on {self.ip}"

        def stop(self) -> None:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=60)
                except Exception:  # noqa: BLE001
                    self.kill()

        def exec(self, cmd: str) -> str:
            import subprocess

            p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600, check=False)
            return p.stdout + p.stderr

    return PeerHost


def nodes(ray) -> list[dict]:
    return sorted((n for n in ray.nodes() if n["Alive"]), key=lambda n: n["NodeManagerAddress"])


def host(ray, ip: str):
    for n in nodes(ray):
        if n["NodeManagerAddress"] == ip:
            return ray.get_actor(f"peer-{n['NodeID']}")
    raise SystemExit(f"no node {ip}")


def up(dist: str, env: dict[str, str]) -> None:
    ray = _ray()
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    files = {name: open(os.path.join(dist, name), "rb").read() for name in os.listdir(dist)}
    if "token" not in files:
        import secrets

        files["token"] = secrets.token_hex(32).encode()
    ns = nodes(ray)
    seeds = ",".join(f"{n['NodeManagerAddress']}:{PORT}" for n in ns)
    PeerHost = _host_class(ray)
    hosts = [
        PeerHost.options(name=f"peer-{n['NodeID']}", lifetime="detached", get_if_exists=True,
                         scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False)
                         ).remote(files, seeds, env)
        for n in ns
    ]
    print("peers:", ray.get([h.describe.remote() for h in hosts]), flush=True)


def down() -> None:
    ray = _ray()
    for n in nodes(ray):
        try:
            h = ray.get_actor(f"peer-{n['NodeID']}")
        except ValueError:
            continue
        try:
            ray.get(h.stop.remote(), timeout=90)
        except Exception as e:  # noqa: BLE001
            print("stop:", e)
        ray.kill(h)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["up", "down", "status", "logs", "kill", "exec"])
    ap.add_argument("--dist", default="/tmp/chimaera-devdist")
    ap.add_argument("--env", action="append", default=[])
    ap.add_argument("--ip")
    ap.add_argument("rest", nargs=argparse.REMAINDER)
    a = ap.parse_args()
    if a.cmd == "up":
        up(a.dist, dict(kv.split("=", 1) for kv in a.env))
    elif a.cmd == "down":
        down()
    else:
        ray = _ray()
        if a.cmd in ("kill", "exec"):
            h = host(ray, a.ip)
            cmd = " ".join(a.rest[1:] if a.rest[:1] == ["--"] else a.rest)
            print(ray.get(h.kill.remote() if a.cmd == "kill" else h.exec.remote(cmd)))
            return
        for n in nodes(ray):
            try:
                h = ray.get_actor(f"peer-{n['NodeID']}")
            except ValueError:
                print(n["NodeManagerAddress"], "no peer")
                continue
            if a.cmd == "status":
                print(n["NodeManagerAddress"], "alive" if ray.get(h.alive.remote()) else "dead")
            else:
                print("=====", n["NodeManagerAddress"], "\n", ray.get(h.log_tail.remote()))


if __name__ == "__main__":
    main()
