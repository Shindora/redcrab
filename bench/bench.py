#!/usr/bin/env python3
"""Local Kafka-protocol benchmark harness for Apache Kafka, Redpanda and redcrab.

Usage (podman with the pinned images already pulled; redcrab also needs cargo and the <arch>-unknown-linux-musl target):
    python3 bench/bench.py run <target> <workload> [--brokers 3] [--seconds 60] [--partitions 100]
        targets:   kafka, kafka-fsync (flush.messages=1), redpanda, redcrab
        workloads: baseline, max, small-batch, backlog
    python3 bench/bench.py run redcrab baseline --bootstrap host.containers.internal:19092 [--replication-factor 1]
    python3 bench/bench.py compare [--all]
    python3 bench/bench.py chart
    python3 bench/bench.py verify-failover <target> [--fault partition]

A run starts the target's cluster (3 brokers at replication factor 3, or with --brokers 1 one broker at replication
factor 1; redcrab from an image built from this checkout), waits for it, creates the topic, drives load with Kafka's own
perf tools from a client container (same pinned Kafka image and client settings for every target), tears the cluster down
with its volumes (then fstrims the VM disk) and writes bench/results/<utc>-<target>-<workload>-<brokers>-broker-rf<rf>.json.
A run writes at most 12 GiB counting replicas and the probe; a max run that would write more is cut short and marked capped.
--bootstrap points a redcrab run at a broker started elsewhere instead. These are LOCAL numbers from a small Podman VM with
memory-capped brokers; real-hardware runs use the OpenMessaging Benchmark files in omb/. compare prints the latest result
per target, workload and setup (broker count and replication factor), or every result with --all; chart draws light and
dark SVGs per setup in bench/charts/. verify-failover starts 3 brokers, kills the leader of a one-partition topic several
times under an acks=all producer, and checks that every acknowledged record can still be consumed; --fault partition cuts
brokers off the network and heals them instead, and also checks that the replicas converge and live reads hold.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import itertools
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

KAFKA_IMAGE = "docker.io/apache/kafka:4.4.0@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f"
REDPANDA_IMAGE = "docker.io/redpandadata/redpanda:v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649"
REDCRAB_IMAGE = "localhost/redcrab-bench:latest"
RUST_ARCH = {"arm64": "aarch64", "amd64": "x86_64"}

BENCH_DIR = Path(__file__).resolve().parent
REPO_DIR = BENCH_DIR.parent
RESULTS_DIR = BENCH_DIR / "results"
NETWORK = "bench_default"
CLUSTER_BOOTSTRAP = "bench-1:9092"
TOPIC = "bench"
BIN = "/opt/kafka/bin"
BROKERS = 3
CLIENT_NAMES = ("bench-admin", "bench-producer", "bench-consumer", "bench-live", *(f"bench-dump-{i}" for i in range(1, BROKERS + 1)))

BROKER_MEM_LIMIT = "384m"
KAFKA_HEAP = "-Xms128m -Xmx128m"
REDPANDA_MEMORY = "256M"
CLIENT_HEAP = "-Xmx96m"
CLIENT_JVM_OPTS = "-XX:+UseSerialGC"
COMPOSE_ENV = {
    "KAFKA_IMAGE": KAFKA_IMAGE,
    "REDPANDA_IMAGE": REDPANDA_IMAGE,
    "REDCRAB_IMAGE": REDCRAB_IMAGE,
    "BROKER_MEM_LIMIT": BROKER_MEM_LIMIT,
    "BROKER_HEAP": KAFKA_HEAP,
    "REDPANDA_MEMORY": REDPANDA_MEMORY,
    "PODMAN_COMPOSE_WARNING_LOGS": "false",
}

RECORD_SIZE = 1024
PARTITIONS = 100
REPLICATION_FACTOR = 3
MIN_INSYNC_REPLICAS = 2
Setup = tuple[int | None, int]
SETUPS: tuple[Setup, ...] = ((1, 1), (BROKERS, REPLICATION_FACTOR))
PRODUCER_PROPS = ("acks=all", "linger.ms=1", "batch.size=131072")
CONSUMER_PROPS = ("fetch.max.bytes=16777216",)
PROBE_SECONDS = 15
DISK_MARGIN = 10 * 2**30
MAX_RUN_BYTES = 12 * 2**30
PROBE_SHARE = 3
WARMUP_SECONDS = 60
TOPIC_CONFIG_KEYS = ("min.insync.replicas", "flush.messages", "flush.ms", "write.caching")
KAFKA_BROKER_CONFIG_KEYS = (
    "log.flush.interval.messages",
    "min.insync.replicas",
    "offsets.topic.replication.factor",
    "transaction.state.log.replication.factor",
    "transaction.state.log.min.isr",
    "share.coordinator.state.topic.replication.factor",
    "share.coordinator.state.topic.min.isr",
)
CONFIG_RE = re.compile(r"^\s+([\w.]+)=(\S*) sensitive=", re.MULTILINE)
ALL_BROKERS = ",".join(f"bench-{i}:9092" for i in range(1, BROKERS + 1))
FAILOVER_KILLS = 3
FAILOVER_RATE = 1000
# A killed container's address vanishes, so connecting to it hangs until the client's 10-30 s setup timeout; 1 s keeps that
# client artefact from dominating the measured failover.
FAILOVER_PRODUCER_PROPS = ("socket.connection.setup.timeout.ms=1000", "socket.connection.setup.timeout.max.ms=1000")
FAILOVER_SETTLE_SECONDS = 10
FAILOVER_TIMEOUT_SECONDS = 60
ACK_RUN_MS = 2000
FAULTS = ("kill", "partition")
LOG_FILES = {"kafka.yml": f"/var/lib/kafka/data/{TOPIC}-0/*.log", "redcrab.yml": "/data/partitions/*-0/*.log"}
RECORD_RE = re.compile(r"^Offset:(\d+)\t(-?\d+)$", re.MULTILINE)
BATCH_RE = re.compile(r"baseOffset: (\d+) lastOffset: (\d+) .*?partitionLeaderEpoch: (-?\d+) .*?crc: (\d+)")
Batch = tuple[int, int, int, int]
NOTE = "LOCAL smoke numbers from a small Podman VM with memory-capped brokers; not a real benchmark"
UNMANAGED = "unmanaged: runs outside the driver"

CHARTS_DIR = BENCH_DIR / "charts"
CHART_WIDTH = 800
CHART_FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
LABEL_COLUMN = 236
BODY_TOP = 112
HEADING_HEIGHT = 24
ROW_HEIGHT = 26
RUN_GAP = 6
GROUP_GAP = 8
CAPTION_CHARS = 116
THEMES = {
    "light": {
        "surface": "#fcfcfb",
        "frame": "#e1e0d9",
        "ink": "#0b0b0b",
        "ink2": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        "bar": "#2a78d6",
        "p50": "#6da7ec",
        "p99": "#2a78d6",
    },
    "dark": {
        "surface": "#1a1a19",
        "frame": "#2c2c2a",
        "ink": "#f0efec",
        "ink2": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
        "bar": "#3987e5",
        "p50": "#1c5cab",
        "p99": "#3987e5",
    },
}


@dataclass(frozen=True)
class Target:
    """A system under test; compose is None when the driver starts nothing."""

    name: str
    compose: str | None
    image: str | None
    flush_messages: str = "9223372036854775807"
    broker_memory: str = UNMANAGED


@dataclass(frozen=True)
class Workload:
    """Producer shape: rate is records/s (-1 = unthrottled); backlog_rate x seconds records are pre-filled when set."""

    name: str
    rate: int
    producer_props: tuple[str, ...] = PRODUCER_PROPS
    key_range: int = 0
    backlog_rate: int = 0


@dataclass(frozen=True)
class Run:
    """One benchmark invocation; brokers is None when the harness starts nothing."""

    target: Target
    workload: Workload
    seconds: int
    partitions: int
    brokers: int | None
    replication_factor: int
    bootstrap: str
    network: str | None

    @property
    def min_insync(self) -> int:
        """Return min.insync.replicas, never above the replication factor."""
        return min(MIN_INSYNC_REPLICAS, self.replication_factor)

    @property
    def nodes(self) -> list[int]:
        """Return the node ids of the brokers the harness starts."""
        return list(range(1, (self.brokers or 0) + 1))


@dataclass(frozen=True)
class ProducerStats:
    """Summary line of kafka-producer-perf-test (steady state when warmup was used)."""

    records: int
    steady_state: bool
    records_per_sec: float
    mb_per_sec: float
    latency_ms: dict[str, float]
    summary: str
    intervals: list[str]


@dataclass(frozen=True)
class ConsumerStats:
    """Summary of kafka-consumer-perf-test; rates exclude group-join time (fetch.* columns)."""

    requested: int
    records: int
    records_per_sec: float
    mb_per_sec: float
    raw: dict[str, str]


@dataclass(frozen=True)
class Isolation:
    """One round of the partition fault: cut partition 0's leader or a follower off the network, then heal it.

    The broker is healed once the producer is acknowledged again (a follower is first kept away for
    FAILOVER_SETTLE_SECONDS); with early_heal, a cut-off leader is healed as soon as another broker reports leading,
    while the producer may still be waiting on it.
    """

    role: str
    early_heal: bool = False


@dataclass
class FaultLog:
    """What the fault rounds of a failover check saw: the client's longest pause per round and anything that went wrong."""

    pauses: list[int] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)


PARTITION_ROUNDS = (
    Isolation("leader"),
    Isolation("leader"),
    Isolation("leader"),
    Isolation("follower"),
    Isolation("leader", early_heal=True),
)


TARGETS = {
    t.name: t
    for t in (
        Target("kafka", "kafka.yml", KAFKA_IMAGE, broker_memory=f"heap {KAFKA_HEAP}"),
        Target("kafka-fsync", "kafka.yml", KAFKA_IMAGE, flush_messages="1", broker_memory=f"heap {KAFKA_HEAP}"),
        Target("redpanda", "redpanda.yml", REDPANDA_IMAGE, broker_memory=f"--smp=1 --memory={REDPANDA_MEMORY}"),
        Target("redcrab", "redcrab.yml", REDCRAB_IMAGE, broker_memory="container cap only"),
    )
}
WORKLOADS = {
    w.name: w
    for w in (
        Workload("baseline", rate=10_000),
        Workload("max", rate=-1),
        Workload("small-batch", rate=1_000, producer_props=("acks=all", "linger.ms=0", "batch.size=16384"), key_range=10_000),
        Workload("backlog", rate=5_000, backlog_rate=50_000),
    )
}
INTERVAL_RE = re.compile(r"^(\d+) records sent, ([\d.]+) records/sec .* max latency\.$", re.MULTILINE)
PRODUCER_RE = re.compile(
    r"(\d+) (steady state )?records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, "
    r"([\d.]+) ms max latency, (\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th, (\d+) ms 99\.9th",
)


def log(message: str) -> None:
    """Write a progress line to stdout."""
    sys.stdout.write(f"{message}\n")
    sys.stdout.flush()


def spawn(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.Popen[str]:
    """Start a command with stdout and stderr merged into one text pipe."""
    log(f"$ {shlex.join(cmd)}")
    return subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)  # noqa: S603


def finish(proc: subprocess.Popen[str], timeout: float = 1800, *, check: bool = True) -> str:
    """Wait for a spawned command and return its output, raising on failure when check is set."""
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        raise
    if check and proc.returncode:
        msg = f"exit {proc.returncode}: {shlex.join(proc.args)}\n{out[-4000:]}"
        raise RuntimeError(msg)
    return out


def sh(cmd: list[str], env: dict[str, str] | None = None, timeout: float = 1800, *, check: bool = True) -> str:
    """Run a command to completion and return its combined output."""
    return finish(spawn(cmd, env), timeout, check=check)


def compose(run: Run, *args: str) -> None:
    """Run podman compose for a managed target with the pinned images, the memory caps and the run's broker count."""
    env = os.environ | COMPOSE_ENV
    env |= {
        "KAFKA_FLUSH_MESSAGES": run.target.flush_messages,
        "KAFKA_QUORUM_VOTERS": ",".join(f"{i}@bench-{i}:9093" for i in run.nodes),
        "REDPANDA_SEEDS": ",".join(f"bench-{i}:33145" for i in run.nodes),
        "REDCRAB_PEERS": ",".join(f"{i}@bench-{i}:9093/bench-{i}:9092" for i in run.nodes),
        "REPLICATION_FACTOR": str(run.replication_factor),
        "MIN_INSYNC_REPLICAS": str(run.min_insync),
    }
    sh(["podman", "compose", "-f", str(BENCH_DIR / str(run.target.compose)), *args], env, timeout=300)


def start_cluster(run: Run) -> None:
    """Remove anything left from an earlier run, then start the run's brokers from local images only."""
    cleanup(run)
    compose(run, "up", "--detach", "--pull=never", *(f"bench-{i}" for i in run.nodes))


def build_redcrab() -> dict[str, str]:
    """Cross-compile redcrab as a static Linux binary for the VM's CPU and package it as REDCRAB_IMAGE.

    Linking uses the rust-lld that ships with Rust, so no C toolchain is needed, and nothing is compiled inside the small
    VM. cargo relinks only when the source changed and Podman reuses its cached layer for an unchanged binary, so this is
    cheap when nothing is stale.

    Returns:
        The build identity recorded in the result file.
    """
    arch = sh(["podman", "info", "--format", "{{.Host.Arch}}"]).strip()
    triple = f"{RUST_ARCH.get(arch, arch)}-unknown-linux-musl"
    env = os.environ | {f"CARGO_TARGET_{triple.upper().replace('-', '_')}_LINKER": "rust-lld"}
    sh(["cargo", "build", "--release", "--locked", "--target", triple, "--manifest-path", str(REPO_DIR / "Cargo.toml")], env)
    binary = REPO_DIR / "target" / triple / "release" / "redcrab"
    with tempfile.TemporaryDirectory() as context:
        shutil.copy2(binary, context)
        sh(["podman", "build", "--pull=never", "--file", str(BENCH_DIR / "Containerfile"), "--tag", REDCRAB_IMAGE, context])
    git = ["git", "-C", str(REPO_DIR)]
    changed = sh([*git, "status", "--porcelain", "--", "src", "Cargo.toml", "Cargo.lock"]).strip()
    return {
        "commit": sh([*git, "rev-parse", "HEAD"]).strip() + ("+uncommitted" if changed else ""),
        "rust_target": triple,
        "rustc": sh(["rustc", "--version"]).strip(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
    }


def running_broker() -> dict[str, object]:
    """Read the image id and container memory limit that bench-1 actually runs with."""
    image_id, memory = sh(["podman", "container", "inspect", "--format", "{{.Image}} {{.HostConfig.Memory}}", "bench-1"]).split()
    return {"image_id": image_id, "container_mem_bytes": int(memory)}


def client(run: Run, role: str, *args: str, options: tuple[str, ...] = ()) -> list[str]:
    """Build a podman command that runs a Kafka CLI tool in a throwaway client container, with extra podman run options."""
    network = ["--network", run.network] if run.network else []
    jvm = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}", "-e", f"KAFKA_JVM_PERFORMANCE_OPTS={CLIENT_JVM_OPTS}"]
    return ["podman", "run", "--rm", "--pull=never", "--name", f"bench-{role}", *network, *options, *jvm, KAFKA_IMAGE, *args]


def wait_ready(run: Run, timeout: float = 240) -> None:
    """Poll the cluster metadata until at least replication-factor brokers are visible."""
    deadline = time.monotonic() + timeout
    while True:
        out = sh(client(run, "admin", f"{BIN}/kafka-broker-api-versions.sh", "--bootstrap-server", run.bootstrap), check=False)
        brokers = set(re.findall(r"\(id: (\d+) rack:", out))
        if len(brokers) >= run.replication_factor:
            log(f"ready: brokers {sorted(brokers)}")
            return
        if time.monotonic() > deadline:
            msg = f"cluster not ready after {timeout}s; saw brokers {sorted(brokers)}\n{out[-2000:]}"
            raise RuntimeError(msg)
        time.sleep(3)


def redpanda_settings() -> dict[str, str]:
    """Turn write caching off cluster-wide and read it back with rpk; also read how bench-1 was started.

    Dev-container mode would show as developer_mode: true in the node config and --unsafe-bypass-fsync among the
    redpanda process arguments, so both are recorded.
    """
    rpk = ["podman", "exec", "bench-1", "rpk", "cluster", "config"]
    sh([*rpk, "set", "write_caching_default", "false"])
    node = sh(["podman", "exec", "bench-1", "cat", "/etc/redpanda/redpanda.yaml"])
    developer = re.search(r"^\s*developer_mode:\s*(\S+)", node, re.MULTILINE)
    return {
        "write_caching_default": sh([*rpk, "get", "write_caching_default"]).strip().strip('"'),
        "developer_mode": developer[1] if developer else "unset (default false)",
        "redpanda_args": sh(["podman", "exec", "bench-1", "cat", "/proc/1/cmdline"]).replace("\0", " ").strip(),
    }


def read_configs(out: str, keys: tuple[str, ...]) -> dict[str, str]:
    """Pick the given keys from kafka-configs.sh --describe --all output."""
    configs = dict(CONFIG_RE.findall(out))
    return {key: configs[key] for key in keys if key in configs}


def kafka_broker_configs(run: Run) -> dict[str, str]:
    """Return broker 1's effective durability and internal-topic replication configs."""
    args = ["--bootstrap-server", run.bootstrap, "--describe", "--all", "--entity-type", "brokers", "--entity-name", "1"]
    return read_configs(sh(client(run, "admin", f"{BIN}/kafka-configs.sh", *args)), KAFKA_BROKER_CONFIG_KEYS)


def create_topic(run: Run) -> dict[str, str]:
    """Create the benchmark topic and return its effective durability-related configs."""
    bootstrap = f"--bootstrap-server {run.bootstrap}"
    script = (
        f"{BIN}/kafka-topics.sh {bootstrap} --create --topic {TOPIC} --partitions {run.partitions} "
        f"--replication-factor {run.replication_factor} --config min.insync.replicas={run.min_insync} && "
        f"{BIN}/kafka-configs.sh {bootstrap} --describe --all --entity-type topics --entity-name {TOPIC}"
    )
    return read_configs(sh(client(run, "admin", "bash", "-c", script)), TOPIC_CONFIG_KEYS)


def producer(run: Run, records: int, rate: int, warmup: int = 0, report_ms: int = 5000) -> list[str]:
    """Build the producer perf-test command; the first warmup records are left out of the steady-state summary."""
    args = [f"{BIN}/kafka-producer-perf-test.sh", "--bootstrap-server", run.bootstrap, "--topic", TOPIC]
    args += ["--num-records", str(records), "--record-size", str(RECORD_SIZE), "--throughput", str(rate)]
    args += ["--reporting-interval", str(report_ms)]
    if warmup:
        args += ["--warmup-records", str(warmup)]
    if run.workload.key_range:
        args += ["--record-key-range", str(run.workload.key_range), "--key-distribution", "random"]
    return client(run, "producer", *args, "--command-property", *run.workload.producer_props)


def consumer(run: Run, records: int) -> list[str]:
    """Build the consumer perf-test command that reads the topic from the earliest offset."""
    args = [f"{BIN}/kafka-consumer-perf-test.sh", "--bootstrap-server", run.bootstrap, "--topic", TOPIC]
    args += ["--num-records", str(records), "--group", f"bench-{time.time_ns()}", "--timeout", "30000"]
    return client(run, "consumer", *args, "--command-property", *CONSUMER_PROPS)


def parse_producer(out: str) -> ProducerStats:
    """Parse the final summary line, preferring the steady-state one."""
    matches = list(PRODUCER_RE.finditer(out))
    if not matches:
        msg = f"no producer summary in output:\n{out[-4000:]}"
        raise RuntimeError(msg)
    m = next((m for m in matches if m[2]), matches[-1])
    latency = {"avg": float(m[5]), "p50": float(m[7]), "p95": float(m[8]), "p99": float(m[9]), "p99.9": float(m[10]), "max": float(m[6])}
    intervals = [line.group(0) for line in INTERVAL_RE.finditer(out)]
    return ProducerStats(int(m[1]), bool(m[2]), float(m[3]), float(m[4]), latency, m[0], intervals)


def probe(run: Run, limit: int) -> dict[str, object]:
    """Produce unthrottled for PROBE_SECONDS or up to limit records; the tool prints no summary when stopped, so use its 1 s reports."""
    proc = spawn(producer(run, limit, -1, report_ms=1000))
    time.sleep(PROBE_SECONDS)
    sh(["podman", "rm", "--force", "--volumes", "--time", "0", "bench-producer"], check=False)
    out = finish(proc, check=False)
    reports = [(int(n), float(rate)) for n, rate in INTERVAL_RE.findall(out)]
    if len(reports) < 2:
        msg = f"probe produced only {len(reports)} interval reports"
        raise RuntimeError(msg)
    tail = [rate for _, rate in reports[len(reports) // 2 :]]
    done = PRODUCER_RE.search(out)
    records = int(done[1]) if done else sum(n for n, _ in reports)
    return {"seconds": PROBE_SECONDS, "limit": limit, "records": records, "records_per_sec": sum(tail) / len(tail)}


def parse_consumer(out: str, requested: int) -> ConsumerStats:
    """Parse the header and data line printed by kafka-consumer-perf-test."""
    lines = out.splitlines()
    index = next((i for i, line in enumerate(lines) if line.startswith("start.time")), None)
    if index is None or index + 1 >= len(lines):
        msg = f"no consumer summary in output:\n{out[-4000:]}"
        raise RuntimeError(msg)
    raw = dict(zip(lines[index].split(", "), lines[index + 1].split(", "), strict=True))
    return ConsumerStats(requested, int(raw["data.consumed.in.nMsg"]), float(raw["fetch.nMsg.sec"]), float(raw["fetch.MB.sec"]), raw)


def broker_memory() -> dict[str, str]:
    """Snapshot memory usage of the bench broker containers."""
    out = sh(["podman", "stats", "--no-stream", "--format", "{{.Name}}={{.MemUsage}}"], check=False)
    return dict(line.split("=", 1) for line in out.splitlines() if re.match(r"bench-\d=", line))


def disk_budget(run: Run) -> int:
    """Return how many records the host disk can still take, each RECORD_SIZE bytes times the replication factor, keeping DISK_MARGIN free.

    A Podman machine's disk is a sparse file under the home directory that only shrinks when cleanup fstrims it after a
    run, so a fast replicated cluster can fill the host disk within one max run, and a full host disk crashes the VM.
    """
    budget = (shutil.disk_usage(Path.home()).free - DISK_MARGIN) // (RECORD_SIZE * run.replication_factor)
    if budget <= 0:
        msg = f"the host disk has less than {DISK_MARGIN // 2**30} GiB free"
        raise RuntimeError(msg)
    return budget


def record_cap(run: Run) -> int:
    """Return how many records one run may write in all, probe included: MAX_RUN_BYTES with replication, within the disk budget.

    The ceiling does not depend on how fast the probe finds the target, so a fast replicated cluster cannot plan a run
    larger than the host disk takes.
    """
    return min(MAX_RUN_BYTES // (RECORD_SIZE * run.replication_factor), disk_budget(run))


def check_budget(run: Run, records: int, limit: int) -> None:
    """Refuse to write more records than the run's record cap leaves for this phase."""
    if records > limit:
        gib = records * RECORD_SIZE * run.replication_factor / 2**30
        msg = f"the run would write {records:,} records ({gib:,.0f} GiB with replication) but may write at most {limit:,}"
        raise RuntimeError(msg)


def drive(run: Run) -> dict[str, object]:
    """Run the producer phase(s) and a consumer pass from the start of the topic.

    The unthrottled probe warms every cluster first (cold Kafka brokers otherwise need about a minute at 1k msg/s before
    small-batch latency settles) and sizes the max run. Fixed-rate phases then run WARMUP_SECONDS longer and leave that
    out of the summary; max excludes its first sixth. The probe may write 1/PROBE_SHARE of the record cap, and a max run
    whose probe asks for more than the rest is cut to it and marked capped.
    """
    if run.workload.backlog_rate:
        return drive_backlog(run)
    cap = record_cap(run)
    probe_limit = cap // PROBE_SHARE
    calibration = probe(run, probe_limit)
    room = cap - probe_limit
    if run.workload.rate < 0:
        planned = int(float(calibration["records_per_sec"]) * run.seconds)
        records = min(planned, room)
        warmup = records // 6
    else:
        warmup = run.workload.rate * WARMUP_SECONDS
        planned = records = warmup + run.workload.rate * run.seconds
        check_budget(run, records, room)
    stats = parse_producer(sh(producer(run, records, run.workload.rate, warmup)))
    produced = int(calibration["records"]) + records
    mem = broker_memory()
    read = parse_consumer(sh(consumer(run, produced)), produced)
    limits = {"max_run_bytes": MAX_RUN_BYTES, "cap_records": cap, "probe_limit": probe_limit, "planned": planned, "records": records}
    return {
        "producer": asdict(stats),
        "consumer": asdict(read),
        "broker_mem_after_load": mem,
        "probe": calibration,
        "record_cap": limits | {"capped": records < planned},
    }


def drive_backlog(run: Run) -> dict[str, object]:
    """Build a backlog unthrottled, then drain it from the start while a fixed-rate producer keeps writing.

    The fill warms the cluster, so the live producer has no warm-up exclusion: its summary covers the drain.
    """
    backlog = run.workload.backlog_rate * run.seconds
    check_budget(run, backlog + run.workload.rate * run.seconds, record_cap(run))
    fill = parse_producer(sh(producer(run, backlog, -1)))
    live = spawn(producer(run, run.workload.rate * run.seconds, run.workload.rate))
    try:
        read = parse_consumer(sh(consumer(run, backlog)), backlog)
        mem = broker_memory()
        stats = parse_producer(finish(live))
    finally:
        live.kill()
    return {"producer": asdict(stats), "consumer": asdict(read), "backlog_fill": asdict(fill), "broker_mem_after_load": mem}


def host_facts(run: Run) -> dict[str, object]:
    """Describe where the numbers came from so they cannot be mistaken for a real benchmark."""
    info = json.loads(sh(["podman", "info", "--format", "json"]))
    return {
        "vm_cpus": info["host"]["cpus"],
        "vm_mem_mib": info["host"]["memTotal"] // 2**20,
        "vm_kernel": info["host"]["kernel"],
        "podman": info["version"]["Version"],
        "brokers": run.brokers,
        "broker_container_mem_limit": BROKER_MEM_LIMIT if run.target.compose else None,
        "broker_memory": run.target.broker_memory,
        "client_heap": CLIENT_HEAP,
        "client_jvm_opts": CLIENT_JVM_OPTS,
    }


def cleanup(run: Run) -> None:
    """Remove client containers and, for managed targets, the cluster with its volumes.

    The Podman VM disk is a sparse file on the host and one max run writes tens of GB into it, so freed blocks are
    handed back with fstrim (best effort; a no-op without a podman machine).
    """
    sh(["podman", "rm", "--force", "--volumes", "--ignore", *CLIENT_NAMES], check=False)
    if run.target.compose:
        compose(run, "down", "--volumes", "--timeout", "10")
        sh(["podman", "machine", "ssh", "sudo fstrim /var"], check=False)


def run_benchmark(run: Run) -> Path:
    """Start the target, run the workload, tear everything down and write the result file."""
    build = build_redcrab() if run.target.image == REDCRAB_IMAGE else None
    started = datetime.now(UTC)
    facts = host_facts(run)
    durability: dict[str, object] = {"producer_acks": "all"}
    broker: dict[str, object] = {"image_id": None, "container_mem_bytes": None}
    try:
        if run.target.compose:
            start_cluster(run)
            broker = running_broker()
        wait_ready(run)
        if run.target.name == "redpanda":
            durability |= redpanda_settings()
        if run.target.compose == "kafka.yml":
            durability["broker_log_flush_interval_messages"] = run.target.flush_messages
            durability["broker"] = kafka_broker_configs(run)
        durability["topic"] = create_topic(run)
        load = drive(run)
    finally:
        cleanup(run)
    workload = asdict(run.workload) | {
        "seconds": run.seconds,
        "warmup_seconds": WARMUP_SECONDS if run.workload.rate > 0 and not run.workload.backlog_rate else 0,
        "partitions": run.partitions,
        "documented_partitions": PARTITIONS,
        "replication_factor": run.replication_factor,
        "min_insync_replicas": run.min_insync,
        "record_size": RECORD_SIZE,
        "consumer_props": CONSUMER_PROPS,
    }
    result = {
        "note": NOTE,
        "timestamp": started.isoformat(timespec="seconds"),
        "target": run.target.name,
        "image": run.target.image or "unmanaged",
        "image_id": broker["image_id"],
        **({"build": build} if build else {}),
        "client_image": KAFKA_IMAGE,
        "bootstrap": run.bootstrap,
        "workload": workload,
        "durability": durability,
        "host": facts | {"broker_container_mem_bytes": broker["container_mem_bytes"]},
        **load,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    layout = f"{run.brokers}-broker" if run.brokers else "unmanaged"
    path = RESULTS_DIR / f"{started:%Y%m%dT%H%M%SZ}-{run.target.name}-{run.workload.name}-{layout}-rf{run.replication_factor}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    return path


def leader_view(run: Run) -> int | None:
    """Return the broker that leads partition 0 as the reachable brokers report it, or None when they report none."""
    args = ["--bootstrap-server", run.bootstrap, "--describe", "--topic", TOPIC]
    out = sh(client(run, "admin", f"{BIN}/kafka-topics.sh", *args), check=False)
    found = re.search(r"Partition: 0\s+Leader: (\d+)", out)
    return int(found[1]) if found else None


def partition_leader(run: Run) -> int:
    """Return the broker that leads partition 0 of the topic, waiting up to FAILOVER_TIMEOUT_SECONDS for one."""
    deadline = time.monotonic() + FAILOVER_TIMEOUT_SECONDS
    while (leader := leader_view(run)) is None:
        if time.monotonic() > deadline:
            msg = f"partition 0 has had no leader for {FAILOVER_TIMEOUT_SECONDS} s"
            raise RuntimeError(msg)
        time.sleep(1)
    return leader


def read_acks(lines: Iterable[str], acked: list[tuple[int, int]], errors: list[str]) -> None:
    """Collect (client time in ms, value) of each record kafka-verifiable-producer reports acknowledged, and each send error."""
    for line in lines:
        event = json.loads(line) if line.startswith("{") else {}
        if event.get("name") == "producer_send_success":
            acked.append((event["timestamp"], int(event["value"])))
        elif event.get("name") == "producer_send_error":
            errors.append(f"{event.get('exception')}: {event.get('message')}")


def read_records(lines: Iterable[str], records: list[tuple[int, int]]) -> None:
    """Collect (offset, value) of each record a console consumer prints with print.offset=true."""
    for line in lines:
        found = RECORD_RE.match(line.rstrip("\n"))
        if found:
            records.append((int(found[1]), int(found[2])))


def follow(cmd: list[str], parse: Callable[..., None], *sinks: list) -> tuple[subprocess.Popen[str], threading.Thread]:
    """Start a client that runs until stopped and parse its output into sinks on a thread of its own."""
    log(f"$ {shlex.join(cmd)}")
    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)  # noqa: S603
    reader = threading.Thread(target=parse, args=(proc.stdout, *sinks), daemon=True)
    reader.start()
    return proc, reader


def stop(name: str, proc: subprocess.Popen[str], reader: threading.Thread) -> None:
    """Stop a client started with follow, letting it print its last lines."""
    sh(["podman", "kill", "--signal", "TERM", name], check=False)
    proc.wait(60)
    reader.join(60)


def await_recovery(acked: list[tuple[int, int]], start: int) -> int | None:
    """Wait until records were acknowledged for ACK_RUN_MS after the longest pause since acked[start]; return that pause in ms.

    Returns:
        The pause, or None when the producer is not back after FAILOVER_TIMEOUT_SECONDS.
    """
    deadline = time.monotonic() + FAILOVER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        times = [t for t, _ in acked[start:]]
        pause, end = max(((b - a, i + 1) for i, (a, b) in enumerate(itertools.pairwise(times))), default=(0, 0))
        if times[-1] - times[end] >= ACK_RUN_MS:
            return pause
        time.sleep(0.5)
    return None


def consume_records(run: Run) -> list[tuple[int, int]]:
    """Read the whole topic with the console consumer and return (offset, value) per record, checking the tool's own count."""
    args = ["--bootstrap-server", run.bootstrap, "--topic", TOPIC, "--from-beginning", "--timeout-ms", "20000"]
    args += ["--formatter-property", "print.offset=true"]
    out = sh(client(run, "consumer", f"{BIN}/kafka-console-consumer.sh", *args), check=False)
    records = [(int(offset), int(value)) for offset, value in RECORD_RE.findall(out)]
    total = re.search(r"Processed a total of (\d+) messages", out)
    if total is None or int(total[1]) != len(records):
        msg = f"consumer output does not add up:\n{out[-2000:]}"
        raise RuntimeError(msg)
    return records


def settle(acked: list[tuple[int, int]]) -> None:
    """Let the producer run undisturbed for FAILOVER_SETTLE_SECONDS before the next fault."""
    time.sleep(FAILOVER_SETTLE_SECONDS)
    if not acked:
        msg = "the producer has no acknowledged records"
        raise RuntimeError(msg)


def kill_rounds(run: Run, acked: list[tuple[int, int]]) -> FaultLog:
    """Kill partition 0's leader FAILOVER_KILLS times, starting it again once the producer is acknowledged again."""
    faults = FaultLog()
    for kill in range(1, FAILOVER_KILLS + 1):
        settle(acked)
        leader = partition_leader(run)
        start = len(acked) - 1
        sh(["podman", "kill", f"bench-{leader}"])
        pause = await_recovery(acked, start)
        if pause is None:
            msg = f"no records acknowledged for {FAILOVER_TIMEOUT_SECONDS} s after the kill"
            raise RuntimeError(msg)
        faults.pauses.append(pause)
        log(f"kill {kill}: bench-{leader} led partition 0; no record was acknowledged for {pause:,} ms")
        sh(["podman", "start", f"bench-{leader}"])
    return faults


def network_address(node: int) -> dict[str, object]:
    """Return bench-<node>'s IP address, MAC address and aliases on the benchmark network, empty when it is not attached."""
    out = sh(["podman", "container", "inspect", "--format", "{{json .NetworkSettings.Networks}}", f"bench-{node}"])
    return (json.loads(out) or {}).get(NETWORK) or {}


def heal(node: int, address: dict[str, object], began: float) -> str:
    """Reconnect bench-<node> with the IP and MAC address it had, as a healed partition would; describe the heal.

    If another container took the IP meanwhile, the node reconnects with a new one and the description says so.
    """
    name = f"bench-{node}"
    aliases = [f"--alias={alias}" for alias in dict.fromkeys(address.get("Aliases") or [])]
    same = ["--ip", str(address["IPAddress"]), "--mac-address", str(address["MacAddress"])]
    try:
        sh(["podman", "network", "connect", *aliases, *same, NETWORK, name])
    except RuntimeError as error:
        log(f"reconnecting {name} with its old address failed, so it gets a new one: {error}")
        sh(["podman", "network", "connect", *aliases, NETWORK, name])
    now = network_address(node).get("IPAddress")
    same = "the same address" if now == address["IPAddress"] else f"a NEW address {now} (was {address['IPAddress']})"
    return f"healed after {time.monotonic() - began:.0f} s with {same}"


def new_leader_note(run: Run, node: int, began: float, faults: FaultLog) -> str:
    """Poll the reachable brokers until one other than bench-<node> leads partition 0; describe when that happened."""
    while time.monotonic() - began < FAILOVER_TIMEOUT_SECONDS:
        leader = leader_view(run)
        if leader is not None and leader != node:
            return f"bench-{leader} was reported leading {time.monotonic() - began:.0f} s after the cut"
        time.sleep(0.5)
    faults.anomalies.append(f"no broker other than the isolated bench-{node} was reported leading for {FAILOVER_TIMEOUT_SECONDS} s")
    return "no other broker was reported leading"


def replica_log(run: Run, node: int) -> list[Batch] | None:
    """List the batches of partition 0's log on bench-<node>, read from its volume with kafka-dump-log.

    Each batch is (base offset, last offset, leader epoch, CRC), so identical lists mean identical replicas.

    Returns:
        The batches, or None for a target whose log files kafka-dump-log cannot read.
    """
    pattern = LOG_FILES.get(str(run.target.compose))
    if pattern is None:
        return None
    script = f'exec {BIN}/kafka-dump-log.sh --files "$(ls {pattern} | paste -sd, -)"'
    options = ("--volumes-from", f"bench-{node}:ro", "--user", "root")
    out = sh(client(replace(run, network="none"), f"dump-{node}", "bash", "-c", script, options=options), check=False)
    return [(int(m[1]), int(m[2]), int(m[3]), int(m[4])) for m in BATCH_RE.finditer(out)]


def log_end(batches: list[Batch] | None) -> int:
    """Return the log end offset of a replica's batches."""
    return batches[-1][1] + 1 if batches else 0


def partition_end(run: Run) -> int | None:
    """Return partition 0's end offset as its leader reports it, or None without an answer."""
    args = ["--bootstrap-server", run.bootstrap, "--topic", TOPIC, "--partitions", "0", "--time", "-1"]
    out = sh(client(run, "admin", f"{BIN}/kafka-get-offsets.sh", *args), check=False)
    found = re.search(rf"^{TOPIC}:0:(\d+)$", out, re.MULTILINE)
    return int(found[1]) if found else None


def catch_up_note(run: Run, node: int, faults: FaultLog) -> str:
    """Wait until the healed bench-<node> holds partition 0 up to the end offset its leader reports now; describe how long that took."""
    if str(run.target.compose) not in LOG_FILES:
        return "catch-up not checked for this target"
    began = time.monotonic()
    while (end := partition_end(run)) is None and time.monotonic() - began < FAILOVER_TIMEOUT_SECONDS:
        time.sleep(1)
    if end is None:
        faults.anomalies.append(f"no end offset for partition 0 for {FAILOVER_TIMEOUT_SECONDS} s after bench-{node} was healed")
        return "no end offset"
    began = time.monotonic()
    while time.monotonic() - began < FAILOVER_TIMEOUT_SECONDS:
        if log_end(replica_log(run, node)) >= end:
            return f"its replica reached offset {end:,} within {time.monotonic() - began:.0f} s"
        time.sleep(1)
    faults.anomalies.append(f"bench-{node} had not reached offset {end:,} {FAILOVER_TIMEOUT_SECONDS} s after the heal")
    return f"its replica had not reached offset {end:,} after {FAILOVER_TIMEOUT_SECONDS} s"


def partition_round(run: Run, label: str, isolation: Isolation, acked: list[tuple[int, int]], faults: FaultLog) -> bool:
    """Cut partition 0's leader or a follower off the network under load, heal it, and wait for its replica to catch up.

    Returns:
        False when the producer got no acknowledgement for FAILOVER_TIMEOUT_SECONDS, which ends the rounds.
    """
    leader = partition_leader(run)
    node = leader if isolation.role == "leader" else min(set(run.nodes) - {leader})
    address = network_address(node)
    start, began = len(acked) - 1, time.monotonic()
    sh(["podman", "network", "disconnect", NETWORK, f"bench-{node}"])
    notes = [f"{label}: cut off the {isolation.role} bench-{node} ({address.get('IPAddress')})"]
    if isolation.role == "leader":
        notes.append(new_leader_note(run, node, began, faults))
    else:
        time.sleep(FAILOVER_SETTLE_SECONDS)
    if isolation.early_heal:
        notes.append(heal(node, address, began))
    pause = await_recovery(acked, start)
    if not isolation.early_heal:
        notes.append(heal(node, address, began))
    if pause is None:
        faults.anomalies.append(f"{label}: no record was acknowledged for {FAILOVER_TIMEOUT_SECONDS} s after bench-{node} was cut off")
    else:
        faults.pauses.append(pause)
        notes.append(f"no record was acknowledged for {pause:,} ms")
    notes.append(catch_up_note(run, node, faults))
    log("; ".join(notes))
    return pause is not None


def partition_rounds(run: Run, acked: list[tuple[int, int]]) -> FaultLog:
    """Run PARTITION_ROUNDS, stopping early if the producer does not recover from one."""
    faults = FaultLog()
    for number, isolation in enumerate(PARTITION_ROUNDS, 1):
        settle(acked)
        label = f"round {number} ({isolation.role}{', healed early' if isolation.early_heal else ''})"
        if not partition_round(run, label, isolation, acked, faults):
            break
    return faults


def await_convergence(run: Run) -> dict[int, list[Batch]]:
    """Read every replica of partition 0 until all hold the same batches or FAILOVER_TIMEOUT_SECONDS pass; return the last reads."""
    deadline = time.monotonic() + FAILOVER_TIMEOUT_SECONDS
    while True:
        logs = {node: replica_log(run, node) or [] for node in run.nodes}
        if len({tuple(batches) for batches in logs.values()}) == 1 or time.monotonic() > deadline:
            return logs
        time.sleep(2)


def divergence(logs: dict[int, list[Batch]]) -> str | None:
    """Describe the first batch where the replicas differ, or None when they are identical."""
    if len({tuple(batches) for batches in logs.values()}) <= 1:
        return None
    index = next(i for i in itertools.count() if len({batches[i] if i < len(batches) else None for batches in logs.values()}) > 1)
    held = "; ".join(f"bench-{node} {batches[index] if index < len(batches) else 'nothing'}" for node, batches in logs.items())
    return f"the replicas differ from batch {index} on (base offset, last offset, leader epoch, CRC): {held}"


def await_live(live: list[tuple[int, int]], consumed: list[tuple[int, int]]) -> None:
    """Give the live consumer up to FAILOVER_SETTLE_SECONDS to reach the last offset the final read found."""
    last = max((offset for offset, _ in consumed), default=-1)
    deadline = time.monotonic() + FAILOVER_SETTLE_SECONDS
    while max((offset for offset, _ in live), default=-1) < last and time.monotonic() < deadline:
        time.sleep(0.5)


def check_partition(consumed: list[tuple[int, int]], live: list[tuple[int, int]], logs: dict[int, list[Batch]], faults: FaultLog) -> bool:
    """Log what the partition rounds left behind: live reads the final log contradicts, replica divergence and anomalies.

    Returns:
        True when every live read matches the final log, the replicas are identical and no round saw an anomaly.
    """
    final = dict(consumed)
    contradicted = [(offset, value) for offset, value in live if final.get(offset) != value]
    reached = max((offset for offset, _ in live), default=-1)
    log(f"the live consumer read {len(live):,} records up to offset {reached:,}; {len(contradicted):,} disagree with the final log")
    if contradicted:
        log(f"CONTRADICTED live reads (offset, value), for example {contradicted[:10]}")
    split = divergence(logs) if logs else None
    if logs:
        ends = ", ".join(f"bench-{node} {log_end(batches):,} ({len(batches):,} batches)" for node, batches in logs.items())
        log(f"replica log end offsets after the heal: {ends}; {split or 'every replica holds the same batches'}")
    else:
        log("replica logs not compared: kafka-dump-log cannot read this target's log files")
    for anomaly in faults.anomalies:
        log(f"ANOMALY: {anomaly}")
    return not contradicted and split is None and not faults.anomalies


def verify_failover(target: Target, fault: str) -> bool:
    """Kill or cut off brokers of a one-partition topic under an acks=all producer, then read the topic back.

    With fault "kill", the leader is killed and started again FAILOVER_KILLS times. With "partition", PARTITION_ROUNDS
    cut a broker off the network shared by the brokers and the clients and heal it, while a live consumer reads along;
    afterwards every replica must hold the same batches and every live read must match the final log. Every acknowledged
    record must be read back. kafka-verifiable-producer turns retries off, so a failed send is reported as a send error,
    and such records may or may not be in the log. The failover time the client sees is the longest pause between
    acknowledgements after each fault, with otherwise default producer settings and acks=all.

    Returns:
        True when no acknowledged record is missing and, for "partition", nothing else went wrong.
    """
    run = Run(
        target=target,
        workload=WORKLOADS["baseline"],
        seconds=0,
        partitions=1,
        brokers=BROKERS,
        replication_factor=REPLICATION_FACTOR,
        bootstrap=ALL_BROKERS,
        network=NETWORK,
    )
    if target.image == REDCRAB_IMAGE:
        build_redcrab()
    acked: list[tuple[int, int]] = []
    errors: list[str] = []
    live: list[tuple[int, int]] = []
    logs: dict[int, list[Batch]] = {}
    try:
        start_cluster(run)
        wait_ready(run)
        if target.name == "redpanda":
            redpanda_settings()
        create_topic(run)
        script = (
            f"printf '%s\\n' {' '.join(FAILOVER_PRODUCER_PROPS)} > /tmp/producer.properties && "
            f"exec {BIN}/kafka-verifiable-producer.sh --bootstrap-server {run.bootstrap} --topic {TOPIC} "
            f"--throughput {FAILOVER_RATE} --acks -1 --command-config /tmp/producer.properties"
        )
        producer = follow(client(run, "producer", "bash", "-c", script), read_acks, acked, errors)
        if fault == "partition":
            args = ["--bootstrap-server", run.bootstrap, "--topic", TOPIC, "--partition", "0", "--offset", "earliest"]
            args += ["--formatter-property", "print.offset=true"]
            reader = follow(client(run, "live", f"{BIN}/kafka-console-consumer.sh", *args), read_records, live)
        faults = partition_rounds(run, acked) if fault == "partition" else kill_rounds(run, acked)
        time.sleep(FAILOVER_SETTLE_SECONDS)
        stop("bench-producer", *producer)
        if fault == "partition" and str(target.compose) in LOG_FILES:
            logs = await_convergence(run)
        consumed = consume_records(run)
        if fault == "partition":
            await_live(live, consumed)
            stop("bench-live", *reader)
    finally:
        cleanup(run)
    values = [value for _, value in consumed]
    distinct = {value for _, value in acked}
    missing = sorted(distinct - set(values))
    log(f"acknowledged {len(distinct):,} distinct records ({len(acked):,} acknowledgements, {len(errors):,} send errors)")
    for error in sorted(set(errors)):
        log(f"send error x{errors.count(error)}: {error}")
    log(f"consumed {len(values):,} records, {len(values) - len(set(values)):,} of them duplicates")
    pauses = ", ".join(f"{pause:,}" for pause in faults.pauses)
    log(f"failover seen by the client (longest pause between acknowledgements after each fault): {pauses} ms")
    healthy = check_partition(consumed, live, logs, faults) if fault == "partition" else True
    log(f"LOST {len(missing):,} acknowledged records, for example {missing[:20]}" if missing else "no acknowledged record is missing")
    return not missing and healthy


def fmt(value: object, digits: int = 0) -> str:
    """Format a number for the comparison table."""
    return f"{value:,.{digits}f}" if isinstance(value, int | float) else str(value)


def load_results() -> list[dict]:
    """Read every result file, in file name order."""
    return [json.loads(path.read_text()) for path in sorted(RESULTS_DIR.glob("*.json"))]


def broker_count(result: dict) -> int | None:
    """Return the run's broker count, from the result file or else the containers that reported memory; None when unknown."""
    return result["host"].get("brokers") or len(result.get("broker_mem_after_load") or {}) or None


def setup_of(result: dict) -> Setup:
    """Return the run's broker count and replication factor, which compare and chart never mix."""
    return broker_count(result), result["workload"]["replication_factor"]


def setup_label(setup: Setup) -> str:
    """Name a setup, for example "1 broker, replication factor 1"."""
    brokers, rf = setup
    count = "unknown broker count" if brokers is None else f"{brokers} broker{'' if brokers == 1 else 's'}"
    return f"{count}, replication factor {rf}"


def sort_key(result: dict) -> tuple[object, ...]:
    """Order results by setup (the like-for-like single broker first), target, workload and time."""
    setup, target, workload = setup_of(result), result["target"], result["workload"]["name"]
    return (
        SETUPS.index(setup) if setup in SETUPS else len(SETUPS),
        str(setup),
        list(TARGETS).index(target) if target in TARGETS else len(TARGETS),
        target,
        list(WORKLOADS).index(workload) if workload in WORKLOADS else len(WORKLOADS),
        workload,
        result["timestamp"],
    )


def latest_results(results: list[dict]) -> list[dict]:
    """Keep the latest result per target, workload and setup."""
    latest: dict[tuple[object, ...], dict] = {}
    for result in results:
        key = (result["target"], result["workload"]["name"], *setup_of(result))
        if key not in latest or result["timestamp"] > latest[key]["timestamp"]:
            latest[key] = result
    return list(latest.values())


def capped(result: dict) -> bool:
    """Tell whether a max run was cut short by the record cap."""
    return bool(result.get("record_cap", {}).get("capped"))


def run_seconds(result: dict) -> float:
    """Return --seconds, or for a capped run its length at the steady-state rate it measured."""
    if capped(result):
        return result["record_cap"]["records"] / result["producer"]["records_per_sec"]
    return result["workload"]["seconds"]


def table_row(result: dict) -> tuple[str, ...]:
    """Format one result as a row of the comparison table."""
    p, c, w = result["producer"], result["consumer"], result["workload"]
    rate = "max" if w["rate"] < 0 else fmt(w["rate"])
    short = "*" if c["records"] < c["requested"] else ""
    layout = f"{broker_count(result) or '?'}/{w['replication_factor']}"
    seconds = f"~{run_seconds(result):.0f} capped" if capped(result) else fmt(w["seconds"])
    seconds += " pre-cap" if w["rate"] < 0 and not under_cap(result) else ""
    row = (result["target"], w["name"], layout, fmt(w["partitions"]), seconds, rate)
    row += (fmt(p["records_per_sec"]), fmt(p["mb_per_sec"], 1))
    row += tuple(fmt(p["latency_ms"][k]) for k in ("avg", "p50", "p95", "p99", "p99.9", "max"))
    return (*row, fmt(c["records_per_sec"]) + short, fmt(c["mb_per_sec"], 1), result["timestamp"])


def compare(*, every_run: bool = False) -> None:
    """Print one table per setup with the latest result per target and workload, or with every result."""
    results = load_results()
    shown = sorted(results if every_run else latest_results(results), key=sort_key)
    if not shown:
        log(f"no results in {RESULTS_DIR}")
        return
    header = ("target", "workload", "brokers/RF", "parts", "sec", "rate/s", "prod rec/s", "prod MB/s", "avg ms", "p50", "p95", "p99")
    header += ("p99.9", "max ms", "cons rec/s", "cons MB/s", "timestamp")
    sections: dict[Setup, list[tuple[str, ...]]] = {}
    for result in shown:
        sections.setdefault(setup_of(result), []).append(table_row(result))
    rows = [header, *(row for section in sections.values() for row in section)]
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]

    def line(row: tuple[str, ...]) -> str:
        return "  ".join(cell.ljust(width) if i < 2 else cell.rjust(width) for i, (cell, width) in enumerate(zip(row, widths, strict=True)))

    for setup, section in sections.items():
        log(f"{setup_label(setup)}:")
        for row in (header, *section):
            log(line(row))
        log("")
    vms = sorted({f"{r['host']['vm_cpus']} CPUs / {r['host']['vm_mem_mib']} MiB" for r in shown})
    caps = sorted({str(r["host"]["broker_container_mem_limit"]) for r in shown})
    log(f"{NOTE}. VM: {', '.join(vms)}; broker container memory cap: {', '.join(caps)}.")
    log("Latency: producer send-to-ack (acks=all) from kafka-producer-perf-test, steady state. Backlog consumer: drain rate under live")
    log("produce. Consumer rates exclude group-join time. * = consumer stopped before reading every requested record. ? = broker")
    cap = MAX_RUN_BYTES // 2**30
    log(f"count not recorded (broker not started by the harness). capped = max run cut to the {cap} GiB record cap (replicas and")
    log("probe included); sec is then its length at the measured rate instead of --seconds. pre-cap = max run from before the cap.")


def harness_managed(result: dict) -> bool:
    """Tell whether the harness started the run's brokers in the VM at the standard memory cap; only those are charted."""
    return result["image"] != "unmanaged" and result["host"]["broker_container_mem_limit"] == BROKER_MEM_LIMIT


def under_cap(result: dict) -> bool:
    """Tell whether a max run ran under the record cap, which can cut it short; older max runs always ran their full time."""
    return result["workload"]["rate"] < 0 and "record_cap" in result


def method(result: dict) -> str:
    """Identify what a run measured (image, build, workload settings, probe, record cap), so only like runs count as repeats.

    A max run from before the record cap never counts as a repeat of one under it, even when the cap did not cut it.
    """
    build = result.get("build", {})
    binary = build.get("binary_sha256") or build.get("commit")
    return json.dumps([result["image"], binary, result["workload"], "probe" in result, capped(result), under_cap(result)], sort_keys=True)


def chart_runs(results: list[dict], setup: Setup) -> dict[tuple[str, str], list[dict]]:
    """Group one setup's harness-managed runs by target and workload, oldest first, keeping repeats of the latest run only."""
    groups: dict[tuple[str, str], list[dict]] = {}
    for result in sorted(results, key=lambda r: r["timestamp"]):
        if setup_of(result) == setup and harness_managed(result):
            groups.setdefault((result["target"], result["workload"]["name"]), []).append(result)
    return {key: [r for r in runs if method(r) == method(runs[-1])] for key, runs in groups.items()}


def describe_workload(name: str) -> str:
    """Summarise a workload's producer shape for its chart heading."""
    workload = WORKLOADS.get(name)
    if workload is None:
        return ""
    if workload.backlog_rate:
        return f"drain a backlog while producing {workload.rate:,} rec/s"
    parts = ["unthrottled" if workload.rate < 0 else f"fixed {workload.rate:,} rec/s"]
    parts += [prop for prop in workload.producer_props if prop not in PRODUCER_PROPS]
    if workload.key_range:
        parts.append(f"{workload.key_range:,} keys")
    return ", ".join(parts)


def spread_example(groups: dict[tuple[str, str], list[dict]]) -> str:
    """Quote the target and workload whose p99 varied most between like runs, as evidence that single runs are noisy."""
    repeated = sorted(key for key, runs in groups.items() if len(runs) > 1)
    if not repeated:
        return ""

    def ratio(key: tuple[str, str]) -> float:
        p99s = [r["producer"]["latency_ms"]["p99"] for r in groups[key]]
        return max(p99s) / max(min(p99s), 1)

    key = max(repeated, key=ratio)
    values = [f"{r['producer']['latency_ms']['p99']:,.0f}" for r in groups[key]]
    return f" (for example, {key[0]} {key[1]} p99 was {', '.join(values[:-1])} and {values[-1]} ms in {len(values)} runs)"


def cap_would_cut(result: dict) -> bool:
    """Tell whether the record cap would cut a max run from before it, at the rate its probe measured."""
    cap = MAX_RUN_BYTES // (RECORD_SIZE * result["workload"]["replication_factor"])
    return result.get("probe", {}).get("records_per_sec", 0) * result["workload"]["seconds"] > cap - cap // PROBE_SHARE


def pre_cap_note(groups: dict[tuple[str, str], list[dict]]) -> str:
    """Name the charted max rows that ran before the record cap and say whether it would have cut them; empty when none."""
    rows = [key for key, runs in groups.items() if runs[-1]["workload"]["rate"] < 0 and not under_cap(runs[-1])]
    rows.sort(key=lambda key: sort_key(groups[key][-1]))
    if not rows:
        return ""
    cut = [f"{target} {workload}" for target, workload in rows if any(cap_would_cut(r) for r in groups[target, workload])]
    verdict = f"it would have cut {and_list(cut)}" if cut else "it would not have cut any of them"
    names = and_list([f"{target} {workload}" for target, workload in rows])
    return f"The {names} rows ran before the record cap; at the rates their probes measured, {verdict}."


def and_list(items: list[str]) -> str:
    """Join items as "a", "a and b" or "a, b and c"."""
    return " and ".join([", ".join(items[:-1]), items[-1]]) if len(items) > 1 else "".join(items)


def chart_notes(setup: Setup, groups: dict[tuple[str, str], list[dict]]) -> list[str]:
    """Caption paragraphs that travel with the image: what the numbers are, where and how they ran, and what is missing."""
    runs = [r for key in sorted(groups) for r in groups[key]]
    note = " ".join(sorted({f"{r['note']}." for r in runs}))
    notes = [f"{note} Publishable results will come from OpenMessaging Benchmark on dedicated Linux hosts with NVMe drives."]
    hosts = sorted({(h["vm_cpus"], h["vm_mem_mib"], h["vm_kernel"], h["podman"]) for h in (r["host"] for r in runs)})
    vms = "; ".join(f"{cpus} CPUs and {mem} MiB of memory, kernel {kernel}, Podman {podman}" for cpus, mem, kernel, podman in hosts)
    isr = "/".join(sorted({str(r["workload"]["min_insync_replicas"]) for r in runs}))
    caps = "/".join(sorted({str(r["host"]["broker_container_mem_limit"]) for r in runs}))
    notes.append(
        f"Hardware (from the result files): a Podman VM with {vms}. It ran the load clients and every system shown, each as "
        f"{setup_label(setup)} (min.insync.replicas {isr}, acks=all), with each broker container capped at {caps}.",
    )
    limits: dict[str, list[str]] = {}
    for r in sorted(runs, key=sort_key):
        names = limits.setdefault(r["host"]["broker_memory"], [])
        names += [] if r["target"] in names else [r["target"]]
    settings = "; ".join(f"{' and '.join(names)}: {limit}" for limit, names in limits.items())
    notes.append(f"Broker settings (from the result files): {settings}. Containers have no CPU limit.")
    seconds = sorted({r["workload"]["seconds"] for r in runs})
    span = f"{seconds[0]}-{seconds[-1]}" if len(seconds) > 1 else f"{seconds[0]}"
    counts = sorted({len(runs) for runs in groups.values()})
    repeats = f"Each mark is a single run of {span} s."
    if counts[-1] > 1:
        n = f"{counts[0]} to {counts[-1]}" if len(counts) > 1 else f"{counts[0]}"
        repeats = f"Each row has {n} runs of {span} s with the same settings: the colored marks and values show the latest run"
        repeats += " and the thin gray marks under them the earlier runs."
    resolution = "Latency percentiles have 1 ms resolution."
    notes.append(f"{repeats} Results vary widely between runs{spread_example(groups)}. {resolution}")
    cut = sorted(key for key, group in groups.items() if capped(group[-1]))
    if cut:
        lengths = "; ".join(
            f"{target} {workload} ran about {' and '.join(f'{run_seconds(r):.0f}' for r in groups[target, workload])} s"
            f" instead of {groups[target, workload][-1]['workload']['seconds']} s"
            for target, workload in cut
        )
        notes.append(
            f"Rows marked capped hit the harness's cap of {MAX_RUN_BYTES // 2**30} GiB written per run, replicas and probe included,"
            f" so each run sent fewer records than its probe planned for the full time, unlike the unmarked rows. At the rates they"
            f" measured, {lengths}.",
        )
    if note := pre_cap_note(groups):
        notes.append(note)
    days =sorted({r["timestamp"][:10] for r in runs})
    when = days[0] if len(days) == 1 else f"{days[0]} to {days[-1]}"
    notes.append(f"Runs from {when}. Full table: python3 bench/bench.py compare (with --all for every run).")
    return notes


def esc(value: object) -> str:
    """Escape text for SVG character data and attribute values."""
    return html.escape(str(value), quote=True)


def num(value: float) -> str:
    """Format a coordinate deterministically with at most one decimal."""
    return f"{round(value, 1):g}"


def element(name: str, body: str = "", **attrs: object) -> str:
    """Render one SVG element; attribute names use _ for -, None values are dropped, and body must be escaped already."""
    rendered = "".join(
        f' {key.replace("_", "-")}="{num(value) if isinstance(value, float) else esc(value)}"'
        for key, value in attrs.items()
        if value is not None
    )
    return f"<{name}{rendered}>{body}</{name}>" if body else f"<{name}{rendered}/>"


@dataclass
class Canvas:
    """The SVG elements of one chart in one colour theme."""

    theme: dict[str, str]
    parts: list[str] = field(default_factory=list)

    def add(self, name: str, body: str = "", **attrs: object) -> None:
        """Append an element."""
        self.parts.append(element(name, body, **attrs))

    def text(self, x: float, y: float, content: str, *, size: float = 12, ink: str = "ink", **attrs: object) -> None:
        """Append one line of text in one of the theme's text colours."""
        self.add("text", esc(content), x=float(x), y=float(y), font_size=size, fill=self.theme[ink], **attrs)


@dataclass(frozen=True)
class Slot:
    """One target's row inside one workload group: its latest run and the like runs before it."""

    target: str
    workload: str
    result: dict | None
    others: tuple[dict, ...]
    y: float


def dot(theme: dict[str, str], x: float, y: float, fill: str) -> str:
    """A 9 px marker with a 2 px surface ring."""
    return element("circle", cx=x, cy=y, r=5.5, fill=theme[fill], stroke=theme["surface"], stroke_width=2)


def bar(theme: dict[str, str], x: float, y: float, width: float) -> str:
    """A 14 px horizontal bar, square at the baseline with a 4 px rounded data end."""
    top, height, r = y - 7, 14.0, min(4.0, width)
    end, bottom = x + width, y + 7
    d = f"M{num(x)} {num(top)}H{num(end - r)}A{num(r)} {num(r)} 0 0 1 {num(end)} {num(top + r)}V{num(top + height - r)}"
    d += f"A{num(r)} {num(r)} 0 0 1 {num(end - r)} {num(bottom)}H{num(x)}Z"
    return element("path", d=d, fill=theme["bar"])


def earlier_range(theme: dict[str, str], a: float, b: float, y: float) -> str:
    """An earlier like run's p50 to p99 as a thin gray line with small end dots, under the latest run's marks."""
    line = element("line", x1=float(a), x2=float(b), y1=float(y), y2=float(y), stroke=theme["muted"], stroke_width=1.5)
    return line + "".join(element("circle", cx=float(c), cy=float(y), r=2.5, fill=theme["muted"]) for c in (a, b))


def earlier_bar(theme: dict[str, str], x: float, y: float, width: float) -> str:
    """An earlier like run's value as a 3 px gray bar under the latest run's bar."""
    return element("rect", x=float(x), y=float(y - 1.5), width=float(max(width, 1)), height=3.0, rx=1.5, fill=theme["muted"])


def value_label(theme: dict[str, str], x: float, y: float, text: str, anchor: str | None = None) -> str:
    """A value beside its mark, in text ink."""
    return element("text", esc(text), x=float(x), y=float(y + 4), font_size=11.5, fill=theme["ink"], text_anchor=anchor)


def swatch(theme: dict[str, str], kind: str, x: float, y: float) -> str:
    """A legend key: a bar stub, an earlier-run range or bar, or a percentile dot."""
    if kind == "bar":
        return bar(theme, x, y, 12)
    if kind == "range":
        return earlier_range(theme, x + 1, x + 11, y)
    if kind == "thin":
        return earlier_bar(theme, x, y, 12)
    return dot(theme, x + 5, y, kind)


def chart_header(canvas: Canvas, title: str, subtitle: str, legend: list[tuple[str, str]], columns: list[tuple[float, str]]) -> None:
    """Draw the title, subtitle, a one-row legend and the column headings above the plot."""
    canvas.text(16, 30, title, size=15, font_weight=600)
    canvas.text(16, 50, subtitle, ink="ink2")
    x = 16.0
    for kind, label in legend:
        canvas.parts.append(swatch(canvas.theme, kind, x, 70))
        canvas.text(x + 18, 74, label)
        x += 18 + len(label) * 6.8 + 24
    for column_x, heading in columns:
        canvas.text(column_x, 102, heading, ink="ink2", font_weight=600)


def chart_rows(canvas: Canvas, groups: dict[tuple[str, str], list[dict]]) -> tuple[list[Slot], list[tuple[float, float]]]:
    """Draw workload headings and target labels; return the row slots and the vertical span of each group's rows."""
    targets = [*TARGETS, *sorted({target for target, _ in groups} - set(TARGETS))]
    present = {workload for _, workload in groups}
    workloads = [*(w for w in WORKLOADS if w in present), *sorted(present - set(WORKLOADS))]
    slots: list[Slot] = []
    spans: list[tuple[float, float]] = []
    y = float(BODY_TOP)
    for workload in workloads:
        name = element("tspan", esc(workload), font_weight=600, fill=canvas.theme["ink"])
        detail = element("tspan", esc(describe_workload(workload)), dx=8, fill=canvas.theme["ink2"])
        canvas.add("text", name + detail, x=16, y=y + 16, font_size=12.5)
        y += HEADING_HEIGHT
        start = y
        for target in targets:
            runs = groups.get((target, workload), [])
            canvas.text(28, y + 17, f"{target} (capped)" if runs and capped(runs[-1]) else target)
            slots.append(Slot(target, workload, runs[-1] if runs else None, tuple(runs[:-1]), y + 13))
            y += ROW_HEIGHT + RUN_GAP * max(len(runs) - 1, 0)
        spans.append((start, y))
        y += GROUP_GAP
    return slots, spans


def no_run(theme: dict[str, str], x: float, y: float) -> str:
    """Mark a target without a result for a workload, so a missing run never reads as zero."""
    return element("text", "no run", x=x, y=y + 4, font_size=11.5, fill=theme["ink2"], font_style="italic")


def ms_label(ms: float) -> str:
    """Format a latency percentile, which the perf tool reports in whole milliseconds."""
    return "<1" if ms < 1 else f"{ms:,.0f}"


def latency_row(theme: dict[str, str], slot: Slot, x: Callable[[float], float]) -> str:
    """The p50 to p99 range of the latest run (connector, two markers, both values), earlier runs under it, and a tooltip."""
    if slot.result is None:
        return no_run(theme, x(1), slot.y)
    latency = slot.result["producer"]["latency_ms"]
    p50, p99 = latency["p50"], latency["p99"]
    x50, x99 = x(p50), x(p99)
    others = [r["producer"]["latency_ms"] for r in slot.others]
    tip = f"{slot.target} {slot.workload}: p50 {p50:,.0f} ms, p99 {p99:,.0f} ms"
    tip += "".join(f"; earlier run p50 {lat['p50']:,.0f} ms, p99 {lat['p99']:,.0f} ms" for lat in others)
    marks = [
        element("title", esc(tip)),
        *(earlier_range(theme, x(lat["p50"]), x(lat["p99"]), slot.y + 12 + i * RUN_GAP) for i, lat in enumerate(others)),
        element("line", x1=x50, x2=x99, y1=slot.y, y2=slot.y, stroke=theme["p50"], stroke_width=2),
        dot(theme, x50, slot.y, "p50"),
        dot(theme, x99, slot.y, "p99"),
        value_label(theme, x50 - 10, slot.y, ms_label(p50), "end"),
        value_label(theme, x99 + 10, slot.y, ms_label(p99)),
    ]
    return element("g", "".join(marks))


def repeat_legend(groups: dict[tuple[str, str], list[dict]], kind: str) -> list[tuple[str, str]]:
    """The legend key for earlier-run marks, when any row has more than one run."""
    return [(kind, "earlier runs, same settings")] if any(len(runs) > 1 for runs in groups.values()) else []


def latency_chart(setup: Setup, groups: dict[tuple[str, str], list[dict]], theme: dict[str, str], notes: list[str]) -> str:
    """Dot plot of publish latency p50 to p99 per workload and target on a log axis, which values from 1 ms to seconds need."""
    canvas = Canvas(theme)
    left, right = LABEL_COLUMN + 36, CHART_WIDTH - 72
    title = f"Publish latency, p50 to p99: {setup_label(setup)}"
    subtitle = "Producer send-to-ack with acks=all at steady state. Left dot p50, right dot p99."
    chart_header(canvas, title, subtitle, [("p50", "p50"), ("p99", "p99"), *repeat_legend(groups, "range")], [(left, "ms, log scale")])
    slots, spans = chart_rows(canvas, groups)
    values = [r["producer"]["latency_ms"][p] for runs in groups.values() for r in runs for p in ("p50", "p99")]
    low = 10 ** math.floor(math.log10(max(min(values), 1)))
    decades = math.ceil(math.log10(max(max(values), low * 10) / low))

    def x(ms: float) -> float:
        return left + math.log10(max(ms, low) / low) / decades * (right - left)

    for power in range(decades + 1):
        tick_ms = low * 10**power
        canvas.parts += [element("line", x1=x(tick_ms), x2=x(tick_ms), y1=top, y2=bottom, stroke=theme["grid"]) for top, bottom in spans]
        canvas.text(x(tick_ms), spans[-1][1] + 16, f"{tick_ms:,}", size=11, ink="muted", text_anchor="middle")
    canvas.parts += [latency_row(theme, slot, x) for slot in slots]
    return chart_document(canvas, spans[-1][1] + 32, title, notes)


def tick_step(peak: float) -> float:
    """Return a 1-2-5 step that splits 0 to peak into at most four intervals."""
    raw = max(peak, 1e-9) / 4
    base = 10 ** math.floor(math.log10(raw))
    return next(base * m for m in (1, 2, 5, 10) if base * m >= raw)


def throughput_row(theme: dict[str, str], slot: Slot, x0: float, scale: float, side: str) -> str:
    """The latest run's MB/s bar and value, earlier runs under it; * marks a consumer that stopped before reading all."""
    if slot.result is None:
        return no_run(theme, x0 + 6, slot.y)
    stats = slot.result[side]
    short = "*" if stats.get("records", 0) < stats.get("requested", 0) else ""
    label = f"{stats['mb_per_sec']:,.1f}{short}"
    width = stats["mb_per_sec"] * scale
    others = [r[side]["mb_per_sec"] for r in slot.others]
    tip = f"{slot.target} {slot.workload}: {side} {label} MB/s" + "".join(f"; earlier run {mb:,.1f} MB/s" for mb in others)
    marks = [element("title", esc(tip)), *(earlier_bar(theme, x0, slot.y + 12 + i * RUN_GAP, mb * scale) for i, mb in enumerate(others))]
    if width > 0:
        marks.append(bar(theme, x0, slot.y, width))
    marks.append(value_label(theme, x0 + width + 6, slot.y, label))
    return element("g", "".join(marks))


def throughput_side(canvas: Canvas, slots: list[Slot], spans: list[tuple[float, float]], x0: float, length: float, side: str) -> None:
    """Draw producer or consumer MB/s for every row on one linear axis shared by all workloads."""
    peak = max(r[side]["mb_per_sec"] for s in slots if s.result for r in (s.result, *s.others))
    step = tick_step(peak)
    top = step * max(1, math.ceil(peak / step))
    for i in range(round(top / step) + 1):
        tick_x = x0 + i * step / top * length
        stroke = canvas.theme["axis" if i == 0 else "grid"]
        canvas.parts += [element("line", x1=tick_x, x2=tick_x, y1=a, y2=b, stroke=stroke) for a, b in spans]
        canvas.text(tick_x, spans[-1][1] + 16, f"{i * step:,g}", size=11, ink="muted", text_anchor="middle")
    canvas.parts += [throughput_row(canvas.theme, slot, x0, length / top, side) for slot in slots]


def throughput_chart(setup: Setup, groups: dict[tuple[str, str], list[dict]], theme: dict[str, str], notes: list[str]) -> str:
    """Horizontal bars of producer and consumer MB/s per workload and target."""
    canvas = Canvas(theme)
    gap = 24
    width = (CHART_WIDTH - 16 - LABEL_COLUMN - gap) / 2
    sides = [(LABEL_COLUMN + i * (width + gap), side) for i, side in enumerate(("producer", "consumer"))]
    title = f"Throughput, MB/s: {setup_label(setup)}"
    subtitle = "Producer send rate and consumer fetch rate (excluding group join). Fixed-rate workloads cap the producer."
    ticks = repeat_legend(groups, "thin")
    legend = [("bar", "latest run"), *ticks] if ticks else []
    chart_header(canvas, title, subtitle, legend, [(x0, f"{side.capitalize()} MB/s") for x0, side in sides])
    slots, spans = chart_rows(canvas, groups)
    for x0, side in sides:
        throughput_side(canvas, slots, spans, x0, width - 52, side)
    if any(runs[-1]["consumer"]["records"] < runs[-1]["consumer"]["requested"] for runs in groups.values()):
        notes = [*notes, "* The consumer stopped before reading every requested record."]
    return chart_document(canvas, spans[-1][1] + 32, title, notes)


def chart_document(canvas: Canvas, top: float, title: str, notes: list[str]) -> str:
    """Add the caption under a hairline and wrap everything in an opaque card, so it reads on any page background."""
    theme = canvas.theme
    canvas.add("line", x1=16, x2=CHART_WIDTH - 16, y1=top, y2=top, stroke=theme["grid"])
    y = top + 20
    for i, note in enumerate(notes):
        for line in textwrap.wrap(note, CAPTION_CHARS):
            canvas.text(16, y, line, size=11, ink="ink" if i == 0 else "ink2", font_weight=600 if i == 0 else None)
            y += 15
        y += 4
    height = math.ceil(y)
    parts = [
        element("title", esc(f"{title} (local smoke test, not a benchmark)")),
        element("rect", x=0.5, y=0.5, width=CHART_WIDTH - 1, height=height - 1, rx=6, fill=theme["surface"], stroke=theme["frame"]),
        *canvas.parts,
    ]
    attrs = {"width": CHART_WIDTH, "height": height, "viewBox": f"0 0 {CHART_WIDTH} {height}", "role": "img", "font_family": CHART_FONT}
    return element("svg", "\n" + "\n".join(parts) + "\n", xmlns="http://www.w3.org/2000/svg", **attrs) + "\n"


def chart() -> None:
    """Write light and dark latency and throughput SVGs per setup to bench/charts/, from the harness-managed runs."""
    results = load_results()
    CHARTS_DIR.mkdir(exist_ok=True)
    for setup in SETUPS:
        groups = chart_runs(results, setup)
        if not groups:
            log(f"no harness-managed results with {setup_label(setup)}")
            continue
        notes = chart_notes(setup, groups)
        for name, draw in (("latency", latency_chart), ("throughput", throughput_chart)):
            for mode, theme in THEMES.items():
                path = CHARTS_DIR / f"{setup[0]}-broker-rf{setup[1]}-{name}-{mode}.svg"
                path.write_text(draw(setup, groups, theme, notes))
                log(f"wrote {path.relative_to(REPO_DIR)}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="benchmark one target with one workload")
    run.add_argument("target", choices=TARGETS)
    run.add_argument("workload", choices=WORKLOADS)
    run.add_argument("--brokers", type=int, choices=(1, BROKERS), help=f"brokers to start and replication factor (default {BROKERS})")
    run.add_argument("--seconds", type=int, default=60, help=f"measured seconds per phase (+{WARMUP_SECONDS}s warm-up at a set rate)")
    run.add_argument("--partitions", type=int, default=PARTITIONS, help=f"topic partitions (default {PARTITIONS})")
    run.add_argument("--bootstrap", help="host:port of a broker started outside the harness (redcrab only)")
    run.add_argument("--replication-factor", type=int, help="topic replication factor (redcrab only, default 1)")
    tables = commands.add_parser("compare", help="one table per setup with the latest result per target and workload")
    tables.add_argument("--all", action="store_true", dest="every_run", help="list every result instead of the latest")
    commands.add_parser("chart", help="light and dark SVG charts per setup, written to bench/charts/")
    failover = commands.add_parser("verify-failover", help="kill or cut off brokers under acks=all load and check for lost records")
    failover.add_argument("target", choices=TARGETS)
    failover.add_argument("--fault", choices=FAULTS, default="kill", help="kill the leader (default) or cut brokers off the network")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Entry point."""
    os.environ.setdefault("REGISTRY_AUTH_FILE", str(BENCH_DIR / "auth.json"))
    args = parse_args(argv)
    if args.command == "compare":
        compare(every_run=args.every_run)
        return 0
    if args.command == "chart":
        chart()
        return 0
    if args.command == "verify-failover":
        return 0 if verify_failover(TARGETS[args.target], args.fault) else 1
    target = TARGETS[args.target]
    if target.name != "redcrab" and (args.bootstrap or args.replication_factor):
        sys.exit("--bootstrap and --replication-factor apply to redcrab only")
    if args.bootstrap and args.brokers:
        sys.exit("--brokers applies only to brokers the harness starts")
    brokers = args.brokers or (1 if args.bootstrap else BROKERS)
    if args.bootstrap:
        target = replace(target, compose=None, image=None, broker_memory=UNMANAGED)
    run = Run(
        target=target,
        workload=WORKLOADS[args.workload],
        seconds=args.seconds,
        partitions=args.partitions,
        brokers=brokers if target.compose else None,
        replication_factor=args.replication_factor or brokers,
        bootstrap=args.bootstrap or CLUSTER_BOOTSTRAP,
        network=NETWORK if target.compose else None,
    )
    path = run_benchmark(run)
    result = json.loads(path.read_text())
    p, c = result["producer"], result["consumer"]
    p99 = p["latency_ms"]["p99"]
    log(f"wrote {path}")
    log(f"producer {p['records_per_sec']:,.0f} rec/s {p['mb_per_sec']} MB/s p99 {p99} ms; consumer {c['mb_per_sec']} MB/s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
