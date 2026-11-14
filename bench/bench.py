#!/usr/bin/env python3
"""Local Kafka-protocol benchmark harness for Apache Kafka, Redpanda and redcrab.

Usage (podman with the pinned images already pulled; redcrab also needs cargo and the <arch>-unknown-linux-musl target):
    python3 bench/bench.py run <target> <workload> [--seconds 60] [--partitions 100]
        targets:   kafka, kafka-fsync (flush.messages=1), redpanda, redcrab
        workloads: baseline, max, small-batch, backlog
    python3 bench/bench.py run redcrab baseline --bootstrap host.containers.internal:19092 [--replication-factor 1]
    python3 bench/bench.py compare
    python3 bench/bench.py chart

A run starts the target's cluster (3 brokers for Kafka and Redpanda; 1 redcrab broker at replication factor 1, from an
image built from this checkout), waits for it, creates the topic, drives load with Kafka's own perf tools from a client
container (same pinned Kafka image and client settings for every target), tears the cluster down with its volumes (then
fstrims the VM disk) and writes bench/results/<utc>-<target>-<workload>.json. --bootstrap points a redcrab run at a broker
started elsewhere instead. These are LOCAL numbers from a small Podman VM with memory-capped brokers; real-hardware runs
use the OpenMessaging Benchmark files in omb/. compare prints the latest result per target and workload; chart draws the
same set as light and dark SVGs in bench/charts/.
"""

from __future__ import annotations

import argparse
import hashlib
import html
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
import time
from collections.abc import Callable
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
CLIENT_NAMES = ("bench-admin", "bench-producer", "bench-consumer")

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
BROKERS = 3
REPLICATION_FACTOR = 3
MIN_INSYNC_REPLICAS = 2
PRODUCER_PROPS = ("acks=all", "linger.ms=1", "batch.size=131072")
CONSUMER_PROPS = ("fetch.max.bytes=16777216",)
PROBE_SECONDS = 15
WARMUP_SECONDS = 60
TOPIC_CONFIG_KEYS = ("min.insync.replicas", "flush.messages", "flush.ms", "write.caching")
NOTE = "LOCAL smoke numbers from a small Podman VM with memory-capped brokers; not a real benchmark"
UNMANAGED = "unmanaged: runs outside the driver"

CHARTS_DIR = BENCH_DIR / "charts"
CHART_WIDTH = 800
CHART_FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
LABEL_COLUMN = 236
BODY_TOP = 112
HEADING_HEIGHT = 24
ROW_HEIGHT = 26
CAVEAT_HEIGHT = 14
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
    brokers: int = BROKERS
    replication_factor: int = REPLICATION_FACTOR


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
    """One benchmark invocation."""

    target: Target
    workload: Workload
    seconds: int
    partitions: int
    replication_factor: int
    bootstrap: str
    network: str | None

    @property
    def min_insync(self) -> int:
        """Return min.insync.replicas, never above the replication factor."""
        return min(MIN_INSYNC_REPLICAS, self.replication_factor)


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


TARGETS = {
    t.name: t
    for t in (
        Target("kafka", "kafka.yml", KAFKA_IMAGE, broker_memory=f"heap {KAFKA_HEAP}"),
        Target("kafka-fsync", "kafka.yml", KAFKA_IMAGE, flush_messages="1", broker_memory=f"heap {KAFKA_HEAP}"),
        Target("redpanda", "redpanda.yml", REDPANDA_IMAGE, broker_memory=f"--smp=1 --memory={REDPANDA_MEMORY}"),
        Target("redcrab", "redcrab.yml", REDCRAB_IMAGE, broker_memory="container cap only", brokers=1, replication_factor=1),
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


def compose(target: Target, *args: str) -> None:
    """Run podman compose for a managed target with the pinned images and memory caps."""
    env = os.environ | COMPOSE_ENV | {"KAFKA_FLUSH_MESSAGES": target.flush_messages}
    sh(["podman", "compose", "-f", str(BENCH_DIR / str(target.compose)), *args], env, timeout=300)


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
        sh(["podman", "build", "--file", str(BENCH_DIR / "Containerfile"), "--tag", REDCRAB_IMAGE, context])
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


def client(run: Run, role: str, *args: str) -> list[str]:
    """Build a podman command that runs a Kafka CLI tool in a throwaway client container."""
    network = ["--network", run.network] if run.network else []
    jvm = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}", "-e", f"KAFKA_JVM_PERFORMANCE_OPTS={CLIENT_JVM_OPTS}"]
    return ["podman", "run", "--rm", "--name", f"bench-{role}", *network, *jvm, KAFKA_IMAGE, *args]


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


def redpanda_write_caching() -> str:
    """Turn Redpanda write caching off cluster-wide and return the value read back."""
    rpk = ["podman", "exec", "bench-1", "rpk", "cluster", "config"]
    sh([*rpk, "set", "write_caching_default", "false"])
    return sh([*rpk, "get", "write_caching_default"]).strip().strip('"')


def create_topic(run: Run) -> dict[str, str]:
    """Create the benchmark topic and return its effective durability-related configs."""
    bootstrap = f"--bootstrap-server {run.bootstrap}"
    script = (
        f"{BIN}/kafka-topics.sh {bootstrap} --create --topic {TOPIC} --partitions {run.partitions} "
        f"--replication-factor {run.replication_factor} --config min.insync.replicas={run.min_insync} && "
        f"{BIN}/kafka-configs.sh {bootstrap} --describe --all --entity-type topics --entity-name {TOPIC}"
    )
    out = sh(client(run, "admin", "bash", "-c", script))
    configs = dict(re.findall(r"^\s+([\w.]+)=(\S*) sensitive=", out, re.MULTILINE))
    return {key: configs[key] for key in TOPIC_CONFIG_KEYS if key in configs}


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


def probe(run: Run) -> dict[str, object]:
    """Produce unthrottled for PROBE_SECONDS, then stop; the tool prints no summary when stopped, so use its 1 s reports."""
    proc = spawn(producer(run, 10**10, -1, report_ms=1000))
    time.sleep(PROBE_SECONDS)
    sh(["podman", "rm", "--force", "--volumes", "--time", "0", "bench-producer"], check=False)
    reports = [(int(n), float(rate)) for n, rate in INTERVAL_RE.findall(finish(proc, check=False))]
    if len(reports) < 4:
        msg = f"probe produced only {len(reports)} interval reports"
        raise RuntimeError(msg)
    tail = [rate for _, rate in reports[len(reports) // 2 :]]
    return {"seconds": PROBE_SECONDS, "records": sum(n for n, _ in reports), "records_per_sec": sum(tail) / len(tail)}


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


def drive(run: Run) -> dict[str, object]:
    """Run the producer phase(s) and a consumer pass from the start of the topic.

    The unthrottled probe warms every cluster first (cold Kafka brokers otherwise need about a minute at 1k msg/s before
    small-batch latency settles) and sizes the max run. Fixed-rate phases then run WARMUP_SECONDS longer and leave that
    out of the summary; max excludes its first sixth.
    """
    if run.workload.backlog_rate:
        return drive_backlog(run)
    calibration = probe(run)
    warmup = run.workload.rate * WARMUP_SECONDS
    records = warmup + run.workload.rate * run.seconds
    if run.workload.rate < 0:
        records = int(float(calibration["records_per_sec"]) * run.seconds)
        warmup = records // 6
    stats = parse_producer(sh(producer(run, records, run.workload.rate, warmup)))
    produced = int(calibration["records"]) + records
    mem = broker_memory()
    read = parse_consumer(sh(consumer(run, produced)), produced)
    return {"producer": asdict(stats), "consumer": asdict(read), "broker_mem_after_load": mem, "probe": calibration}


def drive_backlog(run: Run) -> dict[str, object]:
    """Build a backlog unthrottled, then drain it from the start while a fixed-rate producer keeps writing.

    The fill warms the cluster, so the live producer has no warm-up exclusion: its summary covers the drain.
    """
    backlog = run.workload.backlog_rate * run.seconds
    fill = parse_producer(sh(producer(run, backlog, -1)))
    live = spawn(producer(run, run.workload.rate * run.seconds, run.workload.rate))
    try:
        read = parse_consumer(sh(consumer(run, backlog)), backlog)
        mem = broker_memory()
        stats = parse_producer(finish(live))
    finally:
        live.kill()
    return {"producer": asdict(stats), "consumer": asdict(read), "backlog_fill": asdict(fill), "broker_mem_after_load": mem}


def host_facts(target: Target) -> dict[str, object]:
    """Describe where the numbers came from so they cannot be mistaken for a real benchmark."""
    info = json.loads(sh(["podman", "info", "--format", "json"]))
    return {
        "vm_cpus": info["host"]["cpus"],
        "vm_mem_mib": info["host"]["memTotal"] // 2**20,
        "vm_kernel": info["host"]["kernel"],
        "podman": info["version"]["Version"],
        "brokers": target.brokers if target.compose else None,
        "broker_container_mem_limit": BROKER_MEM_LIMIT if target.compose else None,
        "broker_memory": target.broker_memory,
        "client_heap": CLIENT_HEAP,
        "client_jvm_opts": CLIENT_JVM_OPTS,
    }


def cleanup(target: Target) -> None:
    """Remove client containers and, for managed targets, the cluster with its volumes.

    The Podman VM disk is a sparse file on the host and one max run writes tens of GB into it, so freed blocks are
    handed back with fstrim (best effort; a no-op without a podman machine).
    """
    sh(["podman", "rm", "--force", "--volumes", "--ignore", *CLIENT_NAMES], check=False)
    if target.compose:
        compose(target, "down", "--volumes", "--timeout", "10")
        sh(["podman", "machine", "ssh", "sudo fstrim /var"], check=False)


def run_benchmark(run: Run) -> Path:
    """Start the target, run the workload, tear everything down and write the result file."""
    build = build_redcrab() if run.target.image == REDCRAB_IMAGE else None
    started = datetime.now(UTC)
    facts = host_facts(run.target)
    durability: dict[str, object] = {"producer_acks": "all"}
    broker: dict[str, object] = {"image_id": None, "container_mem_bytes": None}
    try:
        if run.target.compose:
            cleanup(run.target)
            compose(run.target, "up", "--detach")
            broker = running_broker()
        wait_ready(run)
        if run.target.name == "redpanda":
            durability["write_caching_default"] = redpanda_write_caching()
        if run.target.compose == "kafka.yml":
            durability["broker_log_flush_interval_messages"] = run.target.flush_messages
        durability["topic"] = create_topic(run)
        load = drive(run)
    finally:
        cleanup(run.target)
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
    path = RESULTS_DIR / f"{started:%Y%m%dT%H%M%SZ}-{run.target.name}-{run.workload.name}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    return path


def fmt(value: object, digits: int = 0) -> str:
    """Format a number for the comparison table."""
    return f"{value:,.{digits}f}" if isinstance(value, int | float) else str(value)


def load_results() -> list[dict]:
    """Read every result file, in file name order."""
    return [json.loads(path.read_text()) for path in sorted(RESULTS_DIR.glob("*.json"))]


def latest_results(results: list[dict]) -> dict[tuple[str, str], dict]:
    """Keep the latest result per target and workload."""
    latest: dict[tuple[str, str], dict] = {}
    for result in results:
        key = (result["target"], result["workload"]["name"])
        if key not in latest or result["timestamp"] > latest[key]["timestamp"]:
            latest[key] = result
    return latest


def compare() -> None:
    """Print one table with the latest result per target and workload."""
    latest = latest_results(load_results())
    if not latest:
        log(f"no results in {RESULTS_DIR}")
        return
    names = list(TARGETS) + list(WORKLOADS)

    def order(key: tuple[str, str]) -> tuple[int, ...]:
        return tuple(names.index(k) if k in names else len(names) for k in key)

    header = ("target", "workload", "brokers/RF", "parts", "sec", "rate/s", "prod rec/s", "prod MB/s", "avg ms", "p50", "p95", "p99")
    header += ("p99.9", "max ms", "cons rec/s", "cons MB/s", "timestamp")
    rows = [header]
    for key in sorted(latest, key=order):
        r = latest[key]
        p, c, w = r["producer"], r["consumer"], r["workload"]
        lat = p["latency_ms"]
        rate = "max" if w["rate"] < 0 else fmt(w["rate"])
        short = "*" if c["records"] < c["requested"] else ""
        layout = f"{broker_count(r) or '?'}/{w['replication_factor']}"
        row = (r["target"], w["name"], layout, fmt(w["partitions"]), fmt(w["seconds"]), rate)
        row += (fmt(p["records_per_sec"]), fmt(p["mb_per_sec"], 1))
        row += tuple(fmt(lat[k]) for k in ("avg", "p50", "p95", "p99", "p99.9", "max"))
        row += (fmt(c["records_per_sec"]) + short, fmt(c["mb_per_sec"], 1), r["timestamp"])
        rows.append(row)
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for row in rows:
        log("  ".join(cell.ljust(width) if i < 2 else cell.rjust(width) for i, (cell, width) in enumerate(zip(row, widths, strict=True))))
    vms = sorted({f"{r['host']['vm_cpus']} CPUs / {r['host']['vm_mem_mib']} MiB" for r in latest.values()})
    caps = sorted({str(r["host"]["broker_container_mem_limit"]) for r in latest.values()})
    log(f"\n{NOTE}. VM: {', '.join(vms)}; broker container memory cap: {', '.join(caps)}.")
    log("Latency: producer send-to-ack (acks=all) from kafka-producer-perf-test, steady state. Backlog consumer: drain rate under live")
    log("produce. Consumer rates exclude group-join time. * = consumer stopped before reading every requested record.")


def broker_count(result: dict) -> int | None:
    """Return the run's broker count, from the result file or else the containers that reported memory; None when unknown."""
    return result["host"].get("brokers") or len(result.get("broker_mem_after_load") or {}) or None


def replication_gap(result: dict) -> str | None:
    """Describe how the run's broker count or replication factor departs from the reference clusters; None when it matches."""
    rf, brokers = result["workload"]["replication_factor"], broker_count(result)
    if rf == REPLICATION_FACTOR and brokers in {None, BROKERS}:
        return None
    if brokers is None:
        return f"RF {rf}"
    return f"{brokers} broker{'' if brokers == 1 else 's'} at RF {rf}"


def environment_gaps(result: dict) -> list[str]:
    """List how the run's broker environment departs from the harness-managed brokers in the VM."""
    gaps = []
    limit = result["host"]["broker_container_mem_limit"]
    if limit != BROKER_MEM_LIMIT:
        gaps.append(f"memory cap {limit}" if limit else "no memory cap")
    if result["bootstrap"].startswith("host.containers.internal:"):
        gaps.append("broker on the host, outside the VM")
    elif result["image"] == "unmanaged":
        gaps.append("broker not started by the harness")
    return gaps


def not_comparable(result: dict) -> list[str]:
    """List how a run departs from the setup of the harness-managed reference clusters; empty when it matches."""
    gap = replication_gap(result)
    return [gap, *environment_gaps(result)] if gap else environment_gaps(result)


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


def spread_example(results: list[dict]) -> str:
    """Quote the target and workload whose p99 varied most between runs of one setup, as evidence that single runs are noisy."""
    runs: dict[tuple[object, ...], list[float]] = {}
    for result in sorted(results, key=lambda r: r["timestamp"]):
        workload, host = result["workload"], result["host"]
        setup = (result["bootstrap"], result["image"], host["broker_container_mem_limit"], workload["replication_factor"])
        runs.setdefault((result["target"], workload["name"], *setup), []).append(result["producer"]["latency_ms"]["p99"])
    repeated = sorted(key for key, p99s in runs.items() if len(p99s) > 1)
    if not repeated:
        return ""
    key = max(repeated, key=lambda k: max(runs[k]) / max(min(runs[k]), 1))
    values = [f"{p99:,.0f}" for p99 in runs[key]]
    return f" (for example, {key[0]} {key[1]} p99 was {', '.join(values[:-1])} and {values[-1]} ms in {len(values)} runs)"


def chart_notes(results: list[dict], latest: dict[tuple[str, str], dict]) -> list[str]:
    """Caption paragraphs that travel with the image: what the numbers are, where they ran and which marks not to compare."""
    runs = [latest[key] for key in sorted(latest)]
    managed = [r for r in runs if not not_comparable(r)]
    others = [r for r in runs if not_comparable(r)]
    note = " ".join(sorted({f"{r['note']}." for r in runs}))
    notes = [f"{note} Publishable results will come from OpenMessaging Benchmark on dedicated Linux hosts with NVMe drives."]
    hosts = sorted({(h["vm_cpus"], h["vm_mem_mib"], h["vm_kernel"], h["podman"]) for h in (r["host"] for r in runs)})
    vms = "; ".join(f"{cpus} CPUs and {mem} MiB of memory, kernel {kernel}, Podman {podman}" for cpus, mem, kernel, podman in hosts)
    clusters = ""
    if managed:
        brokers = "/".join(sorted({str(n) for r in managed if (n := broker_count(r))}))
        rfs = "/".join(sorted({str(r["workload"]["replication_factor"]) for r in managed}))
        caps = "/".join(sorted({str(r["host"]["broker_container_mem_limit"]) for r in managed}))
        size = f"{brokers}-broker " if brokers else ""
        clusters = f" and the solid marks' {size}clusters with replication factor (RF) {rfs}, each broker capped at {caps}"
    notes.append(f"Hardware (from the result files): a Podman VM with {vms}. It ran the load clients{clusters}.")
    if others:
        names = ", ".join(sorted({r["target"] for r in others}))
        reasons = ", ".join(dict.fromkeys(reason for r in others for reason in not_comparable(r)))
        caveat = "They do not do the same work on the same resources as the solid marks, so they are not comparable."
        if not any(environment_gaps(r) for r in others):
            copies = "/".join(sorted({str(r["workload"]["replication_factor"]) for r in others}))
            caveat = (
                f"They ran in the same VM with the same load client and the same {BROKER_MEM_LIMIT} broker memory cap, but they keep "
                f"{copies} cop{'y' if copies == '1' else 'ies'} of each record instead of {REPLICATION_FACTOR}, so they do less work "
                "per record and are not comparable."
            )
        notes.append(f"Hatched or hollow marks ({names}): {reasons}. {caveat}")
    seconds = sorted({r["workload"]["seconds"] for r in runs})
    span = f"{seconds[0]}-{seconds[-1]}" if len(seconds) > 1 else f"{seconds[0]}"
    spread = spread_example(results)
    resolution = "Latency percentiles have 1 ms resolution."
    notes.append(f"Each mark is a single run of {span} s. Results vary widely between runs{spread}. {resolution}")
    days = sorted({r["timestamp"][:10] for r in runs})
    when = days[0] if len(days) == 1 else f"{days[0]} to {days[-1]}"
    notes.append(f"Latest run per target and workload, from {when}. Full table: python3 bench/bench.py compare.")
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
    """One target's row inside one workload group; y is the vertical centre of its marks."""

    target: str
    workload: str
    result: dict | None
    y: float


def dot(theme: dict[str, str], x: float, y: float, fill: str) -> str:
    """A 9 px marker with a 2 px surface ring; fill "hollow" draws the open marker of a run that is not comparable."""
    if fill == "hollow":
        return element("circle", cx=x, cy=y, r=4.5, fill=theme["surface"], stroke=theme["muted"], stroke_width=2)
    return element("circle", cx=x, cy=y, r=5.5, fill=theme[fill], stroke=theme["surface"], stroke_width=2)


def bar(theme: dict[str, str], x: float, y: float, width: float, *, comparable: bool) -> str:
    """A 14 px horizontal bar, square at the baseline with a 4 px rounded data end; hatched and outlined when not comparable."""
    top, height, r = y - 7, 14.0, min(4.0, width)
    end, bottom = x + width, y + 7
    d = f"M{num(x)} {num(top)}H{num(end - r)}A{num(r)} {num(r)} 0 0 1 {num(end)} {num(top + r)}V{num(top + height - r)}"
    d += f"A{num(r)} {num(r)} 0 0 1 {num(end - r)} {num(bottom)}H{num(x)}Z"
    if comparable:
        return element("path", d=d, fill=theme["bar"])
    return element("path", d=d, fill="url(#hatch)", stroke=theme["muted"], stroke_width=1)


def chart_header(canvas: Canvas, title: str, subtitle: str, legend: list[tuple[str, str]], columns: list[tuple[float, str]]) -> None:
    """Draw the title, subtitle, a one-row legend and the column headings above the plot."""
    canvas.text(16, 30, title, size=15, font_weight=600)
    canvas.text(16, 50, subtitle, ink="ink2")
    x = 16.0
    for swatch, label in legend:
        mark = (
            bar(canvas.theme, x, 70, 12, comparable=swatch == "bar") if swatch in {"bar", "hatch"} else dot(canvas.theme, x + 5, 70, swatch)
        )
        canvas.parts.append(mark)
        canvas.text(x + 18, 74, label)
        x += 18 + len(label) * 6.8 + 24
    for column_x, heading in columns:
        canvas.text(column_x, 102, heading, ink="ink2", font_weight=600)


def chart_rows(canvas: Canvas, latest: dict[tuple[str, str], dict]) -> tuple[list[Slot], list[tuple[float, float]]]:
    """Draw workload headings and target labels; return the row slots and the vertical span of each group's rows."""
    targets = [*TARGETS, *sorted({target for target, _ in latest} - set(TARGETS))]
    workloads = [*WORKLOADS, *sorted({workload for _, workload in latest} - set(WORKLOADS))]
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
            result = latest.get((target, workload))
            reasons = not_comparable(result) if result else []
            canvas.text(28, y + 17, target)
            if reasons:
                canvas.text(28, y + 31, f"{reasons[0]}, not comparable", size=11, ink="ink2", font_style="italic")
            slots.append(Slot(target, workload, result, y + 13))
            y += ROW_HEIGHT + (CAVEAT_HEIGHT if reasons else 0)
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
    """The p50 to p99 range of one run: connector, two markers, both values and a tooltip."""
    if slot.result is None:
        return no_run(theme, x(1), slot.y)
    latency = slot.result["producer"]["latency_ms"]
    p50, p99 = latency["p50"], latency["p99"]
    comparable = not not_comparable(slot.result)
    x50, x99 = x(p50), x(p99)
    marks = [
        element("title", esc(f"{slot.target} {slot.workload}: p50 {p50:,.0f} ms, p99 {p99:,.0f} ms")),
        element("line", x1=x50, x2=x99, y1=slot.y, y2=slot.y, stroke=theme["p50" if comparable else "muted"], stroke_width=2),
        dot(theme, x50, slot.y, "p50" if comparable else "hollow"),
        dot(theme, x99, slot.y, "p99" if comparable else "hollow"),
        element("text", esc(ms_label(p50)), x=x50 - 10, y=slot.y + 4, font_size=11.5, fill=theme["ink"], text_anchor="end"),
        element("text", esc(ms_label(p99)), x=x99 + 10, y=slot.y + 4, font_size=11.5, fill=theme["ink"]),
    ]
    return element("g", "".join(marks))


def latency_chart(latest: dict[tuple[str, str], dict], theme: dict[str, str], notes: list[str]) -> str:
    """Dot plot of publish latency p50 to p99 per workload and target on a log axis, which values from 1 ms to seconds need."""
    canvas = Canvas(theme)
    left, right = LABEL_COLUMN + 36, CHART_WIDTH - 72
    title = "Publish latency, p50 to p99"
    subtitle = "Producer send-to-ack with acks=all at steady state. Left dot p50, right dot p99."
    legend = [("p50", "p50"), ("p99", "p99"), ("hollow", "not comparable, see notes")]
    chart_header(canvas, title, subtitle, legend, [(left, "ms, log scale")])
    slots, spans = chart_rows(canvas, latest)
    values = [s.result["producer"]["latency_ms"][p] for s in slots if s.result for p in ("p50", "p99")]
    low = 10 ** math.floor(math.log10(max(min(values), 1)))
    decades = math.ceil(math.log10(max(max(values), low * 10) / low))

    def x(ms: float) -> float:
        return left + math.log10(max(ms, low) / low) / decades * (right - left)

    for power in range(decades + 1):
        tick = low * 10**power
        canvas.parts += [element("line", x1=x(tick), x2=x(tick), y1=top, y2=bottom, stroke=theme["grid"]) for top, bottom in spans]
        canvas.text(x(tick), spans[-1][1] + 16, f"{tick:,}", size=11, ink="muted", text_anchor="middle")
    canvas.parts += [latency_row(theme, slot, x) for slot in slots]
    return chart_document(canvas, spans[-1][1] + 32, title, notes)


def tick_step(peak: float) -> float:
    """Return a 1-2-5 step that splits 0 to peak into at most four intervals."""
    raw = max(peak, 1e-9) / 4
    base = 10 ** math.floor(math.log10(raw))
    return next(base * m for m in (1, 2, 5, 10) if base * m >= raw)


def throughput_row(theme: dict[str, str], slot: Slot, x0: float, scale: float, side: str) -> str:
    """One run's MB/s bar with its value at the tip; * marks a consumer that stopped before reading every record."""
    if slot.result is None:
        return no_run(theme, x0 + 6, slot.y)
    stats = slot.result[side]
    short = "*" if stats.get("records", 0) < stats.get("requested", 0) else ""
    label = f"{stats['mb_per_sec']:,.1f}{short}"
    width = stats["mb_per_sec"] * scale
    marks = [element("title", esc(f"{slot.target} {slot.workload}: {side} {label} MB/s"))]
    if width > 0:
        marks.append(bar(theme, x0, slot.y, width, comparable=not not_comparable(slot.result)))
    marks.append(element("text", esc(label), x=x0 + width + 6, y=slot.y + 4, font_size=11.5, fill=theme["ink"]))
    return element("g", "".join(marks))


def throughput_side(canvas: Canvas, slots: list[Slot], spans: list[tuple[float, float]], x0: float, length: float, side: str) -> None:
    """Draw producer or consumer MB/s for every row on one linear axis shared by all workloads."""
    peak = max(s.result[side]["mb_per_sec"] for s in slots if s.result)
    step = tick_step(peak)
    top = step * max(1, math.ceil(peak / step))
    for i in range(round(top / step) + 1):
        tick_x = x0 + i * step / top * length
        stroke = canvas.theme["axis" if i == 0 else "grid"]
        canvas.parts += [element("line", x1=tick_x, x2=tick_x, y1=a, y2=b, stroke=stroke) for a, b in spans]
        canvas.text(tick_x, spans[-1][1] + 16, f"{i * step:,g}", size=11, ink="muted", text_anchor="middle")
    canvas.parts += [throughput_row(canvas.theme, slot, x0, length / top, side) for slot in slots]


def throughput_chart(latest: dict[tuple[str, str], dict], theme: dict[str, str], notes: list[str]) -> str:
    """Horizontal bars of producer and consumer MB/s per workload and target."""
    canvas = Canvas(theme)
    gap = 24
    width = (CHART_WIDTH - 16 - LABEL_COLUMN - gap) / 2
    sides = [(LABEL_COLUMN + i * (width + gap), side) for i, side in enumerate(("producer", "consumer"))]
    title = "Throughput, MB/s"
    subtitle = "Producer send rate and consumer fetch rate (excluding group join). Fixed-rate workloads cap the producer."
    legend = [("bar", f"{BROKERS}-broker RF {REPLICATION_FACTOR} cluster"), ("hatch", "not comparable, see notes")]
    chart_header(canvas, title, subtitle, legend, [(x0, f"{side.capitalize()} MB/s") for x0, side in sides])
    slots, spans = chart_rows(canvas, latest)
    for x0, side in sides:
        throughput_side(canvas, slots, spans, x0, width - 52, side)
    if any(r["consumer"]["records"] < r["consumer"]["requested"] for r in latest.values()):
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
    tile = element("rect", width=6, height=6, fill=theme["surface"])
    stripe = element("line", x1=3, y1=0, x2=3, y2=6, stroke=theme["muted"], stroke_width=2)
    hatch = element("pattern", tile + stripe, id="hatch", width=6, height=6, patternUnits="userSpaceOnUse", patternTransform="rotate(45)")
    parts = [
        element("title", esc(f"{title} (local smoke test, not a benchmark)")),
        element("defs", hatch),
        element("rect", x=0.5, y=0.5, width=CHART_WIDTH - 1, height=height - 1, rx=6, fill=theme["surface"], stroke=theme["frame"]),
        *canvas.parts,
    ]
    attrs = {"width": CHART_WIDTH, "height": height, "viewBox": f"0 0 {CHART_WIDTH} {height}", "role": "img", "font_family": CHART_FONT}
    return element("svg", "\n" + "\n".join(parts) + "\n", xmlns="http://www.w3.org/2000/svg", **attrs) + "\n"


def chart() -> None:
    """Write light and dark SVG charts of the latest result per target and workload to bench/charts/."""
    results = load_results()
    latest = latest_results(results)
    if not latest:
        log(f"no results in {RESULTS_DIR}")
        return
    notes = chart_notes(results, latest)
    CHARTS_DIR.mkdir(exist_ok=True)
    for name, draw in (("latency", latency_chart), ("throughput", throughput_chart)):
        for mode, theme in THEMES.items():
            path = CHARTS_DIR / f"{name}-{mode}.svg"
            path.write_text(draw(latest, theme, notes))
            log(f"wrote {path.relative_to(BENCH_DIR.parent)}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="benchmark one target with one workload")
    run.add_argument("target", choices=TARGETS)
    run.add_argument("workload", choices=WORKLOADS)
    run.add_argument("--seconds", type=int, default=60, help=f"measured seconds per phase (+{WARMUP_SECONDS}s warm-up at a set rate)")
    run.add_argument("--partitions", type=int, default=PARTITIONS, help=f"topic partitions (default {PARTITIONS})")
    run.add_argument("--bootstrap", help="host:port of a broker started outside the harness (redcrab only)")
    run.add_argument("--replication-factor", type=int, help="topic replication factor (redcrab only, default 1)")
    commands.add_parser("compare", help="table of the latest result per target and workload")
    commands.add_parser("chart", help="light and dark SVG charts of the same results, written to bench/charts/")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Entry point."""
    os.environ.setdefault("REGISTRY_AUTH_FILE", str(BENCH_DIR / "auth.json"))
    args = parse_args(argv)
    if args.command == "compare":
        compare()
        return 0
    if args.command == "chart":
        chart()
        return 0
    target = TARGETS[args.target]
    if target.name != "redcrab" and (args.bootstrap or args.replication_factor):
        sys.exit("--bootstrap and --replication-factor apply to redcrab only")
    if args.bootstrap:
        target = replace(target, compose=None, image=None, broker_memory=UNMANAGED)
    run = Run(
        target=target,
        workload=WORKLOADS[args.workload],
        seconds=args.seconds,
        partitions=args.partitions,
        replication_factor=args.replication_factor or target.replication_factor,
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
