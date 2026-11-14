#!/usr/bin/env python3
"""Local Kafka-protocol benchmark harness (M0: Apache Kafka and Redpanda; redcrab joins as a third target later).

Usage (podman with the pinned images already pulled):
    python3 bench/bench.py run <target> <workload> [--seconds 60] [--partitions 100]
        targets:   kafka, kafka-fsync (flush.messages=1), redpanda, redcrab
        workloads: baseline, max, small-batch, backlog
    python3 bench/bench.py run redcrab baseline --bootstrap host.containers.internal:19092 [--replication-factor 1]
    python3 bench/bench.py compare

A run starts a 3-broker cluster (nothing for redcrab), waits for it, creates the topic, drives load with Kafka's own
perf tools from a client container (same pinned Kafka image and client settings for every target), tears the cluster
down with its volumes and writes bench/results/<utc>-<target>-<workload>.json. These are LOCAL numbers from a small
Podman VM with memory-capped brokers; the real 100-partition runs belong to the OpenMessaging Benchmark files in omb/.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

KAFKA_IMAGE = "docker.io/apache/kafka:4.4.0@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f"
REDPANDA_IMAGE = "docker.io/redpandadata/redpanda:v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649"

BENCH_DIR = Path(__file__).resolve().parent
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
    "BROKER_MEM_LIMIT": BROKER_MEM_LIMIT,
    "BROKER_HEAP": KAFKA_HEAP,
    "REDPANDA_MEMORY": REDPANDA_MEMORY,
    "PODMAN_COMPOSE_WARNING_LOGS": "false",
}

RECORD_SIZE = 1024
PARTITIONS = 100
REPLICATION_FACTOR = 3
MIN_INSYNC_REPLICAS = 2
PRODUCER_PROPS = ("acks=all", "linger.ms=1", "batch.size=131072")
CONSUMER_PROPS = ("fetch.max.bytes=16777216",)
PROBE_SECONDS = 15
WARMUP_SECONDS = 60
TOPIC_CONFIG_KEYS = ("min.insync.replicas", "flush.messages", "flush.ms", "write.caching")
NOTE = "LOCAL smoke numbers from a small Podman VM with memory-capped brokers; not a real benchmark"


@dataclass(frozen=True)
class Target:
    """A system under test; compose is None when the driver starts nothing."""

    name: str
    compose: str | None
    image: str | None
    flush_messages: str = "9223372036854775807"
    broker_memory: str = "unmanaged: runs outside the driver"


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
        Target("redcrab", None, None),
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

    Fixed-rate phases run WARMUP_SECONDS longer and leave that warm-up out of the summary; max is warmed by the probe and
    excludes its first sixth.
    """
    if run.workload.backlog_rate:
        return drive_backlog(run)
    phases: dict[str, object] = {}
    produced = 0
    warmup = run.workload.rate * WARMUP_SECONDS
    records = warmup + run.workload.rate * run.seconds
    if run.workload.rate < 0:
        phases["probe"] = calibration = probe(run)
        produced += int(calibration["records"])
        records = int(float(calibration["records_per_sec"]) * run.seconds)
        warmup = records // 6
    stats = parse_producer(sh(producer(run, records, run.workload.rate, warmup)))
    produced += records
    mem = broker_memory()
    read = parse_consumer(sh(consumer(run, produced)), produced)
    return {"producer": asdict(stats), "consumer": asdict(read), "broker_mem_after_load": mem, **phases}


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
    started = datetime.now(UTC)
    facts = host_facts(run.target)
    durability: dict[str, object] = {"producer_acks": "all"}
    try:
        if run.target.compose:
            cleanup(run.target)
            compose(run.target, "up", "--detach")
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
        "client_image": KAFKA_IMAGE,
        "bootstrap": run.bootstrap,
        "workload": workload,
        "durability": durability,
        "host": facts,
        **load,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{started:%Y%m%dT%H%M%SZ}-{run.target.name}-{run.workload.name}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    return path


def fmt(value: object, digits: int = 0) -> str:
    """Format a number for the comparison table."""
    return f"{value:,.{digits}f}" if isinstance(value, int | float) else str(value)


def compare() -> None:
    """Print one table with the latest result per target and workload."""
    latest: dict[tuple[str, str], dict] = {}
    for path in RESULTS_DIR.glob("*.json"):
        result = json.loads(path.read_text())
        key = (result["target"], result["workload"]["name"])
        if key not in latest or result["timestamp"] > latest[key]["timestamp"]:
            latest[key] = result
    if not latest:
        log(f"no results in {RESULTS_DIR}")
        return
    names = list(TARGETS) + list(WORKLOADS)

    def order(key: tuple[str, str]) -> tuple[int, ...]:
        return tuple(names.index(k) if k in names else len(names) for k in key)

    header = ("target", "workload", "parts", "sec", "rate/s", "prod rec/s", "prod MB/s", "avg ms", "p50", "p95", "p99", "p99.9", "max ms")
    header += ("cons rec/s", "cons MB/s", "timestamp")
    rows = [header]
    for key in sorted(latest, key=order):
        r = latest[key]
        p, c, w = r["producer"], r["consumer"], r["workload"]
        lat = p["latency_ms"]
        rate = "max" if w["rate"] < 0 else fmt(w["rate"])
        short = "*" if c["records"] < c["requested"] else ""
        row = (r["target"], w["name"], fmt(w["partitions"]), fmt(w["seconds"]), rate, fmt(p["records_per_sec"]), fmt(p["mb_per_sec"], 1))
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


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="benchmark one target with one workload")
    run.add_argument("target", choices=TARGETS)
    run.add_argument("workload", choices=WORKLOADS)
    run.add_argument("--seconds", type=int, default=60, help=f"measured seconds per phase (+{WARMUP_SECONDS}s warm-up at a set rate)")
    run.add_argument("--partitions", type=int, default=PARTITIONS, help=f"topic partitions (default {PARTITIONS})")
    run.add_argument("--bootstrap", help="host:port of an already running broker (redcrab only)")
    run.add_argument("--replication-factor", type=int, help=f"topic replication factor (redcrab only, default {REPLICATION_FACTOR})")
    commands.add_parser("compare", help="table of the latest result per target and workload")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """Entry point."""
    os.environ.setdefault("REGISTRY_AUTH_FILE", str(BENCH_DIR / "auth.json"))
    args = parse_args(argv)
    if args.command == "compare":
        compare()
        return 0
    target = TARGETS[args.target]
    if target.compose and (args.bootstrap or args.replication_factor):
        sys.exit("--bootstrap and --replication-factor apply to redcrab only")
    if not target.compose and not args.bootstrap:
        sys.exit("redcrab needs --bootstrap host:port (e.g. host.containers.internal:19092)")
    run = Run(
        target=target,
        workload=WORKLOADS[args.workload],
        seconds=args.seconds,
        partitions=args.partitions,
        replication_factor=args.replication_factor or REPLICATION_FACTOR,
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
