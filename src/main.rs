use std::collections::HashSet;
use std::io::IsTerminal;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::ExitCode;

use redcrab::{Broker, BrokerConfig, Peer};
use tracing::{Level, error, info};

const USAGE: &str = "\
usage: redcrab [--node-id ID] [--listen ADDR] [--data-dir DIR]
               [--advertised-host HOST] [--advertised-port PORT]
               [--peers ID@RPC_HOST:RPC_PORT/KAFKA_HOST:KAFKA_PORT,... --rpc-listen ADDR]

  --node-id          this node's id, 0..=32767 (default 0; required with --peers)
  --listen           Kafka listener (default 0.0.0.0:9092)
  --data-dir         data directory (default ./redcrab-data)
  --advertised-host  host Kafka clients connect to (default 127.0.0.1; not with --peers)
  --advertised-port  port Kafka clients connect to (default: the listen port; not with --peers)
  --peers            every node of the cluster, this one included: its id, the address of its
                     internal RPC listener, and the Kafka address clients are told to use
  --rpc-listen       internal RPC listener for the other nodes, required with --peers. It has no
                     authentication or encryption: expose it to trusted peers only";

#[derive(Debug)]
struct Args {
    node_id: Option<i32>,
    listen: SocketAddr,
    advertised_host: Option<String>,
    advertised_port: Option<u16>,
    data_dir: PathBuf,
    peers: Vec<Peer>,
    rpc_listen: Option<SocketAddr>,
}

fn parse_args(mut raw: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut args = Args {
        node_id: None,
        listen: SocketAddr::from(([0, 0, 0, 0], 9092)),
        advertised_host: None,
        advertised_port: None,
        data_dir: PathBuf::from("redcrab-data"),
        peers: Vec::new(),
        rpc_listen: None,
    };
    let mut seen = HashSet::new();
    while let Some(flag) = raw.next() {
        let value = raw.next().ok_or_else(|| format!("{flag} needs a value"))?;
        if !seen.insert(flag.clone()) {
            return Err(format!("{flag} given twice"));
        }
        let bad = |e: &dyn std::fmt::Display| format!("invalid {flag} {value:?}: {e}");
        match flag.as_str() {
            "--node-id" => args.node_id = Some(value.parse().map_err(|e| bad(&e))?),
            "--listen" => args.listen = value.parse().map_err(|e| bad(&e))?,
            "--advertised-host" if value.is_empty() => return Err(bad(&"empty host")),
            "--advertised-host" => args.advertised_host = Some(value),
            "--advertised-port" => args.advertised_port = Some(value.parse().map_err(|e| bad(&e))?),
            "--data-dir" => args.data_dir = PathBuf::from(&value),
            "--peers" => args.peers = parse_peers(&value).map_err(|e| bad(&e))?,
            "--rpc-listen" => args.rpc_listen = Some(value.parse().map_err(|e| bad(&e))?),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if args.peers.is_empty() {
        if args.rpc_listen.is_some() {
            return Err("--rpc-listen needs --peers".into());
        }
        return Ok(args);
    }
    let Some(id) = args.node_id else {
        return Err("--node-id is required with --peers".into());
    };
    if args.advertised_host.is_some() || args.advertised_port.is_some() {
        return Err(
            "with --peers, this node's Kafka address comes from its --peers entry; drop \
             --advertised-host and --advertised-port"
                .into(),
        );
    }
    if !args.peers.iter().any(|p| p.id == id) {
        return Err(format!("--peers does not list this node, {id}"));
    }
    if args.peers.len() > 1 && args.rpc_listen.is_none() {
        return Err("--rpc-listen is required when --peers lists other nodes".into());
    }
    Ok(args)
}

/// Parses `ID@RPC_HOST:RPC_PORT/KAFKA_HOST:KAFKA_PORT,...`.
fn parse_peers(value: &str) -> Result<Vec<Peer>, String> {
    value
        .split(',')
        .map(|entry| {
            let shape = || format!("{entry:?} is not ID@RPC_HOST:RPC_PORT/KAFKA_HOST:KAFKA_PORT");
            let (id, addresses) = entry.split_once('@').ok_or_else(shape)?;
            let (rpc, kafka) = addresses.split_once('/').ok_or_else(shape)?;
            let id = id.parse().map_err(|e| format!("node id {id:?}: {e}"))?;
            split_host_port(rpc)?;
            let (host, port) = split_host_port(kafka)?;
            Ok(Peer {
                id,
                rpc_address: rpc.to_owned(),
                host: host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_owned(),
                port,
            })
        })
        .collect()
}

fn split_host_port(address: &str) -> Result<(&str, u16), String> {
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| format!("{address:?} is not HOST:PORT"))?;
    let bracketed = host.starts_with('[') && host.ends_with(']') && host.len() > 2;
    if host.is_empty() || (host.contains(':') && !bracketed) {
        return Err(format!("{address:?} has no valid host"));
    }
    let port = port
        .parse()
        .ok()
        .filter(|&port: &u16| port != 0)
        .ok_or_else(|| format!("{address:?} has no valid port"))?;
    Ok((host, port))
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(args.listen)?;
    listener.set_nonblocking(true)?;
    let local = listener.local_addr()?;
    let rpc_listener = args.rpc_listen.map(TcpListener::bind).transpose()?;
    let node_id = args.node_id.unwrap_or(0);
    let (advertised_host, advertised_port) = match args.peers.iter().find(|p| p.id == node_id) {
        Some(me) => (me.host.clone(), me.port),
        None => (
            args.advertised_host
                .unwrap_or_else(|| "127.0.0.1".to_owned()),
            args.advertised_port.unwrap_or(local.port()),
        ),
    };
    let config = BrokerConfig {
        node_id,
        advertised_host,
        advertised_port,
        data_dir: args.data_dir,
        peers: args.peers,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let broker = Broker::start(config, rpc_listener).await?;
        info!(%local, advertised = %format!("{}:{}", broker.config.advertised_host, broker.config.advertised_port), "listening");
        let listener = tokio::net::TcpListener::from_std(listener)?;
        redcrab::serve(listener, broker).await;
        Ok(())
    })
}

fn main() -> ExitCode {
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(Level::INFO);
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(flags: &[&str]) -> Result<Args, String> {
        parse_args(flags.iter().map(ToString::to_string))
    }

    const THREE: &str = "1@redcrab-1:9093/redcrab-1:9092,2@10.0.0.2:9093/10.0.0.2:9092,\
                         3@[::1]:19093/[::1]:19092";

    #[test]
    fn single_node_flags_keep_their_defaults() {
        let args = parse(&["--data-dir", "/tmp/x", "--advertised-port", "29092"]).unwrap();
        assert_eq!(args.node_id, None);
        assert_eq!(args.advertised_port, Some(29092));
        assert_eq!(args.peers, []);
        assert!(parse(&["--rpc-listen", "0.0.0.0:9093"]).is_err());
        assert!(parse(&["--listen", "nope"]).is_err());
        assert!(parse(&["--data-dir", "a", "--data-dir", "b"]).is_err());
        assert!(parse(&["--node-id"]).is_err());
        assert!(parse(&["--bogus", "1"]).is_err());
    }

    #[test]
    fn peers_flags_describe_every_node() {
        let args = parse(&[
            "--node-id",
            "2",
            "--peers",
            THREE,
            "--rpc-listen",
            "0.0.0.0:9093",
        ])
        .unwrap();
        let ids: Vec<i32> = args.peers.iter().map(|p| p.id).collect();
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(args.peers[0].rpc_address, "redcrab-1:9093");
        assert_eq!(
            (args.peers[1].host.as_str(), args.peers[1].port),
            ("10.0.0.2", 9092)
        );
        assert_eq!(args.peers[2].rpc_address, "[::1]:19093");
        assert_eq!(args.peers[2].host, "::1");
    }

    #[test]
    fn peers_flags_are_validated_strictly() {
        let rpc = ["--rpc-listen", "0.0.0.0:9093"];
        for broken in [
            "1@h:1",
            "1@h:1/h",
            "x@h:1/h:2",
            "1@h:0/h:2",
            "1@:1/h:2",
            "1@h:1/h:99999",
            "1@::1:1/h:2",
        ] {
            assert!(
                parse(&["--node-id", "1", "--peers", broken, rpc[0], rpc[1]]).is_err(),
                "{broken}"
            );
        }
        assert!(
            parse(&["--peers", THREE, rpc[0], rpc[1]]).is_err(),
            "no node id"
        );
        assert!(parse(&["--node-id", "4", "--peers", THREE, rpc[0], rpc[1]]).is_err());
        assert!(
            parse(&["--node-id", "1", "--peers", THREE]).is_err(),
            "no RPC listener"
        );
        assert!(
            parse(&[
                "--node-id",
                "1",
                "--peers",
                THREE,
                rpc[0],
                rpc[1],
                "--advertised-host",
                "h"
            ])
            .is_err()
        );
        assert!(
            parse(&["--node-id", "1", "--peers", "1@h:1/h:2"]).is_ok(),
            "alone"
        );
    }
}
