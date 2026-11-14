use std::io::IsTerminal;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use redcrab::{Broker, BrokerConfig};
use tracing::{Level, error, info};

const USAGE: &str = "usage: redcrab [--listen 0.0.0.0:9092] [--advertised-host 127.0.0.1] [--advertised-port PORT] \
                     [--data-dir ./redcrab-data] [--node-id 0]";

struct Args {
    listen: SocketAddr,
    advertised_host: String,
    advertised_port: Option<u16>,
    data_dir: PathBuf,
    node_id: i32,
}

fn parse_args(mut raw: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut args = Args {
        listen: SocketAddr::from(([0, 0, 0, 0], 9092)),
        advertised_host: "127.0.0.1".to_owned(),
        advertised_port: None,
        data_dir: PathBuf::from("redcrab-data"),
        node_id: 0,
    };
    while let Some(flag) = raw.next() {
        let value = raw.next().ok_or_else(|| format!("{flag} needs a value"))?;
        let bad = |e: &dyn std::fmt::Display| format!("invalid {flag} {value:?}: {e}");
        match flag.as_str() {
            "--listen" => args.listen = value.parse().map_err(|e| bad(&e))?,
            "--advertised-host" => args.advertised_host.clone_from(&value),
            "--advertised-port" => args.advertised_port = Some(value.parse().map_err(|e| bad(&e))?),
            "--data-dir" => args.data_dir = PathBuf::from(&value),
            "--node-id" => args.node_id = value.parse().map_err(|e| bad(&e))?,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    Ok(args)
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(args.listen)?;
    listener.set_nonblocking(true)?;
    let local = listener.local_addr()?;
    let broker = Broker::open(BrokerConfig {
        node_id: args.node_id,
        advertised_host: args.advertised_host,
        advertised_port: args.advertised_port.unwrap_or(local.port()),
        data_dir: args.data_dir,
    })?;
    info!(%local, advertised = %format!("{}:{}", broker.config.advertised_host, broker.config.advertised_port), "listening");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        redcrab::serve(listener, Arc::new(broker)).await;
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
