use std::path::PathBuf;

use clap::Parser;
use mitiflow::{DomainRuntimeConfig, EventBusConfig, MitiflowDomain};
use mitiflow_storage::{AgentYamlConfig, StorageAgent, StorageAgentConfig};
use tracing_subscriber::EnvFilter;

#[cfg(unix)]
async fn wait_for_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    signal(SignalKind::terminate())
        .expect("failed to install SIGTERM handler")
        .recv()
        .await;
}

#[cfg(not(unix))]
async fn wait_for_sigterm() {
    std::future::pending::<()>().await;
}

/// Mitiflow storage agent — manages EventStore partitions via distributed assignment.
#[derive(Parser)]
#[command(name = "mitiflow-storage")]
struct Cli {
    /// Path to YAML configuration file.
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Err(err) = run().await {
        eprintln!("Error: {err}");
        if is_config_error(&err) {
            std::process::exit(2);
        }
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let yaml_config = match cli.config {
        Some(config_path) => Some(AgentYamlConfig::from_file(&config_path)?),
        None => None,
    };

    let domain = open_storage_domain(yaml_config.as_ref()).await?;

    let mut agent = if let Some(yaml_config) = yaml_config {
        // YAML config mode
        let agent_config = yaml_config.into_agent_config()?;
        StorageAgent::start_multi(domain.session(), agent_config).await?
    } else {
        // Legacy env-var mode
        let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX").unwrap_or_else(|_| "mitiflow".into());
        let data_dir =
            std::env::var("MITIFLOW_DATA_DIR").unwrap_or_else(|_| "/tmp/mitiflow-storage".into());
        let node_id = std::env::var("MITIFLOW_NODE_ID").ok();
        let num_partitions: u32 = std::env::var("MITIFLOW_NUM_PARTITIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let replication_factor: u32 = std::env::var("MITIFLOW_REPLICATION_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let capacity: u32 = std::env::var("MITIFLOW_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let bus_config = EventBusConfig::builder(&key_prefix).build()?;

        let mut builder = StorageAgentConfig::builder(PathBuf::from(&data_dir), bus_config)
            .capacity(capacity)
            .num_partitions(num_partitions)
            .replication_factor(replication_factor);

        if let Some(id) = node_id {
            builder = builder.node_id(id);
        }

        let config = builder.build()?;
        StorageAgent::start(domain.session(), config).await?
    };

    tracing::info!("storage agent running, press Ctrl+C to stop");

    // Wait for SIGINT (Ctrl+C) or SIGTERM (container stop)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = wait_for_sigterm() => {},
    }

    tracing::info!("shutting down agent...");
    agent.shutdown().await?;
    domain.shutdown().await?;

    Ok(())
}

fn is_config_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<mitiflow::Error>()
            .is_some_and(|err| matches!(err, mitiflow::Error::Domain(_)))
            || cause
                .downcast_ref::<mitiflow_storage::AgentError>()
                .is_some_and(|err| matches!(err, mitiflow_storage::AgentError::Config(_)))
    })
}

async fn open_storage_domain(config: Option<&AgentYamlConfig>) -> anyhow::Result<MitiflowDomain> {
    let runtime = DomainRuntimeConfig::from_sources(
        "storage",
        config.map(|config| &config.domain),
        config.map(|config| &config.transport),
    )?;
    let domain = runtime.open().await?;
    let transport_profile = format!("{:?}", domain.transport());
    tracing::info!(
        domain.id = %domain.id(),
        namespace.root = %domain.namespace().root(),
        transport.profile = %transport_profile,
        "mitiflow domain started domain.id={} namespace.root={} transport.profile={}",
        domain.id(),
        domain.namespace().root(),
        transport_profile
    );
    Ok(domain)
}
