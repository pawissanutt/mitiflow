//! mitiflow-orchestrator binary entry point.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use mitiflow::{DomainRuntimeConfig, MitiflowDomain};
use mitiflow_orchestrator::{
    Orchestrator, config::OrchestratorYamlConfig, orchestrator::OrchestratorConfig,
};
use tracing_subscriber::EnvFilter;

type MainResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

/// Mitiflow orchestrator control plane.
#[derive(Parser)]
#[command(name = "mitiflow-orchestrator")]
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
        if is_config_error(err.as_ref()) {
            std::process::exit(2);
        }
        std::process::exit(1);
    }
}

async fn run() -> MainResult<()> {
    let cli = Cli::parse();
    let yaml_config = match cli.config {
        Some(path) => Some(OrchestratorYamlConfig::from_file(&path)?),
        None => None,
    };

    let domain = open_orchestrator_domain(yaml_config.as_ref()).await?;

    let config = if let Some(yaml_config) = yaml_config {
        let mut config = yaml_config.into_orch_config(domain.namespace().root().to_string())?;
        if config.http_bind.is_none() {
            config.http_bind = Some(([0, 0, 0, 0], 8080).into());
        }
        config
    } else {
        let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX")
            .unwrap_or_else(|_| domain.namespace().root().to_string());
        let data_dir = std::env::var("MITIFLOW_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./orchestrator_data"));
        let lag_interval_ms: u64 = std::env::var("MITIFLOW_LAG_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        let http_bind: Option<std::net::SocketAddr> = std::env::var("MITIFLOW_HTTP_BIND")
            .ok()
            .and_then(|s| s.parse().ok())
            .or(Some(([0, 0, 0, 0], 8080).into()));
        let bootstrap_topics_from: Option<PathBuf> =
            std::env::var("MITIFLOW_BOOTSTRAP_TOPICS_FROM")
                .ok()
                .map(PathBuf::from);
        let auth_token = auth_token_from_env()?;

        OrchestratorConfig {
            key_prefix,
            data_dir,
            lag_interval: Duration::from_millis(lag_interval_ms),
            admin_prefix: None,
            http_bind,
            auth_token,
            bootstrap_topics_from,
        }
    };

    let mut orchestrator = Orchestrator::new(domain.session(), config)?;
    orchestrator.run().await?;

    tracing::info!("mitiflow-orchestrator running, press Ctrl+C to stop");

    // Wait for SIGINT (Ctrl+C) or SIGTERM (container stop)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = wait_for_sigterm() => {},
    }
    tracing::info!("shutting down orchestrator...");
    orchestrator.shutdown().await;
    domain.shutdown().await?;

    Ok(())
}

fn is_config_error(err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    let mut current = Some(err as &dyn std::error::Error);
    while let Some(cause) = current {
        if cause
            .downcast_ref::<mitiflow::Error>()
            .is_some_and(|err| matches!(err, mitiflow::Error::Domain(_)))
        {
            return true;
        }
        current = cause.source();
    }
    false
}

async fn open_orchestrator_domain(
    config: Option<&OrchestratorYamlConfig>,
) -> MainResult<MitiflowDomain> {
    let runtime = DomainRuntimeConfig::from_sources(
        "orchestrator",
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

fn auth_token_from_env() -> MainResult<Option<String>> {
    Ok(auth_token_from_primary_env()?.or(auth_token_from_legacy_env()?))
}

fn auth_token_from_primary_env() -> MainResult<Option<String>> {
    if let Ok(token) = std::env::var("MITIFLOW_AUTH_TOKEN") {
        return normalize_auth_token(Some(token), "MITIFLOW_AUTH_TOKEN");
    }
    Ok(None)
}

fn auth_token_from_legacy_env() -> MainResult<Option<String>> {
    if let Ok(token) = std::env::var("MITIFLOW_UI_TOKEN") {
        return normalize_auth_token(Some(token), "MITIFLOW_UI_TOKEN");
    }
    Ok(None)
}

fn normalize_auth_token(token: Option<String>, source: &str) -> MainResult<Option<String>> {
    match token {
        Some(token) if token.trim().is_empty() => Err(format!("{source} must not be empty").into()),
        Some(token) => Ok(Some(token)),
        None => Ok(None),
    }
}
