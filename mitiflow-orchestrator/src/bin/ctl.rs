//! mitiflow-ctl — CLI for cluster management via Zenoh queryable endpoints.

use std::time::Duration;

use clap::{Parser, Subcommand};
use mitiflow::{DomainRuntimeConfig, MitiflowDomain, TransportProfile};
use tracing_subscriber::EnvFilter;

type CtlResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(name = "mitiflow-ctl", about = "Mitiflow cluster management CLI")]
struct Cli {
    /// Zenoh key prefix (default: "mitiflow")
    #[arg(long, default_value = "mitiflow")]
    prefix: String,

    /// Admin API prefix (default: "{prefix}/_admin")
    #[arg(long)]
    admin_prefix: Option<String>,

    /// Zenoh endpoint(s) to connect to, comma-separated (defaults to a local isolated domain)
    #[arg(long)]
    connect: Option<String>,

    /// Query timeout in seconds
    #[arg(long, default_value = "5")]
    timeout: u64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Topic management
    Topics {
        #[command(subcommand)]
        action: TopicActions,
    },
    /// Cluster management
    Cluster {
        #[command(subcommand)]
        action: ClusterActions,
    },
}

#[derive(Subcommand)]
enum TopicActions {
    /// List all topics
    List,
    /// Get a specific topic
    Get {
        /// Topic name
        name: String,
    },
}

#[derive(Subcommand)]
enum ClusterActions {
    /// Show all nodes with health and assignment counts
    Nodes,
    /// Show the full partition assignment table
    Assignments,
    /// Drain a node (move all partitions off it)
    Drain {
        /// Node ID to drain
        node_id: String,
    },
    /// Undrain a node (remove drain overrides)
    Undrain {
        /// Node ID to undrain
        node_id: String,
    },
    /// Show current override table
    Overrides,
    /// Show cluster-wide health summary
    Status,
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

async fn run() -> CtlResult<()> {
    let cli = Cli::parse();

    let admin_prefix = cli
        .admin_prefix
        .unwrap_or_else(|| format!("{}/_admin", cli.prefix));
    let timeout = Duration::from_secs(cli.timeout);

    let domain = open_ctl_domain(cli.connect).await?;
    let session = domain.session();

    let route = match &cli.command {
        Commands::Topics { action } => match action {
            TopicActions::List => "topics".to_string(),
            TopicActions::Get { name } => format!("topics/{name}"),
        },
        Commands::Cluster { action } => match action {
            ClusterActions::Nodes => "cluster/nodes".to_string(),
            ClusterActions::Assignments => "cluster/assignments".to_string(),
            ClusterActions::Drain { node_id } => format!("cluster/drain/{node_id}"),
            ClusterActions::Undrain { node_id } => format!("cluster/undrain/{node_id}"),
            ClusterActions::Overrides => "cluster/overrides".to_string(),
            ClusterActions::Status => "cluster/status".to_string(),
        },
    };

    let query_key = format!("{admin_prefix}/{route}");
    let replies = session.get(&query_key).timeout(timeout).await?;

    let mut received = false;
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                let bytes = sample.payload().to_bytes();
                // Try pretty-print as JSON, fall back to raw string
                if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                } else {
                    println!("{}", String::from_utf8_lossy(&bytes));
                }
                received = true;
            }
            Err(err) => {
                eprintln!("Error: {err}");
            }
        }
    }

    if !received {
        eprintln!("No reply received. Is the orchestrator running?");
        std::process::exit(1);
    }

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

async fn open_ctl_domain(connect: Option<String>) -> CtlResult<MitiflowDomain> {
    let runtime = DomainRuntimeConfig::from_sources_with_transport(
        "orchestrator-ctl",
        None,
        None,
        transport_from_connect(connect)?,
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

fn transport_from_connect(connect: Option<String>) -> CtlResult<Option<TransportProfile>> {
    match connect {
        Some(connect) => {
            let endpoints: Vec<String> = connect
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if endpoints.is_empty() {
                return Err("--connect must include at least one endpoint".into());
            }
            Ok(Some(TransportProfile::Client { connect: endpoints }))
        }
        None => Ok(None),
    }
}
