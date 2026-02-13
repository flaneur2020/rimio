mod config;
use clap::{Parser, Subcommand};
use config::Config;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod server;
use rimio_core::InitClusterOperation;
use serde::Deserialize;
use server::run_server;

#[derive(Parser)]
#[command(name = "rimio")]
#[command(about = "Lightweight object storage for edge cloud nodes")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Legacy server command (kept for compatibility)
    Server {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.yaml")]
        config: String,

        /// Override current_node from config at runtime
        #[arg(long)]
        current_node: Option<String>,

        /// Run initialization flow only, then exit
        #[arg(long)]
        init: bool,
    },
    /// Start node with config and auto init-or-join behavior
    Start {
        /// Path to configuration file
        #[arg(long = "conf", default_value = "config.yaml")]
        conf: String,

        /// Current node id
        #[arg(long)]
        node: String,

        /// Run initialization flow only, then exit
        #[arg(long)]
        init: bool,
    },
    /// Join existing cluster from registry URL
    Join {
        /// Registry URL, e.g. cluster://seed1:8400,seed2:8400 or redis://127.0.0.1:6379
        registry_url: String,

        /// Current node id
        #[arg(long)]
        node: String,

        /// Optional listen address; if provided must match bootstrap config
        #[arg(long)]
        listen: Option<String>,

        /// Optional advertise address; if provided must match bootstrap config
        #[arg(long = "advertise-addr")]
        advertise_addr: Option<String>,

        /// Allow takeover for suspect same node (not yet fully implemented)
        #[arg(long = "force-takeover", default_value_t = false)]
        force_takeover: bool,
    },
}

#[derive(Debug, Clone)]
struct JoinInvocation {
    registry_url: String,
    node: String,
    listen: Option<String>,
    advertise_addr: Option<String>,
    force_takeover: bool,
}

#[derive(Debug, Clone)]
enum JoinRegistryTarget {
    Gossip { seeds: Vec<String> },
    Redis { url: String },
    Etcd { endpoints: Vec<String> },
}

#[derive(Debug, Deserialize)]
struct InternalBootstrapPayload {
    found: bool,
    namespace: String,
    state: Option<rimio_core::ClusterState>,
}

fn parse_host_port_entry(value: &str, field: &str) -> std::result::Result<String, String> {
    let trimmed = value.trim();
    let (host_raw, port_raw) = trimmed
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid {} '{}': expected host:port", field, trimmed))?;

    let host = host_raw.trim();
    let port = port_raw.trim();
    if host.is_empty() || port.is_empty() {
        return Err(format!(
            "invalid {} '{}': expected host:port",
            field, trimmed
        ));
    }

    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        return Err(format!(
            "invalid {} '{}': expected host:port",
            field, trimmed
        ));
    }

    let parsed_port = port
        .parse::<u16>()
        .map_err(|_| format!("invalid {} '{}': port must be u16", field, trimmed))?;

    Ok(format!("{}:{}", host, parsed_port))
}

fn parse_cluster_seeds(raw: &str) -> std::result::Result<Vec<String>, String> {
    if raw.trim().is_empty() {
        return Err("registry_url must include at least one seed host:port".to_string());
    }

    let mut parsed_seeds = Vec::new();
    for token in raw.split(',') {
        let seed = token.trim();
        if seed.is_empty() {
            continue;
        }

        parsed_seeds.push(parse_host_port_entry(seed, "seed")?);
    }

    if parsed_seeds.is_empty() {
        return Err("registry_url has no valid seeds".to_string());
    }

    Ok(parsed_seeds)
}

fn parse_etcd_endpoints(raw: &str) -> std::result::Result<Vec<String>, String> {
    if raw.trim().is_empty() {
        return Err("registry_url must include at least one etcd endpoint host:port".to_string());
    }

    let mut parsed_endpoints = Vec::new();
    for token in raw.split(',') {
        let endpoint = token.trim();
        if endpoint.is_empty() {
            continue;
        }

        parsed_endpoints.push(parse_host_port_entry(endpoint, "etcd endpoint")?);
    }

    if parsed_endpoints.is_empty() {
        return Err("registry_url has no valid etcd endpoints".to_string());
    }

    Ok(parsed_endpoints)
}

fn parse_registry_url(url: &str) -> std::result::Result<JoinRegistryTarget, String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err("registry_url cannot be empty".to_string());
    }

    if let Some(seeds_raw) = trimmed.strip_prefix("cluster://") {
        return Ok(JoinRegistryTarget::Gossip {
            seeds: parse_cluster_seeds(seeds_raw)?,
        });
    }

    if trimmed.starts_with("redis://") {
        if trimmed.len() <= "redis://".len() {
            return Err("redis registry_url must include host:port".to_string());
        }

        return Ok(JoinRegistryTarget::Redis {
            url: trimmed.to_string(),
        });
    }

    if let Some(endpoints_raw) = trimmed.strip_prefix("etcd://") {
        return Ok(JoinRegistryTarget::Etcd {
            endpoints: parse_etcd_endpoints(endpoints_raw)?,
        });
    }

    Err(format!(
        "invalid registry_url '{}': expected cluster://host:port[,host:port], redis://..., or etcd://host:port[,host:port]",
        trimmed
    ))
}

fn registry_config_for_join_target(
    target: &JoinRegistryTarget,
    _join: &JoinInvocation,
) -> std::result::Result<config::RegistryConfig, String> {
    match target {
        JoinRegistryTarget::Gossip { seeds } => Ok(config::RegistryConfig {
            backend: config::RegistryBackend::Gossip,
            namespace: Some("default".to_string()),
            etcd: None,
            redis: None,
            gossip: Some(config::GossipConfig {
                transport: "internal_http".to_string(),
                seeds: seeds.clone(),
            }),
        }),
        JoinRegistryTarget::Redis { url } => Ok(config::RegistryConfig {
            backend: config::RegistryBackend::Redis,
            namespace: Some("default".to_string()),
            etcd: None,
            redis: Some(config::RedisConfig {
                url: url.clone(),
                pool_size: 8,
            }),
            gossip: None,
        }),
        JoinRegistryTarget::Etcd { endpoints } => Ok(config::RegistryConfig {
            backend: config::RegistryBackend::Etcd,
            namespace: Some("default".to_string()),
            etcd: Some(config::EtcdConfig {
                endpoints: endpoints.clone(),
            }),
            redis: None,
            gossip: None,
        }),
    }
}

fn should_check_active_node_conflict(target: &JoinRegistryTarget) -> bool {
    !matches!(target, JoinRegistryTarget::Gossip { .. })
}

async fn fetch_bootstrap_state_from_gossip_seeds(
    seeds: &[String],
) -> std::result::Result<(String, rimio_core::ClusterState), String> {
    if seeds.is_empty() {
        return Err("cluster:// registry_url has no seeds".to_string());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|error| format!("failed to build HTTP client: {}", error))?;

    let mut last_error: Option<String> = None;

    for seed in seeds {
        let url = format!("http://{}/internal/v1/cluster/bootstrap", seed);
        let response = match client.get(&url).send().await {
            Ok(response) => response,
            Err(error) => {
                last_error = Some(format!("seed {} unreachable: {}", seed, error));
                continue;
            }
        };

        if !response.status().is_success() {
            last_error = Some(format!(
                "seed {} returned unexpected status {}",
                seed,
                response.status()
            ));
            continue;
        }

        let payload: InternalBootstrapPayload = response
            .json()
            .await
            .map_err(|error| format!("seed {} returned invalid bootstrap JSON: {}", seed, error))?;

        if !payload.found {
            return Err(
                "cluster bootstrap state not found in registry; start the cluster first"
                    .to_string(),
            );
        }

        let state = payload
            .state
            .ok_or_else(|| format!("seed {} bootstrap payload missing state body", seed))?;

        return Ok((payload.namespace, state));
    }

    Err(last_error.unwrap_or_else(|| {
        "all gossip seeds are unreachable while reading bootstrap state".to_string()
    }))
}

fn apply_join_overrides(
    cfg: &mut Config,
    join: &JoinInvocation,
) -> std::result::Result<(), String> {
    if join.force_takeover {
        tracing::warn!(
            "--force-takeover is accepted but takeover guardrails are not fully implemented yet"
        );
    }

    let node_cfg = cfg
        .initial_cluster
        .nodes
        .iter_mut()
        .find(|node| node.node_id == join.node)
        .ok_or_else(|| {
            format!(
                "node '{}' not found in config initial_cluster.nodes",
                join.node
            )
        })?;

    if let Some(listen) = &join.listen {
        if !node_cfg.bind_addr.trim().is_empty() && node_cfg.bind_addr != *listen {
            return Err(format!(
                "--listen '{}' does not match configured bind_addr '{}' for node '{}'",
                listen, node_cfg.bind_addr, join.node
            ));
        }
        node_cfg.bind_addr = listen.clone();
    }

    if let Some(advertise) = &join.advertise_addr {
        if let Some(existing) = &node_cfg.advertise_addr {
            if existing != advertise {
                return Err(format!(
                    "--advertise-addr '{}' does not match configured advertise_addr '{}' for node '{}'",
                    advertise, existing, join.node
                ));
            }
        }
        node_cfg.advertise_addr = Some(advertise.clone());
    }

    if node_cfg.bind_addr.trim().is_empty() {
        return Err(format!(
            "node '{}' has empty bind_addr and no --listen provided",
            join.node
        ));
    }

    if node_cfg
        .advertise_addr
        .as_ref()
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        return Err(format!(
            "node '{}' has empty advertise_addr and no --advertise-addr provided",
            join.node
        ));
    }

    Ok(())
}

async fn run_with_config(mut cfg: Config, current_node: &str, init_only: bool) {
    cfg.initial_cluster
        .nodes
        .sort_by(|left, right| left.node_id.cmp(&right.node_id));

    let registry_builder = cfg.registry_builder_for_node(current_node);

    let init_request = { cfg.to_init_cluster_request_for_node(current_node) };

    let init_operation = InitClusterOperation::new(registry_builder.clone());
    let init_result = match init_operation.run(init_request).await {
        Ok(result) => result,
        Err(error) => {
            tracing::error!("Initialization failed: {}", error);
            std::process::exit(1);
        }
    };

    if init_result.won_bootstrap_race {
        tracing::info!(
            "Node {} won initialization race and persisted bootstrap state",
            current_node
        );
    } else {
        tracing::info!(
            "Node {} reused existing bootstrap state from registry",
            current_node
        );
    }

    if init_only {
        tracing::info!(
            "Initialization completed for node {} (init-only mode)",
            current_node
        );
        return;
    }

    let runtime_config = match config::Config::runtime_from_bootstrap_for_node(
        &init_result.bootstrap_state,
        current_node,
        cfg.registry.clone(),
    ) {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!("Failed to build runtime config: {}", error);
            std::process::exit(1);
        }
    };

    tracing::info!(
        "Node ID: {}, Bind: {}, Slots: {}",
        runtime_config.node.node_id,
        runtime_config.node.bind_addr,
        runtime_config.replication.total_slots
    );

    let runtime_registry_builder = cfg.registry_builder_for_node(current_node);
    let registry = match runtime_registry_builder.build().await {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!("Failed to create runtime registry: {}", error);
            std::process::exit(1);
        }
    };

    if let Err(error) = run_server(runtime_config, registry).await {
        tracing::error!("Server error: {}", error);
        std::process::exit(1);
    }
}

async fn run_join(join: JoinInvocation) {
    let registry_target = match parse_registry_url(&join.registry_url) {
        Ok(value) => value,
        Err(message) => {
            tracing::error!("Invalid registry URL: {}", message);
            std::process::exit(2);
        }
    };

    let join_registry_config = match registry_config_for_join_target(&registry_target, &join) {
        Ok(config) => config,
        Err(message) => {
            tracing::error!("Invalid registry URL: {}", message);
            std::process::exit(2);
        }
    };

    let mut cfg = Config {
        registry: join_registry_config,
        initial_cluster: config::InitialClusterConfig {
            nodes: vec![config::InitialNodeConfig {
                node_id: join.node.clone(),
                bind_addr: join.listen.clone().unwrap_or_default(),
                advertise_addr: join.advertise_addr.clone(),
                disks: vec![config::DiskConfig {
                    path: std::path::PathBuf::from("./demo/join-placeholder"),
                }],
            }],
            replication: config::ReplicationConfig::default(),
        },
        archive: None,
        init_scan: None,
    };

    let mut preflight_registry: Option<std::sync::Arc<dyn rimio_core::Registry>> = None;

    let bootstrap_state: rimio_core::ClusterState = match &registry_target {
        JoinRegistryTarget::Gossip { seeds } => {
            match fetch_bootstrap_state_from_gossip_seeds(seeds).await {
                Ok((namespace, state)) => {
                    cfg.registry.namespace = Some(namespace);
                    state
                }
                Err(message) => {
                    tracing::error!(
                        "failed to read bootstrap state from gossip seeds ({}): {}",
                        join.registry_url,
                        message
                    );
                    std::process::exit(2);
                }
            }
        }
        _ => {
            let registry_builder = cfg.registry_builder_for_node(&join.node);
            let registry = match registry_builder.build().await {
                Ok(registry) => registry,
                Err(error) => {
                    tracing::error!("Failed to connect registry for join: {}", error);
                    std::process::exit(1);
                }
            };

            let bootstrap_bytes = match registry.get_bootstrap_state().await {
                Ok(Some(payload)) => payload,
                Ok(None) => {
                    tracing::error!(
                        "cluster bootstrap state not found in registry {}; start the cluster first",
                        join.registry_url
                    );
                    std::process::exit(2);
                }
                Err(error) => {
                    tracing::error!("failed to read bootstrap state from registry: {}", error);
                    std::process::exit(1);
                }
            };

            let state: rimio_core::ClusterState = match serde_json::from_slice(&bootstrap_bytes) {
                Ok(state) => state,
                Err(error) => {
                    tracing::error!("invalid bootstrap state payload in registry: {}", error);
                    std::process::exit(1);
                }
            };

            preflight_registry = Some(registry);
            state
        }
    };

    cfg.initial_cluster.nodes = bootstrap_state
        .nodes
        .iter()
        .map(|node| config::InitialNodeConfig {
            node_id: node.node_id.clone(),
            bind_addr: node.bind_addr.clone(),
            advertise_addr: node.advertise_addr.clone(),
            disks: node
                .disks
                .iter()
                .map(|disk| config::DiskConfig {
                    path: disk.path.clone(),
                })
                .collect(),
        })
        .collect();
    cfg.initial_cluster.replication = config::ReplicationConfig {
        min_write_replicas: bootstrap_state.replication.min_write_replicas,
        total_slots: bootstrap_state.replication.total_slots,
    };
    cfg.archive = bootstrap_state
        .archive
        .as_ref()
        .map(|archive| config::ArchiveConfig {
            archive_type: archive.archive_type.clone(),
            s3: archive.s3.as_ref().map(|s3| config::S3Config {
                bucket: s3.bucket.clone(),
                region: s3.region.clone(),
                endpoint: s3.endpoint.clone(),
                allow_http: s3.allow_http,
                credentials: config::S3Credentials {
                    access_key_id: s3.credentials.access_key_id.clone(),
                    secret_access_key: s3.credentials.secret_access_key.clone(),
                },
            }),
            redis: archive
                .redis
                .as_ref()
                .map(|redis| config::ArchiveRedisConfig {
                    url: redis.url.clone(),
                    key_prefix: redis.key_prefix.clone(),
                }),
        });

    if let Err(message) = apply_join_overrides(&mut cfg, &join) {
        tracing::error!("join validation failed: {}", message);
        std::process::exit(2);
    }

    let current = join.node.clone();
    let runtime_cfg = match config::Config::runtime_from_bootstrap_for_node(
        &bootstrap_state,
        &current,
        cfg.registry.clone(),
    ) {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!("join runtime build failed: {}", error);
            std::process::exit(2);
        }
    };

    if should_check_active_node_conflict(&registry_target) {
        let registry =
            preflight_registry.expect("preflight registry must exist for non-gossip target");

        let existing_nodes = match registry.get_nodes().await {
            Ok(nodes) => nodes,
            Err(error) => {
                tracing::error!("failed to query existing nodes from registry: {}", error);
                std::process::exit(1);
            }
        };

        if existing_nodes
            .iter()
            .any(|node| node.node_id == current && node.status == rimio_core::NodeStatus::Healthy)
        {
            tracing::error!(
                "join rejected: active node '{}' already exists in registry",
                current
            );
            std::process::exit(2);
        }
    }

    tracing::info!(
        "Join succeeded for node {} via {}",
        runtime_cfg.node.node_id,
        join.registry_url
    );

    let runtime_registry_builder = cfg.registry_builder_for_node(&current);
    let runtime_registry = match runtime_registry_builder.build().await {
        Ok(registry) => registry,
        Err(error) => {
            tracing::error!("failed to create runtime registry for join: {}", error);
            std::process::exit(1);
        }
    };

    if let Err(error) = run_server(runtime_cfg, runtime_registry).await {
        tracing::error!("Server error: {}", error);
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rimio=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            config,
            current_node,
            init,
        } => {
            tracing::info!("Starting Rimio server with config: {}", config);

            let cfg = match Config::from_file(&config) {
                Ok(c) => c,
                Err(error) => {
                    tracing::error!("Failed to load config: {}", error);
                    std::process::exit(1);
                }
            };

            if let Some(override_node) = current_node {
                tracing::info!("Using node override '{}' via CLI", override_node);
                run_with_config(cfg, &override_node, init).await;
                return;
            }

            tracing::error!(
                "legacy `server` command now requires --current-node; please use `rimio start --conf ... --node ...`"
            );
            std::process::exit(2);
        }
        Commands::Start { conf, node, init } => {
            tracing::info!("Starting Rimio with start command, config: {}", conf);

            let cfg = match Config::from_file(&conf) {
                Ok(c) => c,
                Err(error) => {
                    tracing::error!("Failed to load config: {}", error);
                    std::process::exit(1);
                }
            };

            run_with_config(cfg, &node, init).await;
        }
        Commands::Join {
            registry_url,
            node,
            listen,
            advertise_addr,
            force_takeover,
        } => {
            run_join(JoinInvocation {
                registry_url,
                node,
                listen,
                advertise_addr,
                force_takeover,
            })
            .await;
        }
    }
}
