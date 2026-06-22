//! NATS client with JetStream support

use async_nats::jetstream::{self, Context as JetStreamContext};
use async_nats::rustls::pki_types::pem::PemObject;
use async_nats::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use async_nats::rustls::{ClientConfig, RootCertStore};
use async_nats::{ConnectOptions, Event};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::{NatsConfig, TlsConfig};
use crate::error::NatsError;

/// Connection state for NATS client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Disconnected,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Connected => write!(f, "Connected"),
            ConnectionState::Disconnected => write!(f, "Disconnected"),
        }
    }
}

/// NATS client with JetStream support
pub struct NatsClient {
    jetstream: Arc<JetStreamContext>,
    state_rx: watch::Receiver<ConnectionState>,
}

impl NatsClient {
    /// Connect to NATS server
    pub async fn connect(config: &NatsConfig) -> Result<Self, NatsError> {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);

        let state_tx_clone = state_tx.clone();

        let mut options = ConnectOptions::new()
            .event_callback(move |event| {
                let state_tx = state_tx_clone.clone();
                async move {
                    match event {
                        Event::Connected => {
                            info!("NATS connected");
                            let _ = state_tx.send(ConnectionState::Connected);
                        }
                        Event::Disconnected => {
                            warn!("NATS disconnected");
                            let _ = state_tx.send(ConnectionState::Disconnected);
                        }
                        Event::ServerError(err) => error!(error = %err, "NATS server error"),
                        Event::ClientError(err) => error!(error = %err, "NATS client error"),
                        Event::LameDuckMode => warn!("NATS server in lame duck mode"),
                        Event::SlowConsumer(sid) => {
                            warn!(subscription_id = sid, "NATS slow consumer")
                        }
                        _ => {}
                    }
                }
            })
            .retry_on_initial_connect();

        if config.tls.enabled {
            let tls_config = build_rustls_client_config(&config.tls)?;
            options = options.require_tls(true).tls_client_config(tls_config);
        }

        info!(
            urls = ?config.urls,
            tls = config.tls.enabled,
            "Connecting to NATS"
        );

        let client = options.connect(&config.urls[..]).await.map_err(|e| {
            NatsError::Connection(format!(
                "Failed to connect to NATS cluster {:?}: {}",
                config.urls, e
            ))
        })?;

        let jetstream = jetstream::new(client.clone());

        info!("NATS client initialized; awaiting connection");

        Ok(Self {
            jetstream: Arc::new(jetstream),
            state_rx,
        })
    }

    /// Setup JetStream stream
    pub async fn setup_stream(&self, config: &NatsConfig) -> Result<(), NatsError> {
        info!(stream = config.stream_name, "Setting up JetStream stream");

        let configured = config.num_replicas as usize;
        let desired_config = jetstream::stream::Config {
            name: config.stream_name.clone(),
            subjects: vec![format!("{}.>", config.subject)],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_age: config.retention,
            storage: jetstream::stream::StorageType::File,
            num_replicas: configured,
            duplicate_window: config.duplicate_window,
            ..Default::default()
        };

        match self.jetstream.get_stream(&config.stream_name).await {
            Ok(stream) => {
                let stored = stream.cached_info().config.num_replicas;
                if stored != configured {
                    warn!(
                        stored,
                        configured,
                        stream = config.stream_name,
                        "JetStream stream replica count mismatch — admin (ops) must update online via `nats stream update --replicas={configured}` with a privileged identity"
                    );
                } else {
                    info!(
                        stream = config.stream_name,
                        replicas = stored,
                        "Stream already exists with correct replica count"
                    );
                }
                Ok(())
            }
            Err(e)
                if matches!(
                    e.kind(),
                    jetstream::context::GetStreamErrorKind::JetStream(ref js_err)
                    if js_err.error_code() == jetstream::ErrorCode::STREAM_NOT_FOUND
                ) =>
            {
                self.jetstream
                    .create_stream(desired_config)
                    .await
                    .map_err(|e| NatsError::StreamSetup(format!("Failed to create stream: {e}")))?;
                info!(
                    stream = config.stream_name,
                    replicas = configured,
                    "Stream created"
                );
                Ok(())
            }
            Err(e) => Err(NatsError::StreamSetup(format!(
                "Failed to query stream {}: {e}",
                config.stream_name
            ))),
        }
    }

    /// Get the JetStream context
    pub fn jetstream(&self) -> Arc<JetStreamContext> {
        Arc::clone(&self.jetstream)
    }

    /// Get a receiver for connection state changes
    pub fn state_receiver(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    /// Get the current connection state
    pub fn state(&self) -> ConnectionState {
        *self.state_rx.borrow()
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.state() == ConnectionState::Connected
    }
}

/// Normalizes a PEM string that may have been collapsed into a single line.
///
/// Handles two common inline injection patterns:
/// - Spaces replacing newlines: `"-----BEGIN CERTIFICATE----- MIIFxx... -----END CERTIFICATE-----"`
/// - Literal `\n` sequences from env vars or YAML double-quoted strings
fn normalize_pem(pem: &str) -> String {
    // Resolve literal \n sequences (env var injection)
    let pem = pem.replace("\\n", "\n");
    // Restore newlines around PEM markers. Only spaces adjacent to a `-----` boundary are
    // touched; spaces inside labels (e.g. "EC PRIVATE KEY") are flanked by letters and left intact.
    let normalized = pem
        .trim_end()
        .replace("----- ", "-----\n")
        .replace(" -----", "\n-----");
    // Trim trailing whitespace per line to handle multiple adjacent spaces (e.g. "body  -----END").
    let trimmed = normalized
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n");
    trimmed + "\n"
}

/// Build an in-memory rustls `ClientConfig` from PEM strings supplied via env vars.
fn build_rustls_client_config(tls: &TlsConfig) -> Result<ClientConfig, NatsError> {
    for (label, value) in [("ca", &tls.ca), ("cert", &tls.cert), ("key", &tls.key)] {
        if value.trim().is_empty() {
            return Err(NatsError::Tls(format!(
                "TLS enabled but `{label}` PEM content is empty (set NOX_REPLAYER_NATS__TLS__{} env var)",
                label.to_uppercase()
            )));
        }
    }

    let ca = normalize_pem(&tls.ca);
    let cert = normalize_pem(&tls.cert);
    let key = normalize_pem(&tls.key);

    let mut roots = RootCertStore::empty();
    for cert_der in CertificateDer::pem_slice_iter(ca.as_bytes()) {
        let cert_der =
            cert_der.map_err(|e| NatsError::Tls(format!("Failed to parse CA PEM: {e}")))?;
        roots
            .add(cert_der)
            .map_err(|e| NatsError::Tls(format!("Failed to add CA cert to root store: {e}")))?;
    }
    if roots.is_empty() {
        return Err(NatsError::Tls(
            "No CA certificates found in PEM content".to_string(),
        ));
    }

    let cert_chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| NatsError::Tls(format!("Failed to parse client cert PEM: {e}")))?;
    if cert_chain.is_empty() {
        return Err(NatsError::Tls(
            "No client certificates found in PEM content".to_string(),
        ));
    }

    let private_key = PrivateKeyDer::from_pem_slice(key.as_bytes())
        .map_err(|e| NatsError::Tls(format!("Failed to parse client key PEM: {e}")))?;

    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, private_key)
        .map_err(|e| NatsError::Tls(format!("Failed to build rustls client config: {e}")))
}

#[cfg(test)]
mod tests {
    use super::normalize_pem;

    // Fake base64 bodies — content is irrelevant; only the structural normalization matters.
    const BODY_A: &str = "MIIFajCCBFKgAwIBAgISA1aaaaaaaaaaaaaaaaaaaaaa";
    const BODY_B: &str = "MIIFbjCCBFKgAwIBAgISB2bbbbbbbbbbbbbbbbbbbbbb";

    /// A well-formed multi-line PEM block.
    fn pem_block(label: &str, body: &str) -> String {
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
    }

    /// True if no marker still has a space directly adjacent to its `-----` boundary,
    /// i.e. nothing left for the rustls PEM parser to choke on.
    fn no_marker_adjacent_space(s: &str) -> bool {
        !s.contains("----- ") && !s.contains(" -----")
    }

    /// True if no line carries trailing whitespace (RFC 7468 rejects it on base64 lines).
    fn no_trailing_whitespace(s: &str) -> bool {
        s.lines().all(|line| line == line.trim_end())
    }

    // ── Baseline (the documented happy paths) ────────────────────────────────

    #[test]
    fn single_cert_space_collapsed_is_normalized() {
        let collapsed = format!("-----BEGIN CERTIFICATE----- {BODY_A} -----END CERTIFICATE-----");
        let out = normalize_pem(&collapsed);
        assert!(
            no_marker_adjacent_space(&out),
            "residual marker space: {out:?}"
        );
        assert!(
            out.contains(&format!("\n{BODY_A}\n")),
            "body not on its own line: {out:?}"
        );
    }

    #[test]
    fn single_cert_literal_backslash_n_is_resolved() {
        let injected =
            format!("-----BEGIN CERTIFICATE-----\\n{BODY_A}\\n-----END CERTIFICATE-----");
        let out = normalize_pem(&injected);
        assert!(!out.contains("\\n"), "literal \\n not resolved: {out:?}");
        assert_eq!(out, pem_block("CERTIFICATE", BODY_A));
    }

    #[test]
    fn already_valid_pem_is_left_intact() {
        let valid = pem_block("CERTIFICATE", BODY_A);
        assert_eq!(normalize_pem(&valid), valid);
    }

    #[test]
    fn normalize_pem_is_idempotent() {
        let collapsed = format!("-----BEGIN CERTIFICATE----- {BODY_A} -----END CERTIFICATE-----");
        let once = normalize_pem(&collapsed);
        assert_eq!(normalize_pem(&once), once, "second pass changed the output");
    }

    #[test]
    fn multi_word_label_spaces_are_preserved() {
        // S1: spaces *inside* a label (e.g. "EC PRIVATE KEY") must survive — they are
        // flanked by letters, never adjacent to a `-----` boundary.
        let collapsed =
            format!("-----BEGIN EC PRIVATE KEY----- {BODY_A} -----END EC PRIVATE KEY-----");
        let out = normalize_pem(&collapsed);
        assert!(
            out.contains("-----BEGIN EC PRIVATE KEY-----"),
            "label corrupted: {out:?}"
        );
        assert!(
            out.contains("-----END EC PRIVATE KEY-----"),
            "label corrupted: {out:?}"
        );
    }

    // ── W1: mixed-separator multi-block input must be fully normalized ────────

    #[test]
    fn w1_mixed_separator_chain_is_fully_normalized() {
        // CA bundle where block 1 uses literal `\n` and block 2 is space-collapsed.
        // After `\n` resolution block 1 yields a `-----\n`, tripping the early return
        // and leaving block 2's spaces intact — block 2 then fails to parse.
        let mixed = format!(
            "-----BEGIN CERTIFICATE-----\\n{BODY_A}\\n-----END CERTIFICATE----- \
             -----BEGIN CERTIFICATE----- {BODY_B} -----END CERTIFICATE-----"
        );
        let out = normalize_pem(&mixed);
        assert!(
            no_marker_adjacent_space(&out),
            "block 2 left half-normalized (early return fired): {out:?}"
        );
        assert_eq!(
            out.matches("-----END CERTIFICATE-----").count(),
            2,
            "both END markers should survive on their own lines: {out:?}"
        );
    }

    // ── W2: double space before a marker must not leave trailing whitespace ───

    #[test]
    fn w2_double_space_leaves_no_trailing_whitespace() {
        // Realistic shell-expansion artifact: two spaces before `-----END`.
        let collapsed = format!("-----BEGIN CERTIFICATE----- {BODY_A}  -----END CERTIFICATE-----");
        let out = normalize_pem(&collapsed);
        assert!(
            no_trailing_whitespace(&out),
            "base64 line has trailing whitespace (RFC 7468 invalid): {out:?}"
        );
    }
}
