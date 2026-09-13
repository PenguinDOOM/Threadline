use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

const CODEX_KEYRING_SERVICE: &str = "Codex Auth";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthDiscoveryOptions {
    pub chatgpt_local_home: Option<PathBuf>,
    pub codex_home: Option<PathBuf>,
    pub user_home: Option<PathBuf>,
}

impl AuthDiscoveryOptions {
    pub fn from_env() -> Self {
        Self {
            chatgpt_local_home: env_path("CHATGPT_LOCAL_HOME"),
            codex_home: env_path("CODEX_HOME"),
            user_home: env_path("USERPROFILE").or_else(|| env_path("HOME")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthSource {
    CodexKeyring,
    ChatgptLocalAuth,
    CodexHomeAuth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshBoundary {
    NotAvailable,
    RefreshTokenPresent,
}

#[derive(Clone, PartialEq, Eq)]
pub struct LoadedUpstreamAuth {
    pub bearer_token: String,
    pub source: AuthSource,
    pub refresh_boundary: RefreshBoundary,
}

impl fmt::Debug for LoadedUpstreamAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedUpstreamAuth")
            .field("bearer_token", &"[redacted]")
            .field("source", &self.source)
            .field("refresh_boundary", &self.refresh_boundary)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialStoreErrorKind {
    ServiceUnavailable,
    MalformedPayload,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
struct CredentialStoreError {
    kind: CredentialStoreErrorKind,
    message: String,
}

impl CredentialStoreError {
    fn new(kind: CredentialStoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    #[cfg(test)]
    fn kind(&self) -> CredentialStoreErrorKind {
        self.kind
    }
}

trait CredentialStore {
    fn get_secret(
        &self,
        service: &str,
        account: &str,
    ) -> Result<Option<String>, CredentialStoreError>;
}

#[derive(Debug, Default, Clone, Copy)]
struct OsKeyringCredentialStore;

impl CredentialStore for OsKeyringCredentialStore {
    fn get_secret(
        &self,
        service: &str,
        account: &str,
    ) -> Result<Option<String>, CredentialStoreError> {
        let entry = keyring::Entry::new(service, account).map_err(|error| {
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                format!("failed to open OS credential entry: {error}"),
            )
        })?;
        match entry.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                format!("failed to read OS credential entry: {error}"),
            )),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthLoadError {
    #[error("Threadline could not find upstream credentials in any supported auth.json location.")]
    MissingCredentials,

    #[error("Threadline could not read upstream credentials from {path}.")]
    CredentialFileUnreadable { path: PathBuf },

    #[error(
        "Threadline found an auth.json file at {path}, but it did not contain a usable upstream token."
    )]
    CredentialFileMissingToken { path: PathBuf },

    #[error("Threadline could not parse auth.json at {path}.")]
    CredentialFileMalformed { path: PathBuf },
}

#[derive(Debug, Deserialize)]
struct StoredAuthFile {
    #[serde(rename = "OPENAI_API_KEY")]
    openai_api_key: Option<String>,
    tokens: Option<StoredTokens>,
}

#[derive(Debug, Deserialize)]
struct StoredTokens {
    access_token: Option<String>,
    refresh_token: Option<String>,
}

fn codex_keyring_service_and_account(codex_home: &Path) -> std::io::Result<(String, String)> {
    Ok((
        CODEX_KEYRING_SERVICE.to_string(),
        compute_codex_store_key(codex_home),
    ))
}

fn load_codex_keyring_auth(
    store: &impl CredentialStore,
    codex_home: &Path,
) -> Result<Option<LoadedUpstreamAuth>, CredentialStoreError> {
    let (service, account) = codex_keyring_service_and_account(codex_home).map_err(|_| {
        CredentialStoreError::new(
            CredentialStoreErrorKind::ServiceUnavailable,
            "failed to compute Codex keyring account",
        )
    })?;
    let Some(secret) = store.get_secret(&service, &account)? else {
        return Ok(None);
    };

    let file = serde_json::from_str::<StoredAuthFile>(&secret).map_err(|_| {
        CredentialStoreError::new(
            CredentialStoreErrorKind::MalformedPayload,
            "Codex keyring payload could not be parsed.",
        )
    })?;

    let token = file
        .tokens
        .as_ref()
        .and_then(|tokens| non_empty(tokens.access_token.as_deref()))
        .or_else(|| non_empty(file.openai_api_key.as_deref()));

    let Some(token) = token else {
        return Err(CredentialStoreError::new(
            CredentialStoreErrorKind::MalformedPayload,
            "Codex keyring payload did not contain a usable upstream token.",
        ));
    };

    let refresh_boundary = if file
        .tokens
        .as_ref()
        .and_then(|tokens| non_empty(tokens.refresh_token.as_deref()))
        .is_some()
    {
        RefreshBoundary::RefreshTokenPresent
    } else {
        RefreshBoundary::NotAvailable
    };

    Ok(Some(LoadedUpstreamAuth {
        bearer_token: token.to_string(),
        source: AuthSource::CodexKeyring,
        refresh_boundary,
    }))
}

fn compute_codex_store_key(codex_home: &Path) -> String {
    let canonical = codex_home
        .canonicalize()
        .unwrap_or_else(|_| codex_home.to_path_buf());
    let path_str = canonical.to_string_lossy();
    let mut hasher = Sha256::new();
    hasher.update(path_str.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    format!("cli|{}", &hex[..16])
}

pub fn load_upstream_auth(
    options: &AuthDiscoveryOptions,
) -> Result<LoadedUpstreamAuth, AuthLoadError> {
    load_upstream_auth_with_store(options, &OsKeyringCredentialStore)
}

fn load_upstream_auth_with_store(
    options: &AuthDiscoveryOptions,
    store: &impl CredentialStore,
) -> Result<LoadedUpstreamAuth, AuthLoadError> {
    if let Some(codex_home) = non_empty_path(options.codex_home.as_ref()) {
        match load_codex_keyring_auth(store, codex_home) {
            Ok(Some(auth)) => return Ok(auth),
            Ok(None) => {}
            Err(_) => {}
        }
    }

    for (source, root) in auth_search_roots(options) {
        let path = root.join("auth.json");
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(AuthLoadError::CredentialFileUnreadable { path }),
        };

        if metadata.is_dir() {
            return Err(AuthLoadError::CredentialFileUnreadable { path });
        }

        let bytes = fs::read(&path)
            .map_err(|_| AuthLoadError::CredentialFileUnreadable { path: path.clone() })?;
        let file = serde_json::from_slice::<StoredAuthFile>(&bytes)
            .map_err(|_| AuthLoadError::CredentialFileMalformed { path: path.clone() })?;

        let token = file
            .tokens
            .as_ref()
            .and_then(|tokens| non_empty(tokens.access_token.as_deref()))
            .or_else(|| non_empty(file.openai_api_key.as_deref()));

        let Some(token) = token else {
            return Err(AuthLoadError::CredentialFileMissingToken { path });
        };

        let refresh_boundary = if file
            .tokens
            .as_ref()
            .and_then(|tokens| non_empty(tokens.refresh_token.as_deref()))
            .is_some()
        {
            RefreshBoundary::RefreshTokenPresent
        } else {
            RefreshBoundary::NotAvailable
        };

        return Ok(LoadedUpstreamAuth {
            bearer_token: token.to_string(),
            source,
            refresh_boundary,
        });
    }

    Err(AuthLoadError::MissingCredentials)
}

fn auth_search_roots(options: &AuthDiscoveryOptions) -> Vec<(AuthSource, PathBuf)> {
    let mut roots = Vec::new();

    if let Some(path) = non_empty_path(options.chatgpt_local_home.as_ref()) {
        roots.push((AuthSource::ChatgptLocalAuth, path.to_path_buf()));
    }
    if let Some(path) = non_empty_path(options.codex_home.as_ref()) {
        roots.push((AuthSource::CodexHomeAuth, path.to_path_buf()));
    }
    if let Some(user_home) = non_empty_path(options.user_home.as_ref()) {
        roots.push((
            AuthSource::ChatgptLocalAuth,
            user_home.join(".chatgpt-local"),
        ));
        roots.push((AuthSource::CodexHomeAuth, user_home.join(".codex")));
    }

    roots
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn non_empty_path(path: Option<&PathBuf>) -> Option<&PathBuf> {
    path.filter(|path| !path.as_os_str().is_empty())
}

#[cfg(test)]
#[derive(Default, Debug, Clone)]
struct FakeCredentialStore {
    state: std::sync::Arc<std::sync::Mutex<FakeCredentialStoreState>>,
}

#[cfg(test)]
#[derive(Default, Debug)]
struct FakeCredentialStoreState {
    secrets: std::collections::BTreeMap<(String, String), String>,
    read_errors: std::collections::BTreeMap<(String, String), CredentialStoreError>,
}

#[cfg(test)]
impl FakeCredentialStore {
    fn seed_secret(&self, service: &str, account: &str, secret: &str) {
        let mut state = self.state.lock().expect("fake credential store poisoned");
        state.secrets.insert(
            (service.to_string(), account.to_string()),
            secret.to_string(),
        );
    }

    fn read_raw(&self, service: &str, account: &str) -> Option<String> {
        let state = self.state.lock().expect("fake credential store poisoned");
        state
            .secrets
            .get(&(service.to_string(), account.to_string()))
            .cloned()
    }

    fn with_service_error(service: &str, account: &str, error: CredentialStoreError) -> Self {
        let store = Self::default();
        let mut state = store.state.lock().expect("fake credential store poisoned");
        state
            .read_errors
            .insert((service.to_string(), account.to_string()), error);
        drop(state);
        store
    }
}

#[cfg(test)]
impl CredentialStore for FakeCredentialStore {
    fn get_secret(
        &self,
        service: &str,
        account: &str,
    ) -> Result<Option<String>, CredentialStoreError> {
        let state = self.state.lock().expect("fake credential store poisoned");
        if let Some(error) = state
            .read_errors
            .get(&(service.to_string(), account.to_string()))
        {
            return Err(error.clone());
        }

        Ok(state
            .secrets
            .get(&(service.to_string(), account.to_string()))
            .cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        AuthDiscoveryOptions, AuthLoadError, AuthSource, CredentialStoreError,
        CredentialStoreErrorKind, FakeCredentialStore, RefreshBoundary,
        codex_keyring_service_and_account, load_codex_keyring_auth, load_upstream_auth,
        load_upstream_auth_with_store,
    };

    fn seed_legacy_threadline_keyring_secret(
        store: &FakeCredentialStore,
        bearer_token: &str,
        refresh_token: Option<&str>,
    ) {
        let payload = match refresh_token {
            Some(refresh_token) => json!({
                "bearer_token": bearer_token,
                "refresh_token": refresh_token,
            }),
            None => json!({
                "bearer_token": bearer_token,
            }),
        };
        store.seed_secret("Threadline Auth", "default", &payload.to_string());
    }

    fn seed_codex_keyring_payload(
        store: &FakeCredentialStore,
        codex_home: &Path,
        access_token: &str,
        refresh_token: Option<&str>,
    ) {
        let (service, account) =
            codex_keyring_service_and_account(codex_home).expect("codex key should compute");
        let payload = match refresh_token {
            Some(refresh_token) => json!({
                "tokens": {
                    "access_token": access_token,
                    "refresh_token": refresh_token
                }
            }),
            None => json!({
                "tokens": {
                    "access_token": access_token
                }
            }),
        };
        store.seed_secret(&service, &account, &payload.to_string());
    }

    #[test]
    fn codex_store_key_matches_known_codex_home() {
        let (service, account) = codex_keyring_service_and_account(Path::new("~/.codex"))
            .expect("codex key should compute");

        assert_eq!(service, "Codex Auth");
        assert_eq!(account, "cli|940db7b1d0e4eb40");
    }

    #[test]
    fn codex_keyring_payload_is_read_without_rewriting_unknown_fields() {
        let store = FakeCredentialStore::default();
        let (service, account) = codex_keyring_service_and_account(Path::new("~/.codex"))
            .expect("codex key should compute");
        let original_payload = json!({
            "OPENAI_API_KEY": "",
            "tokens": {
                "access_token": "codex-access-token",
                "refresh_token": "codex-refresh-token",
                "unknown_nested": "keep-me"
            },
            "last_refresh": "2026-06-07T00:00:00Z",
            "unknown_top_level": {
                "still": "here"
            }
        })
        .to_string();
        store.seed_secret(&service, &account, &original_payload);

        let auth = load_codex_keyring_auth(&store, Path::new("~/.codex"))
            .expect("codex payload should load")
            .expect("codex auth should exist");

        assert_eq!(auth.bearer_token, "codex-access-token");
        assert_eq!(auth.source, AuthSource::CodexKeyring);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::RefreshTokenPresent);
        assert_eq!(
            store.read_raw(&service, &account).as_deref(),
            Some(original_payload.as_str())
        );
    }

    #[test]
    fn threadline_owned_keyring_entries_are_ignored_during_auth_loading() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        seed_legacy_threadline_keyring_secret(
            &store,
            "threadline-token",
            Some("threadline-refresh"),
        );
        seed_codex_keyring_payload(&store, &codex_home, "codex-token", Some("codex-refresh"));

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("codex keyring auth should load when legacy threadline secret exists");

        assert_eq!(auth.bearer_token, "codex-token");
        assert_eq!(auth.source, AuthSource::CodexKeyring);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::RefreshTokenPresent);
    }

    #[test]
    fn codex_keyring_wins_when_present() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        seed_codex_keyring_payload(&store, &codex_home, "codex-token", Some("codex-refresh"));

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("codex keyring auth should load");

        assert_eq!(auth.bearer_token, "codex-token");
        assert_eq!(auth.source, AuthSource::CodexKeyring);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::RefreshTokenPresent);
    }

    #[test]
    fn codex_auth_file_remains_fallback_when_keyring_is_missing() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"}))
                .expect("json"),
        )
        .expect("auth file");

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth =
            load_upstream_auth_with_store(&options, &store).expect("codex auth file should load");

        assert_eq!(auth.bearer_token, "codex-file-token");
        assert_eq!(auth.source, AuthSource::CodexHomeAuth);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn codex_keyring_service_failure_falls_through_to_codex_auth_file() {
        let temp = TempDir::new().expect("tempdir");
        let codex_home = temp.path().join("codex-home");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"}))
                .expect("json"),
        )
        .expect("auth file");
        let (service, account) =
            codex_keyring_service_and_account(&codex_home).expect("codex key should compute");
        let store = FakeCredentialStore::with_service_error(
            &service,
            &account,
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                "keyring backend unavailable",
            ),
        );

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("codex auth file should load after keyring failure");

        assert_eq!(auth.bearer_token, "codex-file-token");
        assert_eq!(auth.source, AuthSource::CodexHomeAuth);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn malformed_codex_keyring_payload_falls_through_to_supported_auth_file_roots() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"}))
                .expect("json"),
        )
        .expect("auth file");
        let (service, account) =
            codex_keyring_service_and_account(&codex_home).expect("codex key should compute");
        store.seed_secret(&service, &account, r#"{"tokens":{"access_token":}}"#);

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("codex auth file should load after malformed keyring payload");

        assert_eq!(auth.bearer_token, "codex-file-token");
        assert_eq!(auth.source, AuthSource::CodexHomeAuth);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn malformed_codex_keyring_payload_is_reported_without_exposing_secret_values() {
        let store = FakeCredentialStore::default();
        let codex_home = Path::new("~/.codex");
        let (service, account) =
            codex_keyring_service_and_account(codex_home).expect("codex key should compute");
        store.seed_secret(
            &service,
            &account,
            r#"{"tokens":{"access_token":"leaked-secret"}"#,
        );

        let error = load_codex_keyring_auth(&store, codex_home)
            .expect_err("malformed payload should surface as a keyring error");

        assert_eq!(error.kind(), CredentialStoreErrorKind::MalformedPayload);
        assert!(!error.to_string().contains("leaked-secret"));
    }

    #[test]
    fn codex_keyring_is_skipped_when_codex_home_is_unavailable() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let chatgpt_home = temp.path().join("chatgpt-home");
        fs::create_dir_all(&chatgpt_home).expect("chatgpt home");
        fs::write(
            chatgpt_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "chatgpt-file-token"}))
                .expect("json"),
        )
        .expect("auth file");

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: Some(chatgpt_home),
            codex_home: None,
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("chatgpt auth should load without codex home");

        assert_eq!(auth.bearer_token, "chatgpt-file-token");
        assert_eq!(auth.source, AuthSource::ChatgptLocalAuth);
    }

    #[test]
    fn missing_credentials_return_secret_safe_error() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let options = AuthDiscoveryOptions {
            chatgpt_local_home: Some(temp.path().join("chatgpt-home")),
            codex_home: Some(temp.path().join("codex-home")),
            user_home: Some(temp.path().join("user-home")),
        };

        let error =
            load_upstream_auth_with_store(&options, &store).expect_err("missing auth should fail");

        assert_eq!(error, AuthLoadError::MissingCredentials);
        assert!(!error.to_string().contains("codex-token"));
    }

    #[test]
    fn unreadable_auth_file_returns_secret_safe_error() {
        let temp = TempDir::new().expect("tempdir");
        let chatgpt_home = temp.path().join("chatgpt-home");
        fs::create_dir_all(chatgpt_home.join("auth.json")).expect("make unreadable directory");

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: Some(chatgpt_home),
            codex_home: None,
            user_home: None,
        };

        let error = load_upstream_auth(&options).expect_err("directory auth path should fail");

        match &error {
            AuthLoadError::CredentialFileUnreadable { path } => {
                assert_eq!(
                    path.file_name().and_then(|part| part.to_str()),
                    Some("auth.json")
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(!error.to_string().contains("secret-value"));
    }

    #[test]
    fn codex_auth_file_is_used_when_chatgpt_auth_is_missing() {
        let temp = TempDir::new().expect("tempdir");
        let codex_home = temp.path().join("codex-home");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"}))
                .expect("json"),
        )
        .expect("auth file");

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: Some(temp.path().join("chatgpt-home")),
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth(&options).expect("codex auth should load");

        assert_eq!(auth.bearer_token, "codex-file-token");
        assert_eq!(auth.source, AuthSource::CodexHomeAuth);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn chatgpt_auth_reports_refresh_capability_when_refresh_token_exists() {
        let temp = TempDir::new().expect("tempdir");
        let chatgpt_home = temp.path().join("chatgpt-home");
        fs::create_dir_all(&chatgpt_home).expect("chatgpt home");
        fs::write(
            chatgpt_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({
                "tokens": {
                    "access_token": "chatgpt-access-token",
                    "refresh_token": "chatgpt-refresh-token"
                }
            }))
            .expect("json"),
        )
        .expect("auth file");

        let options = AuthDiscoveryOptions {
            chatgpt_local_home: Some(chatgpt_home),
            codex_home: None,
            user_home: None,
        };

        let auth = load_upstream_auth(&options).expect("chatgpt auth should load");

        assert_eq!(auth.bearer_token, "chatgpt-access-token");
        assert_eq!(auth.source, AuthSource::ChatgptLocalAuth);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::RefreshTokenPresent);
    }

    #[test]
    fn loaded_upstream_auth_debug_redacts_bearer_token() {
        let auth = super::LoadedUpstreamAuth {
            bearer_token: "sensitive-token".to_string(),
            source: AuthSource::CodexKeyring,
            refresh_boundary: RefreshBoundary::NotAvailable,
        };

        let debug = format!("{auth:?}");

        assert!(debug.contains("LoadedUpstreamAuth"));
        assert!(debug.contains("bearer_token"));
        assert!(debug.contains("[redacted]"));
        assert!(debug.contains("CodexKeyring"));
        assert!(debug.contains("NotAvailable"));
        assert!(!debug.contains("sensitive-token"));
    }
}
