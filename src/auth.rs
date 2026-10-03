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
    if let Some(codex_home) = non_empty_path(options.codex_home.as_ref())
        && let Ok(Some(auth)) = load_codex_keyring_auth(store, codex_home)
    {
        return Ok(auth);
    }

    for (source, root) in auth_search_roots(options) {
        let path = root.join("auth.json");
        if let Some(file) = read_auth_file(&path)? {
            return auth_from_file(file, source, path);
        }
    }

    Err(AuthLoadError::MissingCredentials)
}

fn read_auth_file(path: &Path) -> Result<Option<StoredAuthFile>, AuthLoadError> {
    let unreadable = || AuthLoadError::CredentialFileUnreadable { path: path.into() };
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(unreadable()),
    };
    if metadata.is_dir() {
        return Err(unreadable());
    }
    let bytes = fs::read(path).map_err(|_| unreadable())?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| AuthLoadError::CredentialFileMalformed { path: path.into() })
}

fn auth_from_file(
    file: StoredAuthFile,
    source: AuthSource,
    path: PathBuf,
) -> Result<LoadedUpstreamAuth, AuthLoadError> {
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

    Ok(LoadedUpstreamAuth {
        bearer_token: token.to_string(),
        source,
        refresh_boundary,
    })
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
mod tests;
