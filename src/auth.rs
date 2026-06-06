use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const CODEX_KEYRING_SERVICE: &str = "Codex Auth";
const THREADLINE_KEYRING_SERVICE: &str = "Threadline Auth";
const THREADLINE_KEYRING_ACCOUNT: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthDiscoveryOptions {
    pub explicit_token: Option<String>,
    pub chatgpt_local_home: Option<PathBuf>,
    pub codex_home: Option<PathBuf>,
    pub user_home: Option<PathBuf>,
}

impl AuthDiscoveryOptions {
    pub fn from_env(explicit_token: Option<String>) -> Self {
        Self {
            explicit_token,
            chatgpt_local_home: env_path("CHATGPT_LOCAL_HOME"),
            codex_home: env_path("CODEX_HOME"),
            user_home: env_path("USERPROFILE").or_else(|| env_path("HOME")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthSource {
    ExplicitOverride,
    ThreadlineKeyring,
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ThreadlineKeyringPayload {
    bearer_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    metadata: std::collections::BTreeMap<String, String>,
}

impl fmt::Debug for ThreadlineKeyringPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreadlineKeyringPayload")
            .field("bearer_token", &"[redacted]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialStoreErrorKind {
    ServiceUnavailable,
    MalformedPayload,
    SerializationFailed,
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
    fn set_secret(
        &self,
        service: &str,
        account: &str,
        secret: &str,
    ) -> Result<(), CredentialStoreError>;

    fn delete_secret(&self, service: &str, account: &str) -> Result<bool, CredentialStoreError>;
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

    fn set_secret(
        &self,
        service: &str,
        account: &str,
        secret: &str,
    ) -> Result<(), CredentialStoreError> {
        let entry = keyring::Entry::new(service, account).map_err(|error| {
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                format!("failed to open OS credential entry: {error}"),
            )
        })?;
        entry.set_password(secret).map_err(|error| {
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                format!("failed to write OS credential entry: {error}"),
            )
        })
    }

    fn delete_secret(&self, service: &str, account: &str) -> Result<bool, CredentialStoreError> {
        let entry = keyring::Entry::new(service, account).map_err(|error| {
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                format!("failed to open OS credential entry: {error}"),
            )
        })?;
        match entry.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(error) => Err(CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                format!("failed to delete OS credential entry: {error}"),
            )),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ThreadlineLoginInput {
    pub bearer_token: String,
    pub refresh_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadlineCredentialSource {
    Keyring,
}

impl ThreadlineCredentialSource {
    fn label(self) -> &'static str {
        match self {
            Self::Keyring => "threadline-keyring",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadlineCredentialStatus {
    pub available: bool,
    pub source: Option<ThreadlineCredentialSource>,
    pub refresh_boundary: RefreshBoundary,
}

impl ThreadlineCredentialStatus {
    pub fn render(&self) -> String {
        if !self.available {
            return "Threadline credentials: unavailable".to_string();
        }

        let source = self
            .source
            .map(ThreadlineCredentialSource::label)
            .unwrap_or("unknown");
        let refresh = match self.refresh_boundary {
            RefreshBoundary::NotAvailable => "not-available",
            RefreshBoundary::RefreshTokenPresent => "present",
        };

        format!("Threadline credentials: available (source: {source}, refresh: {refresh})")
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthCommandError {
    #[error("Threadline credentials could not be stored in the OS credential manager.")]
    CredentialStoreUnavailable,

    #[error("Threadline credentials did not contain a usable token.")]
    MissingToken,
}

impl fmt::Debug for ThreadlineLoginInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreadlineLoginInput")
            .field("bearer_token", &"[redacted]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

pub fn store_threadline_credentials(
    input: &ThreadlineLoginInput,
) -> Result<ThreadlineCredentialStatus, AuthCommandError> {
    store_threadline_credentials_with_store(input, &OsKeyringCredentialStore)
}

pub fn threadline_login_status() -> Result<ThreadlineCredentialStatus, AuthCommandError> {
    threadline_login_status_with_store(&OsKeyringCredentialStore)
}

pub fn logout_threadline_credentials() -> Result<bool, AuthCommandError> {
    logout_threadline_credentials_with_store(&OsKeyringCredentialStore)
}

fn store_threadline_credentials_with_store(
    input: &ThreadlineLoginInput,
    store: &impl CredentialStore,
) -> Result<ThreadlineCredentialStatus, AuthCommandError> {
    let Some(token) = non_empty(Some(input.bearer_token.as_str())) else {
        return Err(AuthCommandError::MissingToken);
    };

    let payload = ThreadlineKeyringPayload {
        bearer_token: token.to_string(),
        refresh_token: input
            .refresh_token
            .as_deref()
            .and_then(|refresh_token| non_empty(Some(refresh_token)))
            .map(str::to_string),
        metadata: std::collections::BTreeMap::new(),
    };

    write_threadline_keyring_payload(store, &payload)
        .map_err(|_| AuthCommandError::CredentialStoreUnavailable)?;
    threadline_login_status_with_store(store)
}

fn threadline_login_status_with_store(
    store: &impl CredentialStore,
) -> Result<ThreadlineCredentialStatus, AuthCommandError> {
    let payload = read_threadline_keyring_payload(store)
        .map_err(|_| AuthCommandError::CredentialStoreUnavailable)?;
    let Some(payload) = payload else {
        return Ok(ThreadlineCredentialStatus {
            available: false,
            source: None,
            refresh_boundary: RefreshBoundary::NotAvailable,
        });
    };

    let Some(_) = non_empty(Some(payload.bearer_token.as_str())) else {
        return Err(AuthCommandError::MissingToken);
    };

    Ok(ThreadlineCredentialStatus {
        available: true,
        source: Some(ThreadlineCredentialSource::Keyring),
        refresh_boundary: if non_empty(payload.refresh_token.as_deref()).is_some() {
            RefreshBoundary::RefreshTokenPresent
        } else {
            RefreshBoundary::NotAvailable
        },
    })
}

fn logout_threadline_credentials_with_store(
    store: &impl CredentialStore,
) -> Result<bool, AuthCommandError> {
    store
        .delete_secret(THREADLINE_KEYRING_SERVICE, THREADLINE_KEYRING_ACCOUNT)
        .map_err(|_| AuthCommandError::CredentialStoreUnavailable)
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

fn read_threadline_keyring_payload(
    store: &impl CredentialStore,
) -> Result<Option<ThreadlineKeyringPayload>, CredentialStoreError> {
    let Some(secret) = store.get_secret(THREADLINE_KEYRING_SERVICE, THREADLINE_KEYRING_ACCOUNT)?
    else {
        return Ok(None);
    };

    serde_json::from_str(&secret).map(Some).map_err(|_| {
        CredentialStoreError::new(
            CredentialStoreErrorKind::MalformedPayload,
            "Threadline keyring payload could not be parsed.",
        )
    })
}

fn write_threadline_keyring_payload(
    store: &impl CredentialStore,
    payload: &ThreadlineKeyringPayload,
) -> Result<(), CredentialStoreError> {
    let serialized = serde_json::to_string(payload).map_err(|_| {
        CredentialStoreError::new(
            CredentialStoreErrorKind::SerializationFailed,
            "Threadline keyring payload could not be serialized.",
        )
    })?;

    store.set_secret(
        THREADLINE_KEYRING_SERVICE,
        THREADLINE_KEYRING_ACCOUNT,
        &serialized,
    )
}

fn load_threadline_keyring_auth(
    store: &impl CredentialStore,
) -> Result<Option<LoadedUpstreamAuth>, CredentialStoreError> {
    let Some(payload) = read_threadline_keyring_payload(store)? else {
        return Ok(None);
    };

    let Some(token) = non_empty(Some(payload.bearer_token.as_str())) else {
        return Err(CredentialStoreError::new(
            CredentialStoreErrorKind::MalformedPayload,
            "Threadline keyring payload did not contain a usable upstream token.",
        ));
    };

    let refresh_boundary = if non_empty(payload.refresh_token.as_deref()).is_some() {
        RefreshBoundary::RefreshTokenPresent
    } else {
        RefreshBoundary::NotAvailable
    };

    Ok(Some(LoadedUpstreamAuth {
        bearer_token: token.to_string(),
        source: AuthSource::ThreadlineKeyring,
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
    if let Some(token) = non_empty(options.explicit_token.as_deref()) {
        return Ok(LoadedUpstreamAuth {
            bearer_token: token.to_string(),
            source: AuthSource::ExplicitOverride,
            refresh_boundary: RefreshBoundary::NotAvailable,
        });
    }

    match load_threadline_keyring_auth(store) {
        Ok(Some(auth)) => return Ok(auth),
        Ok(None) => {}
        Err(_) => {}
    }

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
    writes: Vec<((String, String), String)>,
    read_errors: std::collections::BTreeMap<(String, String), CredentialStoreError>,
    write_errors: std::collections::BTreeMap<(String, String), CredentialStoreError>,
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

    fn writes(&self) -> Vec<((String, String), String)> {
        let state = self.state.lock().expect("fake credential store poisoned");
        state.writes.clone()
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

    fn set_secret(
        &self,
        service: &str,
        account: &str,
        secret: &str,
    ) -> Result<(), CredentialStoreError> {
        let mut state = self.state.lock().expect("fake credential store poisoned");
        if let Some(error) = state
            .write_errors
            .get(&(service.to_string(), account.to_string()))
        {
            return Err(error.clone());
        }

        state.secrets.insert(
            (service.to_string(), account.to_string()),
            secret.to_string(),
        );
        state.writes.push((
            (service.to_string(), account.to_string()),
            secret.to_string(),
        ));
        Ok(())
    }

    fn delete_secret(&self, service: &str, account: &str) -> Result<bool, CredentialStoreError> {
        let mut state = self.state.lock().expect("fake credential store poisoned");
        Ok(state
            .secrets
            .remove(&(service.to_string(), account.to_string()))
            .is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        AuthCommandError, AuthDiscoveryOptions, AuthLoadError, AuthSource, CredentialStoreError,
        CredentialStoreErrorKind, FakeCredentialStore, RefreshBoundary, ThreadlineCredentialSource,
        ThreadlineKeyringPayload, ThreadlineLoginInput, codex_keyring_service_and_account,
        load_codex_keyring_auth, load_upstream_auth, load_upstream_auth_with_store,
        logout_threadline_credentials_with_store, read_threadline_keyring_payload,
        store_threadline_credentials_with_store, threadline_login_status_with_store,
        write_threadline_keyring_payload,
    };

    fn seed_threadline_keyring_payload(
        store: &FakeCredentialStore,
        bearer_token: &str,
        refresh_token: Option<&str>,
    ) {
        let payload = ThreadlineKeyringPayload {
            bearer_token: bearer_token.to_string(),
            refresh_token: refresh_token.map(str::to_string),
            metadata: BTreeMap::new(),
        };
        write_threadline_keyring_payload(store, &payload)
            .expect("threadline keyring payload should write");
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
    fn login_command_defaults_to_keyring_store() {
        let store = FakeCredentialStore::default();

        let status = store_threadline_credentials_with_store(
            &ThreadlineLoginInput {
                bearer_token: "threadline-access-token".to_string(),
                refresh_token: Some("threadline-refresh-token".to_string()),
            },
            &store,
        )
        .expect("threadline login should store credentials in keyring by default");

        assert!(status.available);
        assert_eq!(status.source, Some(ThreadlineCredentialSource::Keyring));
        assert_eq!(
            status.refresh_boundary,
            RefreshBoundary::RefreshTokenPresent
        );
        assert!(
            store.read_raw("Threadline Auth", "default").is_some(),
            "threadline keyring entry should be written"
        );
    }

    #[test]
    fn login_status_reports_source_without_token_values() {
        let store = FakeCredentialStore::default();
        store_threadline_credentials_with_store(
            &ThreadlineLoginInput {
                bearer_token: "threadline-access-token".to_string(),
                refresh_token: Some("threadline-refresh-token".to_string()),
            },
            &store,
        )
        .expect("threadline login should store credentials");

        let status = threadline_login_status_with_store(&store)
            .expect("threadline status should read keyring");
        let rendered = status.render();

        assert_eq!(status.source, Some(ThreadlineCredentialSource::Keyring));
        assert_eq!(
            status.refresh_boundary,
            RefreshBoundary::RefreshTokenPresent
        );
        assert!(rendered.contains("threadline-keyring"));
        assert!(rendered.contains("present"));
        assert!(!rendered.contains("threadline-access-token"));
        assert!(!rendered.contains("threadline-refresh-token"));
        assert!(!rendered.contains("default"));
    }

    #[test]
    fn logout_removes_only_threadline_owned_credentials() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");

        store_threadline_credentials_with_store(
            &ThreadlineLoginInput {
                bearer_token: "threadline-access-token".to_string(),
                refresh_token: None,
            },
            &store,
        )
        .expect("threadline login should store credentials");
        seed_codex_keyring_payload(
            &store,
            &codex_home,
            "codex-access-token",
            Some("codex-refresh-token"),
        );

        let removed = logout_threadline_credentials_with_store(&store)
            .expect("threadline logout should remove only threadline credentials");
        let (codex_service, codex_account) =
            codex_keyring_service_and_account(&codex_home).expect("codex key should compute");

        assert!(removed);
        assert!(store.read_raw("Threadline Auth", "default").is_none());
        assert!(store.read_raw(&codex_service, &codex_account).is_some());
    }

    #[test]
    fn login_store_rejects_empty_tokens() {
        let store = FakeCredentialStore::default();

        let error = store_threadline_credentials_with_store(
            &ThreadlineLoginInput {
                bearer_token: "   ".to_string(),
                refresh_token: None,
            },
            &store,
        )
        .expect_err("empty token should be rejected");

        assert_eq!(error, AuthCommandError::MissingToken);
    }

    #[test]
    fn login_input_debug_redacts_secret_values() {
        let input = ThreadlineLoginInput {
            bearer_token: "threadline-access-token".to_string(),
            refresh_token: Some("threadline-refresh-token".to_string()),
        };

        let debug = format!("{input:?}");

        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("threadline-access-token"));
        assert!(!debug.contains("threadline-refresh-token"));
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
        assert!(
            store.writes().is_empty(),
            "codex payload must remain read-only"
        );
        assert_eq!(
            store.read_raw(&service, &account).as_deref(),
            Some(original_payload.as_str())
        );
    }

    #[test]
    fn threadline_keyring_payload_round_trips_without_exposing_secret_debug() {
        let store = FakeCredentialStore::default();
        let payload = ThreadlineKeyringPayload {
            bearer_token: "threadline-access-token".to_string(),
            refresh_token: Some("threadline-refresh-token".to_string()),
            metadata: BTreeMap::from([("profile".to_string(), "default".to_string())]),
        };

        write_threadline_keyring_payload(&store, &payload)
            .expect("threadline payload should write");
        let round_tripped = read_threadline_keyring_payload(&store)
            .expect("threadline payload should read")
            .expect("threadline payload should exist");

        assert_eq!(round_tripped, payload);
        let debug = format!("{payload:?}");
        assert!(debug.contains("[redacted]"));
        assert!(debug.contains("profile"));
        assert!(!debug.contains("threadline-access-token"));
        assert!(!debug.contains("threadline-refresh-token"));

        let writes = store.writes();
        let ((written_service, written_account), _) =
            writes.last().expect("write should be recorded");
        let (codex_service, codex_account) =
            codex_keyring_service_and_account(Path::new("~/.codex"))
                .expect("codex key should compute");
        assert_ne!(written_service, &codex_service);
        assert_ne!(written_account, &codex_account);
    }

    #[test]
    fn keyring_service_errors_are_distinguishable_from_missing_entries() {
        let missing = read_threadline_keyring_payload(&FakeCredentialStore::default())
            .expect("missing keyring entry should not be an error");
        assert!(missing.is_none());

        let store = FakeCredentialStore::with_service_error(
            "Threadline Auth",
            "default",
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                "keyring backend unavailable",
            ),
        );

        let error = read_threadline_keyring_payload(&store)
            .expect_err("service failure should surface distinctly");

        assert_eq!(error.kind(), CredentialStoreErrorKind::ServiceUnavailable);
        assert!(!error.to_string().contains("threadline-access-token"));
    }

    #[test]
    fn explicit_token_override_wins_over_keyring_sources() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        seed_threadline_keyring_payload(&store, "threadline-token", Some("threadline-refresh"));
        seed_codex_keyring_payload(&store, &codex_home, "codex-token", Some("codex-refresh"));

        let options = AuthDiscoveryOptions {
            explicit_token: Some("override-token".to_string()),
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth =
            load_upstream_auth_with_store(&options, &store).expect("explicit token should load");

        assert_eq!(auth.bearer_token, "override-token");
        assert_eq!(auth.source, AuthSource::ExplicitOverride);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn threadline_keyring_is_used_before_codex_keyring() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        seed_threadline_keyring_payload(&store, "threadline-token", Some("threadline-refresh"));
        seed_codex_keyring_payload(&store, &codex_home, "codex-token", Some("codex-refresh"));

        let options = AuthDiscoveryOptions {
            explicit_token: None,
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("threadline keyring auth should load");

        assert_eq!(auth.bearer_token, "threadline-token");
        assert_eq!(auth.source, AuthSource::ThreadlineKeyring);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::RefreshTokenPresent);
    }

    #[test]
    fn codex_keyring_is_used_when_threadline_credentials_are_missing() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        seed_codex_keyring_payload(&store, &codex_home, "codex-token", Some("codex-refresh"));

        let options = AuthDiscoveryOptions {
            explicit_token: None,
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("codex keyring auth should load");

        assert_eq!(auth.bearer_token, "codex-token");
        assert_eq!(auth.source, AuthSource::CodexKeyring);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::RefreshTokenPresent);
        assert!(store.writes().is_empty());
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
            explicit_token: None,
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
            explicit_token: None,
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
    fn keyring_service_unavailable_falls_through_to_next_source() {
        let temp = TempDir::new().expect("tempdir");
        let chatgpt_home = temp.path().join("chatgpt-home");
        fs::create_dir_all(&chatgpt_home).expect("chatgpt home");
        fs::write(
            chatgpt_home.join("auth.json"),
            serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "chatgpt-file-token"}))
                .expect("json"),
        )
        .expect("auth file");
        let store = FakeCredentialStore::with_service_error(
            "Threadline Auth",
            "default",
            CredentialStoreError::new(
                CredentialStoreErrorKind::ServiceUnavailable,
                "keyring backend unavailable",
            ),
        );

        let options = AuthDiscoveryOptions {
            explicit_token: None,
            chatgpt_local_home: Some(chatgpt_home),
            codex_home: None,
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("file auth should load after keyring failure");

        assert_eq!(auth.bearer_token, "chatgpt-file-token");
        assert_eq!(auth.source, AuthSource::ChatgptLocalAuth);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn threadline_keyring_parse_error_falls_through_without_exposing_secret_values() {
        let temp = TempDir::new().expect("tempdir");
        let store = FakeCredentialStore::default();
        let codex_home = temp.path().join("codex-home");
        store.seed_secret(
            "Threadline Auth",
            "default",
            r#"{"bearer_token":"leaked-secret","refresh_token":}"#,
        );
        seed_codex_keyring_payload(&store, &codex_home, "codex-token", None);

        let options = AuthDiscoveryOptions {
            explicit_token: None,
            chatgpt_local_home: None,
            codex_home: Some(codex_home),
            user_home: None,
        };

        let auth = load_upstream_auth_with_store(&options, &store)
            .expect("codex keyring should load after malformed threadline payload");

        assert_eq!(auth.bearer_token, "codex-token");
        assert_eq!(auth.source, AuthSource::CodexKeyring);
        assert!(!format!("{auth:?}").contains("leaked-secret"));
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
            explicit_token: None,
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
            explicit_token: None,
            chatgpt_local_home: Some(temp.path().join("chatgpt-home")),
            codex_home: Some(temp.path().join("codex-home")),
            user_home: Some(temp.path().join("user-home")),
        };

        let error =
            load_upstream_auth_with_store(&options, &store).expect_err("missing auth should fail");

        assert_eq!(error, AuthLoadError::MissingCredentials);
        assert!(!error.to_string().contains("override-token"));
    }

    #[test]
    fn unreadable_auth_file_returns_secret_safe_error() {
        let temp = TempDir::new().expect("tempdir");
        let chatgpt_home = temp.path().join("chatgpt-home");
        fs::create_dir_all(chatgpt_home.join("auth.json")).expect("make unreadable directory");

        let options = AuthDiscoveryOptions {
            explicit_token: None,
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
            explicit_token: None,
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
            explicit_token: None,
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
            source: AuthSource::ExplicitOverride,
            refresh_boundary: RefreshBoundary::NotAvailable,
        };

        let debug = format!("{auth:?}");

        assert!(debug.contains("LoadedUpstreamAuth"));
        assert!(debug.contains("bearer_token"));
        assert!(debug.contains("[redacted]"));
        assert!(debug.contains("ExplicitOverride"));
        assert!(debug.contains("NotAvailable"));
        assert!(!debug.contains("sensitive-token"));
    }
}
