use std::env;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

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

pub fn load_upstream_auth(
    options: &AuthDiscoveryOptions,
) -> Result<LoadedUpstreamAuth, AuthLoadError> {
    if let Some(token) = non_empty(options.explicit_token.as_deref()) {
        return Ok(LoadedUpstreamAuth {
            bearer_token: token.to_string(),
            source: AuthSource::ExplicitOverride,
            refresh_boundary: RefreshBoundary::NotAvailable,
        });
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
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        AuthDiscoveryOptions, AuthLoadError, AuthSource, RefreshBoundary, load_upstream_auth,
    };

    #[test]
    fn explicit_token_override_wins_without_touching_auth_files() {
        let options = AuthDiscoveryOptions {
            explicit_token: Some("override-token".to_string()),
            chatgpt_local_home: None,
            codex_home: None,
            user_home: None,
        };

        let auth = load_upstream_auth(&options).expect("explicit token should load");

        assert_eq!(auth.bearer_token, "override-token");
        assert_eq!(auth.source, AuthSource::ExplicitOverride);
        assert_eq!(auth.refresh_boundary, RefreshBoundary::NotAvailable);
    }

    #[test]
    fn missing_credentials_return_secret_safe_error() {
        let temp = TempDir::new().expect("tempdir");
        let options = AuthDiscoveryOptions {
            explicit_token: None,
            chatgpt_local_home: Some(temp.path().join("chatgpt-home")),
            codex_home: Some(temp.path().join("codex-home")),
            user_home: Some(temp.path().join("user-home")),
        };

        let error = load_upstream_auth(&options).expect_err("missing auth should fail");

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
