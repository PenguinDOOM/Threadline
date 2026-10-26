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
    let (service, account) =
        codex_keyring_service_and_account(Path::new("~/.codex")).expect("codex key should compute");

    assert_eq!(service, "Codex Auth");
    assert_eq!(account, "cli|940db7b1d0e4eb40");
}

#[test]
fn codex_keyring_payload_is_read_without_rewriting_unknown_fields() {
    let store = FakeCredentialStore::default();
    let (service, account) =
        codex_keyring_service_and_account(Path::new("~/.codex")).expect("codex key should compute");
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
    seed_legacy_threadline_keyring_secret(&store, "threadline-token", Some("threadline-refresh"));
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

    let auth =
        load_upstream_auth_with_store(&options, &store).expect("codex keyring auth should load");

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
        serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"})).expect("json"),
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
        serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"})).expect("json"),
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
        serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"})).expect("json"),
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
        serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "chatgpt-file-token"})).expect("json"),
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
        serde_json::to_vec_pretty(&json!({"OPENAI_API_KEY": "codex-file-token"})).expect("json"),
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
