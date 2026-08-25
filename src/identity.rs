use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use tracing::warn;
use uuid::Uuid;

const INSTALLATION_ID_FILENAME: &str = "installation_id";
const THREADLINE_HOME_ENV: &str = "THREADLINE_HOME";
static INSTALLATION_ID: OnceLock<String> = OnceLock::new();

pub fn installation_id() -> &'static str {
    INSTALLATION_ID
        .get_or_init(|| match load_or_create_installation_id() {
            Ok(installation_id) => installation_id,
            Err(error) => {
                warn!(
                    error_kind = ?error.kind(),
                    "installation_identity_persistence_unavailable"
                );
                Uuid::new_v4().to_string()
            }
        })
        .as_str()
}

fn load_or_create_installation_id() -> io::Result<String> {
    load_or_create_installation_id_at(&threadline_state_dir()?)
}

fn threadline_state_dir() -> io::Result<PathBuf> {
    if let Some(path) = non_empty_env_path(THREADLINE_HOME_ENV) {
        return Ok(path);
    }

    non_empty_env_path("USERPROFILE")
        .or_else(|| non_empty_env_path("HOME"))
        .map(|home| home.join(".threadline"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Threadline could not resolve a persistent state directory",
            )
        })
}

fn non_empty_env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn load_or_create_installation_id_at(state_dir: &Path) -> io::Result<String> {
    fs::create_dir_all(state_dir)?;
    let path = state_dir.join(INSTALLATION_ID_FILENAME);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    file.lock()?;

    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    if let Ok(existing) = Uuid::parse_str(contents.trim()) {
        return Ok(existing.to_string());
    }

    let installation_id = Uuid::new_v4().to_string();
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(installation_id.as_bytes())?;
    file.flush()?;
    file.sync_all()?;

    Ok(installation_id)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::{INSTALLATION_ID_FILENAME, load_or_create_installation_id_at};

    #[test]
    fn installation_identity_is_generated_and_reused() {
        let state_dir = TempDir::new().expect("create state dir");

        let first = load_or_create_installation_id_at(state_dir.path())
            .expect("generate installation identity");
        let second = load_or_create_installation_id_at(state_dir.path())
            .expect("reuse installation identity");

        assert_eq!(first, second);
        Uuid::parse_str(&first).expect("installation identity uuid");
        assert_eq!(
            fs::read_to_string(state_dir.path().join(INSTALLATION_ID_FILENAME))
                .expect("read persisted installation identity"),
            first
        );
    }

    #[test]
    fn invalid_installation_identity_is_replaced() {
        let state_dir = TempDir::new().expect("create state dir");
        fs::write(
            state_dir.path().join(INSTALLATION_ID_FILENAME),
            "not-a-uuid",
        )
        .expect("write invalid identity");

        let resolved = load_or_create_installation_id_at(state_dir.path())
            .expect("replace installation identity");

        Uuid::parse_str(&resolved).expect("replacement installation identity uuid");
        assert_eq!(
            fs::read_to_string(state_dir.path().join(INSTALLATION_ID_FILENAME))
                .expect("read replacement installation identity"),
            resolved
        );
    }
}