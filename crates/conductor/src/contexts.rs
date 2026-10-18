//! Named operator connection contexts stored as strict, non-secret JSON.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Args)]
pub struct ContextArgs {
    /// Output format for context results.
    #[arg(long, value_enum, default_value_t = ContextOutput::Human, global = true)]
    output: ContextOutput,
    #[command(subcommand)]
    command: ContextCommand,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[value(rename_all = "lower")]
enum ContextOutput {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum ContextCommand {
    /// Create a named connection context. Only file references are stored; tokens remain in files.
    Create {
        name: String,
        /// Manager API URL. Plain HTTP is allowed only for loopback addresses.
        #[arg(long)]
        server: String,
        /// Protected file containing an operator bearer token.
        #[arg(long)]
        token_file: Option<PathBuf>,
        /// PEM certificate authority used to verify the manager's TLS certificate.
        #[arg(long)]
        ca_file: Option<PathBuf>,
        /// Per-request timeout in seconds.
        #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=300))]
        timeout_seconds: u64,
    },
    /// List context names and the selected default without displaying credential references.
    List,
    /// Show a context's server and timeout without displaying credential references.
    Show { name: String },
    /// Select the default context used by operator commands.
    Use { name: String },
    /// Delete a context. The selected default is cleared if it names this context.
    Delete { name: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionContext {
    pub server_url: String,
    pub token_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ContextStore {
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    contexts: BTreeMap<String, ConnectionContext>,
}

pub fn run(args: ContextArgs) -> ExitCode {
    match run_inner(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("conductor context: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_inner(args: ContextArgs) -> Result<(), String> {
    let path = store_path()?;
    let mut store = load(&path)?;
    let output = args.output;
    match args.command {
        ContextCommand::Create {
            name,
            server,
            token_file,
            ca_file,
            timeout_seconds,
        } => {
            validate_name(&name)?;
            validate_server_url(&server)?;
            if store.contexts.contains_key(&name) {
                return Err("context already exists".to_owned());
            }
            store.contexts.insert(
                name.clone(),
                ConnectionContext {
                    server_url: server,
                    token_file: token_file.map(normalize_path).transpose()?,
                    ca_file: ca_file.map(normalize_path).transpose()?,
                    timeout_seconds,
                },
            );
            save(&path, &store)?;
            write_result(
                output,
                &serde_json::json!({ "name": name, "created": true }),
                format!("created context {name}"),
            )?;
        }
        ContextCommand::List => {
            let rows = store
                .contexts
                .iter()
                .map(|(name, context)| {
                    serde_json::json!({
                        "name": name,
                        "serverUrl": context.server_url,
                        "default": store.default.as_deref() == Some(name),
                    })
                })
                .collect::<Vec<_>>();
            let human = if rows.is_empty() {
                "No contexts configured.".to_owned()
            } else {
                rows.iter()
                    .map(|row| {
                        let name = row["name"].as_str().unwrap_or_default();
                        let server = row["serverUrl"].as_str().unwrap_or_default();
                        let selected = row["default"].as_bool().unwrap_or_default();
                        format!(
                            "{name}{}  {server}",
                            if selected { " (default)" } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            write_result(output, &serde_json::json!({ "contexts": rows }), human)?;
        }
        ContextCommand::Show { name } => {
            let context = store
                .contexts
                .get(&name)
                .ok_or_else(|| "context does not exist".to_owned())?;
            let value = serde_json::json!({
                "name": name,
                "serverUrl": context.server_url,
                "timeoutSeconds": context.timeout_seconds,
                "hasTokenFile": context.token_file.is_some(),
                "hasCaFile": context.ca_file.is_some(),
                "default": store.default.as_deref() == Some(name.as_str()),
            });
            let human = format!(
                "Name: {name}\nServer: {}\nTimeout: {}s\nAuthentication: {}\nCustom CA: {}\nDefault: {}",
                context.server_url,
                context.timeout_seconds,
                if context.token_file.is_some() {
                    "token file configured"
                } else {
                    "none"
                },
                if context.ca_file.is_some() {
                    "configured"
                } else {
                    "system trust"
                },
                if store.default.as_deref() == Some(name.as_str()) {
                    "yes"
                } else {
                    "no"
                },
            );
            write_result(output, &value, human)?;
        }
        ContextCommand::Use { name } => {
            if !store.contexts.contains_key(&name) {
                return Err("context does not exist".to_owned());
            }
            store.default = Some(name.clone());
            save(&path, &store)?;
            write_result(
                output,
                &serde_json::json!({ "name": name, "selected": true }),
                format!("using context {name}"),
            )?;
        }
        ContextCommand::Delete { name } => {
            if store.contexts.remove(&name).is_none() {
                return Err("context does not exist".to_owned());
            }
            if store.default.as_deref() == Some(&name) {
                store.default = None;
            }
            save(&path, &store)?;
            write_result(
                output,
                &serde_json::json!({ "name": name, "deleted": true }),
                format!("deleted context {name}"),
            )?;
        }
    }
    Ok(())
}

pub fn resolve(context_name: Option<&str>) -> Result<Option<(String, ConnectionContext)>, String> {
    let store = load(&store_path()?)?;
    let selected = context_name.or(store.default.as_deref());
    let Some(name) = selected else {
        return Ok(None);
    };
    let context = store
        .contexts
        .get(name)
        .cloned()
        .ok_or_else(|| "selected context does not exist".to_owned())?;
    Ok(Some((name.to_owned(), context)))
}

pub fn validate_server_url(raw: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(raw)
        .map_err(|_| "server must be an absolute HTTP(S) URL".to_owned())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(
            "server must be a base HTTP(S) URL without credentials, path, query, or fragment"
                .to_owned(),
        );
    }
    if url.scheme() == "http" {
        let host = url
            .host_str()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']');
        if !host.eq_ignore_ascii_case("localhost")
            && !host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
        {
            return Err(
                "plain HTTP is allowed only for a loopback server; use HTTPS remotely".to_owned(),
            );
        }
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(
            "context name must contain 1-64 ASCII letters, digits, dots, underscores, or hyphens"
                .to_owned(),
        );
    }
    Ok(())
}

fn normalize_path(path: PathBuf) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path)
    } else {
        env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|_| "current directory is unavailable".to_owned())
    }
}

fn store_path() -> Result<PathBuf, String> {
    let base = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or_else(|| "set XDG_CONFIG_HOME or HOME to store connection contexts".to_owned())?;
    Ok(base.join("conductor").join("contexts.json"))
}

fn load(path: &Path) -> Result<ContextStore, String> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ContextStore::default());
        }
        Err(_) => return Err("context store could not be read".to_owned()),
    };
    let store: ContextStore = serde_json::from_slice(&contents)
        .map_err(|_| "context store contains invalid or unsupported JSON".to_owned())?;
    for (name, context) in &store.contexts {
        validate_name(name)?;
        validate_server_url(&context.server_url)?;
        if !(1..=300).contains(&context.timeout_seconds) {
            return Err("context store contains an invalid timeout".to_owned());
        }
    }
    if store
        .default
        .as_ref()
        .is_some_and(|name| !store.contexts.contains_key(name))
    {
        return Err("context store default does not name a configured context".to_owned());
    }
    Ok(store)
}

fn save(path: &Path, store: &ContextStore) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "context store path is invalid".to_owned())?;
    fs::create_dir_all(parent)
        .map_err(|_| "context store directory could not be created".to_owned())?;
    let bytes = serde_json::to_vec_pretty(store)
        .map_err(|_| "context store could not be encoded".to_owned())?;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_nanos();
    let temp = parent.join(format!(".contexts-{}-{nonce}.tmp", std::process::id()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .map_err(|_| "context store could not be created".to_owned())?;
    use std::io::Write;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| "context store could not be written".to_owned())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
            .map_err(|_| "context store directory permissions could not be set".to_owned())?;
    }
    fs::rename(&temp, path).map_err(|_| "context store could not be replaced".to_owned())
}

fn write_json(value: &serde_json::Value) -> Result<(), String> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), value)
        .map_err(|_| "context output could not be encoded".to_owned())?;
    println!();
    Ok(())
}

fn write_result(
    output: ContextOutput,
    value: &serde_json::Value,
    human: String,
) -> Result<(), String> {
    match output {
        ContextOutput::Human => {
            println!("{human}");
            Ok(())
        }
        ContextOutput::Json => write_json(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_config_is_strict_and_secret_free_when_shown() {
        let context = ConnectionContext {
            server_url: "https://manager.example.test".to_owned(),
            token_file: Some("/run/secrets/operator".into()),
            ca_file: None,
            timeout_seconds: 10,
        };
        let json = serde_json::to_string(&context).expect("serialize context");
        assert!(!json.contains("test-secret"));
        assert!(
            serde_json::from_str::<ConnectionContext>(
                r#"{"serverUrl":"https://manager.example.test","timeoutSeconds":10,"unknown":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn server_url_requires_tls_off_loopback() {
        assert!(validate_server_url("http://127.0.0.1:8080").is_ok());
        assert!(validate_server_url("http://[::1]:8080").is_ok());
        assert!(validate_server_url("https://manager.example.test").is_ok());
        assert!(validate_server_url("http://manager.example.test").is_err());
        assert!(validate_server_url("https://user:password@manager.example.test").is_err());
    }

    #[test]
    fn context_names_are_bounded_and_header_safe() {
        assert!(validate_name("prod-east_1").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("bad name").is_err());
        assert!(validate_name(&"x".repeat(65)).is_err());
    }
}
