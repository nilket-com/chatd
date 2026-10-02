//! Locations, from the XDG base directories, each overridable for tests and operation.

use std::env;
use std::path::PathBuf;

fn home() -> PathBuf {
    env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/"))
}

fn xdg(var: &str, fallback: &str) -> PathBuf {
    match env::var_os(var) {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => home().join(fallback),
    }
}

/// `$CHATD_STATE_DIR`, else `$XDG_STATE_HOME/chatd`.
pub fn state_dir() -> PathBuf {
    env::var_os("CHATD_STATE_DIR").map(PathBuf::from).unwrap_or_else(|| xdg("XDG_STATE_HOME", ".local/state").join("chatd"))
}

/// `$CHATD_SOCKET`, else `$XDG_RUNTIME_DIR/chatd/chatd.sock`.
pub fn socket_path() -> PathBuf {
    if let Some(p) = env::var_os("CHATD_SOCKET") {
        return PathBuf::from(p);
    }
    let runtime = env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("/run/user/{}", unsafe { libc::getuid() })));
    runtime.join("chatd").join("chatd.sock")
}

/// `$CHATD_CONFIG`, else `$XDG_CONFIG_HOME/chatd/config.toml`.
pub fn config_path() -> PathBuf {
    env::var_os("CHATD_CONFIG").map(PathBuf::from).unwrap_or_else(|| xdg("XDG_CONFIG_HOME", ".config").join("chatd").join("config.toml"))
}
