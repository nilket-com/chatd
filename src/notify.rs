//! systemd readiness (`Type=notify`) without libsystemd: one datagram to `$NOTIFY_SOCKET`, which may
//! be a filesystem path or a Linux abstract address (leading `@`).

use std::io;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};

/// Sends `state` (e.g. "READY=1") if `$NOTIFY_SOCKET` is set. Ok(false) when not under systemd.
pub fn notify(state: &str) -> io::Result<bool> {
    let Some(target) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(false);
    };
    let target = target.to_string_lossy().into_owned();
    let addr = if let Some(name) = target.strip_prefix('@') {
        SocketAddr::from_abstract_name(name.as_bytes())?
    } else if target.starts_with('/') {
        SocketAddr::from_pathname(&target)?
    } else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("NOTIFY_SOCKET {target:?} is neither a path nor @abstract")));
    };
    let sock = UnixDatagram::unbound()?;
    sock.send_to_addr(state.as_bytes(), &addr)?;
    Ok(true)
}
