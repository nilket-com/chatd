//! chatd: the broker daemon (systemd user service, Type=notify).

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chatd::{journal, notify, paths, server};

fn block_signals() -> libc::sigset_t {
    // SAFETY: plain libc signal-mask calls on a zeroed set.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        set
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match (args[1].as_str(), args.get(2)) {
            ("restore", Some(backup)) if args.len() == 3 => {
                match server::restore(&paths::state_dir(), &paths::socket_path(), std::path::Path::new(backup)) {
                    Ok((id, kept)) => {
                        println!("restored {backup} as store {id}; the replaced database is kept in {}", kept.display());
                        return;
                    }
                    Err(e) => {
                        eprintln!("chatd: restore refused: {e}");
                        std::process::exit(1);
                    }
                }
            }
            _ => {
                eprintln!("usage: chatd            (run the daemon)\n       chatd restore <backup.db>   (offline; daemon stopped)");
                std::process::exit(2);
            }
        }
    }
    // Blocked before any thread starts, so only the signal thread below receives them.
    let set = block_signals();
    let config = match server::Config::load(&paths::config_path()) {
        Ok(c) => c,
        Err(e) => {
            journal::log(journal::PRIORITY_ERR, &format!("configuration: {e}"), &[]);
            std::process::exit(1);
        }
    };
    let socket = paths::socket_path();
    let (shared, listener) = match server::prepare(config, &paths::state_dir(), &socket) {
        Ok(x) => x,
        Err(e) => {
            // Never ready: systemd sees the start fail.
            journal::log(journal::PRIORITY_ERR, &format!("startup refused: {e}"), &[]);
            std::process::exit(1);
        }
    };
    let s = shared.clone();
    let sock = socket.clone();
    std::thread::Builder::new()
        .name("signals".into())
        .spawn(move || {
            let mut sig: libc::c_int = 0;
            // SAFETY: waits on the set blocked above.
            unsafe { libc::sigwait(&set, &mut sig) };
            let _ = notify::notify("STOPPING=1");
            journal::log(journal::PRIORITY_NOTICE, &format!("stopping on signal {sig}"), &[]);
            s.shutdown.store(true, Ordering::SeqCst);
            s.hub.wake_all();
            // The adapter kills its child and records the attempt as uncertain; give it a bound.
            let until = Instant::now() + Duration::from_secs(5);
            while s.adapter_child.lock().map(|c| c.is_some()).unwrap_or(false) && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(20));
            }
            std::thread::sleep(Duration::from_millis(100));
            let _ = std::fs::remove_file(&sock);
            std::process::exit(0);
        })
        .expect("signal thread");
    if let Err(e) = server::serve(shared, listener) {
        journal::log(journal::PRIORITY_ERR, &format!("serve failed: {e}"), &[]);
        std::process::exit(1);
    }
}
