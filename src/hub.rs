//! Commit notification: writers announce the new head sequence after each commit; watchers and the
//! adapter sleep on it (no periodic database polling for messages). `wake_all` also ends every
//! current wait (shutdown, or a watcher whose peer closed).

use std::sync::{Condvar, Mutex};
use std::time::Duration;

#[derive(Default)]
struct State {
    head: i64,
    epoch: u64,
}

#[derive(Default)]
pub struct Hub {
    state: Mutex<State>,
    cv: Condvar,
}

impl Hub {
    pub fn new(head: i64) -> Hub {
        Hub { state: Mutex::new(State { head, epoch: 0 }), cv: Condvar::new() }
    }

    pub fn head(&self) -> i64 {
        self.state.lock().unwrap().head
    }

    /// Announces that everything up to `head` is committed (never moves backwards).
    pub fn committed(&self, head: i64) {
        let mut s = self.state.lock().unwrap();
        if head > s.head {
            s.head = head;
        }
        drop(s);
        self.cv.notify_all();
    }

    /// Ends every current wait.
    pub fn wake_all(&self) {
        self.state.lock().unwrap().epoch += 1;
        self.cv.notify_all();
    }

    /// Waits until the head passes `seen`, `wake_all` is called, or `timeout` elapses; returns the
    /// current head.
    pub fn wait_beyond(&self, seen: i64, timeout: Duration) -> i64 {
        let s = self.state.lock().unwrap();
        let epoch = s.epoch;
        let (s, _) = self.cv.wait_timeout_while(s, timeout, |s| s.head <= seen && s.epoch == epoch).unwrap();
        s.head
    }
}
