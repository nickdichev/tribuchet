//! Connected workers' liveness and capacity, and recently departed ones.

use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime};

use super::{HubState, WorkerCaps};

/// A connected worker's entry in `HubState::worker_caps`.
pub(in crate::hub) struct Worker {
    pub(in crate::hub) caps: WorkerCaps,
    /// Wall-clock registration time; `caps.connected_at` cannot be
    /// reported as an absolute time.
    pub(in crate::hub) connected_wall: SystemTime,
    /// Shared with the worker's session, which updates it.
    pub(in crate::hub) live: Arc<Liveness>,
}

impl Worker {
    pub(in crate::hub) fn new(caps: WorkerCaps) -> Self {
        Self {
            caps,
            connected_wall: SystemTime::now(),
            live: Arc::default(),
        }
    }
}

#[derive(Default)]
pub(in crate::hub) struct Liveness {
    /// The worker's last declared free capacity, decremented per
    /// dispatch until its next declaration replaces it.
    pub(in crate::hub) free_slots: AtomicU32,
    last_heartbeat: Mutex<Option<SystemTime>>,
}

impl Liveness {
    pub(in crate::hub) fn heartbeat(&self) {
        *self.last_heartbeat.lock().unwrap() = Some(SystemTime::now());
    }

    /// None until the worker's first heartbeat.
    pub(in crate::hub) fn last_heartbeat(&self) -> Option<SystemTime> {
        *self.last_heartbeat.lock().unwrap()
    }
}

/// A worker whose session ended, kept for `worker_grace`.
pub(in crate::hub) struct Departed {
    pub(in crate::hub) caps: WorkerCaps,
    pub(in crate::hub) at: Instant,
    /// Wall-clock departure time, for status reporting.
    pub(in crate::hub) at_wall: SystemTime,
}

impl HubState {
    /// Remember a departed worker's capabilities so a build for the
    /// platform it served waits for it to reconnect rather than
    /// declining at once.
    pub(in crate::hub) fn record_departed(&self, caps: WorkerCaps) {
        // Prune here too: expected_deadline() only runs for unservable
        // submissions, so a healthy fleet would otherwise accumulate
        // an entry per worker reconnect indefinitely.
        self.departed_within_grace().push(Departed {
            caps,
            at: Instant::now(),
            at_wall: SystemTime::now(),
        });
    }

    /// The departed list with entries older than `worker_grace` pruned.
    pub(in crate::hub) fn departed_within_grace(&self) -> MutexGuard<'_, Vec<Departed>> {
        let now = Instant::now();
        let mut departed = self.departed.lock().unwrap();
        departed.retain(|d| now < d.at + self.worker_grace);
        departed
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use std::sync::atomic::Ordering;

    use super::*;
    use crate::hub::{CapsGuard, Router, route_loop};
    use crate::proto::{Heartbeat, RequestJob, WorkerMessage, worker_message};

    fn caps() -> WorkerCaps {
        WorkerCaps {
            name: "w".into(),
            connected_at: Instant::now(),
            systems: [("x86_64-linux".to_owned(), [].into())].into(),
            max_jobs: 4,
        }
    }

    fn live(state: &HubState, id: u64) -> Arc<Liveness> {
        state.worker_caps.lock().unwrap()[&id].live.clone()
    }

    /// Run `route_loop` over `msgs` until the stream ends.
    async fn route(state: &Arc<HubState>, id: u64, msgs: Vec<worker_message::Msg>) {
        let (tx, rx) = mpsc::channel(16);
        for m in msgs {
            tx.send(WorkerMessage { msg: Some(m) }).await.unwrap();
        }
        drop(tx);
        route_loop(rx, Router::default(), live(state, id), state.clone(), id).await;
    }

    fn request_job(capacity: u32) -> worker_message::Msg {
        worker_message::Msg::RequestJob(RequestJob { capacity })
    }

    #[tokio::test]
    async fn registered_worker_reports_connect_time_and_declared_capacity() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let before = SystemTime::now();
        let guard = CapsGuard::new(state.clone(), caps());
        let connected = state.worker_caps.lock().unwrap()[&guard.id].connected_wall;
        assert!(connected >= before && connected <= SystemTime::now());
        route(&state, guard.id, vec![request_job(3)]).await;
        assert_eq!(live(&state, guard.id).free_slots.load(Ordering::SeqCst), 3);
        // A later, lower declaration replaces the free slots.
        route(&state, guard.id, vec![request_job(1)]).await;
        assert_eq!(live(&state, guard.id).free_slots.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn heartbeat_advances_last_heartbeat() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let guard = CapsGuard::new(state.clone(), caps());
        route(&state, guard.id, vec![request_job(1)]).await;
        assert_eq!(live(&state, guard.id).last_heartbeat(), None);
        let before = SystemTime::now();
        let hb = worker_message::Msg::Heartbeat(Heartbeat::default());
        route(&state, guard.id, vec![hb]).await;
        let first = live(&state, guard.id).last_heartbeat().unwrap();
        assert!(first >= before);
        route(&state, guard.id, vec![request_job(2)]).await;
        assert_eq!(live(&state, guard.id).last_heartbeat(), Some(first));
    }

    #[test]
    fn dropped_worker_leaves_a_timestamped_departure() {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        let guard = CapsGuard::new(state.clone(), caps());
        let id = guard.id;
        let before = SystemTime::now();
        drop(guard);
        assert!(!state.worker_caps.lock().unwrap().contains_key(&id));
        let departed = state.departed_within_grace();
        assert_eq!(departed.len(), 1);
        assert_eq!(departed[0].caps.name, "w");
        assert!(departed[0].at_wall >= before);
    }

    #[test]
    fn departure_older_than_grace_is_pruned() {
        let state = HubState::for_test(Duration::from_secs(30));
        state.departed.lock().unwrap().push(Departed {
            caps: caps(),
            at: Instant::now().checked_sub(Duration::from_mins(1)).unwrap(),
            at_wall: SystemTime::now(),
        });
        state.record_departed(caps());
        assert_eq!(state.departed_within_grace().len(), 1);
    }
}
