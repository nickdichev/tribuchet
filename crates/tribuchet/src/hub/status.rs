//! The `/status` snapshot: workers, running and queued builds, and
//! recently departed workers, as JSON. Times are Unix seconds.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use super::state::HubState;

/// Cap on peer-supplied strings. Long enough for any store path (name
/// limit 211 plus the store prefix), so outputs stay joinable.
const MAX_STR: usize = 256;

#[derive(Serialize)]
struct Status {
    generated_at: u64,
    queue_depth: usize,
    workers: Vec<WorkerStatus>,
    assignments: Vec<AssignmentStatus>,
    queued: Vec<QueuedStatus>,
    departed: Vec<DepartedStatus>,
}

#[derive(Serialize)]
struct WorkerStatus {
    name: String,
    systems: Vec<String>,
    max_jobs: u32,
    free_slots: u32,
    connected_at: u64,
    /// None until the worker's first heartbeat.
    last_heartbeat_at: Option<u64>,
}

#[derive(Serialize)]
struct AssignmentStatus {
    job_id: String,
    worker: String,
    system: String,
    name: String,
    outputs: BTreeMap<String, String>,
    started_at: u64,
}

#[derive(Serialize)]
struct QueuedStatus {
    job_id: String,
    system: String,
    name: String,
    outputs: BTreeMap<String, String>,
    enqueued_at: u64,
}

#[derive(Serialize)]
struct DepartedStatus {
    name: String,
    systems: Vec<String>,
    departed_at: u64,
}

fn unix(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// Truncate to `MAX_STR` bytes on a char boundary.
fn cap(value: &str) -> String {
    let mut end = value.len().min(MAX_STR);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn cap_map(map: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    map.iter().map(|(k, v)| (cap(k), cap(v))).collect()
}

fn systems<'a>(systems: impl Iterator<Item = &'a String>) -> Vec<String> {
    let mut out: Vec<String> = systems.map(|s| cap(s)).collect();
    out.sort();
    out
}

/// The hub's current state as JSON.
pub(super) async fn render_status_json(state: &HubState) -> String {
    // The queue lock is async; take it before the sync ones.
    let queued: Vec<QueuedStatus> = state
        .queue
        .lock()
        .await
        .iter()
        .map(|j| QueuedStatus {
            job_id: cap(&j.id),
            system: cap(&j.req.system),
            name: j.req.env.get("name").map(|n| cap(n)).unwrap_or_default(),
            outputs: cap_map(&j.req.outputs),
            enqueued_at: unix(j.enqueued_at),
        })
        .collect();
    let mut workers: Vec<WorkerStatus> = state
        .worker_caps
        .lock()
        .unwrap()
        .values()
        .map(|w| WorkerStatus {
            name: cap(&w.caps.name),
            systems: systems(w.caps.systems.keys()),
            max_jobs: w.caps.max_jobs,
            free_slots: w.live.free_slots.load(Ordering::SeqCst),
            connected_at: unix(w.connected_wall),
            last_heartbeat_at: w.live.last_heartbeat().map(unix),
        })
        .collect();
    workers.sort_by(|a, b| (&a.name, a.connected_at).cmp(&(&b.name, b.connected_at)));
    let mut assignments: Vec<AssignmentStatus> = state
        .assignments
        .lock()
        .unwrap()
        .iter()
        .map(|(id, a)| AssignmentStatus {
            job_id: cap(id),
            worker: cap(&a.worker),
            system: cap(&a.system),
            name: cap(&a.name),
            outputs: cap_map(&a.outputs),
            started_at: unix(a.started_at),
        })
        .collect();
    assignments.sort_by(|a, b| (a.started_at, &a.job_id).cmp(&(b.started_at, &b.job_id)));
    let departed = state
        .departed_within_grace()
        .iter()
        .map(|d| DepartedStatus {
            name: cap(&d.caps.name),
            systems: systems(d.caps.systems.keys()),
            departed_at: unix(d.at_wall),
        })
        .collect();
    let status = Status {
        generated_at: unix(SystemTime::now()),
        queue_depth: queued.len(),
        workers,
        assignments,
        queued,
        departed,
    };
    serde_json::to_string(&status).unwrap_or_default() + "\n"
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::hub::state::{Assigned, Job, Replay, Worker, WorkerCaps};
    use crate::proto::BuildRequest;

    fn caps(name: &str, system: &str) -> WorkerCaps {
        WorkerCaps {
            name: name.into(),
            connected_at: Instant::now(),
            systems: [(system.to_owned(), [].into())].into(),
            max_jobs: 4,
        }
    }

    fn job(id: &str, name: &str) -> Job {
        let mut req = BuildRequest {
            system: "x86_64-linux".into(),
            ..Default::default()
        };
        req.env.insert("name".into(), name.into());
        req.outputs
            .insert("out".into(), format!("/nix/store/{id}-{name}"));
        Job {
            id: id.into(),
            key: id.into(),
            req,
            tmp_dir_pack: Arc::new(Vec::new()),
            features: vec![],
            replay: Arc::new(Replay::default()),
            listing: Mutex::default(),
            attempts: 0,
            queued_at: Instant::now(),
            enqueued_at: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        }
    }

    /// Two workers (one heartbeating, one fresh), one running build,
    /// one queued build and one departed worker. The guard keeps the
    /// running build assigned.
    async fn populated() -> (Arc<HubState>, Assigned) {
        let state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        for (id, name, system) in [
            (1, "builder-a", "x86_64-linux"),
            (2, "builder-b", "aarch64-linux"),
        ] {
            let mut w = Worker::new(caps(name, system));
            w.connected_wall = UNIX_EPOCH + Duration::from_secs(1_700_000_000 + id);
            state.worker_caps.lock().unwrap().insert(id, w);
        }
        let live = state.worker_caps.lock().unwrap()[&1].live.clone();
        live.free_slots.store(3, Ordering::SeqCst);
        live.heartbeat();
        let running = job("aaaa", "hello-2.12");
        let assigned = state.assign(&running, "builder-a");
        state.queue.lock().await.push_back(job("bbbb", "world-1.0"));
        state.record_departed(caps("builder-c", "x86_64-linux"));
        (state, assigned)
    }

    #[tokio::test]
    async fn populated_hub_fills_every_section() {
        let (state, _assigned) = populated().await;
        let json = render_status_json(&state).await;
        let status: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(status["generated_at"].as_u64().unwrap() > 1_700_000_000);
        assert_eq!(status["queue_depth"], 1);

        let workers = &status["workers"];
        assert_eq!(workers.as_array().unwrap().len(), 2);
        assert_eq!(workers[0]["name"], "builder-a");
        assert_eq!(workers[0]["systems"], serde_json::json!(["x86_64-linux"]));
        assert_eq!(workers[0]["max_jobs"], 4);
        assert_eq!(workers[0]["free_slots"], 3);
        assert_eq!(workers[0]["connected_at"], 1_700_000_001);
        assert!(workers[0]["last_heartbeat_at"].is_u64());
        assert!(workers[1]["last_heartbeat_at"].is_null());
        assert_eq!(workers[1]["free_slots"], 0);

        let running = &status["assignments"][0];
        assert_eq!(running["job_id"], "aaaa");
        assert_eq!(running["worker"], "builder-a");
        assert_eq!(running["system"], "x86_64-linux");
        assert_eq!(running["name"], "hello-2.12");
        assert_eq!(running["outputs"]["out"], "/nix/store/aaaa-hello-2.12");
        assert!(running["started_at"].is_u64());

        let queued = &status["queued"][0];
        assert_eq!(queued["job_id"], "bbbb");
        assert_eq!(queued["name"], "world-1.0");
        assert_eq!(queued["outputs"]["out"], "/nix/store/bbbb-world-1.0");
        assert_eq!(queued["enqueued_at"], 1_700_000_000);

        let departed = &status["departed"][0];
        assert_eq!(departed["name"], "builder-c");
        assert_eq!(departed["systems"], serde_json::json!(["x86_64-linux"]));
        assert!(departed["departed_at"].is_u64());
    }

    #[tokio::test]
    async fn empty_hub_renders_empty_lists() {
        let state = HubState::for_test(Duration::ZERO);
        let v: serde_json::Value = serde_json::from_str(&render_status_json(&state).await).unwrap();
        assert_eq!(v["queue_depth"], 0);
        for section in ["workers", "assignments", "queued", "departed"] {
            assert_eq!(v[section], serde_json::json!([]), "{section}");
        }
    }

    #[tokio::test]
    async fn long_peer_strings_are_truncated() {
        let state = HubState::for_test(Duration::ZERO);
        let long = "é".repeat(300);
        state
            .worker_caps
            .lock()
            .unwrap()
            .insert(1, Worker::new(caps(&long, "x86_64-linux")));
        let v: serde_json::Value = serde_json::from_str(&render_status_json(&state).await).unwrap();
        let name = v["workers"][0]["name"].as_str().unwrap();
        assert!(name.len() <= MAX_STR);
        assert!(long.starts_with(name));
    }

    #[tokio::test]
    async fn departures_past_grace_are_left_out() {
        let state = HubState::for_test(Duration::ZERO);
        state.record_departed(caps("gone", "x86_64-linux"));
        let v: serde_json::Value = serde_json::from_str(&render_status_json(&state).await).unwrap();
        assert_eq!(v["departed"], serde_json::json!([]));
    }
}
