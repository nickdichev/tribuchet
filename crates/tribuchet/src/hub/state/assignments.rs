//! Which worker holds each dispatched job, for status reporting.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use super::{HubState, Job};

/// A dispatched job and the worker running it.
// Read only by tests until a status reader lands.
#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::hub) struct Assignment {
    pub(in crate::hub) worker_id: u64,
    pub(in crate::hub) worker: String,
    pub(in crate::hub) system: String,
    /// The derivation's `name`, empty when the request carries none.
    pub(in crate::hub) name: String,
    /// Output name -> scratch output path.
    pub(in crate::hub) outputs: BTreeMap<String, String>,
    pub(in crate::hub) started_at: SystemTime,
}

/// A job's entry in `HubState::assignments`, removed when this drops.
pub(in crate::hub) struct Assigned {
    state: Arc<HubState>,
    job_id: String,
}

impl HubState {
    /// Record that `worker` now runs `job`.
    pub(in crate::hub) fn assign(
        self: &Arc<Self>,
        job: &Job,
        worker_id: u64,
        worker: &str,
    ) -> Assigned {
        let assignment = Assignment {
            worker_id,
            worker: worker.to_owned(),
            system: job.req.system.clone(),
            name: job.req.env.get("name").cloned().unwrap_or_default(),
            outputs: job.req.outputs.clone(),
            started_at: SystemTime::now(),
        };
        self.assignments
            .lock()
            .unwrap()
            .insert(job.id.clone(), assignment);
        Assigned {
            state: self.clone(),
            job_id: job.id.clone(),
        }
    }
}

impl Drop for Assigned {
    fn drop(&mut self) {
        self.state.assignments.lock().unwrap().remove(&self.job_id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tokio::sync::mpsc;

    use super::super::tests::queued;
    use super::super::{Replay, Worker, WorkerCaps};
    use super::*;
    use crate::hub::relay::WorkerSession;
    use crate::hub::{MAX_JOB_ATTEMPTS, Router, supervise_job};

    fn job() -> Job {
        let replay = Arc::new(Replay::default());
        let mut job = queued("x86_64-linux", replay);
        job.req.env.insert("name".into(), "hello-2.12".into());
        job.req
            .outputs
            .insert("out".into(), "/nix/store/aaa-hello-2.12".into());
        job
    }

    #[test]
    fn dispatch_records_worker_name_and_outputs_until_dropped() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let job = job();
        let before = SystemTime::now();
        let assigned = state.assign(&job, 7, "w7");
        {
            let all = state.assignments.lock().unwrap();
            assert_eq!(all.len(), 1);
            let a = &all["j1"];
            assert_eq!((a.worker_id, a.worker.as_str()), (7, "w7"));
            assert_eq!(a.system, "x86_64-linux");
            assert_eq!(a.name, "hello-2.12");
            assert_eq!(a.outputs, job.req.outputs);
            assert!(a.started_at >= before);
        }
        drop(assigned);
        assert!(state.assignments.lock().unwrap().is_empty());
    }

    /// Run `job` on a worker session that is already gone.
    async fn supervise_on_dead_session(state: &Arc<HubState>, job: Job) {
        let (out_tx, out_rx) = mpsc::channel(1);
        drop(out_rx);
        let (_in_tx, in_rx) = mpsc::channel(1);
        let assigned = state.assign(&job, 1, "w1");
        let sess = Arc::new(WorkerSession::new());
        supervise_job(
            state.clone(),
            job,
            assigned,
            Router::default(),
            out_tx,
            in_rx,
            sess,
        )
        .await;
    }

    #[tokio::test]
    async fn requeue_after_worker_loss_clears_assignment_keeps_enqueued_at() {
        // A capable worker stays connected so the requeued job is kept.
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        state.worker_caps.lock().unwrap().insert(
            9,
            Worker::new(WorkerCaps {
                name: "w9".into(),
                connected_at: Instant::now(),
                systems: [("x86_64-linux".to_owned(), [].into())].into(),
                max_jobs: 1,
            }),
        );
        let mut job = job();
        job.enqueued_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let _rx = job.replay.subscribe().await;
        supervise_on_dead_session(&state, job).await;
        assert!(state.assignments.lock().unwrap().is_empty());
        let queue = state.queue.lock().await;
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].attempts, 1);
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        assert_eq!(queue[0].enqueued_at, at);
    }

    #[tokio::test]
    async fn failed_job_clears_assignment() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let mut job = job();
        job.attempts = MAX_JOB_ATTEMPTS;
        supervise_on_dead_session(&state, job).await;
        assert!(state.assignments.lock().unwrap().is_empty());
        assert!(state.queue.lock().await.is_empty());
    }

    #[tokio::test]
    async fn queue_rebuild_keeps_enqueued_at() {
        let mut state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        // Inside the startup window every platform is awaited, so the
        // job is kept through the rebuild.
        Arc::get_mut(&mut state).unwrap().started_at = Instant::now();
        let mut job = job();
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        job.enqueued_at = at;
        state.queue.lock().await.push_back(job);
        assert!(state.fail_unservable_now().await.is_some());
        assert_eq!(state.queue.lock().await[0].enqueued_at, at);
    }
}
