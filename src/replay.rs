//! Async replay job execution and status tracking.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Lifecycle state of the single replay job slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Idle,
    Running,
    Completed,
    Stopped,
}

/// Snapshot of the current (or most recent) replay job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayJobStatus {
    pub state: JobState,
    pub from_block: u64,
    pub to_block: u64,
    /// Resume position -- same semantics as the old `next_block`.
    /// Running: next unprocessed block. Stopped: block to resume from.
    /// Completed: to_block.saturating_add(1).
    pub current_block: u64,
    pub transactions_published: u64,
    pub events_total: u64,
    pub duplicates: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: u64,
    pub stopped_reason: Option<String>,
}

impl Default for ReplayJobStatus {
    fn default() -> Self {
        Self {
            state: JobState::Idle,
            from_block: 0,
            to_block: 0,
            current_block: 0,
            transactions_published: 0,
            events_total: 0,
            duplicates: 0,
            started_at: None,
            finished_at: None,
            duration_ms: 0,
            stopped_reason: None,
        }
    }
}

impl ReplayJobStatus {
    /// Fresh `Running` status; used by the POST handler before spawning the job.
    pub fn running(from_block: u64, to_block: u64) -> Self {
        Self {
            state: JobState::Running,
            from_block,
            to_block,
            current_block: from_block,
            started_at: Some(Utc::now()),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_status_json_shape_is_idle_with_nulls_and_zeros() {
        let status = ReplayJobStatus::default();
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "state": "Idle",
                "from_block": 0,
                "to_block": 0,
                "current_block": 0,
                "transactions_published": 0,
                "events_total": 0,
                "duplicates": 0,
                "started_at": null,
                "finished_at": null,
                "duration_ms": 0,
                "stopped_reason": null,
            })
        );
    }

    #[test]
    fn running_status_json_shape_has_started_at_and_null_terminal_fields() {
        let status = ReplayJobStatus::running(100, 500);
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "Running");
        assert_eq!(value["from_block"], 100);
        assert_eq!(value["to_block"], 500);
        assert_eq!(value["current_block"], 100);
        assert!(value["finished_at"].is_null());
        assert!(value["stopped_reason"].is_null());
        let started_at = value["started_at"].as_str().unwrap();
        assert!(DateTime::parse_from_rfc3339(started_at).is_ok());
    }

    #[test]
    fn completed_status_json_shape_has_finished_at_and_current_block_past_to() {
        let status = ReplayJobStatus {
            state: JobState::Completed,
            from_block: 10,
            to_block: 20,
            current_block: 21,
            transactions_published: 5,
            events_total: 8,
            duplicates: 1,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            duration_ms: 1234,
            stopped_reason: None,
        };
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "Completed");
        assert_eq!(value["current_block"], 21);
        assert!(value["stopped_reason"].is_null());
        let finished_at = value["finished_at"].as_str().unwrap();
        assert!(DateTime::parse_from_rfc3339(finished_at).is_ok());
    }

    #[test]
    fn stopped_status_json_shape_has_stopped_reason_and_resume_block() {
        let status = ReplayJobStatus {
            state: JobState::Stopped,
            from_block: 10,
            to_block: 500,
            current_block: 42,
            transactions_published: 3,
            events_total: 4,
            duplicates: 0,
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
            duration_ms: 500,
            stopped_reason: Some("rpc error".to_string()),
        };
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], "Stopped");
        assert_eq!(value["stopped_reason"], "rpc error");
        assert_eq!(value["current_block"], 42);
    }

    #[test]
    fn default_is_idle_with_zeroed_fields() {
        let status = ReplayJobStatus::default();
        assert_eq!(status.state, JobState::Idle);
        assert_eq!(status.from_block, 0);
        assert_eq!(status.to_block, 0);
        assert_eq!(status.current_block, 0);
        assert_eq!(status.transactions_published, 0);
        assert_eq!(status.events_total, 0);
        assert_eq!(status.duplicates, 0);
        assert!(status.started_at.is_none());
        assert!(status.finished_at.is_none());
        assert_eq!(status.duration_ms, 0);
        assert!(status.stopped_reason.is_none());
    }

    #[test]
    fn running_sets_state_and_current_block() {
        let status = ReplayJobStatus::running(100, 500);
        assert_eq!(status.state, JobState::Running);
        assert_eq!(status.from_block, 100);
        assert_eq!(status.to_block, 500);
        assert_eq!(status.current_block, 100);
        assert!(status.started_at.is_some());
    }
}
