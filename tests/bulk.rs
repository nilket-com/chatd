//! Isolated stores exercise bulk atomicity, owner scopes, migration and delivery boundaries.
use chatd::{
	proto::{BulkAction, BulkSpec, ErrorCode, Kind, SendReq},
	store::Store,
};
use std::path::PathBuf;

struct Fixture {
	store: Store,
	dir: PathBuf,
}
impl Fixture {
	fn new() -> Self {
		let dir = std::env::temp_dir().join(format!("chatd-bulk-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir(&dir).unwrap();
		let mut store = Store::open(&dir.join("db")).unwrap();
		store.migrate(&["alice".into(), "bob".into(), "eve".into()]).unwrap();
		Self { store, dir }
	}
	fn send(&mut self, from: &str, to: &str, conv: &str, kind: Kind, reply: Option<&str>) -> String {
		self.store
			.send(&SendReq {
				from: from.into(),
				to: Some(to.into()),
				conversation: Some(conv.into()),
				kind,
				reply_to: reply.map(str::to_owned),
				idempotency_key: uuid::Uuid::new_v4().to_string(),
				summary: None,
				body: "sample\nbody".into(),
			})
			.unwrap()
			.id
	}
	fn request(&mut self) -> String {
		self.send("alice", "bob", "work", Kind::Request, None)
	}
	fn spec(&mut self, retire: bool) -> BulkSpec {
		self.send("eve", "alice", "boundary", Kind::Info, None);
		BulkSpec {
			actor: if retire { "alice" } else { "bob" }.into(),
			before_seq: self.store.head_seq().unwrap(),
			peer: Some(if retire { "bob" } else { "alice" }.into()),
			conversation: Some("work".into()),
			action: if retire { BulkAction::Retire { reason: "finished work".into() } } else { BulkAction::Ack { kind: None } },
		}
	}
	fn apply(&mut self, spec: &BulkSpec) -> chatd::proto::BulkResult {
		let preview = self.store.bulk(spec, None).unwrap();
		self.store.bulk(spec, Some(&preview.revision)).unwrap()
	}
}
impl Drop for Fixture {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.dir);
	}
}

#[test]
fn retirement_filters_owned_unanswered_unannotated_requests_and_preserves_history() {
	let mut f = Fixture::new();
	let selected = f.request();
	let other_peer = f.send("alice", "eve", "work", Kind::Request, None);
	f.send("alice", "bob", "other", Kind::Request, None);
	f.send("alice", "bob", "work", Kind::Info, None);
	f.send("bob", "alice", "work", Kind::Request, None);
	let answered = f.request();
	f.send("bob", "alice", "work", Kind::Final, Some(&answered));
	let annotated = f.request();
	f.store.annotate_work("alice", &annotated, None, "rehearsal").unwrap();
	let spec = f.spec(true);
	let p = f.store.bulk(&spec, None).unwrap();
	assert_eq!(p.count, 1);
	assert_eq!(p.sample[0].id, selected);
	let mut broad = spec.clone();
	broad.peer = None;
	assert_eq!(f.store.bulk(&broad, None).unwrap().count, 2);
	broad.conversation = None;
	assert_eq!(f.store.bulk(&broad, None).unwrap().count, 3);
	let historical = f.store.status(None, None, 200, None, None).unwrap();
	let result = f.apply(&spec);
	assert_eq!(result.count, 1);
	let message = f.store.receive("alice", &selected).unwrap();
	assert!(message.received_at_ms.is_none() && message.answered_by.is_none());
	assert!(f.store.delivery_evidence(&message).unwrap().retirement.unwrap().retired);
	let current = f.store.current(None, None, 200, None, false).unwrap();
	assert!(!current.rows.iter().any(|r| r.message.id == selected));
	let history = f.store.current(None, None, 200, None, true).unwrap();
	let retired = history.rows.iter().find(|r| r.message.id == selected).unwrap();
	assert_eq!(retired.state, "retired");
	assert_eq!(retired.annotation_reason.as_deref(), Some("finished work"));
	assert!(history.rows.iter().any(|r| r.message.id == other_peer && r.state == "unreceived"));
	assert_eq!(f.store.annotate_work("alice", &selected, None, "finished work").unwrap_err().code, ErrorCode::Conflict);
	let after = f.store.status(None, None, 200, None, None).unwrap();
	assert_eq!((historical.unanswered_total, historical.unacknowledged_total), (after.unanswered_total, after.unacknowledged_total));
}

#[test]
fn ack_filters_each_kind_and_includes_annotated_requests_only_in_owners_inbox() {
	let mut f = Fixture::new();
	let request = f.request();
	f.store.annotate_work("alice", &request, None, "probe").unwrap();
	f.send("alice", "bob", "work", Kind::Info, None);
	let reverse = f.send("bob", "alice", "work", Kind::Request, None);
	f.send("alice", "bob", "work", Kind::Progress, Some(&reverse));
	f.send("alice", "bob", "work", Kind::Final, Some(&reverse));
	f.send("eve", "bob", "work", Kind::Info, None);
	f.send("alice", "bob", "other", Kind::Info, None);
	let spec = f.spec(false);
	for kind in Kind::ALL {
		let mut one = spec.clone();
		one.action = BulkAction::Ack { kind: Some(kind) };
		assert_eq!(f.store.bulk(&one, None).unwrap().count, 1);
	}
	let mut broad = spec.clone();
	broad.peer = None;
	assert_eq!(f.store.bulk(&broad, None).unwrap().count, 5);
	broad.conversation = None;
	assert_eq!(f.store.bulk(&broad, None).unwrap().count, 6);
	assert_eq!(f.apply(&spec).count, 4);
	assert!(f.store.receive("bob", &request).unwrap().received_at_ms.is_some());
	assert!(f.store.receive("bob", &request).unwrap().answered_by.is_none());
	assert_eq!(f.store.unacknowledged_count("bob", None).unwrap(), 2);
}

#[test]
fn revisions_invalidate_only_when_the_operations_set_changes() {
	for retire in [true, false] {
		for change in ["ack", "progress", "final", "annotation"] {
			let mut f = Fixture::new();
			let id = f.request();
			let spec = f.spec(retire);
			let revision = f.store.bulk(&spec, None).unwrap().revision;
			match change {
				"ack" => {
					f.store.ack("bob", std::slice::from_ref(&id)).unwrap();
				}
				"annotation" => f.store.annotate_work("alice", &id, None, "probe").unwrap(),
				"progress" => {
					f.send("bob", "alice", "work", Kind::Progress, Some(&id));
				}
				"final" => {
					f.send("bob", "alice", "work", Kind::Final, Some(&id));
				}
				_ => unreachable!(),
			}
			let changed = if retire { matches!(change, "final" | "annotation") } else { change != "annotation" };
			let head = f.store.head_seq().unwrap();
			let result = f.store.bulk(&spec, Some(&revision));
			if changed {
				assert_eq!(result.unwrap_err().code, ErrorCode::Conflict);
				assert_eq!(head, f.store.head_seq().unwrap());
			} else {
				assert_eq!(result.unwrap().count, 1);
			}
		}
	}
}

#[test]
fn new_arrivals_do_not_change_revision_and_empty_retries_remain_noops() {
	for retire in [true, false] {
		let mut f = Fixture::new();
		f.request();
		let spec = f.spec(retire);
		let p = f.store.bulk(&spec, None).unwrap();
		let later = f.request();
		assert_eq!(f.store.bulk(&spec, None).unwrap().revision, p.revision);
		assert_eq!(f.store.bulk(&spec, Some(&p.revision)).unwrap().count, 1);
		assert_eq!(f.store.bulk(&spec, Some(&p.revision)).unwrap_err().code, ErrorCode::Conflict);
		let empty = f.store.bulk(&spec, None).unwrap();
		assert_eq!(empty.count, 0);
		let head = f.store.head_seq().unwrap();
		for _ in 0..2 {
			assert_eq!(f.store.bulk(&spec, Some(&empty.revision)).unwrap().count, 0);
		}
		assert_eq!(head, f.store.head_seq().unwrap());
		let message = f.store.receive("bob", &later).unwrap();
		assert!(message.received_at_ms.is_none());
		assert!(f.store.delivery_evidence(&message).unwrap().retirement.is_none());
	}
}

#[test]
fn bounds_and_action_parameters_are_validated_and_bound_to_revision() {
	let mut f = Fixture::new();
	f.request();
	let spec = f.spec(true);
	let p = f.store.bulk(&spec, None).unwrap();
	let mut changed = spec.clone();
	changed.action = BulkAction::Retire { reason: "different reason".into() };
	assert_eq!(f.store.bulk(&changed, Some(&p.revision)).unwrap_err().code, ErrorCode::Conflict);
	changed = spec.clone();
	changed.peer = None; // same IDs, different reviewed scope
	assert_eq!(f.store.bulk(&changed, Some(&p.revision)).unwrap_err().code, ErrorCode::Conflict);
	for bound in [-1, spec.before_seq + 1] {
		changed = spec.clone();
		changed.before_seq = bound;
		assert_eq!(f.store.bulk(&changed, None).unwrap_err().code, ErrorCode::BadRequest);
	}
	changed = spec.clone();
	changed.before_seq = 0;
	assert_eq!(f.apply(&changed).count, 0);
	for reason in [" ".into(), "x".repeat(1025)] {
		changed = spec.clone();
		changed.action = BulkAction::Retire { reason };
		assert_eq!(f.store.bulk(&changed, None).unwrap_err().code, ErrorCode::BadRequest);
	}
}

#[test]
fn large_sets_have_bounded_responses_and_single_atomic_apply() {
	for retire in [true, false] {
		let mut f = Fixture::new();
		for _ in 0..450 {
			f.request();
		}
		let spec = f.spec(retire);
		let result = f.apply(&spec);
		assert_eq!(result.count, 450);
		assert_eq!(result.sample.len(), 10);
		assert!(serde_json::to_vec(&result).unwrap().len() < 8192);
		assert_eq!(f.store.bulk(&spec, None).unwrap().count, 0);
	}
}

#[test]
fn an_injected_failure_rolls_back_changes_and_every_audit_event() {
	for retire in [true, false] {
		let mut f = Fixture::new();
		let first = f.request();
		let last = f.request();
		let spec = f.spec(retire);
		let table = if retire { "work_annotations" } else { "acks" };
		f.store
			.conn()
			.execute_batch(&format!(
				"CREATE TRIGGER fail_bulk BEFORE INSERT ON {table} WHEN NEW.message_id='{last}' BEGIN SELECT RAISE(ABORT,'injected'); END;"
			))
			.unwrap();
		let p = f.store.bulk(&spec, None).unwrap();
		let head = f.store.head_seq().unwrap();
		assert_eq!(f.store.bulk(&spec, Some(&p.revision)).unwrap_err().code, ErrorCode::Storage);
		assert_eq!(head, f.store.head_seq().unwrap());
		assert_eq!(f.store.bulk(&spec, None).unwrap().revision, p.revision);
		let message = f.store.receive("bob", &first).unwrap();
		assert!(message.received_at_ms.is_none());
		assert!(f.store.delivery_evidence(&message).unwrap().retirement.is_none());
	}
}

#[test]
fn concurrent_overlapping_applies_have_one_winner() {
	for retire in [true, false] {
		let mut f = Fixture::new();
		f.request();
		let spec = f.spec(retire);
		let revision = f.store.bulk(&spec, None).unwrap().revision;
		let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
		let mut handles = Vec::new();
		for _ in 0..2 {
			let mut store = Store::open(&f.dir.join("db")).unwrap();
			let spec = spec.clone();
			let revision = revision.clone();
			let barrier = barrier.clone();
			handles.push(std::thread::spawn(move || {
				barrier.wait();
				store.bulk(&spec, Some(&revision))
			}));
		}
		barrier.wait();
		let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
		assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
		assert_eq!(results.iter().filter(|r| r.as_ref().is_err_and(|e| e.code == ErrorCode::Conflict)).count(), 1);
	}
}

#[test]
fn retirement_fences_future_claims_digest_watch_and_redelivery_but_not_claimed_work() {
	let mut f = Fixture::new();
	f.store.endpoint_register("bob", "private", "/nonexistent-private-adapter").unwrap();
	let first = f.request();
	let second = f.request();
	let due = f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap();
	let attempt = f.store.begin_attempt(&due[0]).unwrap().unwrap();
	let spec = f.spec(true);
	assert_eq!(f.apply(&spec).count, 2);
	assert!(f.store.begin_attempt(&due[1]).unwrap().is_none());
	f.store.finish_attempt(attempt, "failed", Some(1), Some("late outcome"), &[0], &[]).unwrap();
	assert!(f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap().is_empty());
	assert_eq!(f.store.queued_for_wake("bob", &[], None, 200).unwrap().1, 0);
	assert_eq!(f.store.unacknowledged_count("bob", None).unwrap(), 0);
	assert!(f.store.unacknowledged_page("bob", None, 0, 200).unwrap().is_empty());
	assert!(!f.store.events_after("bob", None, 0, 200).unwrap().iter().any(|e| e.kind == "message_stored"));
	for id in [first, second] {
		assert_eq!(f.store.redeliver(&id).unwrap_err().code, ErrorCode::Conflict);
	}
}

#[test]
fn attention_counts_exclusions_once_and_preserves_historical_totals() {
	let mut f = Fixture::new();
	f.store.endpoint_register("bob", "private", "/nonexistent-private-adapter").unwrap();
	let a = f.request();
	let b = f.request();
	let c = f.request();
	let d = f.request();
	for delivery in f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap() {
		let attempt = f.store.begin_attempt(&delivery).unwrap().unwrap();
		f.store.finish_attempt(attempt, "failed", Some(1), None, &[0], &[]).unwrap();
	}
	f.store.ack("bob", &[a, b.clone()]).unwrap();
	f.store.annotate_work("alice", &b, None, "probe").unwrap();
	// Retire c only by the exclusive bound, skipping earlier a via a final and b via annotation.
	let a_id = f.store.status(None, None, 200, None, None).unwrap().unanswered.iter().find(|m| m.received && m.id != b).unwrap().id.clone();
	f.send("bob", "alice", "work", Kind::Final, Some(&a_id));
	let mut spec = f.spec(true);
	spec.before_seq = f.store.receive("alice", &d).unwrap().seq;
	assert_eq!(f.apply(&spec).count, 1);
	assert!(f.store.delivery_evidence(&f.store.receive("alice", &c).unwrap()).unwrap().retirement.unwrap().retired);
	let status = f.store.status(Some("bob"), Some("work"), 1, None, None).unwrap();
	assert_eq!(status.uncertain_or_failed_total, 1);
	assert_eq!(status.attention_omitted_total, 3);
	assert_eq!(status.uncertain_or_failed_submissions[0].message_id, d);
	assert_eq!(status.unanswered_total, 3);
	assert_eq!(f.store.status(None, Some("absent"), 1, None, None).unwrap().attention_omitted_total, 0);
}

fn downgrade_to_two(store: &mut Store) {
	store
		.conn()
		.execute_batch("ALTER TABLE work_annotations DROP COLUMN retired; UPDATE meta SET value='2' WHERE key='schema_version';")
		.unwrap();
}

#[test]
fn populated_schema_two_and_one_upgrade_without_losing_evidence() {
	for version in [1, 2] {
		let mut f = Fixture::new();
		let old = f.request();
		let new = f.request();
		let rehearsal = f.request();
		let pending = f.request();
		f.store.annotate_work("alice", &old, Some(&new), "replacement").unwrap();
		f.store.annotate_work("alice", &rehearsal, None, "probe").unwrap();
		f.store.ack("bob", std::slice::from_ref(&new)).unwrap();
		f.store.endpoint_register("bob", "private", "/nonexistent-private-adapter").unwrap();
		let delivery = f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap().remove(0);
		let attempt = f.store.begin_attempt(&delivery).unwrap().unwrap();
		f.store.finish_attempt(attempt, "failed", Some(1), None, &[0], &[]).unwrap();
		downgrade_to_two(&mut f.store);
		if version == 1 {
			f.store.conn().execute_batch("DROP TABLE work_annotations; UPDATE meta SET value='1' WHERE key='schema_version';").unwrap();
		}
		let copy = f.dir.join("copy.db");
		f.store.backup_to(&copy).unwrap();
		let mut migrated = Store::open(&copy).unwrap();
		migrated.migrate(&["alice".into(), "bob".into(), "eve".into()]).unwrap();
		assert!(migrated.receive("bob", &new).unwrap().received_at_ms.is_some());
		assert_eq!(migrated.delivery_evidence(&migrated.receive("alice", &pending).unwrap()).unwrap().attempts_total, 1);
		if version == 2 {
			let history = migrated.current(None, None, 200, None, true).unwrap();
			assert!(history.rows.iter().any(|r| r.message.id == old && r.state == "superseded"));
			assert!(history.rows.iter().any(|r| r.message.id == rehearsal && r.state == "rehearsal"));
		}
		assert_eq!(
			migrated.conn().query_row("SELECT value FROM meta WHERE key='schema_version'", [], |r| r.get::<_, String>(0)).unwrap(),
			"3"
		);
	}
}

#[test]
fn restoring_schema_two_changes_identity_and_rejects_old_revision() {
	let mut f = Fixture::new();
	f.request();
	let spec = f.spec(true);
	let preview = f.store.bulk(&spec, None).unwrap();
	let old_id = f.store.store_id().unwrap();
	downgrade_to_two(&mut f.store);
	let backup = f.dir.join("backup.db");
	f.store.backup_to(&backup).unwrap();
	let state = f.dir.join("restored");
	let socket = f.dir.join("restored.sock");
	let (new_id, _) = chatd::server::restore(&state, &socket, &backup).unwrap();
	assert_ne!(old_id, new_id);
	let mut restored = Store::open(&state.join("chat.db")).unwrap();
	restored.migrate(&["alice".into(), "bob".into(), "eve".into()]).unwrap();
	assert_eq!(restored.bulk(&spec, Some(&preview.revision)).unwrap_err().code, ErrorCode::Conflict);
	assert_eq!(restored.bulk(&spec, None).unwrap().count, 1);
}
