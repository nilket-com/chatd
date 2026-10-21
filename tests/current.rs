use chatd::{
    proto::{Kind, SendReq},
    store::Store,
};

struct Fixture {
    store: Store,
    dir: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("chatd-current-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let mut store = Store::open(&dir.join("db")).unwrap();
        store.migrate(&["alice".into(), "bob".into(), "eve".into()]).unwrap();
        Self { store, dir }
    }
    fn send(&mut self, from: &str, to: &str, conversation: &str, kind: Kind, reply_to: Option<&str>) -> String {
        self.store
            .send(&SendReq {
                from: from.into(),
                to: Some(to.into()),
                conversation: Some(conversation.into()),
                kind,
                reply_to: reply_to.map(str::to_owned),
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                summary: Some("work".into()),
                body: "details".into(),
            })
            .unwrap()
            .id
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn receipt_is_not_completion_and_progress_is_linked() {
    let mut f = Fixture::new();
    let old = f.send("alice", "bob", "work", Kind::Request, None);
    f.store.ack("bob", std::slice::from_ref(&old)).unwrap();
    let unrelated = f.send("bob", "alice", "work", Kind::Request, None);
    let p = f.send("bob", "alice", "work", Kind::Progress, Some(&old));
    let c = f.store.current(Some("alice"), None, 1, None, false).unwrap();
    assert_eq!(c.total, 2);
    assert_eq!(c.rows[0].state, "received-pending");
    assert_eq!(c.rows[0].latest_progress.as_ref().unwrap().id, p);
    let next = f.store.current(Some("alice"), None, 1, c.next_after, false).unwrap();
    assert_eq!(next.rows[0].message.id, unrelated);
    assert!(next.next_after.is_none());
    f.send("bob", "alice", "work", Kind::Final, Some(&old));
    assert_eq!(f.store.current(None, None, 20, None, false).unwrap().total, 1);
    let history = f.store.current(None, None, 20, None, true).unwrap();
    assert_eq!(history.rows[0].state, "final");
    // Historical status retains its existing shape and semantics.
    let legacy = serde_json::to_value(f.store.status(None, None, 20, None, None).unwrap()).unwrap();
    assert!(legacy.get("unanswered").is_some());
    assert!(legacy.get("rows").is_none());
}
#[test]
fn explicit_supersession_is_owned_causal_audited_and_durable() {
    let mut f = Fixture::new();
    let a = f.send("alice", "bob", "work", Kind::Request, None);
    let b = f.send("alice", "bob", "work", Kind::Request, None);
    let other = f.send("alice", "bob", "other", Kind::Request, None);
    assert!(f.store.annotate_work("bob", &a, Some(&b), "replacement").is_err());
    assert!(f.store.annotate_work("eve", &a, Some(&b), "replacement").is_err());
    assert!(f.store.annotate_work("alice", &a, Some(&a), "self").is_err());
    assert!(f.store.annotate_work("alice", &b, Some(&a), "backwards/cycle").is_err());
    assert!(f.store.annotate_work("alice", &a, Some(&other), "cross conversation").is_err());
    assert!(f.store.annotate_work("alice", &a, Some(&b), " ").is_err());
    f.store.annotate_work("alice", &a, Some(&b), "replacement").unwrap();
    let head = f.store.head_seq().unwrap();
    f.store.annotate_work("alice", &a, Some(&b), "replacement").unwrap();
    assert_eq!(f.store.head_seq().unwrap(), head);
    assert!(f.store.annotate_work("alice", &a, None, "rewrite").is_err());
    assert!(f.store.receive("bob", &a).unwrap().received_at_ms.is_none());
    assert!(f.store.receive("bob", &a).unwrap().answered_by.is_none());
    assert_eq!(f.store.current(None, Some("work"), 20, None, false).unwrap().total, 1);
    let events = f.store.log(Some("work"), None, 100).unwrap();
    assert_eq!(events.iter().filter(|e| e.kind == "superseded").count(), 2);
    let mut reopened = Store::open(&f.dir.join("db")).unwrap();
    reopened.migrate(&[]).unwrap();
    let h = reopened.current(None, Some("work"), 20, None, true).unwrap();
    let retired = h.rows.iter().find(|r| r.message.id == a).unwrap();
    assert_eq!(retired.state, "superseded");
    assert_eq!(retired.superseded_by.as_deref(), Some(b.as_str()));
    // A supersession is not a fabricated final and does not mutate legacy reporting.
    assert_eq!(reopened.status(None, Some("work"), 20, None, None).unwrap().unanswered_total, 2);
}
#[test]
fn rehearsal_requires_annotation_not_summary_and_migration_preserves_legacy_data() {
    let mut f = Fixture::new();
    let id = f.send("alice", "bob", "rehearsal", Kind::Request, None);
    assert_eq!(f.store.current(None, None, 20, None, false).unwrap().total, 1);
    f.store.annotate_work("alice", &id, None, "synthetic probe").unwrap();
    assert_eq!(f.store.current(None, None, 20, None, false).unwrap().total, 0);
    assert_eq!(f.store.current(None, None, 20, None, true).unwrap().rows[0].state, "rehearsal");
    let conn = rusqlite::Connection::open(f.dir.join("db")).unwrap();
    conn.execute_batch("DROP TABLE work_annotations").unwrap();
    f.store.migrate(&[]).unwrap();
    assert_eq!(f.store.current(None, None, 20, None, false).unwrap().total, 1);
}

#[test]
fn retirement_fences_new_pushes_digests_and_redelivery_but_keeps_attempt_evidence() {
    let mut f = Fixture::new();
    f.store.endpoint_register("bob", "thread-1", "/bin/true").unwrap();
    let a = f.send("alice", "bob", "work", Kind::Request, None);
    let due = f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap();
    assert_eq!(due.len(), 1);
    let b = f.send("alice", "bob", "work", Kind::Request, None);
    f.store.annotate_work("alice", &a, Some(&b), "new work").unwrap();
    // A stale scheduler selection cannot begin after retirement commits.
    assert!(f.store.begin_attempt(&due[0]).unwrap().is_none());
    assert_eq!(f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap()[0].message.id, b);
    let (digest, total) = f.store.queued_for_wake("bob", &[], None, 20).unwrap();
    assert_eq!(total, 1);
    assert_eq!(digest[0].id, b);
    assert!(f.store.redeliver(&a).is_err());
    assert_eq!(f.store.unacknowledged_count("bob", None).unwrap(), 1);
    assert_eq!(f.store.unacknowledged_page("bob", None, 0, 20).unwrap()[0].id, b);
    assert!(
        !f.store
            .events_after("bob", None, 0, 100)
            .unwrap()
            .iter()
            .any(|e| e.kind == "message_stored" && e.message_id.as_deref() == Some(a.as_str()))
    );
    // Retirement during an in-flight attempt does not invent or erase an outcome.
    let due = f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap();
    let attempt = f.store.begin_attempt(&due[0]).unwrap().unwrap();
    f.store.annotate_work("alice", &b, None, "test delivery").unwrap();
    f.store.finish_attempt(attempt, "uncertain", None, Some("timeout"), &[], &[]).unwrap();
    assert!(f.store.due_deliveries(i64::MAX, &|_| vec![Kind::Request]).unwrap().is_empty());
    assert_eq!(f.store.attempts_for(&b).unwrap()[0].state, "uncertain");
    assert_eq!(f.store.status(None, None, 20, None, None).unwrap().uncertain_or_failed_total, 1);
    assert!(f.store.receive("bob", &b).unwrap().received_at_ms.is_none());
}

#[test]
fn schema_one_upgrade_is_atomic_and_preserves_source_messages() {
    let mut f = Fixture::new();
    let id = f.send("alice", "bob", "work", Kind::Request, None);
    f.store.conn().execute_batch("DROP TABLE work_annotations; UPDATE meta SET value='1' WHERE key='schema_version'").unwrap();
    f.store.migrate(&[]).unwrap();
    assert_eq!(f.store.receive("bob", &id).unwrap().body.as_deref(), Some("details"));
    assert_eq!(f.store.current(None, None, 20, None, false).unwrap().total, 1);
    let version: String = f.store.conn().query_row("SELECT value FROM meta WHERE key='schema_version'", [], |r| r.get(0)).unwrap();
    assert_eq!(version, "2");
    // The same guard shipped in schema-1 binaries will reject the upgraded store.
    assert_ne!(version, "1");
}

#[test]
fn current_activity_is_newest_first_and_snapshot_paging_keeps_old_pending() {
    let mut f = Fixture::new();
    let mut ids = Vec::new();
    for _ in 0..65 {
        ids.push(f.send("alice", "bob", "work", Kind::Request, None));
    }
    let progress = f.send("bob", "alice", "work", Kind::Progress, Some(&ids[0]));
    let first = f.store.current(None, None, 10, None, false).unwrap();
    assert_eq!(first.total, 65);
    assert_eq!(first.rows[0].message.id, ids[0]);
    assert_eq!(first.rows[0].latest_progress.as_ref().unwrap().id, progress);
    assert_eq!(first.rows[1].message.id, ids[64]);
    assert!(first.rows.windows(2).all(|w| w[0].activity_seq > w[1].activity_seq));
    let snapshot = first.snapshot_seq;
    // An unseen old request receives progress after page one. Its old activity
    // remains inside this snapshot; it must not jump over the cursor and vanish.
    f.send("bob", "alice", "work", Kind::Progress, Some(&ids[1]));
    let mut seen: Vec<_> = first.rows.iter().map(|r| r.message.id.clone()).collect();
    let mut cursor = first.next_after;
    while let Some(after) = cursor {
        let page = f.store.current(None, None, 10, Some(after), false).unwrap();
        assert_eq!(page.total, 65);
        assert_eq!(page.snapshot_seq, snapshot);
        seen.extend(page.rows.iter().map(|r| r.message.id.clone()));
        cursor = page.next_after;
    }
    assert_eq!(seen.len(), 65);
    let unique: std::collections::BTreeSet<_> = seen.into_iter().collect();
    assert_eq!(unique, ids.iter().cloned().collect());
    let fresh = f.store.current(None, None, 10, None, false).unwrap();
    assert_eq!(fresh.rows[0].message.id, ids[1]);
    assert!(f.store.current(None, None, 10, Some("broken".into()), false).is_err());
    assert!(f.store.current(None, None, 10, Some("1:2".into()), false).is_err());
}
