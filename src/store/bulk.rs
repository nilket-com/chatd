//! Bounded selection and atomic compare-and-apply for owner-scoped backlog operations.
use super::*;
use crate::proto::{BulkAction, BulkResult, BulkSample, BulkSpec};
use sha2::{Digest, Sha256};

const SAMPLE_SIZE: usize = 10;

fn page(conn: &Connection, spec: &BulkSpec, after: i64) -> Result<Vec<BulkSample>> {
	let (owner, peer, eligible, kind) = match &spec.action {
		BulkAction::Retire { .. } => (
			"sender",
			"recipient",
			"AND m.kind='request' AND NOT EXISTS(SELECT 1 FROM work_annotations w WHERE w.message_id=m.id) AND NOT EXISTS(SELECT 1 FROM messages r WHERE r.reply_to=m.id AND r.kind='final')",
			None,
		),
		BulkAction::Ack { kind } => (
			"recipient",
			"sender",
			"AND NOT EXISTS(SELECT 1 FROM acks a WHERE a.message_id=m.id AND a.reader=m.recipient)",
			kind.map(|k| k.as_str()),
		),
	};
	let mut stmt = conn.prepare(&format!(
		"SELECT m.id,m.seq,m.sender,m.recipient,m.conversation,m.summary FROM messages m
         WHERE m.{owner}=?1 AND m.seq<?2 AND (?3 IS NULL OR m.{peer}=?3)
         AND (?4 IS NULL OR m.conversation=?4) AND (?5 IS NULL OR m.kind=?5)
         AND m.seq>?6 {eligible} ORDER BY m.seq LIMIT ?7"
	))?;
	Ok(stmt
		.query_map(params![spec.actor, spec.before_seq, spec.peer, spec.conversation, kind, after, MAX_PAGE], |r| {
			Ok(BulkSample {
				id: r.get(0)?,
				seq: r.get(1)?,
				sender: r.get(2)?,
				recipient: r.get(3)?,
				conversation: r.get(4)?,
				summary: r.get(5)?,
			})
		})?
		.collect::<std::result::Result<_, _>>()?)
}

impl Store {
	/// Hash and mutate within one transaction. Pages bound RAM, not atomicity: there is one commit.
	pub fn bulk(&mut self, spec: &BulkSpec, revision: Option<&str>) -> Result<BulkResult> {
		self.require_participant(&spec.actor)?;
		if spec.actor == SYSTEM_SENDER {
			return Err(StoreError::new(ErrorCode::BadRequest, "reserved actor"));
		}
		if let Some(peer) = &spec.peer {
			self.require_participant(peer)?;
		}
		if let Some(conversation) = &spec.conversation
			&& !valid_token(conversation, MAX_CONVERSATION_LEN)
		{
			return Err(StoreError::new(ErrorCode::BadRequest, "invalid conversation filter"));
		}
		if let BulkAction::Retire { reason } = &spec.action
			&& (reason.trim().is_empty() || reason.len() > 1024)
		{
			return Err(StoreError::new(ErrorCode::BadRequest, "reason must contain 1-1024 bytes"));
		}
		let tx = self.conn.transaction_with_behavior(if revision.is_some() {
			TransactionBehavior::Immediate
		} else {
			TransactionBehavior::Deferred
		})?;
		let head: i64 = tx.query_row("SELECT coalesce(max(seq),0) FROM events", [], |r| r.get(0))?;
		if spec.before_seq < 0 || spec.before_seq > head {
			return Err(StoreError::new(ErrorCode::BadRequest, format!("before-seq must be between 0 and head {head} (exclusive bound)")));
		}
		let store_id: String = tx.query_row("SELECT value FROM meta WHERE key='store_id'", [], |r| r.get(0))?;
		let generation: String = tx.query_row("SELECT value FROM meta WHERE key='generation'", [], |r| r.get(0))?;
		let mut hash = Sha256::new();
		// JSON tuple fixes field order and escapes strings; length framing separates every ID.
		let header = serde_json::to_vec(&("chatd-bulk-v1", store_id, generation, spec)).expect("serializable bulk selection");
		hash.update((header.len() as u64).to_be_bytes());
		hash.update(header);
		let mut result = BulkResult {
			selection: spec.clone(),
			revision: String::new(),
			count: 0,
			oldest_seq: None,
			newest_seq: None,
			sample: Vec::new(),
			applied: revision.is_some(),
		};
		let mut after = 0;
		loop {
			let rows = page(&tx, spec, after)?;
			if rows.is_empty() {
				break;
			}
			for mut row in rows {
				after = row.seq;
				hash.update((row.id.len() as u64).to_be_bytes());
				hash.update(row.id.as_bytes());
				result.count += 1;
				result.oldest_seq.get_or_insert(row.seq);
				result.newest_seq = Some(row.seq);
				if result.sample.len() < SAMPLE_SIZE {
					row.summary = text::sanitize_line(&row.summary, 160);
					result.sample.push(row);
				}
			}
		}
		result.revision = format!("{:x}", hash.finalize());
		if let Some(revision) = revision {
			if revision != result.revision {
				return Err(StoreError::new(
					ErrorCode::Conflict,
					"matched set or operation changed; preview again (a previous apply may have committed)",
				));
			}
			let now = now_ms();
			after = 0;
			loop {
				let rows = page(&tx, spec, after)?;
				if rows.is_empty() {
					break;
				}
				for row in rows {
					after = row.seq;
					let (event, detail, audiences) = match &spec.action {
						BulkAction::Retire { reason } => {
							tx.execute(
								"INSERT INTO work_annotations(message_id,by_request,rehearsal,actor,reason,at_ms,retired) VALUES(?1,NULL,0,?2,?3,?4,1)",
								params![row.id, spec.actor, reason, now],
							)?;
							(
								"retired",
								json!({"actor":spec.actor,"reason":reason,"revision":revision,"before_seq":spec.before_seq}),
								BTreeSet::from([row.sender.as_str(), row.recipient.as_str()]),
							)
						}
						BulkAction::Ack { .. } => {
							tx.execute("INSERT INTO acks(reader,message_id,at_ms) VALUES(?1,?2,?3)", params![spec.actor, row.id, now])?;
							(
								"received",
								json!({"by":spec.actor,"via":"bulk_ack","revision":revision,"before_seq":spec.before_seq}),
								BTreeSet::from([row.sender.as_str()]),
							)
						}
					};
					for audience in audiences {
						tx.execute(
							"INSERT INTO events(at_ms,kind,audience,message_id,conversation,detail) VALUES(?1,?2,?3,?4,?5,?6)",
							params![now, event, audience, row.id, row.conversation, detail.to_string()],
						)?;
					}
				}
			}
		}
		tx.commit()?;
		Ok(result)
	}
}
