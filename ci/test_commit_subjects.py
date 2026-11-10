"""Signed temporary histories exercise policy without rewriting project history (from rnx)."""
from dataclasses import replace
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from commit_subjects import evaluate, receipt, Policy, HistoryError, IDENTITY


class Fixture:
	def setUp(self):
		self.temp = tempfile.TemporaryDirectory()
		self.addCleanup(self.temp.cleanup)
		self.root = Path(self.temp.name)
		self.repo = self.root / "repo"
		self.repo.mkdir()
		self.env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
		self.env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
		# evaluate() also invokes Git: isolate its environment, not only fixture writes.
		self.environment = patch.dict(os.environ, self.env, clear=True)
		self.environment.start()
		self.addCleanup(self.environment.stop)
		key = self.root / "key"
		subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True, timeout=30)
		allowed = self.root / "allowed"
		allowed.write_text(IDENTITY[1] + " " + key.with_suffix(".pub").read_text())
		self.git("init", "-q", "-b", "main")
		for name, value in [("user.name", IDENTITY[0]), ("user.email", IDENTITY[1]),
			("gpg.format", "ssh"), ("user.signingkey", str(key)), ("gpg.ssh.allowedSignersFile", str(allowed)), ("commit.gpgsign", "true")]:
			self.git("config", name, value)
		for number in range(1, 8):
			self.write(f"plans/{number:04d}_legacy.md", "Grandfathered\n")
		self.base = self.commit("old unregulated history")
		self.policy = Policy(cutoff=self.base)

	def git(self, *args):
		p = subprocess.run(["git", *args], cwd=self.repo, env=self.env, text=True, capture_output=True, timeout=60)
		if p.returncode:
			raise AssertionError(p.stderr)
		return p.stdout.strip()

	def write(self, name, text):
		p = self.repo / name
		p.parent.mkdir(parents=True, exist_ok=True)
		p.write_text(text)

	def commit(self, message, *options):
		self.git("add", "-A")
		self.git("commit", "--allow-empty", *options, "-m", message)
		return self.git("rev-parse", "HEAD")

	def plan(self, number="0008", status="Status: Accepted", extra=False):
		self.write(f"plans/adr/{number}_test.md", f"# Decision\n\n{status}\n")
		self.write(f"plans/impl/{number}_test.md", "# Implementation\n")
		if extra:
			self.write("unrelated", "bad")
		return self.commit(f"plan: adr {number} test")

	def check(self, policy=None):
		return evaluate(self.repo, policy or self.policy)

	def reject(self, text, policy=None):
		with self.assertRaisesRegex(HistoryError, text):
			self.check(policy)


class History(Fixture, unittest.TestCase):
	def test_valid_history_and_pending_plan_from_flat_cutoff(self):
		self.assertEqual(self.check()["status"], "PASS")
		self.plan()
		self.assertEqual(self.check()["history"]["pending"], "0008")
		self.write("tool", "implementation")
		self.commit("impl: adr 0008 checker")
		self.assertIsNone(self.check()["history"]["pending"])
		self.plan("0009")
		self.commit("impl: adr 0009 next")
		self.assertEqual(self.check()["status"], "PASS")

	def test_unsigned(self):
		self.plan()
		self.commit("impl: adr 0008 unsigned", "--no-gpg-sign")
		self.reject("signature|git command failed")

	def test_author_identity(self):
		self.plan()
		self.commit("impl: adr 0008 wrong author", "--author=Wrong Person <wrong@example.org>")
		self.reject("identity")

	def test_committer_identity(self):
		self.plan()
		self.git("config", "user.name", "Wrong Person")
		self.commit("impl: adr 0008 wrong committer", f"--author={IDENTITY[0]} <{IDENTITY[1]}>")
		self.reject("identity")

	def test_body_and_trailers(self):
		for body in ["body", "Co-authored-by: Somebody <somebody@example.org>"]:
			with self.subTest(body=body):
				self.commit("plan: adr 0008 test\n\n" + body)
				self.reject("body")
				self.git("reset", "--hard", self.base)

	def test_subject_forms(self):
		for subject in ["Plan checker", "plan: adr 008 short", "plan: adr 0008 Upper", "plan: adr 0008 scope(x)", "plan: adr 0008 nonascii é", "plan: adr 0008"]:
			with self.subTest(subject=subject):
				self.commit(subject)
				self.reject("subject")
				self.git("reset", "--hard", self.base)

	def test_rnx_label_list_is_not_a_chatd_rule(self):
		self.plan()
		self.commit("impl: adr 0008 cut a1")
		self.assertEqual(self.check()["status"], "PASS")

	def test_implementation_before_plan(self):
		self.commit("impl: adr 0008 checker")
		self.reject("earlier first-parent plan")

	def test_duplicate_plan(self):
		self.plan()
		self.plan()
		self.reject("duplicate plan")

	def test_duplicate_implementation(self):
		self.plan()
		self.commit("impl: adr 0008 first")
		self.commit("impl: adr 0008 second")
		self.reject("implementation limit")

	def test_plan_scope(self):
		self.plan(extra=True)
		self.reject("exactly its paired")

	def test_status_missing_ambiguous_late(self):
		for state in ["", "Status: Accepted\nStatus: Accepted", "\n" * 10 + "Status: Accepted", "Status: "]:
			with self.subTest(state=state):
				self.plan(status=state)
				self.reject("Status")
				self.git("reset", "--hard", self.base)

	def test_missing_and_duplicate_documents(self):
		for category in ["adr", "impl"]:
			for duplicate in [False, True]:
				with self.subTest(category=category, duplicate=duplicate):
					self.plan()
					if duplicate:
						self.write(f"plans/{category}/0008_other.md", "Status: Accepted\n")
					else:
						(self.repo / f"plans/{category}/0008_test.md").unlink()
					self.commit("plan: adr 0008 test", "--amend")
					self.reject("exactly one " + category)
					self.git("reset", "--hard", self.base)

	def test_underscore_snake_paths_and_matching_pair(self):
		for bad in ["0008-test.md", "0008_Upper.md", "0008_not-snake.md", "0008_other.md"]:
			with self.subTest(path=bad):
				self.plan()
				(self.repo / "plans/impl/0008_test.md").rename(self.repo / "plans/impl" / bad)
				self.commit("plan: adr 0008 test", "--amend")
				self.reject("impl document|paired document")
				self.git("reset", "--hard", self.base)

	def test_other_record_edits_including_flat_legacy(self):
		for path in ["plans/0001_legacy.md", "plans/adr/0007_other.md", "plans/impl/0009_future.md"]:
			with self.subTest(path=path):
				self.plan()
				self.write(path, "Status: Accepted\nchanged")
				self.commit("impl: adr 0008 checker")
				self.reject("another record")
				self.git("reset", "--hard", self.base)

	def test_oversize(self):
		self.plan(status="Status: Accepted\n" + "x\n" * 401)
		self.reject("400 lines")

	def test_unknown_and_malformed_cutoff(self):
		self.reject("git command failed|valid object", replace(self.policy, cutoff="1" * 40))
		self.reject("invalid pinned cutoff", replace(self.policy, cutoff="HEAD"))

	def test_cutoff_wrong_chain(self):
		self.git("checkout", "--orphan", "other")
		self.git("rm", "-rf", ".")
		wrong = self.commit("other root")
		self.git("checkout", "main")
		self.reject("ancestor|git command failed", replace(self.policy, cutoff=wrong))

	def test_cutoff_only_on_second_parent(self):
		self.git("checkout", "--orphan", "other")
		self.git("rm", "-rf", ".")
		other = self.commit("other root")
		tree = self.git("rev-parse", "HEAD^{tree}")
		merge = self.git("commit-tree", "-S", tree, "-p", other, "-p", self.base, "-m", "impl: adr 0008 merge")
		self.git("reset", "--hard", merge)
		self.reject("cutoff not on complete first-parent")

	def test_future_merge(self):
		self.plan()
		parent = self.git("rev-parse", "HEAD")
		self.git("checkout", "-b", "side", self.base)
		self.write("smuggled", "code")
		side = self.commit("unvalidated side commit")
		self.git("checkout", "main")
		tree = self.git("rev-parse", "HEAD^{tree}")
		merge = self.git("commit-tree", "-S", tree, "-p", parent, "-p", side, "-m", "impl: adr 0008 checker")
		self.git("reset", "--hard", merge)
		self.reject("unreviewed merge")

	def test_replace_refs(self):
		self.plan()
		self.git("replace", self.base, self.git("rev-parse", "HEAD"))
		self.reject("replacement history")

	def test_grafts(self):
		self.write(".git/info/grafts", self.base + "\n")
		self.reject("grafted")

	def test_shallow(self):
		clone = self.root / "shallow"
		self.git("clone", "--depth=1", self.repo.as_uri(), str(clone))
		with self.assertRaisesRegex(HistoryError, "complete history"):
			evaluate(clone, self.policy)

	def test_dirty_tree_is_not_committed_evidence(self):
		self.write("plans/adr/0008_test.md", "Status: Accepted\n")
		self.commit("plan: adr 0008 test")
		self.write("plans/impl/0008_test.md", "uncommitted fix\n")
		self.reject("impl document")


class Sequence(Fixture, unittest.TestCase):
	def test_interleaved(self):
		self.plan()
		bad = self.plan("0009")
		result = json.loads(json.dumps(receipt(self.repo, self.policy)))
		self.assertEqual(result["status"], "FAIL")
		self.assertEqual(result["commit"], bad)
		self.assertEqual(result["rule"], "sequence.implementation")

	def test_numbers(self):
		for number in ("0009", "0010", "0007"):
			with self.subTest(number=number):
				self.plan(number)
				self.reject("expected next record 0008|predates adoption")
				self.git("reset", "--hard", self.base)

	def test_wrong_impl(self):
		self.plan()
		self.commit("impl: adr 0008 first")
		self.plan("0009")
		self.commit("impl: adr 0010 wrong record")
		self.reject("earlier first-parent plan|expected immediate")

	def test_withdrawal(self):
		self.plan()
		self.write("plans/adr/0008_test.md", "Status: Withdrawn\n")
		self.commit("impl: adr 0008 withdraw")
		self.plan("0009")
		self.assertEqual(self.check()["status"], "PASS")

	def test_withdrawal_product(self):
		self.plan()
		self.write("plans/adr/0008_test.md", "Status: Withdrawn\n")
		self.write("product", "not allowed")
		self.commit("impl: adr 0008 withdraw")
		self.reject("withdrawal must")

	def test_withdrawn_plan(self):
		self.plan(status="Status: Withdrawn")
		self.reject("withdrawal must")

	def test_cli_json_failure_receipt(self):
		# Production uses its pinned cutoff, absent from this fixture: it must fail as JSON, not traceback.
		script = Path(__file__).with_name("commit_subjects.py")
		out = subprocess.run([sys.executable, "-B", str(script), "--repo", str(self.repo)], capture_output=True, text=True, timeout=60)
		self.assertEqual(out.returncode, 1)
		self.assertEqual(json.loads(out.stdout)["status"], "FAIL")
		self.assertNotIn("Traceback", out.stderr)


if __name__ == "__main__":
	unittest.main()
