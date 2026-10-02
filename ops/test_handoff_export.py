#!/usr/bin/env python3
"""Tests for handoff_export.py. Each test runs a real, isolated chatd (temporary state, socket and
config) to build its store, then stops it. Attempt histories are written into the stopped store,
since they test the exporter's reading of them.

CHATD_BIN and CHATD_CTL name the binaries (default: ~/.local/bin/chatd and chatctl).
"""

import fcntl
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
EXPORT = os.path.join(HERE, "handoff_export.py")
BIN = os.environ.get("CHATD_BIN", os.path.expanduser("~/.local/bin/chatd"))
CTL = os.environ.get("CHATD_CTL", os.path.expanduser("~/.local/bin/chatctl"))


def read_json(path):
    with open(path) as f:
        return json.load(f)


def read_text(path):
    with open(path) as f:
        return f.read()


class Store:
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="chatd-export-test-")
        self.state = os.path.join(self.dir, "state")
        os.makedirs(os.path.join(self.dir, "run"))
        with open(os.path.join(self.dir, "config.toml"), "w") as f:
            f.write("heartbeat_ms = 300\n")
        self.env = dict(os.environ, CHATD_STATE_DIR=self.state, CHATD_SOCKET=os.path.join(self.dir, "run", "chatd.sock"),
                        CHATD_CONFIG=os.path.join(self.dir, "config.toml"), CHATD_NO_JOURNAL="1", CHATD_CTL=CTL)
        self.env.pop("NOTIFY_SOCKET", None)
        self.daemon = None
        self.n = 0

    def start(self):
        self.daemon = subprocess.Popen([BIN], env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(200):
            if subprocess.run([CTL, "health"], env=self.env, capture_output=True).returncode == 0:
                return
            time.sleep(0.05)
        raise RuntimeError("chatd did not start")

    def stop(self):
        if self.daemon:
            self.daemon.send_signal(signal.SIGTERM)
            self.daemon.wait(10)
            self.daemon = None

    def ctl(self, *args, stdin=""):
        r = subprocess.run([CTL, "--json", *args], env=self.env, input=stdin, capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        return json.loads(r.stdout) if r.stdout.strip() else None

    def send(self, frm, to, summary, kind="request"):
        self.n += 1
        return self.ctl("send", "--as", frm, "--to", to, "--conversation", "t", "--kind", kind,
                        "--idempotency-key", f"k{self.n}", "--summary", summary, stdin=f"body of {summary}\n")["id"]

    def reply(self, frm, to_message, final=True):
        self.n += 1
        args = ["reply", "--as", frm, "--to-message", to_message, "--idempotency-key", f"k{self.n}"] + (["--final"] if final else [])
        return self.ctl(*args, stdin=f"reply to {to_message}\n")["id"]

    def ack(self, who, mid):
        self.ctl("ack", "--as", who, mid)

    def sql(self, statement, *args):
        c = sqlite3.connect(os.path.join(self.state, "chat.db"))
        try:
            with c:
                c.execute(statement, args)
        finally:
            c.close()

    def attempts(self, mid, states):
        for s in states:
            self.sql("INSERT INTO attempts (message_id, endpoint, generation, state, started_at_ms) VALUES (?, 'codex', 1, ?, 1)", mid, s)

    def export(self, mode, name="out", hook=None):
        out = os.path.join(self.dir, name)
        env = dict(self.env, CHATD_EXPORT_TEST_HOOK=hook) if hook else self.env
        r = subprocess.run([sys.executable, EXPORT, mode, out], env=env, capture_output=True, text=True)
        return r, out

    def export_async(self, mode, name="out", hook=None):
        out = os.path.join(self.dir, name)
        env = dict(self.env, CHATD_EXPORT_TEST_HOOK=hook) if hook else self.env
        return subprocess.Popen([sys.executable, EXPORT, mode, out], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True), out

    def cleanup(self):
        self.stop()
        shutil.rmtree(self.dir, ignore_errors=True)


class ExportTest(unittest.TestCase):
    def setUp(self):
        self.s = Store()
        self.s.start()

    def tearDown(self):
        self.s.cleanup()

    def refused(self, mode="offline"):
        r, out = self.s.export(mode)
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("REFUSED", r.stderr)
        self.assertFalse(os.path.exists(out), "no handoff at the output path")
        self.assertFalse(os.path.exists(out + ".partial"), "no partial export left behind")
        return r

    def handoff(self, mode="offline"):
        r, out = self.s.export(mode)
        self.assertEqual(r.returncode, 0, r.stderr)
        return read_json(os.path.join(out, "handoff.json")), read_text(os.path.join(out, "handoff.md")), out

    # --- refusals -------------------------------------------------------------------------------

    def test_offline_refuses_a_running_daemon(self):
        self.assertIn("is held", self.refused().stderr)

    def test_offline_refuses_a_held_lock_even_if_health_fails(self):
        # Codex's probe: the lock is held, but nothing answers health (a different socket here).
        self.s.stop()
        with open(os.path.join(self.s.state, "chatd.lock"), "a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.s.env["CHATD_SOCKET"] = os.path.join(self.s.dir, "run", "nothing-here.sock")
            self.assertIn("is held", self.refused().stderr)

    def test_offline_refuses_a_missing_store(self):
        self.s.stop()
        os.remove(os.path.join(self.s.state, "chat.db"))
        self.assertIn("does not exist", self.refused().stderr)

    def test_offline_refuses_a_pending_restore(self):
        self.s.stop()
        with open(os.path.join(self.s.state, "RESTORE-IN-PROGRESS"), "w") as f:
            f.write("{}")
        self.assertIn("pending restore", self.refused().stderr)
        os.remove(os.path.join(self.s.state, "RESTORE-IN-PROGRESS"))
        os.makedirs(os.path.join(self.s.state, "restore-candidate"))
        self.assertIn("pending restore", self.refused().stderr)

    def test_a_restore_appearing_at_lock_acquisition_is_refused(self):
        # Codex's interleaving: the marker (or candidate) exists once the lock is held. Every
        # decision is taken under the lock, so the export refuses rather than publishing the old store.
        self.s.stop()
        r, out = self.s.export("offline", hook="after-lock:restore-marker")
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIn("pending restore", r.stderr)
        self.assertFalse(os.path.exists(out) or os.path.exists(out + ".partial"))
        os.remove(os.path.join(self.s.state, "RESTORE-IN-PROGRESS"))
        r, out = self.s.export("offline", hook="after-lock:restore-candidate")
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIn("pending restore", r.stderr)

    def test_a_store_missing_at_lock_acquisition_is_refused(self):
        self.s.stop()
        r, out = self.s.export("offline", hook="after-lock:remove-db")
        self.assertEqual(r.returncode, 1, r.stderr)
        self.assertIn("does not exist", r.stderr)
        self.assertFalse(os.path.exists(out) or os.path.exists(out + ".partial"))

    def test_a_failure_after_publication_keeps_the_output_and_reports_no_success(self):
        self.s.send("claude", "codex", "x")
        self.s.stop()
        r, out = self.s.export("offline", hook="after-rename:fail-fsync")
        self.assertEqual(r.returncode, 3, r.stderr)
        self.assertEqual(r.stdout, "", "no success receipt")
        self.assertIn("published output retained", r.stderr)
        self.assertIn("NOT a successful handoff", r.stderr)
        self.assertNotIn("no handoff written", r.stderr)
        self.assertTrue(os.path.exists(os.path.join(out, "handoff.json")), "the published output is kept")
        self.assertFalse(os.path.exists(out + ".partial"))

    def test_a_preexisting_partial_is_refused_and_preserved(self):
        self.s.stop()
        part = os.path.join(self.s.dir, "out.partial")
        os.makedirs(part)
        with open(os.path.join(part, "KEEP"), "w") as f:
            f.write("another export's evidence")
        r, out = self.s.export("offline")
        self.assertEqual(r.returncode, 1)
        self.assertIn("left untouched", r.stderr)
        self.assertEqual(read_text(os.path.join(part, "KEEP")), "another export's evidence")
        # Also when the failure would otherwise trigger cleanup (a missing store).
        os.remove(os.path.join(self.s.state, "chat.db"))
        r, _ = self.s.export("offline")
        self.assertEqual(r.returncode, 1)
        self.assertTrue(os.path.exists(os.path.join(part, "KEEP")))

    def test_an_existing_output_and_an_unrelated_partial_are_preserved(self):
        self.s.stop()
        r, out = self.s.export("offline")
        self.assertEqual(r.returncode, 0, r.stderr)
        before = read_text(os.path.join(out, "handoff.json"))
        os.makedirs(out + ".partial")
        with open(os.path.join(out + ".partial", "KEEP"), "w") as f:
            f.write("x")
        r2, _ = self.s.export("offline")
        self.assertEqual(r2.returncode, 1)
        self.assertEqual(read_text(os.path.join(out, "handoff.json")), before)
        self.assertTrue(os.path.exists(os.path.join(out + ".partial", "KEEP")))

    def test_two_concurrent_exporters_for_one_destination(self):
        self.s.send("claude", "codex", "x")
        self.s.stop()
        first, out = self.s.export_async("offline", hook="after-reserve:sleep=1.5")
        deadline = time.time() + 5
        while not os.path.exists(out + ".partial") and time.time() < deadline:
            time.sleep(0.02)
        self.assertTrue(os.path.exists(out + ".partial"), "the first exporter reserved its partial")
        r2, _ = self.s.export("offline")
        self.assertEqual(r2.returncode, 1)
        self.assertIn("left untouched", r2.stderr)
        stdout, stderr = first.communicate(timeout=30)
        self.assertEqual(first.returncode, 0, stderr)
        self.assertTrue(os.path.exists(os.path.join(out, "handoff.json")), "the first export was not disturbed")
        self.assertFalse(os.path.exists(out + ".partial"))

    def test_an_unknown_schema_is_refused(self):
        self.s.stop()
        self.s.sql("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
        self.assertIn("not supported", self.refused().stderr)

    def test_a_corrupt_store_is_refused(self):
        self.s.send("claude", "codex", "x")
        self.s.stop()
        # Garbage over page 1's b-tree (after the 100-byte header) and over every WAL frame, so no
        # intact copy of the schema survives in either file.
        for name, start in (("chat.db", 100), ("chat.db-wal", 32)):
            p = os.path.join(self.s.state, name)
            if os.path.exists(p):
                size = os.path.getsize(p)
                with open(p, "r+b") as f:
                    f.seek(start)
                    f.write(b"\xde\xad\xbe\xef" * ((size - start) // 4))
        self.refused()

    def test_online_refuses_without_a_daemon(self):
        self.s.stop()
        self.refused("online")

    # --- delivery from the whole attempt history ---------------------------------------------------

    def test_delivery_is_judged_from_the_whole_history(self):
        cases = {
            "uncertain-then-failed": (["uncertain", "failed"], "UNCERTAIN"),
            "started": (["started"], "UNCERTAIN"),
            "submitted-then-failed": (["submitted", "failed"], "submitted at least once"),
            "failed-only": (["failed", "failed"], "all 2 recorded attempt(s) failed"),
            "never": ([], "no recorded submission attempt"),
        }
        ids = {name: self.s.send("claude", "codex", name) for name in cases}
        acked = self.s.send("claude", "codex", "acked-after-uncertain")
        self.s.ack("codex", acked)
        self.s.stop()
        for name, (states, _) in cases.items():
            self.s.attempts(ids[name], states)
        self.s.attempts(acked, ["uncertain"])
        h, md, _ = self.handoff()
        by_id = {m["id"]: m for m in h["outstanding"]}
        for name, (_, expect) in cases.items():
            self.assertIn(expect, by_id[ids[name]]["delivery"], name)
        self.assertIn("receipt established", by_id[acked]["delivery"])
        self.assertNotIn("not delivered", md)

    # --- reply context ---------------------------------------------------------------------------

    def test_an_outstanding_reply_carries_its_answered_parent_as_context(self):
        req = self.s.send("claude", "codex", "the question")
        self.s.ack("codex", req)
        fin = self.s.reply("codex", req, final=True)  # claude never acks the answer
        self.s.stop()
        h, md, _ = self.handoff()
        ids = [m["id"] for m in h["outstanding"]]
        self.assertEqual(ids, [fin], "the answered, acknowledged request is not revived as outstanding work")
        ctx = h["outstanding"][0]["context"]["parent_request"]
        self.assertEqual(ctx["id"], req)
        self.assertEqual(ctx["body"], "body of the question\n")
        self.assertEqual([a["reader"] for a in ctx["acknowledgements"]], ["codex"])
        self.assertEqual([r["id"] for r in ctx["replies"]], [fin])
        self.assertIn("context (parent request, not outstanding)", md)
        self.assertIn("body of the question", md)

    # --- online and offline agree ----------------------------------------------------------------

    def test_online_and_offline_exports_agree(self):
        a = self.s.send("claude", "codex", "one")
        self.s.send("codex", "claude", "two", kind="info")
        self.s.ack("codex", a)
        on, _, _ = self.handoff("online")
        self.s.stop()
        r, out = self.s.export("offline", "out2")
        self.assertEqual(r.returncode, 0, r.stderr)
        off = read_json(os.path.join(out, "handoff.json"))
        self.assertEqual(on["outstanding"], off["outstanding"])
        self.assertEqual(on["watermark"]["head_seq"], off["watermark"]["head_seq"])
        modes = {os.path.relpath(os.path.join(root, n), out): oct(os.stat(os.path.join(root, n)).st_mode & 0o777)
                 for root, ds, fs in os.walk(out) for n in ds + fs}
        self.assertTrue(all(m in ("0o700", "0o600") for m in modes.values()), modes)

    def test_an_existing_output_is_never_overwritten(self):
        self.s.stop()
        r, out = self.s.export("offline")
        self.assertEqual(r.returncode, 0)
        r2, _ = self.s.export("offline")
        self.assertEqual(r2.returncode, 1)
        self.assertIn("already exists", r2.stderr)


if __name__ == "__main__":
    unittest.main()
