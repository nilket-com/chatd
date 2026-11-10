#!/usr/bin/env python3
"""Read-only committed metadata and ADR lineage checks (adapted from rnx)."""
import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess

IDENTITY = ("Joel Bondurant", "em@joelbondurant.com")
MAX_IMPLEMENTATIONS = 1
SUBJECT = re.compile(r"^(plan|impl): adr ([0-9]{4}) ([a-z0-9][\x20-\x7e]*)$")
DOC = re.compile(r"plans/(adr|impl)/([0-9]{4})_([a-z0-9]+(?:_[a-z0-9]+)*)\.md$")


class HistoryError(ValueError):
	def __init__(self, message, *, commit=None, rule=None):
		super().__init__(message)
		self.commit = commit
		self.rule = rule


@dataclass(frozen=True)
class Policy:
	cutoff: str = "1c0ac6827ad44a9e663f5d6c4b3f6353b75af77e"


def git(repo, *args):
	p = subprocess.run(["git", "--no-replace-objects", "-C", str(repo), *args],
		capture_output=True, text=True, encoding="utf-8", errors="strict", timeout=60)
	if p.returncode:
		raise HistoryError(p.stderr.strip() or "git command failed")
	return p.stdout


def need(condition, message, rule="history"):
	if not condition:
		raise HistoryError(message, rule=rule)


def metadata(repo, commit):
	fields = git(repo, "show", "-s", "--format=%H%x00%P%x00%an%x00%ae%x00%cn%x00%ce", commit).rstrip("\n").split("\0")
	need(len(fields) == 6 and fields[0] == commit, "malformed metadata")
	message = git(repo, "show", "-s", "--format=%B", commit)
	need(tuple(fields[2:4]) == IDENTITY and tuple(fields[4:6]) == IDENTITY, "author/committer identity mismatch")
	need(not message.partition("\n")[2].strip(), "commit body forbidden")
	subject = message.split("\n", 1)[0]
	match = SUBJECT.fullmatch(subject)
	need(match is not None and not any(c in subject for c in "()"), "invalid subject")
	kind, number, _ = match.groups()
	need(int(number) >= 8, "record number predates adoption")
	git(repo, "verify-commit", commit)
	return {"commit": commit, "parents": fields[1].split(), "subject": subject, "kind": kind, "number": number}


def paths(repo, commit):
	return git(repo, "ls-tree", "-r", "--name-only", commit).splitlines()


def status(text):
	lines = text.splitlines()
	found = [line for line in lines if line.startswith("Status:")]
	need(len(found) == 1 and bool(re.fullmatch(r"Status: \S.*", found[0])) and found[0] in lines[:10], "missing/ambiguous opening Status")
	need(len(lines) <= 400, "decision exceeds 400 lines; separate reviewed policy required")


def document(repo, commit, tree, number, category):
	found = [p for p in tree if (m := DOC.fullmatch(p)) and m[1] == category and m[2] == number]
	need(len(found) == 1, "record must have exactly one " + category + " document: " + number, "document.unique")
	if category == "adr":
		status(git(repo, "show", commit + ":" + found[0]))
	return found[0]


def segment(repo, base, head):
	git(repo, "merge-base", "--is-ancestor", base, head)
	need(base in git(repo, "rev-list", "--first-parent", head).splitlines(), "cutoff not on complete first-parent chain", "history.cutoff")
	commits = git(repo, "rev-list", "--reverse", "--first-parent", base + ".." + head).splitlines()
	previous = base
	previous_tree = paths(repo, base)
	pending = None
	plans, implementations, rows = {}, {}, []
	for commit in commits:
		try:
			row = metadata(repo, commit)
			need(row["parents"] and row["parents"][0] == previous, "cutoff not on complete first-parent chain", "history.cutoff")
			need(len(row["parents"]) == 1, "unreviewed merge/second-parent history", "history.merge")
			tree = paths(repo, commit)
			changed = git(repo, "diff-tree", "--no-commit-id", "--name-only", "--no-renames", "-r", previous, commit).splitlines()
			kind, number = row["kind"], row["number"]
			if kind == "plan":
				need(number not in plans, "duplicate plan", "lineage.plan")
			else:
				need(number in plans, "implementation missing earlier first-parent plan", "lineage.plan")
				implementations[number] = implementations.get(number, 0) + 1
				need(implementations[number] <= MAX_IMPLEMENTATIONS, "implementation limit exceeded", "lineage.implementation")
			if pending is None:
				need(kind == "plan", "expected next plan after implementation", "sequence.plan")
				numbers = [int(m[2]) for path in previous_tree if (m := DOC.fullmatch(path)) and m[1] == "adr" and int(m[2]) >= 8]
				expected = max([7, *numbers]) + 1
				need(int(number) == expected, f"expected next record {expected:04d}, got {number}", "sequence.number")
				pending = number
			else:
				need(kind == "impl" and number == pending, f"expected immediate implementation of {pending}", "sequence.implementation")
				pending = None
			adr = document(repo, commit, tree, number, "adr")
			impl = document(repo, commit, tree, number, "impl")
			need(Path(adr).name == Path(impl).name, "paired document names differ", "document.pair")
			allowed = {adr, impl}
			text = git(repo, "show", commit + ":" + adr)
			state = next(line.removeprefix("Status: ") for line in text.splitlines() if line.startswith("Status:"))
			if state.casefold().startswith("withdrawn"):
				need(state == "Withdrawn" and kind == "impl" and bool(changed) and set(changed) <= allowed,
					"withdrawal must be an implementation changing only its paired documents with Status: Withdrawn", "sequence.withdrawal")
			if kind == "plan":
				need(set(changed) == allowed, "plan must change exactly its paired documents", "scope.plan")
				plans[number] = commit
			else:
				for path in changed:
					if path.startswith("plans/"):
						need(path in allowed, "implementation changes another record's documents", "scope.implementation")
				row["plan"] = plans[number]
			row["documents"] = sorted(allowed)
			rows.append(row)
			previous = commit
			previous_tree = tree
		except HistoryError as error:
			if error.commit is None:
				error.commit = commit
			if error.rule is None:
				error.rule = "metadata"
			raise
	need(previous == head, "empty segment head mismatch")
	return {"base": base, "head": head, "commits": rows, "plans": plans, "pending": pending}


def evaluate(repo, policy=Policy()):
	repo = Path(repo).resolve()
	need(git(repo, "rev-parse", "--is-shallow-repository").strip() == "false", "complete history required")
	need(not git(repo, "for-each-ref", "--format=%(refname)", "refs/replace").strip(), "replacement history forbidden")
	grafts = Path(git(repo, "rev-parse", "--git-path", "info/grafts").strip())
	if not grafts.is_absolute():
		grafts = repo / grafts
	need(not grafts.exists(), "grafted history forbidden")
	head = git(repo, "rev-parse", "HEAD^{commit}").strip()
	need(bool(re.fullmatch(r"[0-9a-f]{40}", policy.cutoff)), "invalid pinned cutoff", "history.cutoff")
	git(repo, "cat-file", "-e", policy.cutoff + "^{commit}")
	history = segment(repo, policy.cutoff, head)
	return {"status": "PASS", "head": head, "max_implementations": MAX_IMPLEMENTATIONS, "history": history}


def receipt(repo, policy=Policy()):
	try:
		return evaluate(repo, policy)
	except (HistoryError, OSError, UnicodeError, subprocess.TimeoutExpired) as error:
		result = {"status": "FAIL", "error": str(error)}
		if isinstance(error, HistoryError):
			if error.commit:
				result["commit"] = error.commit
			if error.rule:
				result["rule"] = error.rule
		return result



def main():
	p = argparse.ArgumentParser(description=__doc__)
	p.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
	args = p.parse_args()
	result = receipt(args.repo)
	print(json.dumps(result, indent=2))
	return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
	raise SystemExit(main())
