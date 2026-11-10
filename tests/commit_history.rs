//! Read-only committed-history check and isolated signed-history controls.
use std::path::Path;
use std::process::Command;

fn python_check(args: &[&str]) {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	assert!(root.is_dir(), "repository root is missing: {}", root.display());
	let output = match Command::new("python3").arg("-B").args(args).current_dir(root).output() {
		Ok(output) => output,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			// Write directly so the test harness cannot hide the explicit skip notice on success.
			use std::io::Write;
			std::io::stderr()
				.lock()
				.write_all(b"SKIP commit-history check: python3 is missing; history was not verified\n")
				.expect("could not print skip notice");
			return;
		}
		Err(error) => panic!("could not run Python history check: {error}"),
	};
	assert!(
		output.status.success(),
		"history check failed ({}):\n{}\n{}",
		output.status,
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
}

#[test]
fn committed_history_matches_policy() {
	python_check(&["ci/commit_subjects.py"]);
}

#[test]
fn signed_history_policy_controls() {
	python_check(&["-m", "unittest", "discover", "-s", "ci", "-p", "test_commit_subjects.py"]);
}
