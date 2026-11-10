//! Read-only Rust formatting enforcement and isolated failure controls.
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

// Cargo supplies the executable used to build this test, avoiding an unrelated Cargo on PATH.
const CARGO: &str = env!("CARGO");

#[derive(Debug, PartialEq)]
enum Check {
	Passed,
	MissingFormatter,
}

fn missing_component(stderr: &str) -> bool {
	// Rustup shims exist even when their component is absent. Recognize only its specific
	// missing-component diagnostic, not arbitrary formatter/toolchain/spawn failures.
	let first = stderr.lines().next().unwrap_or_default();
	["cargo-fmt", "rustfmt"].iter().any(|component| {
		first
			.strip_prefix(&format!("error: '{component}' is not installed for the toolchain '"))
			.and_then(|tail| tail.strip_suffix("'."))
			.is_some_and(|toolchain| !toolchain.is_empty() && !toolchain.contains('\''))
	})
}

fn check_format(cargo: &OsStr, root: &Path) -> Result<Check, String> {
	let output = Command::new(cargo)
		.args(["fmt", "--check"])
		.current_dir(root)
		.output()
		.map_err(|e| format!("could not run cargo fmt --check: {e}"))?;
	if output.status.success() {
		return Ok(Check::Passed);
	}
	let stderr = String::from_utf8_lossy(&output.stderr);
	if output.stdout.is_empty() && missing_component(&stderr) {
		return Ok(Check::MissingFormatter);
	}
	Err(format!("cargo fmt --check failed ({}):\n{}\n{}", output.status, String::from_utf8_lossy(&output.stdout), stderr))
}

fn verified(check: Check) -> bool {
	if check == Check::Passed {
		return true;
	}
	// Direct stderr bypasses the test harness's capture of successful test output.
	std::io::stderr()
		.lock()
		.write_all(b"SKIP formatting check: selected toolchain's rustfmt component is missing; formatting was not verified\n")
		.expect("could not print formatting skip notice");
	false
}

#[test]
fn repository_rust_is_formatted() {
	verified(check_format(OsStr::new(CARGO), Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap_or_else(|e| panic!("{e}")));
}

struct Fixture(PathBuf);
impl Fixture {
	fn new() -> Self {
		let dir = std::env::temp_dir().join(format!("chatd-format-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir_all(dir.join("src")).unwrap();
		std::fs::write(dir.join("Cargo.toml"), "[package]\nname = 'format-control'\nversion = '0.0.0'\nedition = '2024'\n[workspace]\n")
			.unwrap();
		std::fs::write(dir.join("rustfmt.toml"), "hard_tabs = true\n").unwrap();
		std::fs::write(dir.join("src/lib.rs"), "pub fn value() -> u8 {\n\t1\n}\n").unwrap();
		Self(dir)
	}
	fn check(&self) -> Result<Check, String> {
		check_format(OsStr::new(CARGO), &self.0)
	}
}
impl Drop for Fixture {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

#[test]
fn misformatted_fixture_fails_with_diagnostics_without_rewriting() {
	let fixture = Fixture::new();
	if !verified(fixture.check().unwrap_or_else(|e| panic!("{e}"))) {
		return;
	}
	let bad = "pub fn value()->u8{1}\n";
	std::fs::write(fixture.0.join("src/lib.rs"), bad).unwrap();
	let error = fixture.check().expect_err("misformatted code must fail");
	assert!(error.contains("Diff in") && error.contains("src/lib.rs"), "{error}");
	assert_eq!(std::fs::read_to_string(fixture.0.join("src/lib.rs")).unwrap(), bad);
}

#[test]
fn invalid_formatter_config_is_not_a_skip() {
	let fixture = Fixture::new();
	if !verified(fixture.check().unwrap_or_else(|e| panic!("{e}"))) {
		return;
	}
	std::fs::write(fixture.0.join("rustfmt.toml"), "hard_tabs = [\n").unwrap();
	let error = fixture.check().expect_err("invalid formatter configuration must fail");
	assert!(error.contains("TOML") && error.contains("hard_tabs"), "{error}");
}

#[test]
fn missing_cargo_is_not_a_skip() {
	let fixture = Fixture::new();
	assert!(check_format(fixture.0.join("missing-cargo").as_os_str(), &fixture.0).unwrap_err().contains("could not run"));
}

#[test]
fn only_explicit_missing_formatter_diagnostics_allow_skipping() {
	for component in ["cargo-fmt", "rustfmt"] {
		assert!(missing_component(&format!(
			"error: '{component}' is not installed for the toolchain 'stable-x86_64-unknown-linux-gnu'.\nhelp: install the component\n"
		)));
	}
	for diagnostic in [
		"error: no such command: `fmt`",
		"error: toolchain 'broken' is not installed",
		"error: 'rustc' is not installed for the toolchain 'stable'.",
		"error: could not execute rustfmt: Permission denied",
		"error: invalid rustfmt.toml",
		"Diff in src/lib.rs:1:",
		"",
	] {
		assert!(!missing_component(diagnostic), "must fail, not skip: {diagnostic}");
	}
}
