import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { publicationCandidate } from "../../scripts/verification/release-publish-trigger.mjs";

const sha = "a".repeat(40);
const tag = "v1.2.3";
const refs = [{ tag, sha }];
const releases = [{ tag_name: tag, draft: false, prerelease: false }];
const passed = {
	context: "continuous-integration/drone/push",
	state: "success",
	sha,
};

test("release-first publication resumes on Drone success after the former deadline", () => {
	assert.equal(
		publicationCandidate(
			"workflow_dispatch",
			{ inputs: { tag } },
			refs,
			releases,
		).revision,
		sha,
	);
	assert.equal(
		publicationCandidate(
			"status",
			{ ...passed, updated_at: "2026-01-01T01:00:00Z" },
			refs,
			releases,
		).releaseTag,
		tag,
	);
});

test("Drone-first publication waits for the release and resumes on its dispatch or publication event", () => {
	assert.equal(publicationCandidate("status", passed, refs, []).publish, false);
	for (const [eventName, event] of [
		["workflow_dispatch", { inputs: { tag } }],
		["release", { action: "published", release: releases[0] }],
		["push", { ref: `refs/tags/${tag}` }],
	]) {
		assert.equal(
			publicationCandidate(eventName, event, refs, releases).revision,
			sha,
		);
	}
});

test("failed, pending, unrelated and unreleased status revisions cannot select a publication", () => {
	for (const event of [
		{ ...passed, state: "pending" },
		{ ...passed, state: "failure" },
		{ ...passed, state: "error" },
		{ ...passed, context: "other-ci" },
		{ ...passed, sha: "b".repeat(40) },
	]) {
		assert.equal(
			publicationCandidate("status", event, refs, releases).publish,
			false,
		);
	}
	assert.equal(
		publicationCandidate("status", passed, refs, [
			{ ...releases[0], draft: true },
		]).publish,
		false,
	);
	assert.equal(
		publicationCandidate(
			"status",
			{ ...passed, state: "success" },
			refs,
			releases,
		).publish,
		true,
	);
});

test("duplicate success selects the same exact release; invalid or ambiguous candidates are rejected", () => {
	for (let delivery = 0; delivery < 2; delivery += 1) {
		assert.deepEqual(publicationCandidate("status", passed, refs, releases), {
			publish: true,
			reason: "release_and_revision_selected",
			releaseTag: tag,
			revision: sha,
		});
	}
	for (const invalid of [
		"main",
		"v1.2",
		"v1.2.3-01",
		"v1.2.3+meta",
		"v1.2.3\nlatest",
		"v1.2.3\n",
	]) {
		assert.throws(() =>
			publicationCandidate(
				"workflow_dispatch",
				{ inputs: { tag: invalid } },
				refs,
				releases,
			),
		);
	}
	assert.throws(() =>
		publicationCandidate(
			"status",
			{ ...passed, sha: "invalid" },
			refs,
			releases,
		),
	);
	assert.throws(() =>
		publicationCandidate(
			"status",
			passed,
			[...refs, { tag: "codex-pooler-v1.2.3", sha }],
			[...releases, { ...releases[0], tag_name: "codex-pooler-v1.2.3" }],
		),
	);
});

test("CLI resolves real lightweight and annotated git tags, and writes deferred outputs", (t) => {
	const root = mkdtempSync(join(tmpdir(), "release-trigger-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const git = (...args) =>
		execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
	git("init", "--quiet");
	git(
		"-c",
		"user.name=Example",
		"-c",
		"user.email=example@example.com",
		"commit",
		"--quiet",
		"--allow-empty",
		"-m",
		"initial",
	);
	const revision = git("rev-parse", "HEAD");
	git(
		"-c",
		"user.name=Example",
		"-c",
		"user.email=example@example.com",
		"tag",
		"-a",
		tag,
		"-m",
		"release",
	);
	git("tag", "not-a-release");
	writeFileSync(
		join(root, "event.json"),
		JSON.stringify({ ...passed, sha: revision }),
	);
	writeFileSync(join(root, "releases.json"), JSON.stringify([releases]));
	const script = new URL(
		"../../scripts/verification/release-publish-trigger.mjs",
		import.meta.url,
	).pathname;
	const args = [
		script,
		"status",
		join(root, "event.json"),
		join(root, "releases.json"),
		join(root, "output"),
	];
	const result = JSON.parse(
		execFileSync(process.execPath, args, { cwd: root, encoding: "utf8" }),
	);
	assert.equal(result.revision, revision);
	assert.match(readFileSync(join(root, "output"), "utf8"), /publish=true/);
	writeFileSync(join(root, "releases.json"), "[]");
	assert.equal(
		JSON.parse(
			execFileSync(process.execPath, args, { cwd: root, encoding: "utf8" }),
		).publish,
		false,
	);
	assert.match(
		readFileSync(join(root, "output"), "utf8"),
		/reason=published_release_not_found/,
	);
	assert.equal(
		spawnSync(process.execPath, [script, "--help"], { cwd: root }).status,
		0,
	);
});
