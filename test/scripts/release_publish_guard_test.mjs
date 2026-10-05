import assert from "node:assert/strict";
import { execFileSync, spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
	registryEvidence,
	releasePublication,
} from "../../scripts/verification/release-publish-guard.mjs";

const sha = "a".repeat(40);
const success = {
	sha,
	statuses: [
		{ context: "continuous-integration/drone/push", state: "success" },
	],
};
const absentAliases = {
	versionTag: { state: "missing" },
	latest: { state: "missing" },
	minor: { state: "missing" },
};
const release = (tag, extra = {}) => ({
	tag_name: tag,
	draft: false,
	prerelease: false,
	...extra,
});

test("a tested newest stable release gets global and minor aliases", () => {
	const releases = [
		release("codex-pooler-v0.9.7"),
		release("codex-pooler-v0.10.0"),
	];
	assert.deepEqual(
		releasePublication(
			"codex-pooler-v0.10.0",
			sha,
			success,
			releases,
			absentAliases,
		).tags,
		["0.10.0", "0.10", "latest"],
	);
});

test("back-publishing never moves a newer global or minor alias", () => {
	const releases = [release("v0.9.7"), release("v0.9.9"), release("v0.10.0")];
	assert.deepEqual(
		releasePublication("v0.9.7", sha, success, releases, absentAliases).tags,
		["0.9.7"],
	);
	assert.deepEqual(
		releasePublication("v0.9.9", sha, success, releases, absentAliases).tags,
		["0.9.9", "0.9"],
	);
});

test("a prerelease publishes its immutable tag without stable aliases", () => {
	const releases = [
		release("v1.0.0-rc.1", { prerelease: true }),
		release("v0.9.7"),
	];
	assert.deepEqual(
		releasePublication("v1.0.0-rc.1", sha, success, releases, absentAliases)
			.tags,
		["1.0.0-rc.1"],
	);
});

test("unverified revisions and failed or missing required checks cannot publish", () => {
	const releases = [release("v0.10.0")];
	for (const status of [
		{ ...success, sha: "b".repeat(40) },
		{ sha, statuses: [] },
		{ sha, statuses: [{ context: "another-check", state: "success" }] },
		{
			sha,
			statuses: [
				{ context: "continuous-integration/drone/push", state: "pending" },
			],
		},
		{
			sha,
			statuses: [
				{ context: "continuous-integration/drone/push", state: "failure" },
			],
		},
	])
		assert.throws(() => releasePublication("v0.10.0", sha, status, releases));
});

test("invalid, missing, draft and mutable release inputs are rejected", () => {
	for (const tag of [
		"main",
		"v01.2.3",
		"v0.10",
		"v0.10.0\nlatest",
		"v0.10.0+metadata",
	]) {
		assert.throws(() => releasePublication(tag, sha, success, [release(tag)]));
	}
	assert.throws(() => releasePublication("v0.10.0", sha, success, []));
	assert.throws(() =>
		releasePublication("v0.10.0", sha, success, [
			release("v0.10.0", { draft: true }),
		]),
	);
});

test("CLI writes reviewed tags and refuses failed evidence without modifying its output", (t) => {
	const root = mkdtempSync(join(tmpdir(), "release-policy-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	const statusPath = join(root, "status.json");
	const releasesPath = join(root, "releases.json");
	const output = join(root, "output");
	const registryPath = join(root, "registry.json");
	writeFileSync(registryPath, JSON.stringify(absentAliases));
	writeFileSync(statusPath, JSON.stringify(success));
	writeFileSync(
		releasesPath,
		JSON.stringify([[release("v0.10.0")], [release("v0.9.7")]]),
	);
	const script = new URL(
		"../../scripts/verification/release-publish-guard.mjs",
		import.meta.url,
	);
	const args = [
		script.pathname,
		"v0.10.0",
		sha,
		statusPath,
		releasesPath,
		registryPath,
		output,
	];
	const result = JSON.parse(
		execFileSync(process.execPath, args, { encoding: "utf8" }),
	);
	assert.deepEqual(result.tags, ["0.10.0", "0.10", "latest"]);
	const written = readFileSync(output, "utf8");
	assert.ok(written.includes("type=raw,value=latest\nRELEASE_TAGS\n"));
	writeFileSync(statusPath, JSON.stringify({ sha, statuses: [] }));
	assert.notEqual(spawnSync(process.execPath, args).status, 0);
	assert.equal(readFileSync(output, "utf8"), written);
});

test("registry aliases cannot regress when a newer release is removed from the inventory", () => {
	const registry = {
		versionTag: { state: "missing" },
		latest: { state: "present", version: "1.4.9" },
		minor: { state: "present", version: "1.4.8" },
	};
	assert.deepEqual(
		releasePublication("v1.4.2", sha, success, [release("v1.4.2")], registry)
			.tags,
		["1.4.2"],
	);
});

test("unknown or malformed registry evidence withholds aliases but keeps the verified version", () => {
	for (const registry of [
		{
			versionTag: { state: "missing" },
			latest: { state: "unavailable", reason: "registry_read_failed" },
			minor: { state: "unavailable" },
		},
		{
			versionTag: { state: "missing" },
			latest: { state: "present", version: "invalid" },
			minor: { state: "present", version: "2.0.0" },
		},
	]) {
		const result = releasePublication(
			"v1.2.3",
			sha,
			success,
			[release("v1.2.3")],
			registry,
		);
		assert.deepEqual(result.tags, ["1.2.3"]);
		assert.equal(result.aliasStatus.latest, "registry_evidence_unavailable");
		assert.equal(result.aliasStatus["1.2"], "registry_evidence_unavailable");
	}
});

const manifestType = "application/vnd.oci.image.manifest.v1+json";
const indexType = "application/vnd.oci.image.index.v1+json";
const digest = (char) => `sha256:${char.repeat(64)}`;

test("an existing matching version is reused while missing aliases remain recoverable", () => {
	const result = releasePublication(
		"v1.2.3",
		sha,
		success,
		[release("v1.2.3")],
		{
			...absentAliases,
			versionTag: {
				state: "present",
				version: "1.2.3",
				revision: sha,
				digest: digest("9"),
			},
		},
	);
	assert.equal(result.imageAction, "reuse");
	assert.equal(result.imageDigest, digest("9"));
	assert.deepEqual(result.tags, ["1.2", "latest"]);
});

test("duplicate delivery reuses the image even when no stable alias is eligible", () => {
	const result = releasePublication(
		"v1.2.3",
		sha,
		success,
		[release("v1.2.3"), release("v1.2.4")],
		{
			...absentAliases,
			versionTag: {
				state: "present",
				version: "1.2.3",
				revision: sha,
				digest: digest("9"),
			},
		},
	);
	assert.equal(result.imageAction, "reuse");
	assert.deepEqual(result.tags, []);
	assert.deepEqual(result.imageTags, ["1.2.3"]);
});

test("an existing prerelease is reused without assigning any stable alias", () => {
	const version = "1.2.3-rc.1";
	const result = releasePublication(
		`v${version}`,
		sha,
		success,
		[release(`v${version}`, { prerelease: true })],
		{
			versionTag: {
				state: "present",
				version,
				revision: sha,
				digest: digest("9"),
			},
		},
	);
	assert.equal(result.imageAction, "reuse");
	assert.deepEqual(result.tags, []);
	assert.deepEqual(result.imageTags, [version]);
});

test("conflicting, unknown or malformed immutable image evidence never authorizes a build", () => {
	for (const versionTag of [
		undefined,
		{ state: "unavailable" },
		{ state: "present", version: "1.2.4", revision: sha, digest: digest("9") },
		{
			state: "present",
			version: "1.2.3",
			revision: "b".repeat(40),
			digest: digest("9"),
		},
		{ state: "present", version: "1.2.3", revision: sha, digest: "invalid" },
	]) {
		assert.throws(
			() =>
				releasePublication("v1.2.3", sha, success, [release("v1.2.3")], {
					...absentAliases,
					versionTag,
				}),
			/immutable/,
		);
	}
});

async function registryFixture(t, mode) {
	const calls = [];
	const server = createServer((request, response) => {
		calls.push({
			path: request.url,
			authorization: request.headers.authorization,
		});
		const leaf = request.url.split("/").at(-1);
		const send = (code, body) => {
			response.writeHead(code, {
				"content-type": "application/json",
				"docker-content-digest": digest("9"),
			});
			response.end(JSON.stringify(body));
		};
		if (mode.startsWith("token-")) {
			if (request.url.startsWith("/token?")) {
				if (mode === "token-failed") return send(403, {});
				if (mode === "token-missing") return send(200, {});
				return send(200, { token: "synthetic-registry-token" });
			}
			if (request.headers.authorization !== "Bearer synthetic-registry-token")
				return send(401, {});
		}
		if (mode === "read-timeout") return;
		if (leaf === "1.2.3") {
			if (mode === "version-present" || mode === "version-conflict")
				return send(200, {
					mediaType: indexType,
					manifests: ["a", "b"].map((char) => ({
						mediaType: manifestType,
						digest: digest(char),
					})),
				});
			return send(404, { errors: [{ code: "MANIFEST_UNKNOWN" }] });
		}
		if (mode === "connection-reset") return request.socket.destroy();
		if (mode === "read-failed")
			return send(503, { errors: [{ code: "UNAVAILABLE" }] });
		if (mode === "auth-failed")
			return send(401, { errors: [{ code: "UNAUTHORIZED" }] });
		if (leaf === "1.2" && mode === "minor-missing")
			return send(404, { errors: [{ code: "MANIFEST_UNKNOWN" }] });
		if (mode === "untyped-404")
			return send(404, { errors: [{ code: "DENIED" }] });
		if (mode === "null-descriptor" && (leaf === "latest" || leaf === "1.2"))
			return send(200, { mediaType: indexType, manifests: [null] });
		if (mode === "empty-platforms")
			return send(200, { mediaType: indexType, manifests: [] });
		if (mode === "unknown-manifest")
			return send(200, { mediaType: "application/example" });
		if (
			mode === "platform-failed" &&
			(leaf === digest("a") || leaf === digest("b"))
		)
			return send(503, {});
		if (mode === "blob-failed" && request.url.includes("/blobs/"))
			return send(503, {});
		if (mode === "redirect-config" && request.url.includes("/blobs/")) {
			response.writeHead(307, { location: `/config/${leaf}` });
			response.end();
			return;
		}
		if (leaf === "latest" || leaf === "1.2")
			return send(200, {
				mediaType: indexType,
				manifests: ["a", "b"].map((char) => ({
					mediaType: manifestType,
					digest: digest(char),
				})),
			});
		if (leaf === digest("a") || leaf === digest("b"))
			return send(200, {
				mediaType: manifestType,
				config: { digest: digest(leaf === digest("a") ? "c" : "d") },
			});
		if (leaf === digest("c") || leaf === digest("d"))
			return send(200, {
				config: {
					Labels: {
						"org.opencontainers.image.version": mode.startsWith("version-")
							? "1.2.3"
							: mode === "disagree" && leaf === digest("d")
								? "1.2.8"
								: mode === "malformed"
									? "not-a-version"
									: "1.2.9",
						"org.opencontainers.image.revision":
							mode === "version-conflict" ? "b".repeat(40) : sha,
					},
				},
			});
		return send(404, { errors: [{ code: "MANIFEST_UNKNOWN" }] });
	});
	t.after(
		() =>
			new Promise((resolve) => {
				server.closeAllConnections();
				server.close(resolve);
			}),
	);
	await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
	return {
		baseURL: `http://127.0.0.1:${server.address().port}`,
		repository: "sample/image",
		timeoutMs: mode === "read-timeout" ? 20 : 1000,
		calls,
	};
}

for (const mode of ["version-present", "version-conflict"]) {
	test(`immutable image identity crosses the real registry HTTP boundary: ${mode}`, async (t) => {
		const evidence = await registryEvidence(
			"v1.2.3",
			await registryFixture(t, mode),
		);
		assert.equal(evidence.versionTag.digest, digest("9"));
		if (mode === "version-present")
			assert.equal(
				releasePublication(
					"v1.2.3",
					sha,
					success,
					[release("v1.2.3")],
					evidence,
				).imageAction,
				"reuse",
			);
		else
			assert.throws(
				() =>
					releasePublication(
						"v1.2.3",
						sha,
						success,
						[release("v1.2.3")],
						evidence,
					),
				/immutable/,
			);
	});
}

for (const mode of [
	"consistent",
	"disagree",
	"minor-missing",
	"read-failed",
	"auth-failed",
	"untyped-404",
	"malformed",
	"read-timeout",
	"connection-reset",
	"null-descriptor",
	"redirect-config",
	"empty-platforms",
	"unknown-manifest",
	"platform-failed",
	"blob-failed",
]) {
	test(`real registry HTTP boundary classifies ${mode}`, async (t) => {
		const options = await registryFixture(t, mode);
		const evidence = await registryEvidence("v1.2.3", options);
		if (mode === "read-timeout") {
			assert.equal(evidence.versionTag.state, "unavailable");
			assert.throws(
				() =>
					releasePublication(
						"v1.2.3",
						sha,
						success,
						[release("v1.2.3")],
						evidence,
					),
				/immutable/,
			);
			return;
		}
		const result = releasePublication(
			"v1.2.3",
			sha,
			success,
			[release("v1.2.3")],
			evidence,
		);
		if (mode === "consistent" || mode === "redirect-config") {
			assert.deepEqual(evidence.latest, { state: "present", version: "1.2.9" });
			assert.deepEqual(result.tags, ["1.2.3"]);
		} else if (mode === "minor-missing") {
			assert.equal(evidence.minor.state, "missing");
			assert.deepEqual(result.tags, ["1.2.3", "1.2"]);
		} else {
			assert.equal(evidence.latest.state, "unavailable");
			assert.equal(evidence.minor.state, "unavailable");
			assert.deepEqual(result.tags, ["1.2.3"]);
		}
		const root = mkdtempSync(join(tmpdir(), "registry-cli-boundary-"));
		t.after(() => rmSync(root, { recursive: true, force: true }));
		writeFileSync(join(root, "status"), JSON.stringify(success));
		writeFileSync(
			join(root, "releases"),
			JSON.stringify([[release("v1.2.3")]]),
		);
		writeFileSync(join(root, "registry"), JSON.stringify(evidence));
		const script = new URL(
			"../../scripts/verification/release-publish-guard.mjs",
			import.meta.url,
		);
		const cli = spawnSync(
			process.execPath,
			[
				script.pathname,
				"v1.2.3",
				sha,
				join(root, "status"),
				join(root, "releases"),
				join(root, "registry"),
				join(root, "output"),
			],
			{ encoding: "utf8" },
		);
		assert.equal(cli.status, 0);
		assert.deepEqual(JSON.parse(cli.stdout).tags, result.tags);
		if (evidence.latest.state === "unavailable")
			assert.match(
				cli.stderr,
				/release alias withheld: latest registry_evidence_unavailable/,
			);
		if (mode === "disagree")
			assert.equal(evidence.latest.reason, "platform_version_disagreement");
		if (mode === "read-failed")
			assert.equal(evidence.latest.reason, "registry_http_503");
		if (mode === "auth-failed")
			assert.equal(evidence.latest.reason, "registry_http_401");
		if (mode === "untyped-404")
			assert.equal(evidence.latest.reason, "registry_http_404");
	});
}

for (const mode of [
	"token-anonymous",
	"token-basic",
	"token-failed",
	"token-missing",
]) {
	test(`registry CLI exercises real token exchange: ${mode}`, async (t) => {
		const fixture = await registryFixture(t, mode);
		const root = mkdtempSync(join(tmpdir(), "registry-auth-cli-"));
		t.after(() => rmSync(root, { recursive: true, force: true }));
		const outputPath = join(root, "evidence.json");
		const args = [
			"--import",
			new URL("./support/registry_fetch_route.mjs", import.meta.url).pathname,
			new URL(
				"../../scripts/verification/release-publish-guard.mjs",
				import.meta.url,
			).pathname,
			"--registry",
			"v1.2.3",
			outputPath,
		];
		const child = spawn(process.execPath, args, {
			env: {
				...process.env,
				TEST_REGISTRY_ORIGIN: fixture.baseURL,
				GH_TOKEN: mode === "token-basic" ? "synthetic-fixture-token" : "",
				GITHUB_ACTOR: mode === "token-basic" ? "example-user" : "",
			},
		});
		t.after(() => {
			if (child.exitCode === null) child.kill();
		});
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (chunk) => {
			stdout += chunk;
		});
		child.stderr.on("data", (chunk) => {
			stderr += chunk;
		});
		const code = await new Promise((resolve, reject) => {
			child.once("error", reject);
			child.once("close", resolve);
		});
		assert.equal(code, 0, stderr);
		const evidence = JSON.parse(readFileSync(outputPath, "utf8"));
		assert.deepEqual(JSON.parse(stdout), evidence);
		const tokens = fixture.calls.filter((call) =>
			call.path.startsWith("/token?"),
		);
		assert.equal(tokens.length, 1);
		assert.equal(
			new URL(tokens[0].path, fixture.baseURL).searchParams.get("scope"),
			"repository:icoretech/codex-pooler:pull",
		);
		assert.equal(
			tokens[0].authorization,
			mode === "token-basic"
				? `Basic ${Buffer.from("example-user:synthetic-fixture-token").toString("base64")}`
				: undefined,
		);
		if (mode === "token-failed" || mode === "token-missing") {
			assert.deepEqual(evidence, {
				versionTag: { state: "unavailable", reason: "registry_auth_failed" },
				latest: { state: "unavailable", reason: "registry_auth_failed" },
				minor: { state: "unavailable", reason: "registry_auth_failed" },
			});
			assert.equal(fixture.calls.length, 2);
		} else {
			assert.deepEqual(evidence.latest, { state: "present", version: "1.2.9" });
			assert.deepEqual(evidence.minor, evidence.latest);
			assert.ok(
				fixture.calls
					.slice(2)
					.every(
						(call) => call.authorization === "Bearer synthetic-registry-token",
					),
			);
		}
		assert.ok(!stdout.includes("synthetic-registry-token"));
	});
}

test("release flags must match version stability", () => {
	for (const [tag, prerelease] of [
		["v1.2.3", true],
		["v1.2.3-rc.1", false],
	]) {
		assert.throws(
			() =>
				releasePublication(
					tag,
					sha,
					success,
					[release(tag, { prerelease })],
					absentAliases,
				),
			/prerelease flag/,
		);
	}
});

test("CLI refuses unreadable or malformed immutable registry evidence", (t) => {
	const root = mkdtempSync(join(tmpdir(), "registry-cli-unavailable-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	writeFileSync(join(root, "status"), JSON.stringify(success));
	writeFileSync(join(root, "releases"), JSON.stringify([[release("v1.2.3")]]));
	const script = new URL(
		"../../scripts/verification/release-publish-guard.mjs",
		import.meta.url,
	);
	for (const malformed of [false, true]) {
		if (malformed) writeFileSync(join(root, "registry"), "invalid-json");
		const cli = spawnSync(
			process.execPath,
			[
				script.pathname,
				"v1.2.3",
				sha,
				join(root, "status"),
				join(root, "releases"),
				join(root, "registry"),
			],
			{ encoding: "utf8" },
		);
		assert.notEqual(cli.status, 0);
		assert.match(cli.stderr, /immutable/);
	}
});
