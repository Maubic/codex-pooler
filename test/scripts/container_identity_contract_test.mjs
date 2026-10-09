import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
	existsSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	symlinkSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { verifyIdentity } from "../../scripts/verification/container-identity.mjs";

const script = new URL(
	"../../scripts/verification/container-identity.mjs",
	import.meta.url,
);
const dockerfile = readFileSync(
	new URL("../../Dockerfile", import.meta.url),
	"utf8",
);

test("release account allocation and image metadata select the same fixed non-root identity", () => {
	assert.match(dockerfile, /groupadd --system --gid 999 codex_pooler/);
	assert.match(dockerfile, /useradd --system --uid 999 --gid codex_pooler/);
	assert.match(dockerfile, /^USER 999:999$/m);
});

for (const args of [
	[],
	["latest"],
	["--image", "example:latest"],
	["--image", `sha256:${"a".repeat(64)}`, "--unknown"],
]) {
	test(`identity verifier refuses incomplete or mutable input: ${args.join(" ") || "empty"}`, () => {
		const result = spawnSync(process.execPath, [script.pathname, ...args], {
			encoding: "utf8",
		});
		assert.equal(result.status, 1);
		assert.match(result.stderr, /usage: container-identity/);
	});
}

test("identity verifier documents its image and platform contract", () => {
	const result = spawnSync(process.execPath, [script.pathname, "--help"], {
		encoding: "utf8",
	});
	assert.equal(result.status, 0);
	assert.match(result.stdout, /--image.*--platform/);
});

for (const aliasKind of ["file", "directory"]) {
	for (const args of [["--help"], ["--invalid"], []]) {
		test(`CLI ${aliasKind} alias executes ${args.join(" ") || "missing-input refusal"}`, (t) => {
			const root = mkdtempSync(join(tmpdir(), "container-identity-entry-"));
			t.after(() => {
				rmSync(root, { recursive: true, force: true });
				assert.equal(existsSync(root), false);
			});
			const target = fileURLToPath(script);
			const alias = join(root, "owned alias");
			symlinkSync(aliasKind === "file" ? target : dirname(target), alias);
			const entry =
				aliasKind === "file" ? alias : join(alias, "container-identity.mjs");
			const result = spawnSync(process.execPath, [entry, ...args], {
				encoding: "utf8",
			});
			assert.ifError(result.error);
			assert.equal(result.signal, null);
			assert.equal(result.status, args[0] === "--help" ? 0 : 1);
			assert.match(
				args[0] === "--help" ? result.stdout : result.stderr,
				/usage: container-identity/,
			);
		});
	}
}

test("stdin module import does not execute the verifier CLI", () => {
	const result = spawnSync(process.execPath, ["--input-type=module", "-"], {
		input: `import { verifyIdentity } from ${JSON.stringify(script.href)}; console.log(typeof verifyIdentity);`,
		encoding: "utf8",
	});
	assert.ifError(result.error);
	assert.equal(result.status, 0, result.stderr);
	assert.equal(result.stdout, "function\n");
	assert.equal(result.stderr, "");
});

const output =
	"uid=999\ngid=999\ngroups=999\nrelease_owner=999:999\ncodex_pooler 0.11.5\n";
const metadata = {
	Id: `sha256:${"a".repeat(64)}`,
	Os: "linux",
	Architecture: "arm64",
	Config: { User: "999:999" },
};

test("complete release identity requires matching default and explicit runtime observations", () => {
	assert.equal(
		verifyIdentity(metadata, "linux/arm64", output, output).releaseAccess,
		true,
	);
});

for (const [name, image, expectedPlatform, actual, explicit] of [
	[
		"named image user",
		{ ...metadata, Config: { User: "codex_pooler" } },
		"linux/arm64",
		output,
		output,
	],
	[
		"root image user",
		{ ...metadata, Config: { User: "0" } },
		"linux/arm64",
		output,
		output,
	],
	["wrong platform", metadata, "linux/amd64", output, output],
	[
		"wrong runtime uid",
		metadata,
		"linux/arm64",
		output.replace("uid=999", "uid=0"),
		output,
	],
	[
		"unexpected supplementary group",
		metadata,
		"linux/arm64",
		output.replace("groups=999", "groups=999 0"),
		output.replace("groups=999", "groups=999 0"),
	],
	[
		"wrong release owner",
		metadata,
		"linux/arm64",
		output.replace("release_owner=999:999", "release_owner=0:0"),
		output.replace("release_owner=999:999", "release_owner=0:0"),
	],
	[
		"missing release boundary",
		metadata,
		"linux/arm64",
		output.replace("codex_pooler 0.11.5\n", ""),
		output.replace("codex_pooler 0.11.5\n", ""),
	],
]) {
	test(`release identity refuses ${name}`, () =>
		assert.throws(() =>
			verifyIdentity(image, expectedPlatform, actual, explicit),
		));
}
