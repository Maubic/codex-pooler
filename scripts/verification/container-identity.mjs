import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

const usage =
	"usage: container-identity --image <sha256:id|repository@sha256:digest> --platform <linux/amd64|linux/arm64>";
const probe =
	'set -eu; printf "uid=%s\\ngid=%s\\ngroups=%s\\n" "$(id -u)" "$(id -g)" "$(id -G)"; stat -c "release_owner=%u:%g" /app/bin/codex_pooler; test -x /app/bin/codex_pooler; test -r /app/releases/start_erl.data; /app/bin/codex_pooler version';

export function verifyIdentity(
	metadata,
	platform,
	defaultOutput,
	explicitOutput,
) {
	assert.equal(metadata.Os, "linux", "image must target Linux");
	assert.equal(
		`linux/${metadata.Architecture}`,
		platform,
		"image platform mismatch",
	);
	assert.equal(
		metadata.Config.User,
		"999:999",
		"image USER must be numeric 999:999",
	);
	assert.equal(
		defaultOutput,
		explicitOutput,
		"default and explicit identity differ",
	);
	assert.match(
		defaultOutput,
		/^uid=999\ngid=999\ngroups=999\nrelease_owner=999:999\ncodex_pooler \d+\.\d+\.\d+(?:[^\n]*)\n$/,
		"release identity/access result is incomplete",
	);
	return {
		imageId: metadata.Id,
		platform,
		user: "999:999",
		defaultEqualsExplicit: true,
		releaseAccess: true,
	};
}

function docker(args) {
	return execFileSync("docker", args, {
		encoding: "utf8",
		timeout: 60_000,
		stdio: ["ignore", "pipe", "pipe"],
	});
}

export function main(args) {
	if (args.length === 1 && args[0] === "--help") {
		process.stdout.write(`${usage}\n`);
		return;
	}
	if (
		args.length !== 4 ||
		args[0] !== "--image" ||
		args[2] !== "--platform" ||
		!/^(?:sha256:[a-f0-9]{64}|[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[a-f0-9]{64})$/.test(
			args[1],
		) ||
		!/^linux\/(?:amd64|arm64)$/.test(args[3])
	)
		throw new Error(usage);
	const [, image, , platform] = args;
	const [metadata] = JSON.parse(
		docker(["image", "inspect", "--platform", platform, image]),
	);
	assert.equal(
		metadata.Config.User,
		"999:999",
		"image USER must be numeric 999:999",
	);
	assert.equal(
		`linux/${metadata.Architecture}`,
		platform,
		"image platform mismatch",
	);
	const owner = randomUUID();
	const outputs = [];
	for (const explicit of [false, true]) {
		const name = `container-identity-${owner}-${Number(explicit)}`;
		try {
			docker([
				"create",
				"--pull=never",
				"--platform",
				platform,
				"--name",
				name,
				"--label",
				`verification.container-identity=${owner}`,
				"--network",
				"none",
				"--read-only",
				"--cap-drop=ALL",
				"--security-opt=no-new-privileges",
				...(explicit ? ["--user", "999:999"] : []),
				"--entrypoint",
				"/bin/sh",
				image,
				"-c",
				probe,
			]);
			outputs.push(docker(["start", "--attach", name]));
			const [state] = JSON.parse(docker(["inspect", name]));
			assert.equal(state.State.ExitCode, 0, "release identity probe failed");
		} finally {
			const owned = () =>
				docker([
					"container",
					"ls",
					"--all",
					"--filter",
					`name=^/${name}$`,
					"--filter",
					`label=verification.container-identity=${owner}`,
					"--format",
					"{{.ID}}",
				]).trim();
			if (owned()) {
				const [state] = JSON.parse(docker(["inspect", name]));
				assert.equal(
					state.Config.Labels["verification.container-identity"],
					owner,
					"cleanup ownership mismatch",
				);
				docker(["rm", "--force", name]);
				assert.equal(owned(), "", "owned container remains after cleanup");
			}
		}
	}
	process.stdout.write(
		`${JSON.stringify({ ...verifyIdentity(metadata, platform, outputs[0], outputs[1]), ownedContainersRemoved: true })}\n`,
	);
}

function isEntrypoint() {
	if (!process.argv[1]) return false;
	try {
		return (
			realpathSync(process.argv[1]) ===
			realpathSync(fileURLToPath(import.meta.url))
		);
	} catch (error) {
		if (error.code === "ENOENT") return false;
		throw error;
	}
}

if (isEntrypoint()) {
	try {
		main(process.argv.slice(2));
	} catch (error) {
		process.stderr.write(`${error.message}\n`);
		process.exitCode = 1;
	}
}
