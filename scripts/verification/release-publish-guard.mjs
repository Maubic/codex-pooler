import { appendFile, readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const stableVersion =
	/^(?:codex-pooler-)?v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$/;
const requiredStatus = "continuous-integration/drone/push";

export function parseTag(tag) {
	const match = typeof tag === "string" ? stableVersion.exec(tag) : null;
	if (!match || match[0] !== tag)
		throw new Error(
			"release tag must be a complete version without build metadata",
		);
	const [, major, minor, patch, suffix = ""] = match;
	if (
		suffix !== "" &&
		suffix
			.slice(1)
			.split(".")
			.some(
				(part) =>
					!part ||
					(/^[0-9]+$/.test(part) && part.length > 1 && part.startsWith("0")),
			)
	) {
		throw new Error("invalid prerelease version");
	}
	return {
		version: `${major}.${minor}.${patch}${suffix}`,
		parts: [major, minor, patch].map(BigInt),
		minor: `${major}.${minor}`,
		prerelease: suffix !== "",
	};
}

function newer(left, right) {
	for (let index = 0; index < 3; index += 1) {
		if (left.parts[index] !== right.parts[index])
			return left.parts[index] > right.parts[index];
	}
	return false;
}

export function releasePublication(tag, sha, status, releases, registry = {}) {
	const selected = parseTag(tag);
	if (!/^[a-f0-9]{40}$/.test(sha) || status.sha !== sha)
		throw new Error("CI status does not describe the checked-out revision");
	const check = status.statuses?.find(
		(item) => item.context === requiredStatus,
	);
	if (check?.state !== "success")
		throw new Error(
			"required Drone push status has not succeeded for this revision",
		);
	const release = releases.find((item) => item.tag_name === tag);
	if (!release || release.draft)
		throw new Error("a published release must exist before image publication");
	if (release.prerelease !== selected.prerelease)
		throw new Error("release prerelease flag disagrees with its version");
	const immutable = registry?.versionTag;
	if (immutable?.state !== "missing" && immutable?.state !== "present")
		throw new Error("immutable image registry evidence is unavailable");
	if (
		immutable.state === "present" &&
		(immutable.version !== selected.version ||
			immutable.revision !== sha ||
			!digestPattern.test(immutable.digest))
	)
		throw new Error(
			"immutable image does not match the release version, revision and digest",
		);
	const stable = releases
		.filter((item) => !item.draft && !item.prerelease)
		.flatMap((item) => {
			try {
				const version = parseTag(item.tag_name);
				return version.prerelease ? [] : [version];
			} catch {
				return [];
			}
		});
	const tags = immutable.state === "missing" ? [selected.version] : [];
	const aliasStatus = {};
	if (!selected.prerelease) {
		for (const [alias, key] of [
			[selected.minor, "minor"],
			["latest", "latest"],
		]) {
			const evidence = registry?.[key];
			let current;
			try {
				if (evidence?.state === "present") {
					current = parseTag(evidence.version);
					if (
						current.prerelease ||
						(key === "minor" && current.minor !== selected.minor)
					)
						throw new Error("invalid alias version");
				} else if (evidence?.state !== "missing")
					throw new Error("registry evidence unavailable");
			} catch {
				aliasStatus[alias] = "registry_evidence_unavailable";
				continue;
			}
			const ahead =
				(current && newer(current, selected)) ||
				stable.some(
					(version) =>
						(key === "latest" || version.minor === selected.minor) &&
						newer(version, selected),
				);
			aliasStatus[alias] = ahead ? "newer_version_exists" : "publish";
			if (!ahead) tags.push(alias);
		}
	}
	return {
		version: selected.version,
		tags,
		aliasStatus,
		imageAction: immutable.state === "missing" ? "build" : "reuse",
		imageDigest: immutable.state === "present" ? immutable.digest : "",
		imageTags: [...new Set([selected.version, ...tags])],
	};
}

const manifestTypes = [
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
];
const indexTypes = [
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
];
const digestPattern = /^sha256:[a-f0-9]{64}$/;

// Registry results are reduced to bounded state/version metadata. A read error
// is never evidence that a tag is absent; only MANIFEST_UNKNOWN at HTTP404 is.
export async function registryEvidence(tag, options = {}) {
	const selected = parseTag(tag);
	const base = options.baseURL ?? "https://ghcr.io";
	const repository = options.repository ?? "icoretech/codex-pooler";
	const timeoutMs = options.timeoutMs ?? 15_000;
	const headers = { accept: [...manifestTypes, ...indexTypes].join(", ") };
	const readJson = async (response) => {
		const reader = response.body?.getReader();
		if (!reader) throw new Error("empty registry body");
		const chunks = [];
		let size = 0;
		try {
			while (true) {
				const { done, value } = await reader.read();
				if (done) break;
				size += value.byteLength;
				if (size > 1_048_576) throw new Error("registry body too large");
				chunks.push(Buffer.from(value));
			}
			return JSON.parse(Buffer.concat(chunks).toString("utf8"));
		} finally {
			await reader.cancel();
		}
	};
	const jsonRequest = async (path, allowRedirects = false) => {
		try {
			const signal = AbortSignal.timeout(timeoutMs);
			let url = new URL(path, base);
			for (let hop = 0; hop <= 3; hop += 1) {
				// Config blobs may redirect to signed CDN URLs. Credentials never
				// leave the registry origin, and redirects share one deadline.
				const requestHeaders =
					url.origin === new URL(base).origin
						? headers
						: { accept: headers.accept };
				const response = await fetch(url, {
					headers: requestHeaders,
					redirect: "manual",
					signal,
				});
				if ([301, 302, 303, 307, 308].includes(response.status)) {
					await response.body?.cancel();
					if (!allowRedirects || hop === 3)
						return { error: "registry_redirect_refused" };
					url = new URL(response.headers.get("location"), url);
					if (
						url.username ||
						url.password ||
						(url.protocol !== "https:" && url.origin !== new URL(base).origin)
					)
						return { error: "registry_redirect_refused" };
					continue;
				}
				return {
					status: response.status,
					body: await readJson(response),
					digest: response.headers.get("docker-content-digest"),
				};
			}
		} catch {
			return { error: "registry_read_failed" };
		}
	};
	const first = await jsonRequest(
		`/v2/${repository}/manifests/${selected.version}`,
	);
	if (first.status === 401 && base === "https://ghcr.io") {
		const tokenHeaders = {};
		if (process.env.GH_TOKEN && process.env.GITHUB_ACTOR)
			tokenHeaders.authorization = `Basic ${Buffer.from(`${process.env.GITHUB_ACTOR}:${process.env.GH_TOKEN}`).toString("base64")}`;
		try {
			const tokenResponse = await fetch(
				`${base}/token?service=ghcr.io&scope=${encodeURIComponent(`repository:${repository}:pull`)}`,
				{
					headers: tokenHeaders,
					redirect: "error",
					signal: AbortSignal.timeout(timeoutMs),
				},
			);
			if (!tokenResponse.ok) throw new Error("registry authentication failed");
			const token = (await readJson(tokenResponse)).token;
			if (typeof token !== "string" || !token)
				throw new Error("registry token missing");
			headers.authorization = `Bearer ${token}`;
		} catch {
			return {
				versionTag: { state: "unavailable", reason: "registry_auth_failed" },
				latest: { state: "unavailable", reason: "registry_auth_failed" },
				minor: { state: "unavailable", reason: "registry_auth_failed" },
			};
		}
	}
	const readAlias = async (alias, immutable = false) => {
		const root = await jsonRequest(`/v2/${repository}/manifests/${alias}`);
		if (
			root.status === 404 &&
			Array.isArray(root.body?.errors) &&
			root.body.errors.length > 0 &&
			root.body.errors.every((error) => error?.code === "MANIFEST_UNKNOWN")
		)
			return { state: "missing" };
		if (root.error || root.status !== 200)
			return {
				state: "unavailable",
				reason: root.error ?? `registry_http_${root.status}`,
			};
		if (immutable && !digestPattern.test(root.digest))
			return { state: "unavailable", reason: "invalid_image_digest" };
		const manifests = [];
		if (manifestTypes.includes(root.body?.mediaType)) manifests.push(root.body);
		else if (
			indexTypes.includes(root.body?.mediaType) &&
			Array.isArray(root.body.manifests)
		) {
			const descriptors = root.body.manifests.filter(
				(item) =>
					item?.annotations?.["vnd.docker.reference.type"] !==
					"attestation-manifest",
			);
			if (!descriptors.length || descriptors.length > 8)
				return { state: "unavailable", reason: "invalid_platform_count" };
			for (const descriptor of descriptors) {
				if (
					!digestPattern.test(descriptor?.digest) ||
					!manifestTypes.includes(descriptor.mediaType)
				)
					return {
						state: "unavailable",
						reason: "invalid_manifest_descriptor",
					};
				const manifest = await jsonRequest(
					`/v2/${repository}/manifests/${descriptor.digest}`,
				);
				if (
					manifest.error ||
					manifest.status !== 200 ||
					!manifestTypes.includes(manifest.body?.mediaType)
				)
					return {
						state: "unavailable",
						reason: "platform_manifest_unavailable",
					};
				manifests.push(manifest.body);
			}
		} else return { state: "unavailable", reason: "invalid_manifest" };
		const versions = [];
		const revisions = [];
		for (const manifest of manifests) {
			if (!digestPattern.test(manifest.config?.digest))
				return { state: "unavailable", reason: "invalid_config_descriptor" };
			const config = await jsonRequest(
				`/v2/${repository}/blobs/${manifest.config.digest}`,
				true,
			);
			if (config.error || config.status !== 200)
				return { state: "unavailable", reason: "image_config_unavailable" };
			try {
				const labels = config.body?.config?.Labels;
				const version = parseTag(labels?.["org.opencontainers.image.version"]);
				if (version.prerelease && !immutable)
					throw new Error("prerelease alias");
				versions.push(version.version);
				if (immutable) {
					const revision = labels?.["org.opencontainers.image.revision"];
					if (!/^[a-f0-9]{40}$/.test(revision ?? ""))
						throw new Error("invalid image revision");
					revisions.push(revision);
				}
			} catch {
				return {
					state: "unavailable",
					reason: immutable
						? "invalid_image_identity"
						: "invalid_image_version",
				};
			}
		}
		if (new Set(versions).size !== 1)
			return { state: "unavailable", reason: "platform_version_disagreement" };
		if (immutable && new Set(revisions).size !== 1)
			return { state: "unavailable", reason: "platform_revision_disagreement" };
		return {
			state: "present",
			version: versions[0],
			...(immutable ? { revision: revisions[0], digest: root.digest } : {}),
		};
	};
	const safeAlias = async (alias, immutable = false) => {
		try {
			return await readAlias(alias, immutable);
		} catch {
			return { state: "unavailable", reason: "malformed_registry_evidence" };
		}
	};
	const versionTag = await safeAlias(selected.version, true);
	if (selected.prerelease) return { versionTag };
	return {
		versionTag,
		latest: await safeAlias("latest"),
		minor: await safeAlias(selected.minor),
	};
}

async function main(args) {
	if (args.length === 3 && args[0] === "--registry") {
		const evidence = await registryEvidence(args[1]);
		await writeFile(args[2], JSON.stringify(evidence));
		console.log(JSON.stringify(evidence));
		return;
	}
	if (args.length === 1 && args[0] === "--help") {
		console.log(
			"usage: node release-publish-guard.mjs TAG SHA STATUS_JSON RELEASES_JSON REGISTRY_JSON [GITHUB_OUTPUT]; --registry TAG EVIDENCE_JSON",
		);
		return;
	}
	if (args.length < 5 || args.length > 6)
		throw new Error(
			"expected tag, revision, status, releases and registry files",
		);
	const [tag, sha, statusFile, releasesFile, registryFile, output] = args;
	const [status, pages, registry] = await Promise.all([
		readFile(statusFile, "utf8"),
		readFile(releasesFile, "utf8"),
		readFile(registryFile, "utf8").catch(() => "{}"),
	]);
	let registryState;
	try {
		registryState = JSON.parse(registry);
	} catch {
		registryState = {};
	}
	const releases = JSON.parse(pages);
	const result = releasePublication(
		tag,
		sha,
		JSON.parse(status),
		releases.flat(),
		registryState,
	);
	for (const [alias, state] of Object.entries(result.aliasStatus)) {
		if (state === "registry_evidence_unavailable")
			console.error(
				`release alias withheld: ${alias} registry_evidence_unavailable`,
			);
	}
	if (output) {
		const dockerTags = result.tags
			.map((value) => `type=raw,value=${value}`)
			.join("\n");
		const imageTags = result.imageTags
			.map((value) => `ghcr.io/icoretech/codex-pooler:${value}`)
			.join("\n");
		await appendFile(
			output,
			`version=${result.version}\nimage_action=${result.imageAction}\nimage_digest=${result.imageDigest}\ndocker_tags<<RELEASE_TAGS\n${dockerTags}\nRELEASE_TAGS\nalias_tags<<RELEASE_ALIASES\n${result.tags.join("\n")}\nRELEASE_ALIASES\nimage_tags<<IMAGE_TAGS\n${imageTags}\nIMAGE_TAGS\n`,
		);
	}
	console.log(JSON.stringify(result));
}

if (
	process.argv[1] &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	await main(process.argv.slice(2));
}
