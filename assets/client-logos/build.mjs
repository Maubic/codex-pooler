import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const directory = dirname(fileURLToPath(import.meta.url));
const output = resolve(directory, "../../priv/static/images/client-logos");
const manifestFile = join(directory, "manifest.json");
const args = process.argv.slice(2);

if (args.length === 1 && args[0] === "--help") {
	console.log(
		"Usage: node assets/client-logos/build.mjs\nRebuild normalized SVG and transparent grayscale PNG assets from the tracked sources and manifest. Requires rsvg-convert and ImageMagick.",
	);
	process.exit(0);
}
if (args.length > 0) {
	console.error("Unsupported argument. Use --help for usage.");
	process.exit(2);
}

const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
const temporary = mkdtempSync(join(tmpdir(), "client-logos-"));
const run = (binary, parameters) =>
	execFileSync(binary, parameters, { encoding: "utf8" }).trim();

function prepareSvg(name, source) {
	let svg = source
		.replace(/<\?xml[^>]*\?>/g, "")
		.replace(/<!--[\s\S]*?-->/g, "")
		.replace(/<!DOCTYPE[^>]*>/g, "")
		.replace(/<metadata[\s\S]*?<\/metadata>/g, "")
		.trim();

	// These recipes apply only to the inspected, vendored marks. Keep their paths.
	if (name === "opencode") {
		svg = svg
			.replaceAll('fill="#CFCECD"', 'fill="currentColor" opacity="0.28"')
			.replaceAll('fill="#211E1E"', 'fill="currentColor"');
	} else if (name === "omp") {
		const body = svg
			.match(/<svg\b[^>]*>([\s\S]*)<\/svg>/)[1]
			.replaceAll('fill="#fafafa"', 'fill="white"')
			.replaceAll('fill="#f97316"', 'fill="white"')
			.replaceAll('fill="#0d0d0d"', 'fill="black"');
		svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 90"><defs><mask id="omp-ink" maskUnits="userSpaceOnUse" x="0" y="0" width="120" height="90">${body}</mask></defs><rect width="120" height="90" fill="currentColor" mask="url(#omp-ink)"/></svg>`;
	} else if (name === "codex-pooler") {
		svg = svg
			.replace(/<rect\b[^>]*\/>/, "")
			.replace(/#[0-9a-f]{6}/gi, "currentColor");
	} else if (name === "windmill") {
		svg = svg.replace(
			/<style[\s\S]*?<\/style>/,
			"<style>.st2{fill:currentColor;opacity:.4}.st3{fill:currentColor}</style>",
		);
	} else if (name === "pi") {
		svg = svg
			.replaceAll('fill="#F09082"', 'fill="currentColor"')
			.replaceAll('fill="#4D9ABF"', 'fill="currentColor" opacity="0.6"')
			.replaceAll('fill="#F1BE58"', 'fill="currentColor" opacity="0.8"');
	} else {
		// Monochrome source paints; white clip-path geometry is not visible paint.
		svg = svg.replace(
			/\b(fill|stroke)="(?:#[0-9a-f]{3,8}|black)"/gi,
			'$1="currentColor"',
		);
	}

	svg = svg.replace(/[ \t]+$/gm, "");
	const match = svg.match(/<svg\b([^>]*)>([\s\S]*)<\/svg>\s*$/);
	if (!match) throw new Error(`Invalid SVG source: ${name}`);
	const attributes = match[1].replace(
		/\s(?:width|height|x|y|style)="[^"]*"/g,
		"",
	);
	if (!attributes.includes("viewBox="))
		throw new Error(`Missing viewBox: ${name}`);
	return { attributes, body: match[2] };
}

function bounds(png) {
	const text = run("magick", [
		png,
		"-alpha",
		"extract",
		"-threshold",
		"0",
		"-bordercolor",
		"black",
		"-border",
		"1",
		"-format",
		"%@",
		"info:",
	]);
	const match = text.match(/^(\d+)x(\d+)\+(\d+)\+(\d+)$/);
	if (!match) throw new Error(`Invalid painted bounds: ${text}`);
	const [, width, height, x, y] = match.map(Number);
	if (width === 0 || height === 0) throw new Error("Empty logo");
	return { width, height, x: x - 1, y: y - 1 };
}

try {
	mkdirSync(output, { recursive: true });
	for (const logo of manifest.logos) {
		if (!/^[a-z0-9-]+$/.test(logo.name)) throw new Error("Invalid asset name");
		const sourceFile = join(directory, "sources", logo.source);
		const source = readFileSync(sourceFile);
		logo.source_sha256 = createHash("sha256").update(source).digest("hex");

		if (logo.source.endsWith(".webp")) {
			logo.format = "png";
			logo.sizes = manifest.png_sizes.filter((size) => size <= 128);
			for (const size of logo.sizes) {
				const content = Math.round((size * manifest.content) / manifest.canvas);
				run("magick", [
					sourceFile,
					"-colorspace",
					"Gray",
					"-colorspace",
					"sRGB",
					"-trim",
					"+repage",
					"-resize",
					`${content}x${content}`,
					"-gravity",
					"center",
					"-background",
					"none",
					"-extent",
					`${size}x${size}`,
					"-strip",
					join(output, `${logo.name}-${size}.png`),
				]);
			}
			continue;
		}

		const { attributes, body } = prepareSvg(logo.name, source.toString("utf8"));
		const draft = join(temporary, `${logo.name}.svg`);
		const raster = join(temporary, `${logo.name}.png`);
		writeFileSync(
			draft,
			`<svg${attributes} width="1024" height="1024">${body}</svg>`,
		);
		run("rsvg-convert", [
			"--width",
			"1024",
			"--height",
			"1024",
			"--output",
			raster,
			draft,
		]);
		const box = bounds(raster);
		const scale = manifest.content / Math.max(box.width, box.height);
		const x = (manifest.canvas - box.width * scale) / 2 - box.x * scale;
		const y = (manifest.canvas - box.height * scale) / 2 - box.y * scale;
		const normalized = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="currentColor"><g transform="translate(${x} ${y}) scale(${scale})"><svg${attributes} width="1024" height="1024">${body}</svg></g></svg>\n`;
		const target = join(output, `${logo.name}.svg`);
		writeFileSync(target, normalized);
		logo.format = "svg";
		logo.painted_bounds_xywh_at_1024 = [box.x, box.y, box.width, box.height];
		logo.sizes = manifest.png_sizes;

		const tinted = join(temporary, `${logo.name}-gray.svg`);
		writeFileSync(
			tinted,
			normalized.replace("<svg xmlns=", '<svg style="color:#666666" xmlns='),
		);
		for (const size of logo.sizes) {
			run("rsvg-convert", [
				"--width",
				String(size),
				"--height",
				String(size),
				"--output",
				join(output, `${logo.name}-${size}.png`),
				tinted,
			]);
		}
	}
	writeFileSync(manifestFile, `${JSON.stringify(manifest, null, 2)}\n`);
	console.log(`Built ${manifest.logos.length} logos in ${output}`);
} finally {
	rmSync(temporary, { recursive: true, force: true });
}
