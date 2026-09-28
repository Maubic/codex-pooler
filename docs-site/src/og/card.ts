// Social preview cards for the docs: 1200x630 JPEGs in the landing card's palette. The text
// is drawn as SVG paths from the site's own Roboto Condensed files, so a card looks the same
// on every build machine whatever fonts it has, and sharp rasterises the whole card once.
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import opentype, { type Font } from "opentype.js";
import sharp from "sharp";

// Builds run from docs-site/, locally and in the Pages workflow.
const ROOT = process.cwd();
const load = (file: string) => {
  const bytes = readFileSync(resolve(ROOT, "src/og/fonts", file));
  return opentype.parse(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
};
const BOLD = load("roboto-condensed-700.ttf");
const REGULAR = load("roboto-condensed-400.ttf");
const ICON = readFileSync(resolve(ROOT, "public/icon.svg"), "utf8").replace(/^[\s\S]*?<svg[^>]*>/, "").replace(/<\/svg>\s*$/, "");

const W = 1200;
const H = 630;
const X = 72;

type Options = { size: number; width?: number; lineHeight?: number; maxLines?: number; tracking?: number };

// Greedy word wrap on measured widths; the last allowed line ends with an ellipsis when text is left over.
function wrap(font: Font, text: string, { size, width = Infinity, maxLines = Infinity, tracking = 0 }: Options) {
  const fits = (line: string) => font.getAdvanceWidth(line, size, { tracking }) <= width;
  const lines: string[] = [];
  for (const word of text.split(/\s+/)) {
    const last = lines.at(-1);
    if (last !== undefined && fits(`${last} ${word}`)) lines[lines.length - 1] = `${last} ${word}`;
    else lines.push(word);
  }
  if (lines.length > maxLines) {
    let line = lines[maxLines - 1];
    while (line.includes(" ") && !fits(`${line}…`)) line = line.slice(0, line.lastIndexOf(" "));
    lines.splice(maxLines - 1, lines.length, `${line.replace(/[,.;:]$/, "")}…`);
  }
  return lines;
}

// One block of text as a single SVG path, its top edge at y.
function block(font: Font, text: string, x: number, y: number, fill: string, options: Options) {
  const { size, lineHeight = size * 1.15, tracking = 0 } = options;
  const lines = wrap(font, text, options);
  const ascent = (font.ascender / font.unitsPerEm) * size;
  const d = lines.map((line, i) => font.getPath(line, x, y + ascent + i * lineHeight, size, { tracking }).toPathData(2)).join("");
  const width = Math.max(...lines.map((line) => font.getAdvanceWidth(line, size, { tracking })));
  return { svg: `<path d="${d}" fill="${fill}"/>`, height: lines.length * lineHeight, lines: lines.length, width };
}

export async function docsCard({ title, description, section, path }: { title: string; description?: string; section: string; path: string }) {
  // Brand line beside the mascot: the product in orange, then where the page sits.
  const brand = block(BOLD, "CODEX POOLER", X + 74, 74, "#f28a00", { size: 30, tracking: 60 });
  const trail = block(BOLD, section ? `DOCS  ·  ${section.toUpperCase()}` : "DOCS", X + 74 + brand.width + 14, 74, "#8a8175", { size: 30, tracking: 60 });

  // The title shrinks until it fits three lines.
  let size = 76;
  let heading = block(BOLD, title, X, 182, "#17140f", { size, width: 860, lineHeight: size * 1.02 });
  while (heading.lines > 3 && size > 52) {
    size -= 8;
    heading = block(BOLD, title, X, 182, "#17140f", { size, width: 860, lineHeight: size * 1.02 });
  }

  const summaryTop = 182 + heading.height + 24;
  const room = Math.floor((H - 132 - summaryTop) / 40);
  const summary = description && room > 0 ? block(REGULAR, description, X, summaryTop, "#4a443c", { size: 32, width: 800, lineHeight: 40, maxLines: Math.min(3, room) }) : null;
  const url = block(REGULAR, `www.codex-pooler.com${path.replace(/\/$/, "")}`, X, H - 96, "#6f675c", { size: 27 });

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">
    <defs>
      <pattern id="grid" width="32" height="32" patternUnits="userSpaceOnUse"><path d="M32 0H0V32" fill="none" stroke="#17140f" stroke-opacity="0.05"/></pattern>
      <linearGradient id="fade" x1="0" x2="1"><stop offset="0.5" stop-color="#fff" stop-opacity="0"/><stop offset="1" stop-color="#fff"/></linearGradient>
      <mask id="right"><rect width="${W}" height="${H}" fill="url(#fade)"/></mask>
      <radialGradient id="glow" cx="0.84" cy="0.5" r="0.5"><stop offset="0" stop-color="#ffaa3c" stop-opacity="0.24"/><stop offset="1" stop-color="#ffaa3c" stop-opacity="0"/></radialGradient>
    </defs>
    <rect width="${W}" height="${H}" fill="#fbf7f0"/>
    <rect width="${W}" height="${H}" fill="url(#grid)" mask="url(#right)"/>
    <rect width="${W}" height="${H}" fill="url(#glow)"/>
    <g opacity="0.08"><svg x="920" y="310" width="260" height="260" viewBox="0 0 64 64">${ICON}</svg></g>
    <svg x="${X}" y="62" width="56" height="56" viewBox="0 0 64 64">${ICON}</svg>
    <rect x="${X}" y="154" width="64" height="6" rx="3" fill="#ff9900"/>
    ${brand.svg}${trail.svg}${heading.svg}${summary?.svg ?? ""}${url.svg}
  </svg>`;

  return sharp(Buffer.from(svg)).jpeg({ quality: 84, mozjpeg: true }).toBuffer();
}
