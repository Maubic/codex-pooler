// Docs images are served from public/ as they are. This integration adds a Sätteri hast
// plugin that gives each Markdown image from public/ its intrinsic width and height, so the
// page keeps its layout while the image arrives. The first image of a page, usually the
// banner that is its largest paint, loads eagerly with high priority; every later one loads
// lazily.
import { readFileSync } from "node:fs";

// Width and height from the file header, for the formats the docs use.
export function imageSize(bytes) {
  if (bytes.length > 24 && bytes.readUInt32BE(0) === 0x89504e47) return { width: bytes.readUInt32BE(16), height: bytes.readUInt32BE(20) };
  if (bytes.length > 30 && bytes.toString("ascii", 0, 4) === "RIFF" && bytes.toString("ascii", 8, 12) === "WEBP") {
    const chunk = bytes.toString("ascii", 12, 16);
    if (chunk === "VP8X") return { width: 1 + bytes.readUIntLE(24, 3), height: 1 + bytes.readUIntLE(27, 3) };
    if (chunk === "VP8L") {
      const bits = bytes.readUInt32LE(21);
      return { width: 1 + (bits & 0x3fff), height: 1 + ((bits >> 14) & 0x3fff) };
    }
    if (chunk === "VP8 ") return { width: bytes.readUInt16LE(26) & 0x3fff, height: bytes.readUInt16LE(28) & 0x3fff };
  }
  if (bytes[0] === 0xff && bytes[1] === 0xd8) {
    for (let i = 2; i + 9 < bytes.length; ) {
      if (bytes[i] !== 0xff) {
        i++;
        continue;
      }
      const marker = bytes[i + 1];
      if (marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc) return { width: bytes.readUInt16BE(i + 7), height: bytes.readUInt16BE(i + 5) };
      i += 2 + bytes.readUInt16BE(i + 2);
    }
  }
  return null;
}

export function docsImagesPlugin(publicDir) {
  const sizes = new Map();
  const sizeOf = (path) => {
    if (!sizes.has(path)) {
      let size = null;
      try {
        size = imageSize(readFileSync(new URL(`.${path}`, publicDir)));
      } catch {}
      sizes.set(path, size);
    }
    return sizes.get(path);
  };

  // A factory runs once per document, so "first image" means the first on this page.
  return () => {
    let first = true;
    return {
      name: "codex-pooler-docs-images",
      element: {
        filter: ["img"],
        visit(node, ctx) {
          const src = node.properties?.src;
          if (typeof src !== "string" || !src.startsWith("/") || src.startsWith("//")) return;
          const size = sizeOf(decodeURIComponent(src.split(/[?#]/)[0]));
          if (size && node.properties.width == null && node.properties.height == null) {
            ctx.setProperty(node, "width", size.width);
            ctx.setProperty(node, "height", size.height);
          }
          if (node.properties.loading == null) ctx.setProperty(node, "loading", first ? "eager" : "lazy");
          if (first) ctx.setProperty(node, "fetchpriority", "high");
          ctx.setProperty(node, "decoding", "async");
          first = false;
        },
      },
    };
  };
}

export default function docsImages() {
  return {
    name: "codex-pooler-docs-images",
    hooks: {
      "astro:config:setup": ({ config }) => {
        const hastPlugins = config.markdown.processor?.options?.hastPlugins;
        if (!Array.isArray(hastPlugins)) throw new Error("docs images: expected the Sätteri Markdown processor");
        hastPlugins.push(docsImagesPlugin(config.publicDir));
      },
    },
  };
}
