# Client logos

The source marks and provenance live beside this file. `manifest.json` records each source URL, revision when available, checksum, output format and exported sizes. Product marks belong to their respective owners; library attribution does not transfer trademark ownership. The AI SDK wordmark is retained for larger uses; request rows use the Vercel symbol.

Normalized SVGs and transparent grayscale PNGs live in `priv/static/images/client-logos/`. SVGs use a 24x24 canvas with the painted mark fitted proportionally into a centered 20x20 box. Request rows use the SVG as an alpha mask and inherit the UI's neutral text tone. PNG sizes are 16, 24, 32, 48, 64, 128 and 256 pixels. LiteLLM's official train source is a 160px raster, so its exports stop at 128px; it remains a grayscale image instead of masquerading as a vector or losing its internal detail in a silhouette mask.

Regenerate with `node assets/client-logos/build.mjs`. The standalone build requires ImageMagick and `rsvg-convert`, has no network access and does not run during application builds. Sources are vendored so runtime and tests do not depend on an external website. The SVG recipes preserve path geometry, treat OMP's dark connector slots as knockouts, remove the Pooler favicon's background tile, and retain secondary tones where a single silhouette would merge meaningful shapes.

When adding a mark, inspect the source, add its provenance, regenerate, and check native 14px/16px output on light and dark backgrounds. A recognizable client prefix can select a logo; a generic SDK or runtime user agent does not identify the harness behind it. Keep the existing fallback icon when no logo or reliable classification is available.
