import { defineConfig } from "astro/config";

// Public landing page for Codex Pooler, served from the domain root.
// The docs stay on their own host for now; every docs link goes through
// `DOCS_URL` in src/data/site.ts so a later move is a one-line change.
export default defineConfig({
  site: "https://www.codex-pooler.com",
  trailingSlash: "ignore",
  build: {
    inlineStylesheets: "auto",
  },
});
