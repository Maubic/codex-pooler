import { defineCollection } from "astro:content";
import { docsLoader, i18nLoader } from "@astrojs/starlight/loaders";
import { docsSchema, i18nSchema } from "@astrojs/starlight/schema";

// The landing page is the site root, so the docs are served under /docs
// without moving their files: every entry id gets a docs/ prefix. The 404
// page keeps its root id, so it stays the 404 page for the whole site.
const underDocs = ({ entry, data }: { entry: string; data: Record<string, unknown> }) => {
  const slug = typeof data.slug === "string" ? data.slug : entry.replace(/\.mdx?$/, "").replace(/(^|\/)index$/, "");
  if (slug === "404") return slug;
  return slug ? `docs/${slug}` : "docs";
};

export const collections = {
  docs: defineCollection({
    loader: docsLoader({ generateId: underDocs }),
    schema: docsSchema(),
  }),
  i18n: defineCollection({
    loader: i18nLoader(),
    schema: i18nSchema(),
  }),
};
