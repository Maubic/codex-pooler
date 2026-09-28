// Docs pages link to each other with root-relative URLs such as
// /operators/pools/, while the pages are served under /docs next to the
// landing page. This integration adds a Sätteri hast plugin that prefixes
// those page links with /docs in rendered Markdown and MDX. Links to files
// (a path ending in an extension, such as /llms.txt or an image in public/)
// stay at the site root, where public/ is served, and so do protocol-relative
// and relative URLs, anchors, and links that already carry the prefix.
const LINK_ATTRIBUTES = ["href", "src"];

export function docsLinksPlugin(prefix) {
  const withPrefix = (value) => {
    if (!value.startsWith("/") || value.startsWith("//")) return value;
    const path = value.split(/[?#]/)[0];
    if (path === prefix || path.startsWith(`${prefix}/`) || /\.[a-z0-9]+$/i.test(path)) return value;
    return `${prefix}${value}`;
  };

  const rewrite = (node, ctx, name, value) => {
    if (typeof value !== "string") return;
    const next = withPrefix(value);
    if (next !== value) ctx.setProperty(node, name, next);
  };

  const jsxLinks = {
    filter: ["a", "img"],
    visit(node, ctx) {
      for (const attribute of node.attributes ?? []) {
        if (attribute.type === "mdxJsxAttribute" && LINK_ATTRIBUTES.includes(attribute.name)) rewrite(node, ctx, attribute.name, attribute.value);
      }
    },
  };

  return {
    name: "codex-pooler-docs-links",
    element: {
      filter: ["a", "img"],
      visit(node, ctx) {
        for (const name of LINK_ATTRIBUTES) rewrite(node, ctx, name, node.properties?.[name]);
      },
    },
    mdxJsxFlowElement: jsxLinks,
    mdxJsxTextElement: jsxLinks,
  };
}

export default function docsLinks({ prefix }) {
  return {
    name: "codex-pooler-docs-links",
    hooks: {
      "astro:config:setup": ({ config }) => {
        const hastPlugins = config.markdown.processor?.options?.hastPlugins;
        if (!Array.isArray(hastPlugins)) throw new Error("docs links: expected the Sätteri Markdown processor");
        hastPlugins.push(docsLinksPlugin(prefix.replace(/\/+$/, "")));
      },
    },
  };
}
