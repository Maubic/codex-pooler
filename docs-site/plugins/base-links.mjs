// Docs pages link to each other and to files in public/ with root-relative
// URLs such as /operators/pools/. The docs are served under a base path, so
// this integration adds a Sätteri hast plugin that prefixes those URLs with
// the base in rendered Markdown and MDX. URLs that already carry the base,
// protocol-relative and relative URLs, and anchors are left alone.
const LINK_ATTRIBUTES = ["href", "src"];

export function baseLinksPlugin(base) {
  const prefix = base.replace(/\/+$/, "");

  const withBase = (value) => {
    if (!value.startsWith("/") || value.startsWith("//")) return value;
    if (value === prefix || /^[/?#]/.test(value.slice(prefix.length)) && value.startsWith(prefix)) return value;
    return `${prefix}${value}`;
  };

  const rewrite = (node, ctx, name, value) => {
    if (typeof value !== "string") return;
    const next = withBase(value);
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
    name: "codex-pooler-base-links",
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

export default function baseLinks({ base }) {
  return {
    name: "codex-pooler-docs-base-links",
    hooks: {
      "astro:config:setup": ({ config }) => {
        const hastPlugins = config.markdown.processor?.options?.hastPlugins;
        if (!Array.isArray(hastPlugins)) throw new Error("docs base links: expected the Sätteri Markdown processor");
        if (base.replace(/\/+$/, "")) hastPlugins.push(baseLinksPlugin(base));
      },
    },
  };
}
