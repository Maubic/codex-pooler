// Names shared by the docs head and the docs social cards.

export const SITE = "https://www.codex-pooler.com";
export const BRAND = "Codex Pooler";

// Section names for a docs entry id such as "docs/clients/aider", matching the sidebar groups.
const SECTIONS: Record<string, string> = {
  "getting-started": "Getting started",
  clients: "Clients",
  reference: "Reference",
  operators: "Operators",
  deployment: "Deployment",
  monitoring: "Monitoring",
  discovery: "Guides",
};

export const sectionOf = (id: string) => SECTIONS[id.split("/")[1] ?? ""] ?? "";

// Starlight appends " | Codex Pooler"; a title that already names the product keeps its own words.
export const fullTitle = (title: string) => (/codex pooler/i.test(title) ? title : `${title} | ${BRAND}`);

// The docs home is titled after the product, which the card's brand line already shows.
export const cardTitle = (id: string, title: string) => (id === "docs" ? "Self-hosted Codex gateway docs" : title);

export const cardUrl = (id: string) => `${SITE}/og/${id}.jpg`;
