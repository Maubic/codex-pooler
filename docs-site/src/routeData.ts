// Docs head, page by page: the title without a doubled brand, the social card, and one
// TechArticle with its breadcrumbs, tied to the landing page's Organization, WebSite and
// SoftwareApplication by @id. The 404 page only turns indexing off.
import { defineRouteMiddleware, type StarlightRouteData } from "@astrojs/starlight/route-data";
import { BRAND, SITE, cardTitle, cardUrl, fullTitle, sectionOf } from "./og/meta";

type Head = StarlightRouteData["head"][number];

const key = (entry: Head) => String(entry.attrs?.property ?? entry.attrs?.name ?? "");
const meta = (attr: "name" | "property", name: string, content: string): Head => ({ tag: "meta", attrs: { [attr]: name, content } });

// Starlight's own values for these are replaced below.
const REPLACED = new Set(["og:title", "og:locale", "og:image", "og:image:width", "og:image:height", "og:image:type", "og:image:alt", "twitter:image", "twitter:image:alt"]);

export const onRequest = defineRouteMiddleware(({ locals }) => {
  const route = locals.starlightRoute;
  const { entry } = route;
  const head = route.head as Head[];

  if (entry.id === "404") {
    route.head = head.map((item) => (item.tag === "meta" && key(item) === "robots" ? meta("name", "robots", "noindex,follow") : item));
    return;
  }

  const ownTitle = entry.data.head?.find((item) => item.tag === "title")?.content;
  const title = ownTitle ?? fullTitle(entry.data.title);
  const url = `${SITE}/${entry.id}/`;
  const image = cardUrl(entry.id);
  const imageAlt = `${cardTitle(entry.id, entry.data.title)}, from the ${BRAND} docs`;
  const section = sectionOf(entry.id);

  const crumbs = [{ name: BRAND, item: `${SITE}/` }, ...(entry.id === "docs" ? [{ name: "Docs" }] : [{ name: "Docs", item: `${SITE}/docs/` }, ...(section ? [{ name: section }] : []), { name: entry.data.title }])];

  const graph = {
    "@context": "https://schema.org",
    "@graph": [
      {
        "@type": "TechArticle",
        "@id": `${url}#article`,
        headline: title.length <= 110 ? title : entry.data.title,
        description: entry.data.description,
        url,
        image,
        inLanguage: "en",
        ...(route.lastUpdated ? { dateModified: route.lastUpdated.toISOString() } : {}),
        isPartOf: { "@id": `${SITE}/#website` },
        about: { "@id": `${SITE}/#software` },
        publisher: { "@id": `${SITE}/#organization` },
      },
      {
        "@type": "BreadcrumbList",
        itemListElement: crumbs.map((crumb, index) => ({ "@type": "ListItem", position: index + 1, name: crumb.name, ...("item" in crumb ? { item: crumb.item } : {}) })),
      },
    ],
  };

  route.head = [
    ...head.filter((item) => !(item.tag === "meta" && REPLACED.has(key(item)))).map((item) => (item.tag === "title" ? { ...item, content: title } : item)),
    meta("property", "og:title", title),
    meta("property", "og:locale", "en_US"),
    meta("property", "og:image", image),
    meta("property", "og:image:width", "1200"),
    meta("property", "og:image:height", "630"),
    meta("property", "og:image:type", "image/jpeg"),
    meta("property", "og:image:alt", imageAlt),
    meta("name", "twitter:image", image),
    meta("name", "twitter:image:alt", imageAlt),
    { tag: "script", attrs: { type: "application/ld+json" }, content: JSON.stringify(graph) },
  ];
});
