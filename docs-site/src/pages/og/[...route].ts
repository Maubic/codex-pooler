// One social preview card per docs page, at /og/<entry id>.jpg.
import type { APIRoute, GetStaticPaths } from "astro";
import { getCollection, type CollectionEntry } from "astro:content";
import { docsCard } from "../../og/card";
import { cardTitle, sectionOf } from "../../og/meta";

export const getStaticPaths = (async () => {
  const pages = await getCollection("docs", (entry) => entry.id !== "404");
  return pages.map((entry) => ({ params: { route: `${entry.id}.jpg` }, props: { entry } }));
}) satisfies GetStaticPaths;

export const GET: APIRoute<{ entry: CollectionEntry<"docs"> }> = async ({ props: { entry } }) => {
  const jpg = await docsCard({
    title: cardTitle(entry.id, entry.data.title),
    description: entry.data.description,
    section: sectionOf(entry.id),
    path: `/${entry.id}/`,
  });
  return new Response(new Uint8Array(jpg), { headers: { "Content-Type": "image/jpeg" } });
};
