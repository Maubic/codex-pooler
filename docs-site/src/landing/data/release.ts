import { getEntry } from "astro:content";
import manifest from "../../../../.release-please-manifest.json";

// Release facts come from this repository, so the landing page always agrees
// with the deployment docs: release-please keeps the manifest on the latest
// release, and Renovate moves the Helm guide to a new chart once it has been
// out a day.
export const RELEASE_VERSION: string = manifest["."];
export const RELEASE_URL = `https://github.com/icoretech/codex-pooler/releases/tag/codex-pooler-v${RELEASE_VERSION}`;

export async function getChartVersion(): Promise<string> {
  const guide = await getEntry("docs", "docs/deployment/helm");
  const pin = guide?.body?.match(/--version\s+(\d+\.\d+\.\d+)\s+\\/);
  if (!pin) throw new Error("landing: the Helm guide no longer pins a chart --version");
  return pin[1];
}

// The star count is read from the GitHub API at build time. It is optional:
// offline or rate limited, the nav shows the button without a count.
let cachedStars: Promise<number | null> | undefined;

export function getStars(): Promise<number | null> {
  cachedStars ??= (async () => {
    const headers: Record<string, string> = { Accept: "application/vnd.github+json" };
    // A CI token avoids the anonymous rate limit; it is optional.
    const token = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.GITHUB_TOKEN;
    if (token) headers.Authorization = `Bearer ${token}`;
    try {
      const response = await fetch("https://api.github.com/repos/icoretech/codex-pooler", { headers, signal: AbortSignal.timeout(5000) });
      const repo = response.ok ? await response.json() : null;
      return typeof repo?.stargazers_count === "number" ? repo.stargazers_count : null;
    } catch {
      return null;
    }
  })();
  return cachedStars;
}

export function formatStars(stars: number): string {
  return stars >= 1000 ? `${(stars / 1000).toFixed(stars >= 10000 ? 0 : 1)}k` : String(stars);
}
