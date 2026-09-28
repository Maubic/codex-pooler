import manifest from "../../../.release-please-manifest.json";
import helmGuide from "../../../docs-site/src/content/docs/deployment/helm.mdx?raw";
import { REPO_URL } from "./site";

// Release facts come from this repository, so the page always agrees with the
// deployment docs: release-please keeps the manifest on the latest release,
// and Renovate moves the Helm guide to a new chart once it has been out a day.
export const RELEASE_VERSION: string = manifest["."];
export const RELEASE_URL = `${REPO_URL}/releases/tag/codex-pooler-v${RELEASE_VERSION}`;

const chartPin = helmGuide.match(/--version\s+(\d+\.\d+\.\d+)\s+\\/);
if (!chartPin) throw new Error("site: the Helm guide no longer pins a chart --version");
export const CHART_VERSION: string = chartPin[1];

// The star count is read from the GitHub API at build time. It is optional:
// offline or rate limited, the nav shows the button without a count.
const API = "https://api.github.com/repos/icoretech/codex-pooler";

let cachedStars: Promise<number | null> | undefined;

export function getStars(): Promise<number | null> {
  cachedStars ??= (async () => {
    const headers: Record<string, string> = { Accept: "application/vnd.github+json" };
    // A CI token avoids the anonymous rate limit; it is optional.
    const token = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.GITHUB_TOKEN;
    if (token) headers.Authorization = `Bearer ${token}`;
    try {
      const response = await fetch(API, { headers, signal: AbortSignal.timeout(5000) });
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
