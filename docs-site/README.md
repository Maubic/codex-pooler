# Codex Pooler website

The public website at [www.codex-pooler.com](https://www.codex-pooler.com), in one [Astro](https://astro.build) project: the landing page at the site root and the documentation, built with [Starlight](https://starlight.astro.build), under `/docs`.

## Develop

```sh
npm ci
npm run dev      # http://localhost:4321/ and http://localhost:4321/docs/
```

From the repository root, `make dev` also reinstalls this project's locked dependencies before starting Phoenix. Use `make dev-docs-deps` to refresh only the website dependencies; it does not start or build the website.

## Check and build

```sh
npm run check    # astro check and the docs contract checks
npm run build    # static site in dist/
npm run preview  # serve the build locally
```

## Layout

| Path | What |
| --- | --- |
| `src/pages/index.astro` | The landing page, assembled from `src/landing/components/` |
| `src/landing/` | Landing page components, layout, styles, data and images |
| `src/content/docs/` | Documentation pages. `src/content.config.ts` gives their ids a `docs/` prefix, so they are served under `/docs` without living in a `docs/` folder |
| `plugins/docs-links.mjs` | Adds `/docs` to root-relative page links in the docs, so pages keep linking with paths like `/operators/pools/` |
| `plugins/docs-images.mjs` | Gives docs images from `public/` their width and height, loads a page's first image eagerly with high priority and the rest lazily |
| `src/routeData.ts` | Starlight route middleware: page titles without a doubled brand, the social card tags, and a TechArticle with breadcrumbs per docs page |
| `src/og/`, `src/pages/og/` | Build-time social cards, one 1200x630 JPEG per docs page at `/og/<page id>.jpg`, with the text drawn as paths from the Roboto Condensed files in `src/og/fonts/` |
| `public/` | Files served at the site root: images (the docs banners are 1440px WebP copies of the README banners), `llms.txt`, `answers.md`, `pricing.md`, `robots.txt`, the landing page's social card `og.jpg`, the monitoring dashboards, and the landing page's logos and icons |
| `scripts/`, `dashboards/` | Checks run by `npm run check`, and the dashboard build |

The 404 page (`src/content/docs/404.mdx`) serves the whole site and forwards links from before the docs moved under `/docs` to their new address.

The landing page carries one structured-data graph (Organization, WebSite, SoftwareApplication) that every docs page points at by `@id`. The docs use the same variable Roboto Condensed WOFF2 files as the landing page, from `@fontsource-variable/roboto-condensed`.

## Landing page

`src/pages/index.astro` assembles the sections from `src/landing/components/`:

| Section | Component | Notes |
| --- | --- | --- |
| Nav | `Nav.astro` | Sticky, with a mobile menu |
| Hero | `Hero.astro` | Live routing console: client requests pass the Key, Pool, Eligible and Route checks inside a Pool and land on the account with the most quota left, with a request log underneath. Same quota model as the simulator: no spontaneous refills, only saved resets |
| Logo strip | `LogoStrip.astro` | Marquee of supported tools, each linking to its setup guide |
| How it works | `Simulator.astro` | "Flip the switch": the same four tools and four accounts without and with Pooler, with an LED status badge. With Pooler the gateway holds two Pools (Product team, Automation), each serving its own tools from its own accounts. Bars show quota left and only drain (faster on smaller plans); an empty account comes back to 100% only when a saved reset is spent. Mirrors the product rules: a Pool spends a saved reset only once all its accounts are dry, one at a time, waiting for confirmation before another; an expiring reset on an account with some usage is spent in its last hour. Without Pooler, Account C's saved reset counts down and expires unused. OpenAI occasionally banks a new saved reset |
| Pools | `Pools.astro` | Three example Pools with their own accounts, keys and rules, one account shared between two |
| Just one account? | `OneAccount.astro` | Five agents signed in with one Codex login next to the same agents holding Pool keys: a copy of the login asks to sign in again, key counts climb, one key pauses without touching the rest. Linked from the FAQ |
| Guardrails | `Guardrails.astro` | Example requests walk the checks in the order Pooler applies them (firewall, key, Pool switches, model, request size, token windows); a request that fails a check stops there with the documented status and code |
| OpenAI-compatible | `Compat.astro` | Codex subscriptions in tools without Codex support: a generic settings example and buttons to every tool's setup guide |
| Features | `Features.astro` | Bento: keys, Observatory, MCP, saved resets, Stats and request log, alerts |
| Everything else | `AllFeatures.astro` | Grouped list of the remaining features, each backed by the docs |
| Dashboard | `Dashboard.astro` | Eight tabs of dark-theme admin captures in a browser frame whose URL follows the tab |
| Get started | `GetStarted.astro` | Three steps and a terminal with Docker Compose and Helm tabs, both pinned to the latest stable release |
| Trust | `Trust.astro` | Self-hosting, secrets, privacy, license, and what Pooler is not |
| FAQ | `Faq.astro` | Native `<details>` accordion |
| Final CTA and footer | `FinalCta.astro`, `Footer.astro` | |

Every link into the docs goes through `docs()` in `src/landing/data/site.ts`, which points at `/docs`. The supported tools and their guide links live in the same file.

All motion respects `prefers-reduced-motion`, and the animations pause when their section is off screen or the tab is hidden.

The release shown in the hero and pinned in the Compose instructions comes from `.release-please-manifest.json`, and the chart version pinned in the Helm instructions comes from the Helm deployment guide, which Renovate keeps on the latest chart. Both change only through commits, so the landing page always matches the docs. The GitHub star count is read from the GitHub API at build time and left out when the API can't be reached; the Pages workflow passes `GITHUB_TOKEN` to avoid the anonymous rate limit.

### Writing rules

The page makes public claims about the product, so keep them measured and checkable against the docs:

- Describe `/v1` support as OpenAI-compatible for the requests coding tools need, never as full OpenAI API parity.
- Never suggest that Pooler gets around OpenAI's limits or terms.
- The license is the Elastic License 2.0: say "source available" or "free to self-host", not "open source".
- Keep examples generic: `codex-pooler.example.com`, "Account A", "Nightly agent". No real account, Pool, install or person names.
- Pools are plural: the point is splitting accounts and traffic into as many Pools as you need, so never sell Pooler as "one pool".
- Quota bars always show what is left and drain. In the animations they return to full only when a saved reset is spent; don't show accounts resetting on their own.
- Don't single out one tool: name tools only as examples among several.
- No "demo data" or "illustration" disclaimers on product visuals; the captures come from deterministic fixtures, not from anyone's traffic.
- Privacy is a fact to state once, not the headline. Traffic can leave through the operator's own HTTP proxy, so never claim requests go "straight" to OpenAI.

### Assets

| Path | Source |
| --- | --- |
| `src/landing/assets/pooler.webp` | The mascot, from `.github/assets/pooler.png` (used in the closing call to action) |
| `src/landing/assets/screens/*.webp` | Dark-theme admin captures (1440×900 at 2×) taken from the deterministic screenshot fixtures, converted to WebP. Retake them when the admin UI changes |
| `public/logos/*.svg` | Normalized monochrome marks from `priv/static/images/client-logos/`, with provenance in `assets/client-logos/manifest.json` and the upstream licenses next to it |
| `public/icon.svg` | The mascot's face, drawn for small sizes: the site icon for every page |
| `public/og.jpg` | 1200×630 social preview |

Product names and logos belong to their owners. OpenAI and Codex are trademarks of OpenAI; Codex Pooler is not affiliated with OpenAI.

## Deploy

Pushes to `main` that touch `docs-site/` or `.release-please-manifest.json` run `.github/workflows/pages.yml`, which checks, builds and publishes `dist/` to GitHub Pages. The custom domain `www.codex-pooler.com` is set in the repository's **Settings → Pages**.
