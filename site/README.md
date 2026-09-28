# Codex Pooler website

The landing page at [www.codex-pooler.com](https://www.codex-pooler.com). The documentation in [`../docs-site`](../docs-site) is published with it, under `/docs`.

A single static page built with [Astro](https://astro.build), with no CSS framework and no client-side libraries: the animations are hand-written CSS and small TypeScript modules.

## Develop

```sh
npm ci
npm run dev      # http://localhost:4321
```

Links into the docs point at `/docs/...` on the same host, which `npm run dev` does not serve. Follow them in a full build (below) or on the published site.

## Build

```sh
npm run check    # astro check (types and templates)
npm run build    # static site in dist/
```

To build the whole published site from the repository root, landing page and docs together, as the Pages workflow does:

```sh
npm ci --prefix site && npm run build --prefix site
npm ci --prefix docs-site && npm run build --prefix docs-site
cp -R docs-site/dist site/dist/docs
cp docs-site/dist/404.html docs-site/dist/llms.txt site/dist/
npm run preview --prefix site
```

The release shown in the hero and pinned in the Compose instructions comes from `.release-please-manifest.json`, and the chart version pinned in the Helm instructions comes from the Helm deployment guide, which Renovate keeps on the latest chart. Both change only through commits, so the page always matches the docs. The GitHub star count is read from the GitHub API at build time and left out when the API can't be reached; the Pages workflow passes `GITHUB_TOKEN` to avoid the anonymous rate limit.

## Page structure

`src/pages/index.astro` assembles the sections from `src/components/`:

| Section | Component | Notes |
| --- | --- | --- |
| Nav | `Nav.astro` | Sticky, with a mobile menu |
| Hero | `Hero.astro` | Live routing console: client requests pass the Key, Pool, Eligible and Route checks inside a Pool and land on the account with the most quota left, with a request log underneath. Same quota model as the simulator: no spontaneous refills, only saved resets |
| Logo strip | `LogoStrip.astro` | Marquee of supported tools, each linking to its setup guide |
| How it works | `Simulator.astro` | "Flip the switch": the same four tools and four accounts without and with Pooler, with an LED status badge. With Pooler the gateway holds two Pools (Product team, Automation), each serving its own tools from its own accounts. Bars show quota left and only drain (faster on smaller plans); an empty account comes back to 100% only when a saved reset is spent. Mirrors the product rules: a Pool spends a saved reset only once all its accounts are dry, one at a time, waiting for confirmation before another; an expiring reset on an account with some usage is spent in its last hour. Without Pooler, Account C's saved reset counts down and expires unused. OpenAI occasionally banks a new saved reset |
| Pools | `Pools.astro` | Three example Pools with their own accounts, keys and rules, one account shared between two |
| OpenAI-compatible | `Compat.astro` | Codex subscriptions in tools without Codex support: a generic settings example and buttons to every tool's setup guide |
| Features | `Features.astro` | Bento: keys, Observatory, MCP, saved resets, Stats and request log, alerts |
| Everything else | `AllFeatures.astro` | Grouped list of the remaining features, each backed by the docs |
| Dashboard | `Dashboard.astro` | Eight tabs of dark-theme admin captures in a browser frame whose URL follows the tab |
| Get started | `GetStarted.astro` | Three steps and a terminal with Docker Compose and Helm tabs, both pinned to the latest stable release |
| Trust | `Trust.astro` | Self-hosting, secrets, privacy, license, and what Pooler is not |
| FAQ | `Faq.astro` | Native `<details>` accordion |
| Final CTA and footer | `FinalCta.astro`, `Footer.astro` | |

Every link into the docs goes through `docs()` in `src/data/site.ts`, which points at `/docs` on the same host. The supported tools and their guide links live in the same file.

All motion respects `prefers-reduced-motion`, and the animations pause when their section is off screen or the tab is hidden.

## Writing rules

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

## Assets

| Path | Source |
| --- | --- |
| `src/assets/pooler.webp` | The mascot, from `.github/assets/pooler.png` (used in the closing call to action) |
| `src/assets/screens/*.webp` | Dark-theme admin captures (1440×900 at 2×) taken from the deterministic screenshot fixtures, converted to WebP. Retake them when the admin UI changes |
| `public/logos/*.svg` | Normalized monochrome marks from `priv/static/images/client-logos/`, with provenance in `assets/client-logos/manifest.json`; the upstream licenses are in `licenses/` |
| `public/favicon.svg` | The mascot's face, drawn for small sizes |
| `public/og.jpg` | 1200×630 social preview |

Product names and logos belong to their owners. OpenAI and Codex are trademarks of OpenAI; Codex Pooler is not affiliated with OpenAI.

## Deploy

Pushes to `main` that touch `site/`, `docs-site/` or `.release-please-manifest.json` run `.github/workflows/pages.yml`. It checks and builds both sites, puts the docs under `/docs`, and publishes the result to GitHub Pages; the custom domain `www.codex-pooler.com` is set in the repository's **Settings → Pages**. The docs' 404 page serves the whole site and forwards links from before the docs moved under `/docs` to their new address.
