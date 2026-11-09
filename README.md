# emailfaces

Resolve the avatar associated with an email address from multiple sources. Written in TypeScript, with a Promise API
(plus an optional [RxJS API](#observable-api)) and first-class rate-limit handling.

```ts
import { createAvatarResolver } from "emailfaces";

const avatars = createAvatarResolver();

await avatars.resolve("someone@example.com");
// → { email: 'someone@example.com', source: 'gravatar', url: 'https://gravatar.com/avatar/…?s=200' }
// → null when no source has one
```

Requires a runtime with `fetch` and Web Crypto (Node ≥ 18, browsers, Deno, Bun, edge workers).

## Sources

| Source      | Factory                        | How                                                                                                     | Default limits                           |
| ----------- | ------------------------------ | ------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| Gravatar    | `gravatar()`                   | `HEAD /avatar/<sha256>?d=404`                                                                           | 10 concurrent                            |
| Libravatar  | `libravatar({ baseUrl? })`     | same protocol, federated                                                                                | 4 concurrent                             |
| GitLab      | `gitlab({ baseUrl?, token? })` | `/api/v4/avatar?email=` (public email; Gravatar fallback ignored)                                       | 60/min, 4 concurrent                     |
| GitHub      | `github({ token? })`           | decodes `…@users.noreply.github.com` offline; otherwise user search on public email (single match only) | 10/min (30/min with token), 2 concurrent |
| Domain icon | `domainIcon()`                 | organization icon for non-free-mail domains (opt-in)                                                    | 4 concurrent                             |

Default order: `gravatar, libravatar, gitlab, github`. Pick and order your own:

```ts
import {
  createAvatarResolver,
  gravatar,
  github,
  gitlab,
  domainIcon,
} from "emailfaces";

const avatars = createAvatarResolver({
  sources: [
    gravatar(),
    github({ token: process.env.GITHUB_TOKEN }),
    gitlab(),
    domainIcon(),
  ],
});
```

### Custom sources

A source is a name, a `RateLimiter`, and a `lookup` that returns a URL or `null`, either directly, as a
Promise, or as an Observable (its first value is used; completing empty means "not found").
Make HTTP calls through `ctx.request` to get rate limiting, timeouts, retries, and abort-on-cancel for free.
`ctx.signal` is aborted when the lookup is no longer needed; pass it to any other async work.

```ts
import { RateLimiter, type AvatarSource } from "emailfaces";

const mySource: AvatarSource = {
  name: "directory",
  limiter: new RateLimiter({
    maxConcurrent: 5,
    window: { requests: 100, perMs: 60_000 },
  }),
  lookup: async (email, ctx) => {
    const res = await ctx.request(
      `https://directory.internal/users?email=${encodeURIComponent(email.address)}`,
      {
        read: (res) => res.json() as Promise<{ photo?: string }>,
      },
    );
    return res.status === 200 ? (res.body.photo ?? null) : null;
  },
};
```

The same source as an Observable (unsubscribing still aborts the request):

```ts
import { from, map } from "rxjs";

const mySource$: AvatarSource = {
  name: "directory",
  limiter: new RateLimiter({ maxConcurrent: 5 }),
  lookup: (email, ctx) =>
    from(
      ctx.request(
        `https://directory.internal/users?email=${encodeURIComponent(email.address)}`,
        {
          read: (res) => res.json() as Promise<{ photo?: string }>,
        },
      ),
    ).pipe(
      map((res) => (res.status === 200 ? (res.body.photo ?? null) : null)),
    ),
};
```

## Strategies

- `strategy: 'sequential'` (default) — tries sources one by one, stops at the first hit. Spends the least quota.
- `strategy: 'parallel'` — queries every source at once, and returns the highest-priority hit as soon as all
  higher-priority sources have missed. Slower sources still running are cancelled. Lowest latency.

## Rate limits

Each source has its own `RateLimiter` with:

- **Concurrency cap** (`maxConcurrent`)
- **Sliding window** (`window: { requests, perMs }`) — requests are queued and spaced, not dropped
- **Server-driven pauses** — `429`, `Retry-After`, and `X-RateLimit-*` / `RateLimit-*` headers (including GitHub's
  `403` + `x-ratelimit-remaining: 0`) pause the source until reset. When a response shows the quota is
  used up, the limiter also pauses ahead of time so the next request doesn't get rejected.

A rate-limited request is retried once the pause ends. If waiting would take longer than `maxWaitMs`
(default 30s), the source is skipped for that lookup and reported as `rate_limited`, and the resolver
moves on to the next source. For batch jobs that should just wait, set `maxWaitMs: Infinity`.

To share a budget, for example several resolvers using one GitHub token, pass the same limiter:

```ts
const ghLimiter = new RateLimiter({
  maxConcurrent: 2,
  window: { requests: 30, perMs: 60_000 },
});
const a = createAvatarResolver({
  sources: [github({ token, rateLimit: ghLimiter })],
});
const b = createAvatarResolver({
  sources: [github({ token, rateLimit: ghLimiter })],
});
```

## API

```ts
const avatars = createAvatarResolver(options);

// All accept { signal?: AbortSignal }; aborting cancels in-flight HTTP requests.
avatars.resolve(email); // Promise<Avatar | null>
avatars.lookup(email); // Promise<SourceOutcome[]> – every source's result
avatars.resolveMany(emails, { concurrency: 4 }); // Promise<{ email, avatar, error? }[]>
```

`SourceOutcome.status` is `found` | `not_found` | `rate_limited` | `error`. A source that fails never
fails the whole resolution; it is counted as a miss, and its `error` is kept on the outcome
(`RateLimitedError`, `HttpError`, `RequestTimeoutError`, or whatever the source threw). Invalid emails
reject with `InvalidEmailError`. In `resolveMany`, an invalid email is reported in its own result and
the rest of the batch continues.

### Observable API

Import from `emailfaces/rxjs` to get a resolver that also has an RxJS API. This entry re-exports
everything else, so it can be your only import. Unsubscribing cancels in-flight HTTP requests.

```ts
import { createAvatarResolver } from "emailfaces/rxjs";

const avatars = createAvatarResolver(options);

avatars.resolve$(email); // Observable<Avatar | null>
avatars.lookup$(email); // Observable<SourceOutcome>, emitted as they arrive
avatars.resolveMany$(emails$, { concurrency: 4 }); // Observable<{ email, avatar, error? }>; array, iterable or Observable input
```

Errors behave as in the Promise API: `resolve$` / `lookup$` error with `InvalidEmailError`, and
`resolveMany$` reports invalid emails per item.

RxJS is a regular dependency used internally, so the root entry works without installing it yourself.

### Options

| Option                              | Default             |                                                                 |
| ----------------------------------- | ------------------- | --------------------------------------------------------------- |
| `sources`                           | `defaultSources()`  | ordered by priority                                             |
| `strategy`                          | `'sequential'`      | or `'parallel'`                                                 |
| `size`                              | `200`               | pixels, forwarded to providers                                  |
| `fetch`                             | `globalThis.fetch`  |                                                                 |
| `timeoutMs`                         | `8000`              | per HTTP request (`RequestTimeoutError`)                        |
| `retries`                           | `2`                 | transient errors (5xx, network, timeout) and rate-limit retries |
| `maxWaitMs`                         | `30000`             | max wait on a rate limit before the source is skipped           |
| `cache`                             | `new MemoryCache()` | any `AvatarCache` (sync or async `get`/`set`), or `false`       |
| `cacheTtlMs` / `negativeCacheTtlMs` | 24h / 1h            | how long hits and misses stay cached                            |
| `fallback`                          | —                   | URL or `(email) => url` returned when nothing is found          |

Identical lookups that are already running share one set of requests.

## Development

```sh
pnpm test        # vitest
pnpm typecheck
pnpm build       # → dist/ (ESM + .d.ts)
```
