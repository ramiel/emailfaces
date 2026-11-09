import { setTimeout as sleep } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';
import {
  AvatarResolver,
  InvalidEmailError,
  RateLimiter,
  domainIcon,
  github,
  gitlab,
  gravatar,
  retryAfterMs,
  sha256Hex,
  type AvatarSource,
} from '../src/index.js';
import { fakeFetch, json, status } from './helpers.js';

const fixed = (name: string, url: string | null, ms = 0, calls?: string[]): AvatarSource => ({
  name,
  limiter: new RateLimiter(),
  lookup: async () => {
    calls?.push(name);
    if (ms) await sleep(ms);
    return url;
  },
});

describe('resolve (strategies)', () => {
  it('sequential stops at the first hit and never calls later sources', async () => {
    const calls: string[] = [];
    const r = new AvatarResolver({ sources: [fixed('a', null, 0, calls), fixed('b', 'B', 0, calls), fixed('c', 'C', 0, calls)] });
    expect(await r.resolve('  Foo@Example.com ')).toEqual({ email: 'foo@example.com', source: 'b', url: 'B' });
    expect(calls).toEqual(['a', 'b']);
  });

  it('parallel respects priority even when a lower-priority source answers first', async () => {
    const r = new AvatarResolver({ strategy: 'parallel', sources: [fixed('slow', 'S', 60), fixed('fast', 'F', 5)] });
    expect((await r.resolve('a@b.co'))?.source).toBe('slow');
  });

  it('parallel returns early and aborts lower-priority sources', async () => {
    let aborted = false;
    const hang: AvatarSource = {
      name: 'hang',
      limiter: new RateLimiter(),
      lookup: (_, ctx) =>
        new Promise((_, reject) =>
          ctx.signal.addEventListener('abort', () => {
            aborted = true;
            reject(ctx.signal.reason);
          }),
        ),
    };
    const r = new AvatarResolver({ strategy: 'parallel', sources: [fixed('a', 'A', 5), hang] });
    expect((await r.resolve('a@b.co'))?.url).toBe('A');
    expect(aborted).toBe(true);
  });

  it('returns null, or the fallback, when nothing is found', async () => {
    expect(await new AvatarResolver({ sources: [fixed('a', null)] }).resolve('a@b.co')).toBeNull();
    const r = new AvatarResolver({ sources: [fixed('a', null)], fallback: (e) => `https://x/${e}` });
    expect(await r.resolve('a@b.co')).toEqual({ email: 'a@b.co', source: 'fallback', url: 'https://x/a@b.co' });
  });

  it('rejects invalid emails', async () => {
    await expect(new AvatarResolver({ sources: [] }).resolve('nope')).rejects.toBeInstanceOf(InvalidEmailError);
  });

  it('treats a failing source as a miss and moves on', async () => {
    const broken: AvatarSource = { name: 'broken', limiter: new RateLimiter(), lookup: () => { throw new Error('boom'); } };
    const r = new AvatarResolver({ sources: [broken, fixed('ok', 'OK')] });
    expect((await r.resolve('a@b.co'))?.source).toBe('ok');
    const outcomes = await r.lookup('a@b.co');
    expect(outcomes.find((o) => o.source === 'broken')?.status).toBe('error');
  });
});

describe('caching and deduplication', () => {
  it('caches hits and misses', async () => {
    const calls: string[] = [];
    const r = new AvatarResolver({ sources: [fixed('a', null, 0, calls), fixed('b', 'B', 0, calls)] });
    await r.resolve('a@b.co');
    await r.resolve('A@B.CO');
    expect(calls).toEqual(['a', 'b']);
    const outcomes = await r.lookup('a@b.co');
    expect(outcomes.every((o) => 'cached' in o && o.cached)).toBe(true);
  });

  it('shares concurrent lookups for the same email', async () => {
    const calls: string[] = [];
    const r = new AvatarResolver({ cache: false, sources: [fixed('a', 'A', 20, calls)] });
    await Promise.all([r.resolve('a@b.co'), r.resolve('a@b.co'), r.resolve('a@b.co')]);
    expect(calls).toEqual(['a']);
  });
});

describe('abort', () => {
  it('rejects with the abort reason and aborts the underlying fetch', async () => {
    const { fetch } = fakeFetch({ 'gravatar.com': () => status(200) }, { delayMs: 200 });
    const r = new AvatarResolver({ fetch, sources: [gravatar()] });
    const ctrl = new AbortController();
    const p = r.resolve('a@b.co', { signal: ctrl.signal });
    setTimeout(() => ctrl.abort(new Error('stop')), 10);
    await expect(p).rejects.toThrow('stop');
  });
});

describe('rate limits over HTTP', () => {
  it('pauses on 429 + Retry-After, then retries and succeeds', async () => {
    let n = 0;
    const { fetch, calls } = fakeFetch({ 'gravatar.com': () => (n++ === 0 ? status(429, { 'retry-after': '0.1' }) : status(200)) });
    const r = new AvatarResolver({ fetch, sources: [gravatar()] });
    const t0 = Date.now();
    expect((await r.resolve('a@b.co'))?.source).toBe('gravatar');
    expect(calls).toHaveLength(2);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(95);
  });

  it('skips a source whose rate-limit wait exceeds maxWaitMs, reporting rate_limited', async () => {
    const { fetch } = fakeFetch({
      'api.github.com': () => status(403, { 'x-ratelimit-remaining': '0', 'x-ratelimit-reset': String(Math.floor(Date.now() / 1000) + 3600) }),
      'gravatar.com': () => status(404),
    });
    const gh = github();
    const r = new AvatarResolver({ fetch, maxWaitMs: 1000, sources: [gh, gravatar()] });
    const outcomes = await r.lookup('a@b.co');
    expect(outcomes.find((o) => o.source === 'github')?.status).toBe('rate_limited');
    expect(gh.limiter.pausedFor).toBeGreaterThan(3_000_000);
    // Subsequent lookups fail fast instead of hammering the API.
    const t0 = Date.now();
    const again = await r.lookup('other@b.co');
    expect(again.find((o) => o.source === 'github')?.status).toBe('rate_limited');
    expect(Date.now() - t0).toBeLessThan(200);
  });

  it('retries transient 5xx errors', async () => {
    let n = 0;
    const { fetch } = fakeFetch({ 'gravatar.com': () => (n++ < 2 ? status(503) : status(200)) });
    const r = new AvatarResolver({ fetch, sources: [gravatar()] });
    expect((await r.resolve('a@b.co'))?.source).toBe('gravatar');
    expect(n).toBe(3);
  });

  it('parses Retry-After and reset headers', () => {
    const now = 1_700_000_000_000;
    expect(retryAfterMs(new Headers({ 'retry-after': '3' }), now)).toBe(3000);
    expect(retryAfterMs(new Headers({ 'retry-after': new Date(now + 5000).toUTCString() }), now)).toBe(5000);
    expect(retryAfterMs(new Headers({ 'x-ratelimit-reset': String(now / 1000 + 10) }), now)).toBe(10_000);
    expect(retryAfterMs(new Headers({ 'ratelimit-reset': '7' }), now)).toBe(7000);
    expect(retryAfterMs(new Headers(), now)).toBeUndefined();
  });
});

describe('sources', () => {
  it('gravatar probes the sha256 hash with d=404', async () => {
    const { fetch, calls } = fakeFetch({ 'gravatar.com': () => status(200) });
    const r = new AvatarResolver({ fetch, size: 64, sources: [gravatar()] });
    const hash = await sha256Hex('a@b.co');
    expect((await r.resolve('a@b.co'))?.url).toBe(`https://gravatar.com/avatar/${hash}?s=64`);
    expect(calls[0]).toBe(`https://gravatar.com/avatar/${hash}?s=64&d=404`);
  });

  it('github decodes noreply addresses without any request', async () => {
    const { fetch, calls } = fakeFetch({});
    const r = new AvatarResolver({ fetch, sources: [github()] });
    expect((await r.resolve('123+octo@users.noreply.github.com'))?.url).toBe('https://avatars.githubusercontent.com/u/123?s=200');
    expect((await r.resolve('octo@users.noreply.github.com'))?.url).toBe('https://github.com/octo.png?size=200');
    expect(calls).toHaveLength(0);
  });

  it('github only accepts unambiguous search matches', async () => {
    const { fetch } = fakeFetch({
      'one%40': () => json({ total_count: 1, items: [{ avatar_url: 'https://avatars.githubusercontent.com/u/1?v=4' }] }),
      'two%40': () => json({ total_count: 2, items: [{ avatar_url: 'x' }, { avatar_url: 'y' }] }),
    });
    const r = new AvatarResolver({ fetch, sources: [github({ token: 't' })] });
    expect((await r.resolve('one@x.io'))?.url).toBe('https://avatars.githubusercontent.com/u/1?v=4&s=200');
    expect(await r.resolve('two@x.io')).toBeNull();
  });

  it('gitlab ignores its gravatar fallback', async () => {
    const { fetch } = fakeFetch({
      'mine%40': () => json({ avatar_url: 'https://gitlab.com/uploads/-/system/user/avatar/1/a.png' }),
      'other%40': () => json({ avatar_url: 'https://secure.gravatar.com/avatar/abc?s=200&d=identicon' }),
    });
    const r = new AvatarResolver({ fetch, sources: [gitlab()] });
    expect((await r.resolve('mine@x.io'))?.url).toBe('https://gitlab.com/uploads/-/system/user/avatar/1/a.png');
    expect(await r.resolve('other@x.io')).toBeNull();
  });

  it('domainIcon skips free-mail domains', async () => {
    const { fetch, calls } = fakeFetch({ 'duckduckgo.com': () => status(200) });
    const r = new AvatarResolver({ fetch, sources: [domainIcon()] });
    expect(await r.resolve('someone@gmail.com')).toBeNull();
    expect((await r.resolve('someone@acme.io'))?.url).toBe('https://icons.duckduckgo.com/ip3/acme.io.ico');
    expect(calls).toHaveLength(1);
  });
});

describe('resolveMany', () => {
  it('reports invalid emails without failing the batch', async () => {
    const r = new AvatarResolver({ sources: [fixed('a', 'A')] });
    const results = await r.resolveMany(['x@y.io', 'bad', 'z@y.io']);
    const byEmail = Object.fromEntries(results.map((x) => [x.email, x]));
    expect(byEmail['x@y.io']?.avatar?.url).toBe('A');
    expect(byEmail['z@y.io']?.avatar?.url).toBe('A');
    expect(byEmail['bad']?.avatar).toBeNull();
    expect(byEmail['bad']?.error).toBeInstanceOf(InvalidEmailError);
  });
});
