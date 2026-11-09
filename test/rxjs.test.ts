import { NEVER, Subject, finalize, lastValueFrom, map, of, timer, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { AvatarResolver, InvalidEmailError, RateLimiter, createAvatarResolver, type AvatarSource } from '../src/rxjs.js';

const obs = (name: string, url: string | null, ms = 0): AvatarSource => ({
  name,
  limiter: new RateLimiter(),
  lookup: () => (ms ? timer(ms).pipe(map(() => url)) : of(url)),
});

describe('emailfaces/rxjs', () => {
  it('exposes both the Observable and the Promise API', async () => {
    const r = createAvatarResolver({ sources: [obs('a', 'A')] });
    expect(r).toBeInstanceOf(AvatarResolver);
    expect(await lastValueFrom(r.resolve$('a@b.co'))).toMatchObject({ source: 'a', url: 'A' });
    expect(await r.resolve('a@b.co')).toMatchObject({ source: 'a', url: 'A' });
  });

  it('lookup$ streams outcomes as they arrive', async () => {
    const r = new AvatarResolver({ cache: false, sources: [obs('slow', 'S', 40), obs('fast', null, 5)] });
    const order = await lastValueFrom(r.lookup$('a@b.co').pipe(map((o) => o.source), toArray()));
    expect(order).toEqual(['fast', 'slow']);
  });

  it('resolveMany$ accepts an Observable of emails and reports invalid ones per item', async () => {
    const emails$ = new Subject<string>();
    const r = new AvatarResolver({ sources: [obs('a', 'A')] });
    const results = lastValueFrom(r.resolveMany$(emails$).pipe(toArray()));
    ['x@y.io', 'bad', 'z@y.io'].forEach((e) => emails$.next(e));
    emails$.complete();
    const byEmail = Object.fromEntries((await results).map((x) => [x.email, x]));
    expect(byEmail['x@y.io']?.avatar?.url).toBe('A');
    expect(byEmail['bad']?.error).toBeInstanceOf(InvalidEmailError);
  });

  it('an Observable source completing empty counts as a miss', async () => {
    const empty: AvatarSource = { name: 'empty', limiter: new RateLimiter(), lookup: () => of() };
    const r = new AvatarResolver({ sources: [empty, obs('b', 'B')] });
    expect((await r.resolve('a@b.co'))?.source).toBe('b');
  });

  it('unsubscribes Observable sources that lose the parallel race', async () => {
    let torndown = false;
    const hang: AvatarSource = {
      name: 'hang',
      limiter: new RateLimiter(),
      lookup: () => NEVER.pipe(finalize(() => (torndown = true))),
    };
    const r = new AvatarResolver({ strategy: 'parallel', sources: [obs('a', 'A', 5), hang] });
    expect((await lastValueFrom(r.resolve$('a@b.co')))?.url).toBe('A');
    expect(torndown).toBe(true);
  });

  it('treats a source that throws synchronously as an error outcome', async () => {
    const broken: AvatarSource = { name: 'broken', limiter: new RateLimiter(), lookup: () => { throw new Error('boom'); } };
    const outcomes = await new AvatarResolver({ sources: [broken] }).lookup('a@b.co');
    expect(outcomes[0]?.status).toBe('error');
  });
});
