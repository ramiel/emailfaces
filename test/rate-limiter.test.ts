import { defer, delay, firstValueFrom, forkJoin, lastValueFrom, of, timer, map } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { RateLimitedError, RateLimiter } from '../src/index.js';

const job = (log: number[], t0: number, ms = 0) => defer(() => {
  log.push(Date.now() - t0);
  return of(true).pipe(delay(ms));
});

describe('RateLimiter', () => {
  it('enforces a sliding window', async () => {
    const limiter = new RateLimiter({ window: { requests: 2, perMs: 150 } });
    const t0 = Date.now();
    const starts: number[] = [];
    await lastValueFrom(forkJoin(Array.from({ length: 5 }, () => limiter.schedule(job(starts, t0)))));
    starts.sort((a, b) => a - b);
    expect(starts[1]!).toBeLessThan(50);
    expect(starts[2]!).toBeGreaterThanOrEqual(140);
    expect(starts[4]!).toBeGreaterThanOrEqual(290);
  });

  it('caps concurrency', async () => {
    const limiter = new RateLimiter({ maxConcurrent: 2 });
    let active = 0;
    let peak = 0;
    const work = defer(() => {
      peak = Math.max(peak, ++active);
      return timer(20).pipe(map(() => active--));
    });
    await lastValueFrom(forkJoin(Array.from({ length: 6 }, () => limiter.schedule(work))));
    expect(peak).toBe(2);
  });

  it('frees a queued slot when the waiter unsubscribes', async () => {
    const limiter = new RateLimiter({ maxConcurrent: 1 });
    const blocker = limiter.schedule(timer(30)).subscribe();
    const abandoned = limiter.schedule(of('abandoned')).subscribe();
    abandoned.unsubscribe();
    expect(await firstValueFrom(limiter.schedule(of('next')))).toBe('next');
    blocker.unsubscribe();
  });

  it('waits out server-driven pauses', async () => {
    const limiter = new RateLimiter();
    limiter.pause(100);
    const t0 = Date.now();
    await firstValueFrom(limiter.schedule(of(1)));
    expect(Date.now() - t0).toBeGreaterThanOrEqual(95);
  });

  it('fails fast when the wait exceeds the deadline', async () => {
    const limiter = new RateLimiter();
    limiter.pause(10_000);
    await expect(firstValueFrom(limiter.schedule(of(1), Date.now() + 100))).rejects.toBeInstanceOf(RateLimitedError);
  });
});
