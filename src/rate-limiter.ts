import { Observable, defer, of, switchMap, throwError, timer, type Subscription } from 'rxjs';
import { RateLimitedError } from './errors.js';

export interface RateLimitOptions {
  /** Maximum number of in-flight requests. Default: unlimited. */
  maxConcurrent?: number;
  /** Sliding-window limit, e.g. `{ requests: 10, perMs: 60_000 }`. Default: none. */
  window?: { requests: number; perMs: number };
}

/**
 * Per-source limiter combining a concurrency cap, a sliding-window request budget,
 * and server-driven pauses (429 / Retry-After / X-RateLimit-Reset).
 *
 * Share one instance between sources or resolvers to share a budget.
 */
export class RateLimiter {
  private active = 0;
  private readonly queue: Array<() => void> = [];
  /** Start times (ms epoch) of granted or reserved permits, ascending. */
  private starts: number[] = [];
  private pausedUntil = 0;

  constructor(private readonly options: RateLimitOptions = {}) {}

  /** Stop granting permits for `ms` (extends, never shortens, an existing pause). */
  pause(ms: number): void {
    this.pausedUntil = Math.max(this.pausedUntil, Date.now() + ms);
  }

  /** Remaining pause in ms (0 when not paused). */
  get pausedFor(): number {
    return Math.max(0, this.pausedUntil - Date.now());
  }

  /**
   * Runs `work` once a concurrency slot and a rate permit are available.
   * Fails with `RateLimitedError` if the permit would be granted after `deadline` (ms epoch).
   * Unsubscribing while queued frees the place in line.
   * @internal Stripped from the published typings: RxJS is an implementation detail.
   */
  schedule<T>(work: Observable<T>, deadline = Infinity): Observable<T> {
    return this.withSlot(this.permit(deadline).pipe(switchMap(() => work)));
  }

  private withSlot<T>(work: Observable<T>): Observable<T> {
    const max = this.options.maxConcurrent ?? Infinity;
    return new Observable<T>((subscriber) => {
      let inner: Subscription | undefined;
      const start = () => {
        this.active++;
        inner = work.subscribe(subscriber);
      };
      if (this.active < max) start();
      else this.queue.push(start);

      return () => {
        if (inner) {
          inner.unsubscribe();
          this.active--;
          this.queue.shift()?.();
        } else {
          const i = this.queue.indexOf(start);
          if (i >= 0) this.queue.splice(i, 1);
        }
      };
    });
  }

  private permit(deadline: number): Observable<void> {
    return defer(() => {
      const wait = this.reserve(deadline);
      if (wait === null) return throwError(() => new RateLimitedError(this.nextAvailableIn()));
      if (wait <= 0) return of(undefined);
      // A server-driven pause may have been set while we waited: re-check.
      return timer(wait).pipe(switchMap(() => (this.pausedFor > 0 ? this.permit(deadline) : of(undefined))));
    });
  }

  /** Synchronously books the next start time; returns the delay, or null if past the deadline. */
  private reserve(deadline: number): number | null {
    const now = Date.now();
    const start = this.earliestStart(now);
    if (start > deadline) return null;
    if (this.options.window) this.starts.push(start);
    return start - now;
  }

  private earliestStart(now: number): number {
    let start = Math.max(now, this.pausedUntil);
    const win = this.options.window;
    if (win) {
      this.starts = this.starts.filter((t) => t > now - win.perMs);
      const n = this.starts.length;
      if (n >= win.requests) start = Math.max(start, this.starts[n - win.requests]! + win.perMs);
      if (n > 0) start = Math.max(start, this.starts[n - 1]!);
    }
    return start;
  }

  private nextAvailableIn(): number {
    const now = Date.now();
    return this.earliestStart(now) - now;
  }
}

/** Accepts either options or a shared instance. */
export function toLimiter(value: RateLimitOptions | RateLimiter | undefined, defaults: RateLimitOptions): RateLimiter {
  return value instanceof RateLimiter ? value : new RateLimiter({ ...defaults, ...value });
}
