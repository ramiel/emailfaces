import {
  EMPTY,
  Observable,
  catchError,
  concat,
  defaultIfEmpty,
  defer,
  filter,
  finalize,
  first,
  firstValueFrom,
  from,
  fromEvent,
  lastValueFrom,
  map,
  merge,
  mergeMap,
  of,
  scan,
  shareReplay,
  take,
  takeUntil,
  tap,
  throwError,
  throwIfEmpty,
  toArray,
} from 'rxjs';
import { MemoryCache, type AvatarCache, type CacheEntry } from './cache.js';
import { parseEmail } from './email.js';
import { InvalidEmailError, RateLimitedError } from './errors.js';
import { request$ } from './http.js';
import { defaultSources } from './sources/index.js';
import type { Avatar, AvatarSource, EmailInfo, FetchLike, SourceContext, SourceOutcome } from './types.js';

export type Strategy =
  /** Query sources one at a time in order; stop at the first hit. Spends the least quota. */
  | 'sequential'
  /** Query all sources at once; return the highest-priority hit as soon as it's certain. Lowest latency. */
  | 'parallel';

export interface ResolverOptions {
  /** Ordered by priority. Default: gravatar, libravatar, gitlab, github. */
  sources?: AvatarSource[];
  /** Default: `'sequential'`. */
  strategy?: Strategy;
  /** Avatar size in pixels, passed to providers that support it. Default: 200. */
  size?: number;
  /** Custom fetch implementation. Default: `globalThis.fetch`. */
  fetch?: FetchLike;
  /** Per-request timeout. Default: 8000ms. */
  timeoutMs?: number;
  /** Retries for transient failures (5xx, network, timeout) and rate limiting. Default: 2. */
  retries?: number;
  /**
   * Longest a single source lookup may wait on rate limits before it's skipped
   * (reported as `rate_limited`). Use `Infinity` for batch jobs that should just wait. Default: 30s.
   */
  maxWaitMs?: number;
  /** Result cache, or `false` to disable. Default: in-memory LRU. */
  cache?: AvatarCache | false;
  /** TTL for found avatars. Default: 24h. */
  cacheTtlMs?: number;
  /** TTL for "no avatar here" results. Default: 1h. */
  negativeCacheTtlMs?: number;
  /** URL (or factory) used when no source has an avatar. */
  fallback?: string | ((email: string) => string);
}

export interface CallOptions {
  signal?: AbortSignal;
}

export interface ManyResult {
  readonly email: string;
  readonly avatar: Avatar | null;
  readonly error?: unknown;
}

export class AvatarResolver {
  private readonly sources: AvatarSource[];
  private readonly strategy: Strategy;
  private readonly size: number;
  private readonly fetchFn: FetchLike;
  private readonly timeoutMs: number;
  private readonly retries: number;
  private readonly maxWaitMs: number;
  private readonly cache: AvatarCache | undefined;
  private readonly cacheTtlMs: number;
  private readonly negativeCacheTtlMs: number;
  private readonly fallback: ResolverOptions['fallback'];
  private readonly inflight = new Map<string, Observable<SourceOutcome>>();

  constructor(options: ResolverOptions = {}) {
    this.sources = options.sources ?? defaultSources();
    const names = new Set(this.sources.map((s) => s.name));
    if (names.size !== this.sources.length) throw new Error('Source names must be unique');
    this.strategy = options.strategy ?? 'sequential';
    this.size = options.size ?? 200;
    const fetchFn = options.fetch ?? globalThis.fetch;
    if (!fetchFn) throw new Error('No fetch implementation available; pass `fetch` in options');
    this.fetchFn = fetchFn.bind(globalThis);
    this.timeoutMs = options.timeoutMs ?? 8_000;
    this.retries = options.retries ?? 2;
    this.maxWaitMs = options.maxWaitMs ?? 30_000;
    this.cache = options.cache === false ? undefined : (options.cache ?? new MemoryCache());
    this.cacheTtlMs = options.cacheTtlMs ?? 24 * 3_600_000;
    this.negativeCacheTtlMs = options.negativeCacheTtlMs ?? 3_600_000;
    this.fallback = options.fallback;
  }

  // ---------------------------------------------------------------- Observable API

  /** Best avatar for `email` according to source priority, or null. Errors on invalid email. */
  resolve$(email: string): Observable<Avatar | null> {
    return this.parse$(email).pipe(
      mergeMap((info) => {
        const hit$ = this.strategy === 'parallel' ? this.firstByPriority$(info) : this.firstSequential$(info);
        return hit$.pipe(
          map((o): Avatar | null => (o ? { email: info.address, source: o.source, url: o.url } : this.fallbackFor(info))),
        );
      }),
    );
  }

  /** Every source's outcome as it arrives (all sources are queried concurrently). */
  lookup$(email: string): Observable<SourceOutcome> {
    return this.parse$(email).pipe(mergeMap((info) => merge(...this.sources.map((s) => this.runSource(s, info)))));
  }

  /** Resolve many emails with bounded concurrency; each result is emitted as soon as it's ready. */
  resolveMany$(emails: Iterable<string> | Observable<string>, { concurrency = 4 }: { concurrency?: number } = {}): Observable<ManyResult> {
    return from(emails).pipe(
      mergeMap(
        (email) =>
          this.resolve$(email).pipe(
            map((avatar): ManyResult => ({ email, avatar })),
            catchError((error: unknown) => of<ManyResult>({ email, avatar: null, error })),
          ),
        concurrency,
      ),
    );
  }

  // ---------------------------------------------------------------- Promise API

  resolve(email: string, options: CallOptions = {}): Promise<Avatar | null> {
    return firstValueFrom(abortable(this.resolve$(email), options.signal));
  }

  lookup(email: string, options: CallOptions = {}): Promise<SourceOutcome[]> {
    return lastValueFrom(abortable(this.lookup$(email).pipe(toArray()), options.signal));
  }

  resolveMany(emails: Iterable<string>, options: CallOptions & { concurrency?: number } = {}): Promise<ManyResult[]> {
    return lastValueFrom(abortable(this.resolveMany$(emails, options).pipe(toArray()), options.signal));
  }

  // ---------------------------------------------------------------- internals

  private parse$(email: string): Observable<EmailInfo> {
    return defer(() => {
      const info = parseEmail(email);
      return info ? of(info) : throwError(() => new InvalidEmailError(email));
    });
  }

  private fallbackFor(info: EmailInfo): Avatar | null {
    if (!this.fallback) return null;
    const url = typeof this.fallback === 'function' ? this.fallback(info.address) : this.fallback;
    return { email: info.address, source: 'fallback', url };
  }

  /** `concat` subscribes lazily, so later sources are never hit once one succeeds. */
  private firstSequential$(info: EmailInfo): Observable<Found | null> {
    return concat(...this.sources.map((s) => this.runSource(s, info))).pipe(first(isFound, null));
  }

  /**
   * All sources race, but a hit only wins once every higher-priority source has missed.
   * `take(1)` then cancels whatever is still running.
   */
  private firstByPriority$(info: EmailInfo): Observable<Found | null> {
    const indexed$ = this.sources.map((s, i) => this.runSource(s, info).pipe(map((outcome) => ({ i, outcome }))));
    return merge(...indexed$).pipe(
      scan((settled, { i, outcome }) => {
        const next = settled.slice();
        next[i] = outcome;
        return next;
      }, new Array<SourceOutcome | undefined>(this.sources.length)),
      map((settled) => {
        for (const outcome of settled) {
          if (!outcome) return undefined; // a higher-priority source is still pending
          if (isFound(outcome)) return outcome;
        }
        return undefined;
      }),
      filter((o): o is Found => o !== undefined),
      take(1),
      defaultIfEmpty(null),
    );
  }

  /** Cache → (deduplicated) live lookup. Never errors: failures become outcomes. */
  private runSource(source: AvatarSource, info: EmailInfo): Observable<SourceOutcome> {
    const key = `${source.name}:${this.size}:${info.address}`;
    return defer(() => {
      const existing = this.inflight.get(key);
      if (existing) return existing;
      const shared$ = concat(this.fromCache$(source.name, key), this.fromSource$(source, info, key)).pipe(
        take(1),
        finalize(() => this.inflight.delete(key)),
        shareReplay({ bufferSize: 1, refCount: true }),
      );
      this.inflight.set(key, shared$);
      return shared$;
    });
  }

  private fromCache$(name: string, key: string): Observable<SourceOutcome> {
    const cache = this.cache;
    if (!cache) return EMPTY;
    return defer(async () => cache.get(key)).pipe(
      filter((entry): entry is CacheEntry => entry !== undefined),
      map((entry) => toOutcome(name, entry.url, true)),
      catchError(() => EMPTY), // a broken cache must not break lookups
    );
  }

  private fromSource$(source: AvatarSource, info: EmailInfo, key: string): Observable<SourceOutcome> {
    return defer(() => source.lookup(info, this.contextFor(source))).pipe(
      defaultIfEmpty(null),
      take(1),
      map((url) => toOutcome(source.name, url, false)),
      tap((outcome) => this.store(key, outcome)),
      catchError((error: unknown) =>
        of<SourceOutcome>(
          error instanceof RateLimitedError
            ? { source: source.name, status: 'rate_limited', retryAfterMs: error.retryAfterMs, error }
            : { source: source.name, status: 'error', error },
        ),
      ),
    );
  }

  private contextFor(source: AvatarSource): SourceContext {
    const policy = {
      fetch: this.fetchFn,
      limiter: source.limiter,
      timeoutMs: this.timeoutMs,
      retries: this.retries,
      deadline: Date.now() + this.maxWaitMs,
    };
    return { size: this.size, request: (url, options = {}) => request$(url, options, policy) };
  }

  private store(key: string, outcome: SourceOutcome): void {
    if (!this.cache || outcome.status === 'error' || outcome.status === 'rate_limited') return;
    const url = outcome.status === 'found' ? outcome.url : null;
    const ttl = url ? this.cacheTtlMs : this.negativeCacheTtlMs;
    Promise.resolve()
      .then(() => this.cache!.set(key, { url }, ttl))
      .catch(() => undefined);
  }
}

type Found = Extract<SourceOutcome, { status: 'found' }>;

const isFound = (o: SourceOutcome): o is Found => o.status === 'found';

function toOutcome(source: string, url: string | null, cached: boolean): SourceOutcome {
  return url ? { source, status: 'found', url, cached } : { source, status: 'not_found', cached };
}

function abortable<T>(source$: Observable<T>, signal?: AbortSignal): Observable<T> {
  if (!signal) return source$;
  return defer(() => {
    if (signal.aborted) return throwError(() => signal.reason);
    return source$.pipe(
      takeUntil(fromEvent(signal, 'abort')),
      throwIfEmpty(() => signal.reason),
    );
  });
}

export function createAvatarResolver(options?: ResolverOptions): AvatarResolver {
  return new AvatarResolver(options);
}
