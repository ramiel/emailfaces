import { Observable, TimeoutError, map, of, retry, throwError, timeout, timer } from 'rxjs';
import { HttpError, RateLimitedError } from './errors.js';
import type { RateLimiter } from './rate-limiter.js';
import type { FetchLike, HttpResponse, RequestOptions } from './types.js';

const DEFAULT_RATE_LIMIT_PAUSE_MS = 60_000;

/** A single fetch, aborted on unsubscribe. The body is read inside the abortable scope. */
export function fetch$<T>(fetchFn: FetchLike, url: string, options: RequestOptions<T>): Observable<HttpResponse<T>> {
  return new Observable<HttpResponse<T>>((subscriber) => {
    const controller = new AbortController();
    const read = options.read ?? discardBody;
    fetchFn(url, {
      method: options.method ?? 'GET',
      headers: options.headers,
      redirect: 'follow',
      signal: controller.signal,
    })
      .then(async (res) => ({ status: res.status, headers: res.headers, body: (await read(res)) as T }))
      .then(
        (res) => {
          subscriber.next(res);
          subscriber.complete();
        },
        (err: unknown) => subscriber.error(err),
      );
    return () => controller.abort();
  });
}

async function discardBody(res: Response): Promise<void> {
  await res.body?.cancel().catch(() => undefined);
}

/** Milliseconds until the provider allows more requests, derived from standard and de-facto headers. */
export function retryAfterMs(headers: Headers, now = Date.now()): number | undefined {
  const retryAfter = headers.get('retry-after');
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
    const date = Date.parse(retryAfter);
    if (!Number.isNaN(date)) return Math.max(0, date - now);
  }
  const reset = Number(headers.get('x-ratelimit-reset') ?? headers.get('ratelimit-reset') ?? NaN);
  if (Number.isFinite(reset)) {
    // Large values are epoch seconds (GitHub, GitLab); small ones are deltas (IETF draft).
    return Math.max(0, reset > 1e9 ? reset * 1000 - now : reset * 1000);
  }
  return undefined;
}

function remainingQuota(headers: Headers): number | undefined {
  const raw = headers.get('x-ratelimit-remaining') ?? headers.get('ratelimit-remaining');
  if (raw === null) return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

/** Translates rate-limit signals into limiter pauses / errors, and 5xx into HttpError. */
function inspect<T>(res: HttpResponse<T>, url: string, limiter: RateLimiter): HttpResponse<T> {
  const remaining = remainingQuota(res.headers);
  const exhausted = res.status === 429 || (res.status === 403 && remaining === 0);
  if (exhausted) {
    const wait = retryAfterMs(res.headers) ?? DEFAULT_RATE_LIMIT_PAUSE_MS;
    limiter.pause(wait);
    throw new RateLimitedError(wait);
  }
  if (remaining === 0) {
    // Quota used up by this very request: hold off proactively until reset.
    const wait = retryAfterMs(res.headers);
    if (wait !== undefined) limiter.pause(wait);
  }
  if (res.status >= 500) throw new HttpError(res.status, url);
  return res;
}

export interface RequestPolicy {
  fetch: FetchLike;
  limiter: RateLimiter;
  timeoutMs: number;
  retries: number;
  /** Absolute time (ms epoch) after which we stop waiting for the limiter. */
  deadline: number;
}

function isTransient(err: unknown): boolean {
  if (err instanceof HttpError) return err.status >= 500;
  if (err instanceof TimeoutError) return true;
  return err instanceof TypeError; // fetch network failure
}

/** fetch$ + limiter + timeout + retry policy. */
export function request$<T>(url: string, options: RequestOptions<T>, policy: RequestPolicy): Observable<HttpResponse<T>> {
  const { limiter, deadline } = policy;
  const attempt$ = limiter.schedule(
    fetch$(policy.fetch, url, options).pipe(
      timeout({ first: policy.timeoutMs }),
      map((res) => inspect(res, url, limiter)),
    ),
    deadline,
  );

  return attempt$.pipe(
    retry({
      count: policy.retries,
      delay: (err: unknown, attempt) => {
        if (err instanceof RateLimitedError) {
          // The limiter is already paused; re-scheduling waits for it, unless that's past the deadline.
          return Date.now() + limiter.pausedFor <= deadline ? of(0) : throwError(() => err);
        }
        if (isTransient(err)) return timer(Math.min(250 * 2 ** (attempt - 1), 4_000));
        return throwError(() => err);
      },
    }),
  );
}
