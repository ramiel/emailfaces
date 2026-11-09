import type { RateLimiter } from './rate-limiter.js';

export interface EmailInfo {
  /** Normalized (trimmed, lowercased) address. */
  readonly address: string;
  readonly local: string;
  readonly domain: string;
}

export interface Avatar {
  readonly email: string;
  /** Name of the source that produced the avatar (or `"fallback"`). */
  readonly source: string;
  readonly url: string;
}

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export interface HttpResponse<T> {
  readonly status: number;
  readonly headers: Headers;
  readonly body: T;
}

export interface RequestOptions<T> {
  method?: string;
  headers?: Record<string, string>;
  /**
   * Reads the body while the request is still abortable and under timeout.
   * Defaults to discarding the body.
   */
  read?: (response: Response) => Promise<T>;
}

/** Handed to every source lookup. */
export interface SourceContext {
  /**
   * Performs an HTTP request through the source's rate limiter, with timeout,
   * abort-on-cancel (via `signal`), transient-error retries and rate-limit header handling.
   * Resolves for any status except 429 / exhausted-quota responses (which reject with
   * `RateLimitedError`) and 5xx (which reject with `HttpError` after retries).
   */
  request<T = void>(url: string, options?: RequestOptions<T>): Promise<HttpResponse<T>>;
  /** Requested avatar size in pixels. */
  readonly size: number;
  /**
   * Aborted when the lookup is no longer needed (caller aborted, or a higher-priority source won).
   * `request` already honours it; pass it on to any other async work the source does.
   */
  readonly signal: AbortSignal;
}

/**
 * Structural Observable type (an RxJS `Observable` satisfies it), declared here so the
 * root entry doesn't depend on RxJS typings.
 */
export interface Subscribable<T> {
  subscribe(observer: { next: (value: T) => void; error: (err: unknown) => void; complete: () => void }): { unsubscribe(): void };
}

/**
 * What a source's `lookup` may return: the URL (or null), a Promise of it, or an Observable
 * whose first value is used (completing empty counts as null; unsubscribing cancels it).
 */
export type LookupResult = string | null | Promise<string | null> | Subscribable<string | null>;

export interface AvatarSource {
  readonly name: string;
  readonly limiter: RateLimiter;
  /** The avatar URL, or null when the source has none. See `LookupResult`. */
  lookup(email: EmailInfo, ctx: SourceContext): LookupResult;
}

export type SourceOutcome =
  | { readonly source: string; readonly status: 'found'; readonly url: string; readonly cached: boolean }
  | { readonly source: string; readonly status: 'not_found'; readonly cached: boolean }
  | { readonly source: string; readonly status: 'rate_limited'; readonly retryAfterMs?: number; readonly error: Error }
  | { readonly source: string; readonly status: 'error'; readonly error: unknown };
