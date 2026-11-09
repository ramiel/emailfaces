import type { Observable } from 'rxjs';
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
   * abort-on-unsubscribe, transient-error retries and rate-limit header handling.
   * Resolves for any status except 429 / exhausted-quota responses (which become
   * `RateLimitedError`) and 5xx (which become `HttpError` after retries).
   */
  request<T = void>(url: string, options?: RequestOptions<T>): Observable<HttpResponse<T>>;
  /** Requested avatar size in pixels. */
  readonly size: number;
}

export interface AvatarSource {
  readonly name: string;
  readonly limiter: RateLimiter;
  /** Emit the avatar URL, or null / complete empty when the source has none. */
  lookup(email: EmailInfo, ctx: SourceContext): Observable<string | null>;
}

export type SourceOutcome =
  | { readonly source: string; readonly status: 'found'; readonly url: string; readonly cached: boolean }
  | { readonly source: string; readonly status: 'not_found'; readonly cached: boolean }
  | { readonly source: string; readonly status: 'rate_limited'; readonly retryAfterMs?: number; readonly error: Error }
  | { readonly source: string; readonly status: 'error'; readonly error: unknown };
