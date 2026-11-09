import type { FetchLike } from '../src/index.js';

export type Route = (url: string, init?: RequestInit) => Response | Promise<Response>;

/** Fake fetch: records calls, routes by URL substring, honours AbortSignal. */
export function fakeFetch(routes: Record<string, Route>, { delayMs = 0 } = {}) {
  const calls: string[] = [];
  const fetch: FetchLike = (url, init) => {
    calls.push(url);
    return new Promise<Response>((resolve, reject) => {
      const t = setTimeout(async () => {
        const match = Object.keys(routes).find((k) => url.includes(k));
        try {
          resolve(match ? await routes[match]!(url, init) : new Response(null, { status: 404 }));
        } catch (e) {
          reject(e);
        }
      }, delayMs);
      init?.signal?.addEventListener('abort', () => {
        clearTimeout(t);
        reject(new DOMException('Aborted', 'AbortError'));
      });
    });
  };
  return { fetch, calls };
}

export const json = (body: unknown, init: ResponseInit = {}) =>
  new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' }, ...init });

export const status = (code: number, headers: Record<string, string> = {}) => new Response(null, { status: code, headers });
