import { HttpError } from '../errors.js';
import { toLimiter, type RateLimitOptions, type RateLimiter } from '../rate-limiter.js';
import type { AvatarSource } from '../types.js';

export interface GitHubOptions {
  /** Personal access token: raises the search limit from 10 to 30 requests/minute. */
  token?: string;
  rateLimit?: RateLimitOptions | RateLimiter;
  apiUrl?: string;
}

interface UserSearch {
  total_count: number;
  items: Array<{ avatar_url: string }>;
}

const NOREPLY_RE = /^(?:(\d+)\+)?([a-z0-9-]+)@users\.noreply\.github\.com$/;

function withSize(url: string, size: number): string {
  const u = new URL(url);
  u.searchParams.set('s', String(size));
  return u.toString();
}

/**
 * GitHub: decodes `…@users.noreply.github.com` addresses offline, otherwise searches
 * users whose *public* profile email matches. Only unambiguous (single) matches count.
 */
export function github(options: GitHubOptions = {}): AvatarSource {
  const apiUrl = (options.apiUrl ?? 'https://api.github.com').replace(/\/$/, '');
  const headers: Record<string, string> = {
    accept: 'application/vnd.github+json',
    'x-github-api-version': '2022-11-28',
    'user-agent': 'emailfaces',
  };
  if (options.token) headers.authorization = `Bearer ${options.token}`;

  return {
    name: 'github',
    // Search API: 10 req/min anonymous, 30 req/min authenticated.
    limiter: toLimiter(options.rateLimit, { maxConcurrent: 2, window: { requests: options.token ? 30 : 10, perMs: 60_000 } }),
    lookup: async (email, ctx) => {
      const noreply = NOREPLY_RE.exec(email.address);
      if (noreply) {
        const [, id, login] = noreply;
        return id ? `https://avatars.githubusercontent.com/u/${id}?s=${ctx.size}` : `https://github.com/${login}.png?size=${ctx.size}`;
      }

      const q = encodeURIComponent(`${email.address} in:email`);
      const url = `${apiUrl}/search/users?q=${q}&per_page=2`;
      const res = await ctx.request<UserSearch | undefined>(url, { headers, read: (r) => (r.ok ? r.json() : Promise.resolve(undefined)) });
      if (res.status === 422) return null;
      if (res.status !== 200 || !res.body) throw new HttpError(res.status, url);
      const [user] = res.body.items;
      return res.body.total_count === 1 && user ? withSize(user.avatar_url, ctx.size) : null;
    },
  };
}
