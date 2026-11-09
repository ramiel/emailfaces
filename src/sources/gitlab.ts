import { HttpError } from '../errors.js';
import { toLimiter, type RateLimitOptions, type RateLimiter } from '../rate-limiter.js';
import type { AvatarSource } from '../types.js';

export interface GitLabOptions {
  /** Defaults to https://gitlab.com; point to a self-managed instance if needed. */
  baseUrl?: string;
  token?: string;
  rateLimit?: RateLimitOptions | RateLimiter;
}

/** GitLab falls back to these when the user has no uploaded avatar; those aren't GitLab avatars. */
const EXTERNAL_AVATAR_HOSTS = /(^|\.)(gravatar\.com|libravatar\.org)$/;

/** GitLab `/api/v4/avatar?email=` — matches users' public email. */
export function gitlab(options: GitLabOptions = {}): AvatarSource {
  const base = (options.baseUrl ?? 'https://gitlab.com').replace(/\/$/, '');
  const headers: Record<string, string> = { accept: 'application/json' };
  if (options.token) headers['private-token'] = options.token;

  return {
    name: 'gitlab',
    limiter: toLimiter(options.rateLimit, { maxConcurrent: 4, window: { requests: 60, perMs: 60_000 } }),
    lookup: async (email, ctx) => {
      const url = `${base}/api/v4/avatar?email=${encodeURIComponent(email.address)}&size=${ctx.size}`;
      const res = await ctx.request<{ avatar_url?: string | null } | undefined>(url, {
        headers,
        read: (r) => (r.ok ? r.json() : Promise.resolve(undefined)),
      });
      if (res.status === 404) return null;
      if (res.status !== 200) throw new HttpError(res.status, url);
      const avatar = res.body?.avatar_url;
      if (!avatar) return null;
      const resolved = new URL(avatar, base);
      return EXTERNAL_AVATAR_HOSTS.test(resolved.hostname) ? null : resolved.toString();
    },
  };
}
