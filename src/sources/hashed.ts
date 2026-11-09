import { sha256Hex } from '../email.js';
import { HttpError } from '../errors.js';
import { toLimiter, type RateLimitOptions, type RateLimiter } from '../rate-limiter.js';
import type { AvatarSource } from '../types.js';

export interface HashedSourceOptions {
  rateLimit?: RateLimitOptions | RateLimiter;
  /** Override the service base URL (e.g. a self-hosted Libravatar). */
  baseUrl?: string;
}

/**
 * Gravatar-protocol services: `<base>/<sha256(email)>?d=404` answers 404 when no avatar exists,
 * so a HEAD request tells us whether one is there.
 */
export function hashedAvatarSource(name: string, defaultBase: string, options: HashedSourceOptions, defaults: RateLimitOptions): AvatarSource {
  const base = (options.baseUrl ?? defaultBase).replace(/\/$/, '');
  return {
    name,
    limiter: toLimiter(options.rateLimit, defaults),
    lookup: async (email, ctx) => {
      const url = `${base}/${await sha256Hex(email.address)}?s=${ctx.size}`;
      const res = await ctx.request(`${url}&d=404`, { method: 'HEAD' });
      if (res.status === 200) return url;
      if (res.status === 404) return null;
      throw new HttpError(res.status, url);
    },
  };
}

export const gravatar = (options: HashedSourceOptions = {}): AvatarSource =>
  hashedAvatarSource('gravatar', 'https://gravatar.com/avatar', options, { maxConcurrent: 10 });

/** Libravatar's central server performs federated (DNS SRV) lookups and falls back to Gravatar. */
export const libravatar = (options: HashedSourceOptions = {}): AvatarSource =>
  hashedAvatarSource('libravatar', 'https://seccdn.libravatar.org/avatar', options, { maxConcurrent: 4 });
