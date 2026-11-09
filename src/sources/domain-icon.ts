import { HttpError } from '../errors.js';
import { toLimiter, type RateLimitOptions, type RateLimiter } from '../rate-limiter.js';
import type { AvatarSource } from '../types.js';

/** Mailbox providers whose icon says nothing about the person. */
export const FREE_MAIL_DOMAINS: ReadonlySet<string> = new Set([
  'gmail.com', 'googlemail.com', 'yahoo.com', 'yahoo.co.uk', 'yahoo.fr', 'yahoo.it', 'ymail.com',
  'hotmail.com', 'hotmail.it', 'hotmail.co.uk', 'outlook.com', 'outlook.it', 'live.com', 'live.it', 'msn.com',
  'icloud.com', 'me.com', 'mac.com', 'aol.com', 'proton.me', 'protonmail.com', 'pm.me', 'gmx.com', 'gmx.de',
  'gmx.net', 'web.de', 'yandex.com', 'yandex.ru', 'mail.ru', 'zoho.com', 'fastmail.com', 'hey.com', 'tutanota.com',
  'qq.com', '163.com', '126.com', 'libero.it', 'virgilio.it', 'tiscali.it', 'alice.it', 'orange.fr', 'free.fr',
  'laposte.net', 't-online.de', 'comcast.net', 'verizon.net', 'att.net',
]);

export interface DomainIconOptions {
  /** Domains to never use (defaults to common free-mail providers). */
  ignoreDomains?: ReadonlySet<string>;
  rateLimit?: RateLimitOptions | RateLimiter;
}

/**
 * Organization icon for the email's domain (via DuckDuckGo's icon service, which 404s when unknown).
 * Opt-in: it's a company logo, not a personal avatar — best used as the last source.
 */
export function domainIcon(options: DomainIconOptions = {}): AvatarSource {
  const ignore = options.ignoreDomains ?? FREE_MAIL_DOMAINS;
  return {
    name: 'domain',
    limiter: toLimiter(options.rateLimit, { maxConcurrent: 4 }),
    lookup: async (email, ctx) => {
      if (ignore.has(email.domain)) return null;
      const url = `https://icons.duckduckgo.com/ip3/${encodeURIComponent(email.domain)}.ico`;
      const res = await ctx.request(url);
      if (res.status === 200) return url;
      if (res.status === 404) return null;
      throw new HttpError(res.status, url);
    },
  };
}
