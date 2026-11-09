import { HttpError } from '../errors.js';
import { toLimiter, type RateLimitOptions, type RateLimiter } from '../rate-limiter.js';
import type { AvatarSource, SourceContext } from '../types.js';
import { FREE_MAIL_DOMAINS } from './domain-icon.js';

/**
 * Custom TXT lookup; same shape as Node's `dns.promises.resolveTxt` (each record is a list of
 * string chunks). Rejecting with code `ENODATA` / `ENOTFOUND` means "no record".
 */
export type TxtResolver = (name: string, signal: AbortSignal) => Promise<string[][]>;

export interface BimiOptions {
  /** Domains to never use (defaults to common free-mail providers, whose logo says nothing about the person). */
  ignoreDomains?: ReadonlySet<string>;
  /**
   * Only accept records that publish authority evidence (`a=`, a VMC/CMC certificate URL).
   * The certificate itself is not validated. Default: false.
   */
  requireAuthority?: boolean;
  /** DNS-over-HTTPS JSON endpoint. Default: Cloudflare (`https://cloudflare-dns.com/dns-query`). */
  dohUrl?: string;
  /**
   * Resolve TXT records yourself instead of using DNS-over-HTTPS, e.g. Node's
   * `(name) => dns.promises.resolveTxt(name)`. These lookups don't go through the rate limiter.
   */
  resolveTxt?: TxtResolver;
  rateLimit?: RateLimitOptions | RateLimiter;
}

interface DohResponse {
  /** DNS RCODE: 0 = NOERROR, 3 = NXDOMAIN. */
  Status: number;
  Answer?: Array<{ type: number; data: string }>;
}

const TXT = 16;
const NXDOMAIN = 3;
const BIMI_VERSION_RE = /^\s*v\s*=\s*BIMI1\s*(;|$)/i;

/**
 * BIMI (Brand Indicators for Message Identification): the logo a domain publishes at
 * `default._bimi.<domain>`. Falls back to the parent domain once (`mail.acme.io` → `acme.io`).
 * Opt-in, like `domainIcon()`: it's a brand logo (an SVG), not a personal avatar.
 */
export function bimi(options: BimiOptions = {}): AvatarSource {
  const ignore = options.ignoreDomains ?? FREE_MAIL_DOMAINS;
  const dohUrl = options.dohUrl ?? 'https://cloudflare-dns.com/dns-query';
  const { resolveTxt, requireAuthority = false } = options;

  return {
    name: 'bimi',
    limiter: toLimiter(options.rateLimit, { maxConcurrent: 4 }),
    lookup: async (email, ctx) => {
      const lookupTxt = (name: string) => (resolveTxt ? customTxt(resolveTxt, name, ctx.signal) : dohTxt(dohUrl, name, ctx));
      for (const domain of candidateDomains(email.domain)) {
        if (ignore.has(domain)) return null;
        const records = (await lookupTxt(`default._bimi.${domain}`)).filter((r) => BIMI_VERSION_RE.test(r));
        if (records.length === 0) continue;
        // More than one BIMI record makes the published assertion invalid.
        return records.length === 1 ? logoFrom(records[0]!, requireAuthority) : null;
      }
      return null;
    },
  };
}

/** The domain itself, then its parent (approximation of the DMARC organizational domain). */
function candidateDomains(domain: string): string[] {
  const labels = domain.split('.');
  return labels.length > 2 ? [domain, labels.slice(-2).join('.')] : [domain];
}

function logoFrom(record: string, requireAuthority: boolean): string | null {
  const tags = new Map<string, string>();
  for (const part of record.split(';')) {
    const eq = part.indexOf('=');
    if (eq > 0) tags.set(part.slice(0, eq).trim().toLowerCase(), part.slice(eq + 1).trim());
  }
  if (requireAuthority && !httpsUrl(tags.get('a'))) return null;
  // An empty `l=` is an explicit "no logo". Older drafts allowed a comma-separated list: take the first.
  return httpsUrl(tags.get('l')?.split(',')[0]);
}

function httpsUrl(value: string | undefined): string | null {
  if (!value) return null;
  try {
    const url = new URL(value.trim());
    return url.protocol === 'https:' ? url.toString() : null;
  } catch {
    return null;
  }
}

async function dohTxt(dohUrl: string, name: string, ctx: SourceContext): Promise<string[]> {
  const url = `${dohUrl}?name=${encodeURIComponent(name)}&type=TXT`;
  const res = await ctx.request<DohResponse | undefined>(url, {
    headers: { accept: 'application/dns-json' },
    read: (r) => (r.ok ? r.json() : Promise.resolve(undefined)),
  });
  if (res.status !== 200 || !res.body) throw new HttpError(res.status, url);
  if (res.body.Status === NXDOMAIN) return [];
  if (res.body.Status !== 0) throw new Error(`DNS lookup for ${name} failed (RCODE ${res.body.Status})`);
  return (res.body.Answer ?? []).filter((a) => a.type === TXT).map((a) => unquoteTxt(a.data));
}

async function customTxt(resolve: TxtResolver, name: string, signal: AbortSignal): Promise<string[]> {
  try {
    return (await resolve(name, signal)).map((chunks) => chunks.join(''));
  } catch (err) {
    const code = (err as { code?: unknown } | null)?.code;
    if (code === 'ENODATA' || code === 'ENOTFOUND') return [];
    throw err;
  }
}

/** DoH TXT data may be one or more quoted chunks (`"v=BIMI1; " "l=…"`); join them. */
function unquoteTxt(data: string): string {
  if (!data.startsWith('"')) return data;
  return Array.from(data.matchAll(/"((?:[^"\\]|\\.)*)"/g), (m) => m[1]!.replace(/\\(.)/g, '$1')).join('');
}
