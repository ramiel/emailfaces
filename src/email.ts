import type { EmailInfo } from './types.js';

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** Trims and lowercases an address. Returns null when it doesn't look like an email. */
export function parseEmail(input: string): EmailInfo | null {
  const address = input.trim().toLowerCase();
  if (!EMAIL_RE.test(address)) return null;
  const at = address.lastIndexOf('@');
  return { address, local: address.slice(0, at), domain: address.slice(at + 1) };
}

/** Hex SHA-256 using Web Crypto (Node >= 20, browsers, Deno, Bun, workers). */
export async function sha256Hex(value: string): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
}
