/** `url: null` records a negative result ("this source has no avatar for this email"). */
export interface CacheEntry {
  readonly url: string | null;
}

/** Pluggable cache; implement with Redis, KV, etc. Methods may be sync or async. */
export interface AvatarCache {
  get(key: string): CacheEntry | undefined | Promise<CacheEntry | undefined>;
  set(key: string, value: CacheEntry, ttlMs: number): void | Promise<void>;
}

/** In-memory LRU cache with per-entry TTL. */
export class MemoryCache implements AvatarCache {
  private readonly entries = new Map<string, { value: CacheEntry; expiresAt: number }>();

  constructor(private readonly maxEntries = 10_000) {}

  get(key: string): CacheEntry | undefined {
    const hit = this.entries.get(key);
    if (!hit) return undefined;
    this.entries.delete(key);
    if (hit.expiresAt <= Date.now()) return undefined;
    this.entries.set(key, hit); // bump recency
    return hit.value;
  }

  set(key: string, value: CacheEntry, ttlMs: number): void {
    this.entries.delete(key);
    this.entries.set(key, { value, expiresAt: Date.now() + ttlMs });
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next().value as string;
      this.entries.delete(oldest);
    }
  }

  clear(): void {
    this.entries.clear();
  }
}
