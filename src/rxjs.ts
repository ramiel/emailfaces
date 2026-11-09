/**
 * `emailfaces/rxjs`: everything from the root entry, plus a resolver with an Observable API.
 * Unsubscribing cancels in-flight HTTP requests.
 */
import type { Observable, ObservableInput } from 'rxjs';
import { AvatarResolver as PromiseAvatarResolver, type ManyResult, type ResolverOptions } from './resolver.js';
import type { Avatar, SourceOutcome } from './types.js';

export * from './index.js';

export class AvatarResolver extends PromiseAvatarResolver {
  /** Best avatar for `email` according to source priority, or null. Errors on invalid email. */
  resolve$(email: string): Observable<Avatar | null> {
    return this.resolveStream(email);
  }

  /** Every source's outcome, emitted as it arrives (all sources are queried concurrently). */
  lookup$(email: string): Observable<SourceOutcome> {
    return this.lookupStream(email);
  }

  /**
   * Resolve many emails (an array, iterable, or Observable) with bounded concurrency.
   * Each result is emitted as soon as it's ready; invalid emails are reported per item.
   */
  resolveMany$(emails: ObservableInput<string>, { concurrency = 4 }: { concurrency?: number } = {}): Observable<ManyResult> {
    return this.resolveManyStream(emails, { concurrency });
  }
}

export function createAvatarResolver(options?: ResolverOptions): AvatarResolver {
  return new AvatarResolver(options);
}
