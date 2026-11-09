import type { AvatarSource } from '../types.js';
import { github } from './github.js';
import { gitlab } from './gitlab.js';
import { gravatar, libravatar } from './hashed.js';

export { gravatar, libravatar, hashedAvatarSource, type HashedSourceOptions } from './hashed.js';
export { github, type GitHubOptions } from './github.js';
export { gitlab, type GitLabOptions } from './gitlab.js';
export { domainIcon, FREE_MAIL_DOMAINS, type DomainIconOptions } from './domain-icon.js';

/** Cheapest / most generous sources first; GitHub (10 req/min anonymous) last. */
export function defaultSources(): AvatarSource[] {
  return [gravatar(), libravatar(), gitlab(), github()];
}
