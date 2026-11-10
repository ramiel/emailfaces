#!/usr/bin/env node
// `emailfaces` command: query avatar sources for one or more emails and print what each one answers.

import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { parseArgs } from 'node:util';
import { lastValueFrom, map, tap } from 'rxjs';
import {
  bimi,
  createAvatarResolver,
  domainIcon,
  github,
  gitlab,
  gravatar,
  libravatar,
  type AvatarResolver,
  type AvatarSource,
  type SourceOutcome,
} from './rxjs.js';

const FACTORIES: Record<string, () => AvatarSource> = {
  gravatar: () => gravatar(),
  libravatar: () => libravatar(),
  gitlab: () => gitlab({ token: process.env.GITLAB_TOKEN }),
  github: () => github({ token: process.env.GITHUB_TOKEN }),
  domain: () => domainIcon(),
  bimi: () => bimi(),
};

const HELP = `Usage: emailfaces [options] <email...>
       echo someone@example.com | emailfaces [options]

Queries every selected source and prints each outcome as it arrives, then the resolved avatar.

Options:
  -s, --sources <list>   comma-separated, in priority order (default: ${Object.keys(FACTORIES).join(',')})
  -p, --parallel         resolve with the 'parallel' strategy (default: sequential)
      --size <px>        avatar size (default: 200)
      --timeout <ms>     per-request timeout (default: 8000)
      --max-wait <ms>    max rate-limit wait per source (default: 30000)
      --json             print JSON lines instead of a table
  -v, --version
  -h, --help

Env: GITHUB_TOKEN, GITLAB_TOKEN are passed to their sources when set. NO_COLOR disables colors.
Exit code: 0 on success, 1 if any email failed (e.g. invalid), 2 on bad usage.`;

type Options = { json: boolean };
type TimedOutcome = SourceOutcome & { ms: number };

await main();

async function main(): Promise<void> {
  let parsed;
  try {
    parsed = parseArgs({
      allowPositionals: true,
      options: {
        sources: { type: 'string', short: 's' },
        parallel: { type: 'boolean', short: 'p', default: false },
        size: { type: 'string' },
        timeout: { type: 'string' },
        'max-wait': { type: 'string' },
        json: { type: 'boolean', default: false },
        version: { type: 'boolean', short: 'v', default: false },
        help: { type: 'boolean', short: 'h', default: false },
      },
    });
  } catch (err) {
    return usageError(`${message(err)}\n\n${HELP}`);
  }
  const { values, positionals } = parsed;

  if (values.help) return void console.log(HELP);
  if (values.version) return void console.log(version());

  const names = (values.sources ?? Object.keys(FACTORIES).join(',')).split(',').map((s) => s.trim()).filter(Boolean);
  const unknown = names.filter((n) => !(n in FACTORIES));
  if (unknown.length) return usageError(`Unknown source(s): ${unknown.join(', ')}. Available: ${Object.keys(FACTORIES).join(', ')}`);

  const emails = positionals.length ? positionals : await readStdin();
  if (!emails.length) return usageError(`No email given.\n\n${HELP}`);

  let resolver: AvatarResolver;
  try {
    resolver = createAvatarResolver({
      sources: names.map((n) => FACTORIES[n]!()),
      strategy: values.parallel ? 'parallel' : 'sequential',
      size: num(values.size, 'size'),
      timeoutMs: num(values.timeout, 'timeout'),
      maxWaitMs: num(values['max-wait'], 'max-wait'),
    });
  } catch (err) {
    return usageError(message(err));
  }

  const opts: Options = { json: values.json };
  let failed = false;
  for (const email of emails) {
    try {
      await run(resolver, email, opts);
    } catch (err) {
      failed = true;
      if (opts.json) console.log(JSON.stringify({ email, error: message(err) }));
      else console.error(`  ${red('✗')} ${message(err)}`);
    }
  }
  process.exitCode = failed ? 1 : 0;
}

async function run(resolver: AvatarResolver, email: string, { json }: Options): Promise<void> {
  if (!json) console.log(`\n${bold(email)}`);
  const t0 = performance.now();
  // lookup$ streams each source's outcome as soon as it settles, so the timing is per source.
  await lastValueFrom(
    resolver.lookup$(email).pipe(
      map((outcome): TimedOutcome => ({ ...outcome, ms: Math.round(performance.now() - t0) })),
      tap((o) => (json ? console.log(JSON.stringify(toJson(email, o))) : printOutcome(o))),
    ),
    { defaultValue: undefined },
  );
  // Outcomes are cached now, so this only applies the strategy / priority rules.
  const avatar = await resolver.resolve(email);
  if (json) console.log(JSON.stringify({ email, resolved: avatar }));
  else console.log(`  → ${avatar ? `${green(avatar.source)} ${avatar.url}` : dim('no avatar')}`);
}

function toJson(email: string, o: TimedOutcome): object {
  return 'error' in o ? { email, ...o, error: message(o.error) } : { email, ...o };
}

function printOutcome(o: TimedOutcome): void {
  const time = dim(`${String(o.ms).padStart(5)}ms`);
  let icon: string;
  let detail: string;
  switch (o.status) {
    case 'found':
      [icon, detail] = [green('✓'), o.url + (o.cached ? dim(' (cached)') : '')];
      break;
    case 'not_found':
      [icon, detail] = [dim('·'), dim('not found') + (o.cached ? dim(' (cached)') : '')];
      break;
    case 'rate_limited':
      [icon, detail] = [yellow('⏳'), `rate limited${o.retryAfterMs !== undefined ? `, retry in ${Math.ceil(o.retryAfterMs / 1000)}s` : ''}`];
      break;
    case 'error':
      [icon, detail] = [red('✗'), red(message(o.error))];
      break;
  }
  console.log(`  ${icon} ${o.source.padEnd(11)} ${time}  ${detail}`);
}

async function readStdin(): Promise<string[]> {
  if (process.stdin.isTTY) return [];
  const lines: string[] = [];
  for await (const line of createInterface({ input: process.stdin })) if (line.trim()) lines.push(line.trim());
  return lines;
}

function num(value: string | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) throw new Error(`--${name} must be a non-negative number`);
  return n;
}

function version(): string {
  const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')) as { version: string };
  return pkg.version;
}

function message(err: unknown): string {
  return err instanceof Error ? `${err.name}: ${err.message}` : String(err);
}

function usageError(msg: string): void {
  console.error(msg);
  process.exitCode = 2;
}

function color(code: number): (s: string) => string {
  return (s) => (process.stdout.isTTY && !process.env.NO_COLOR ? `\x1b[${code}m${s}\x1b[0m` : s);
}
function bold(s: string): string { return color(1)(s); }
function dim(s: string): string { return color(2)(s); }
function red(s: string): string { return color(31)(s); }
function green(s: string): string { return color(32)(s); }
function yellow(s: string): string { return color(33)(s); }
