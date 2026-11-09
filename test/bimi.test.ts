import { describe, expect, it } from 'vitest';
import { AvatarResolver, bimi, type TxtResolver } from '../src/index.js';
import { fakeFetch, json } from './helpers.js';

const txt = (...records: string[]) => () => json({ Status: 0, Answer: records.map((data) => ({ type: 16, data })) });
const nxdomain = () => json({ Status: 3 });
const q = (domain: string) => `name=default._bimi.${domain}&`;

describe('bimi', () => {
  it('reads the logo from the DoH TXT record, joining quoted chunks', async () => {
    const { fetch, calls } = fakeFetch({
      [q('acme.io')]: txt('"v=BIMI1; l=https://acme.io/" "logo.svg; a=https://acme.io/vmc.pem"'),
    });
    const r = new AvatarResolver({ fetch, sources: [bimi()] });
    expect(await r.resolve('someone@acme.io')).toEqual({ email: 'someone@acme.io', source: 'bimi', url: 'https://acme.io/logo.svg' });
    expect(calls[0]).toBe('https://cloudflare-dns.com/dns-query?name=default._bimi.acme.io&type=TXT');
  });

  it('falls back to the parent domain', async () => {
    const { fetch, calls } = fakeFetch({
      [q('mail.acme.io')]: nxdomain,
      [q('acme.io')]: txt('v=BIMI1; l=https://acme.io/logo.svg'),
    });
    const r = new AvatarResolver({ fetch, sources: [bimi()] });
    expect((await r.resolve('someone@mail.acme.io'))?.url).toBe('https://acme.io/logo.svg');
    expect(calls).toHaveLength(2);
  });

  it('ignores non-BIMI TXT records', async () => {
    const { fetch } = fakeFetch({ [q('acme.io')]: txt('"some-verification=abc"', '"v=BIMI1; l=https://acme.io/logo.svg"') });
    const r = new AvatarResolver({ fetch, sources: [bimi()] });
    expect((await r.resolve('someone@acme.io'))?.url).toBe('https://acme.io/logo.svg');
  });

  it('treats declined, non-https, ambiguous, and (optionally) unauthenticated records as misses', async () => {
    const { fetch } = fakeFetch({
      [q('declined.io')]: txt('"v=BIMI1; l=;"'),
      [q('plain.io')]: txt('"v=BIMI1; l=http://plain.io/logo.svg"'),
      [q('twice.io')]: txt('"v=BIMI1; l=https://twice.io/a.svg"', '"v=BIMI1; l=https://twice.io/b.svg"'),
      [q('noauth.io')]: txt('"v=BIMI1; l=https://noauth.io/logo.svg"'),
    });
    const r = new AvatarResolver({ fetch, sources: [bimi()] });
    expect(await r.resolve('a@declined.io')).toBeNull();
    expect(await r.resolve('a@plain.io')).toBeNull();
    expect(await r.resolve('a@twice.io')).toBeNull();
    expect((await r.resolve('a@noauth.io'))?.url).toBe('https://noauth.io/logo.svg');
    const strict = new AvatarResolver({ fetch, sources: [bimi({ requireAuthority: true })] });
    expect(await strict.resolve('a@noauth.io')).toBeNull();
  });

  it('skips free-mail domains without any request', async () => {
    const { fetch, calls } = fakeFetch({});
    const r = new AvatarResolver({ fetch, sources: [bimi()] });
    expect(await r.resolve('someone@gmail.com')).toBeNull();
    expect(calls).toHaveLength(0);
  });

  it('supports a custom TXT resolver (Node dns style)', async () => {
    const asked: string[] = [];
    const resolveTxt: TxtResolver = async (name) => {
      asked.push(name);
      if (name === 'default._bimi.acme.io') return [['v=BIMI1; ', 'l=https://acme.io/logo.svg']];
      throw Object.assign(new Error('no data'), { code: 'ENODATA' });
    };
    const { fetch, calls } = fakeFetch({});
    const r = new AvatarResolver({ fetch, sources: [bimi({ resolveTxt })] });
    expect((await r.resolve('x@mail.acme.io'))?.url).toBe('https://acme.io/logo.svg');
    expect(asked).toEqual(['default._bimi.mail.acme.io', 'default._bimi.acme.io']);
    expect(calls).toHaveLength(0);
  });

  it('reports DNS failures as an error outcome', async () => {
    const { fetch } = fakeFetch({ [q('acme.io')]: () => json({ Status: 2 }) });
    const [outcome] = await new AvatarResolver({ fetch, sources: [bimi()] }).lookup('a@acme.io');
    expect(outcome?.status).toBe('error');
  });
});
