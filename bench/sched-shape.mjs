// node bench/sched-shape.mjs <node-url> [N=10]
// The shape of a schedule POST's reply: status, every header and the body,
// with the values that differ per message (IDs, slots, times, signatures)
// replaced by their kind. Run against a node with process-direct-schedule
// on and one with it off, then diff: the direct path must answer the same.
// Also posts the same message twice (the second must be refused the same
// way) and one with a bad signature (refused before any slot).
import crypto from 'node:crypto';
import { signDataItem, generateWallet } from './ans104.mjs';
const node = (process.argv[2] || 'http://127.0.0.1:18735').replace(/\/$/, '');
const N = Number(process.argv[3] || 10);
const jwk = generateWallet();
const tags = (o) => Object.entries(o).map(([name, value]) => ({ name, value }));
const kind = (v) => /^[A-Za-z0-9_-]{43}$/.test(v) ? '<id>' : /^\d+$/.test(v) ? '<int>' : /^\d{10,}/.test(v) ? '<time>' : v.length > 120 ? `<${v.length > 400 ? 'long' : 'mid'}>` : v.replace(/[A-Za-z0-9_-]{43}/g, '<id>').replace(/\d{6,}/g, '<n>');
async function post(path, body) {
  const res = await fetch(node + path, { method: 'POST', headers: { 'content-type': 'application/ans104' }, body });
  const h = {};
  for (const [k, v] of res.headers) if (!['date', 'server-timing', 'connection', 'keep-alive', 'content-length', 'transfer-encoding'].includes(k)) h[k] = kind(v);
  const b = Buffer.from(await res.arrayBuffer());
  return { status: res.status, h, body: b.length > 200 ? `<${b.length > 2000 ? 'long' : 'mid'}>` : kind(b.toString()) };
}
const sched = (await (await fetch(node + '/~meta@1.0/info/scheduler-address')).text()).trim();
const spawn = await post('/~scheduler@1.0/schedule', await signDataItem(jwk, { data: 'function compute(req) return { patches = {{ path = "/n", value = "1" }} } end',
  tags: tags({ device: 'process@1.0', type: 'Process', 'scheduler-device': 'scheduler@1.0', 'execution-device': 'lua-patch@1.0',
    'scheduler-location': sched, 'content-type': 'application/lua', 'random-seed': crypto.randomBytes(8).toString('hex') }) }));
const pid = (await (await fetch(node + '/~scheduler@1.0/schedule', { method: 'POST', headers: { 'content-type': 'application/ans104' },
  body: await signDataItem(jwk, { data: 'x', tags: tags({ device: 'process@1.0', type: 'Process', 'scheduler-device': 'scheduler@1.0', 'execution-device': 'lua-patch@1.0', 'scheduler-location': sched, 'content-type': 'application/lua', 'random-seed': crypto.randomBytes(8).toString('hex') }) }) })).headers.get('process'));
void spawn;
const msg = () => signDataItem(jwk, { target: pid, data: '', tags: tags({ type: 'Message', action: 'Tick', 'random-seed': crypto.randomBytes(8).toString('hex') }) });
const out = [];
// The first post starts the server (generic path); the rest go direct when on.
for (let i = 0; i < N; i++) out.push(['post', await post(`/${pid}~process@1.0/schedule`, await msg())]);
const again = await msg();
out.push(['first', await post(`/${pid}~process@1.0/schedule`, again)]);
out.push(['repeat', await post(`/${pid}~process@1.0/schedule`, again)]);
const bad = Buffer.from(await msg()); bad[10] ^= 0xff;
out.push(['bad-signature', await post(`/${pid}~process@1.0/schedule`, bad)]);
const wrong = await signDataItem(jwk, { target: sched, data: '', tags: tags({ type: 'Message', action: 'Tick', 'random-seed': '1' }) });
out.push(['wrong-target', await post(`/${pid}~process@1.0/schedule`, wrong)]);
const uniq = [...new Set(out.slice(1, N).map(([, r]) => JSON.stringify(r)))];
console.log(JSON.stringify({ posts: uniq, first: out[N][1], repeat: out[N + 1][1], bad: out[N + 2][1], wrongTarget: out[N + 3][1] }, null, 1));
