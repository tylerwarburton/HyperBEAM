// node bench/view-compare.mjs <node-url> [N=50]
// The published-key view (hb_process_view) must answer `now/<key>' exactly as
// the generic path does. `only-if-cached: true' sends the same read the
// generic way (same max-age, infinity) so each pair is compared byte for
// byte: status, body and every response header but date/server timing ones.
// Present keys, a key the contract deletes, an absent key and a key in
// another case are checked after every slot.
import crypto from 'node:crypto';
import { signDataItem, generateWallet } from './ans104.mjs';

const node = (process.argv[2] || 'http://127.0.0.1:18734').replace(/\/$/, '');
const N = Number(process.argv[3] || 50);
const jwk = generateWallet();
const LUA = `
Count = Count or 0
function compute(req)
  Count = Count + 1
  local p = {{ path = '/count', value = tostring(Count) }, { path = '/nested', value = { a = tostring(Count), b = 'x' } }}
  if Count % 3 == 0 then p[#p + 1] = { path = '/flip', delete = true } else p[#p + 1] = { path = '/flip', value = 'on' .. Count } end
  return { patches = p, results = { output = { data = tostring(Count) } } }
end`;
const tags = (o) => Object.entries(o).map(([name, value]) => ({ name, value }));
async function post(path, item) {
  const res = await fetch(node + path, { method: 'POST', headers: { 'content-type': 'application/ans104' }, body: await signDataItem(jwk, item) });
  if (!res.ok) throw new Error(`${path} ${res.status} ${await res.text()}`);
  await res.arrayBuffer();
  return res.headers;
}
const sched = (await (await fetch(node + '/~meta@1.0/info/address')).text()).trim();
const pid = (await post('/~scheduler@1.0/schedule', { data: LUA, tags: tags({ device: 'process@1.0', type: 'Process',
  'scheduler-device': 'scheduler@1.0', 'execution-device': 'lua-patch@1.0', 'scheduler-location': sched,
  'content-type': 'application/lua', 'random-seed': crypto.randomBytes(8).toString('hex') }) })).get('process');
const IGNORE = new Set(['date', 'server-timing', 'connection', 'keep-alive', 'content-length', 'transfer-encoding']);
const snap = async (key, generic) => {
  const path = key.startsWith('compute&') ? key : `now/${key}`;
  const res = await fetch(`${node}/${pid}~process@1.0/${path}`, { headers: generic ? { 'only-if-cached': 'true' } : {} });
  const h = {}; for (const [k, v] of res.headers) if (!IGNORE.has(k)) h[k] = v;
  return { status: res.status, body: Buffer.from(await res.arrayBuffer()).toString('base64'), h };
};
let compared = 0, diffs = 0;
const until = async (slot) => { for (let i = 0; i < 400; i++) { const r = await snap('at-slot', true); if (r.status === 200 && Number(Buffer.from(r.body, 'base64')) >= slot) return; await new Promise((x) => setTimeout(x, 25)); } };
for (let i = 1; i <= N; i++) {
  const h = await post(`/${pid}~process@1.0/schedule`, { target: pid, data: '', tags: tags({ type: 'Message', action: 'Tick', 'random-seed': crypto.randomBytes(8).toString('hex') }) });
  await until(Number(h.get('slot')));
  const s = Number(h.get('slot'));
  for (const key of ['count', 'nested', 'flip', 'absent-key', 'Count', 'at-slot', 'results',
    `compute&slot=${s}/results/output/data`, `compute&slot=${s}/results/output`, `compute&slot=${Math.max(1, s - 3)}/results/output/data`,
    `compute&slot=${s}/results/absent`]) {
    const [fast, slow] = [await snap(key, false), await snap(key, true)];
    compared++;
    if (JSON.stringify(fast) !== JSON.stringify(slow)) {
      diffs++;
      if (diffs <= 5) console.log(`slot ${i} key ${key}\n  view    ${JSON.stringify(fast).slice(0, 400)}\n  generic ${JSON.stringify(slow).slice(0, 400)}`);
    }
  }
}
console.log(JSON.stringify({ pid, compared, diffs }));
process.exit(diffs ? 1 : 0);
