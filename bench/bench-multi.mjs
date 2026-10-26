// node bench.mjs <node-url> [N=500] [C=32]
// Trivial lua@5.3a counter process. Reports, in order:
//   rtt     sign -> schedule -> changed state back (sequential, the headline)
//   burst   schedule N concurrently, then one `now` computes the backlog
//   mixed   schedule N concurrently while a reader drives `now`; wall until count==N
import crypto from 'node:crypto';
import { readState, nodeAddress } from './hbclient.mjs';
import { signDataItem } from './ans104.mjs';
// Both spawn and messages are ANS-104, the path the browser uses.
async function postItem(path, item) {
  const body = await signDataItem(jwk, item);
  const res = await fetch(node + path + '?codec-device=ans104@1.0', { method: 'POST',
    headers: { 'content-type': 'application/ans104', 'accept-bundle': 'true' }, body });
  const txt = res.ok ? '' : (res.headers.get('details') || await res.text()).slice(0, 600);
  if (!res.ok) throw new Error(`${path} ${res.status} ${txt}`);
  await res.arrayBuffer().catch(() => {});
  return res.headers;
}
async function spawnProcess({ lua }) {
  const sched = await nodeAddress(node);
  const h = await postItem('/~scheduler@1.0/schedule', { data: lua, tags: [
    ['device', 'process@1.0'], ['type', 'Process'], ['scheduler-device', 'scheduler@1.0'],
    ['execution-device', 'lua@5.3a'], ['scheduler-location', sched], ['content-type', 'application/lua'],
    ['random-seed', String(Math.random())]].map(([name, value]) => ({ name, value })) });
  return h.get('process');
}
const sendMessage = ({ process: pid, tags }) => postItem(`/${pid}~process@1.0/schedule`, { target: pid, data: '',
  tags: [['type', 'Message'], ['action', 'Ping'], ['random-seed', String(Math.random())], ...Object.entries(tags)]
    .map(([name, value]) => ({ name, value })) });

const node = (process.argv[2] || 'http://localhost:8734').replace(/\/$/, '');
const N = Number(process.argv[3] || 500);
const C = Number(process.argv[4] || 32);
const P = Number(process.argv[5] || 1);
const { privateKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 4096, publicExponent: 65537 });
const jwk = privateKey.export({ format: 'jwk' });

const lua = `
function compute(base, req, opts)
  base.count = math.tointeger((base.count or 0) + 1)
  base.results = { output = { data = tostring(base.count) } }
  return base
end`;

const now = () => performance.now();
const med = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };
const p95 = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length * 0.95)]; };
async function count(pid) {
  const r = await readState({ node, process: pid, path: 'now/count' });
  if (r.status !== 200 || r.value.startsWith('<')) throw new Error(`now/count ${r.status} ${r.value.slice(0, 200)}`);
  return Number(r.value);
}
const send = (pid, i) => sendMessage({ node, jwk, process: pid, action: 'Ping', tags: { seq: String(i) }, push: false });
async function pool(n, c, fn) {
  let next = 0; const errs = [];
  await Promise.all(Array.from({ length: c }, async () => {
    while (next < n) { const i = next++; try { await fn(i); } catch (e) { errs.push(e.message); } }
  }));
  return errs;
}
async function untilCount(pid, target, timeoutMs = 600000) {
  const t = now();
  for (;;) {
    const c = await count(pid).catch(() => -1);
    if (c >= target) return c;
    if (now() - t > timeoutMs) throw new Error(`timeout at count ${c}/${target}`);
  }
}

const fresh = async () => {
  const pid = await spawnProcess({ lua }); console.error('pid', pid);
  await send(pid, 0); await untilCount(pid, 1);
  return pid;
};


// P processes, N messages each, scheduled concurrently; then compute all P backlogs at once.
const pids = await Promise.all(Array.from({ length: P }, fresh));
let t = now();
const errs = await pool(N * P, C, (i) => send(pids[i % P], i));
const sched = now() - t;
t = now();
await Promise.all(pids.map((pid) => untilCount(pid, 1 + N)));
const comp = now() - t;
console.log(JSON.stringify({ node, P, N, C, schedule_per_s: +(N * P / sched * 1000).toFixed(1),
  compute_total_per_s: +(N * P / comp * 1000).toFixed(1), compute_per_process_per_s: +(N / comp * 1000).toFixed(1),
  errors: errs.length }));
