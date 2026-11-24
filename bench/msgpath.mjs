// node bench/msgpath.mjs <node-url> [--procs 1] [--clients 32] [--seconds 30]
//                         [--idle 30] [--msg trivial|pair] [--out file.json]
//                         [--container name] [--poll-ms 0]
//
// The per-message path of one process (or --procs N at once), on a local node.
// Each process is lua-patch@1.0 doing a trivial mutation (a counter); with
// --msg pair every message is the size of an order-book pair message (14
// tags, ~700 bytes of data) and the contract also writes a 200-byte value.
//
// Round trip is the headline: POST a signed ANS-104 message, and see the
// process's published state (`now/count`) reach that message's slot. One
// watcher per process polls `now/count` back to back and stamps the first
// time each value is seen; a message is confirmed when the watcher sees
// count >= its slot. Messages are signed ahead of time in worker threads, so
// the client's RSA-4096 signing is not in the measurement or its bottleneck.
//
//   idle  one client per process, sequential: the round trip with no queue
//   sat   --clients closed-loop clients per process (each waits for its
//         message to be confirmed before sending the next): confirmed msg/s
//         per process and node-wide, and the round trip under that load
//
// The node's /~hyperbuddy@1.0/phases is read around each window; the report
// gives each phase's delta: calls per confirmed message and mean us per call.
// With --container (a local Docker container running the node), the
// container's CPU time (cgroup cpu.stat) per confirmed message too: the
// number that sets node-wide capacity, and steadier than msg/s when other
// work shares the machine.
import crypto from 'node:crypto';
import os from 'node:os';
import fs from 'node:fs';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { execFileSync } from 'node:child_process';
import { signDataItem, generateWallet } from './ans104.mjs';

const LUA = `
Count = Count or 0
function compute(req)
  Count = Count + 1
  local patches = {{ path = '/count', value = tostring(Count) }}
  if req.body and req.body.side then
    patches[2] = { path = '/last', value = string.rep('x', 200) .. tostring(Count) }
  end
  return { patches = patches, results = { output = { data = tostring(Count) } } }
end
`;

function messageItem(pid, kind) {
  const tags = [['type', 'Message'], ['action', 'Ping'], ['random-seed', crypto.randomBytes(8).toString('hex')]];
  let data = '';
  if (kind === 'pair') {
    tags.push(['side', 'bid'], ['price', '1234567'], ['quantity', '250000000'], ['pair', 'RUNE/PUSD'],
      ['client-order-id', crypto.randomBytes(12).toString('hex')], ['time-in-force', 'GTC'],
      ['post-only', 'false'], ['reduce-only', 'false'], ['expiry', '0'], ['ledger', crypto.randomBytes(32).toString('base64url')],
      ['nonce', String(Date.now())]);
    data = JSON.stringify({ orders: Array.from({ length: 6 }, (_, i) => ({ side: 'bid', price: 1234567 + i, qty: 250000000, id: crypto.randomBytes(16).toString('hex') })) });
  }
  return { target: pid, data, tags: tags.map(([name, value]) => ({ name, value })) };
}

if (!isMainThread) {
  // Signer: sign whatever the main thread asks for, as fast as it asks.
  const { jwk } = workerData;
  parentPort.on('message', async ({ id, pid, kind }) => {
    const body = await signDataItem(jwk, messageItem(pid, kind));
    parentPort.postMessage({ id, body });
  });
} else {
  await main();
}

async function main() {
  const args = process.argv.slice(2);
  const node = (args.find((a) => !a.startsWith('--')) || 'http://localhost:18734').replace(/\/$/, '');
  const opt = (k, d) => { const i = args.indexOf(`--${k}`); return i >= 0 ? args[i + 1] : d; };
  const P = Number(opt('procs', 1));
  const C = Number(opt('clients', 32));
  const SECONDS = Number(opt('seconds', 30));
  const IDLE = Number(opt('idle', 30));
  const KIND = opt('msg', 'trivial');
  const OUT = opt('out', null);
  const CONTAINER = opt('container', null);
  // A pause between a watcher's polls of now/count: 0 polls back to back
  // (the round-trip measurement); larger values show what the reads cost.
  const POLL_MS = Number(opt('poll-ms', 0));
  const cpuUs = () => {
    if (!CONTAINER) return null;
    // cgroup v2 (cpu.stat, microseconds) or v1 (cpuacct.usage, nanoseconds).
    const t = execFileSync('docker', ['exec', CONTAINER, 'sh', '-c',
      'cat /sys/fs/cgroup/cpu.stat 2>/dev/null || echo ns $(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)'],
      { encoding: 'utf8' });
    const v2 = /usage_usec (\d+)/.exec(t);
    return v2 ? Number(v2[1]) : Number(/ns (\d+)/.exec(t)[1]) / 1000;
  };
  const now = () => performance.now();

  const jwk = generateWallet();
  // Signers: a pool that keeps a buffer of signed messages per process.
  const nWorkers = Math.max(2, Math.min(os.cpus().length - 2, 12));
  const workers = Array.from({ length: nWorkers }, () => new Worker(new URL(import.meta.url), { workerData: { jwk } }));
  const pending = new Map(); let nextId = 0; let rr = 0;
  for (const w of workers) w.on('message', ({ id, body }) => { const r = pending.get(id); pending.delete(id); r(Buffer.from(body)); });
  const sign = (pid) => new Promise((resolve) => { const id = nextId++; pending.set(id, resolve); workers[rr++ % nWorkers].postMessage({ id, pid, kind: KIND }); });
  const buffers = new Map();
  const take = (pid) => {
    const b = buffers.get(pid);
    while (b.length < 2 * C + 8) b.push(sign(pid));
    return b.shift();
  };

  async function post(path, body) {
    const res = await fetch(node + path, { method: 'POST',
      headers: { 'content-type': 'application/ans104', 'accept-bundle': 'true' }, body });
    const slot = res.headers.get('slot');
    const txt = res.ok ? '' : (res.headers.get('details') || await res.text()).slice(0, 400);
    if (res.ok) await res.arrayBuffer().catch(() => {});
    if (!res.ok) throw new Error(`${path} ${res.status} ${txt}`);
    return { headers: res.headers, slot: slot === null ? null : Number(slot) };
  }
  async function getText(path) {
    const res = await fetch(node + path, { headers: { accept: 'text/plain' } });
    return { status: res.status, text: await res.text() };
  }
  const phases = async () => JSON.parse((await getText('/~hyperbuddy@1.0/phases')).text);

  const sched = (await getText('/~meta@1.0/info/address')).text.trim();
  async function spawn() {
    const body = await signDataItem(jwk, { data: LUA, tags: [
      ['device', 'process@1.0'], ['type', 'Process'], ['scheduler-device', 'scheduler@1.0'],
      ['execution-device', 'lua-patch@1.0'], ['scheduler-location', sched], ['content-type', 'application/lua'],
      ['random-seed', crypto.randomBytes(8).toString('hex')]].map(([name, value]) => ({ name, value })) });
    const { headers } = await post('/~scheduler@1.0/schedule', body);
    return headers.get('process');
  }

  // Watcher: polls now/count back to back; resolves waiters for slot <= count.
  function watcher(pid) {
    const w = { pid, count: 0, waiters: [], stop: false, polls: 0, firstSeen: new Map() };
    w.until = (slot) => slot <= w.count ? Promise.resolve(now()) : new Promise((r) => w.waiters.push({ slot, r }));
    w.done = (async () => {
      while (!w.stop) {
        let r;
        try { r = await getText(`/${pid}~process@1.0/now/count`); } catch { continue; }
        w.polls++;
        if (POLL_MS > 0) await new Promise((r) => setTimeout(r, POLL_MS));
        const c = Number(r.text);
        if (r.status !== 200 || !Number.isFinite(c)) continue;
        if (c > w.count) {
          const t = now(); w.count = c;
          w.waiters = w.waiters.filter((x) => (x.slot <= c ? (x.r(t), false) : true));
        }
      }
    })();
    return w;
  }

  const pct = (a, p) => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); return +s[Math.min(s.length - 1, Math.floor(s.length * p))].toFixed(1); };

  // One message: POST, then wait for the watcher to see its slot.
  async function one(pid, w) {
    const body = await take(pid);
    const t0 = now();
    const { slot } = await post(`/${pid}~process@1.0/schedule`, body);
    const ts = now();
    const t1 = await w.until(slot);
    return { rtt: t1 - t0, sched: ts - t0, slot };
  }

  function phaseDelta(a, b, confirmed) {
    const out = {};
    for (const [k, v] of Object.entries(b.phases)) {
      const u = a.phases[k] || { count: 0, total_us: 0 };
      const n = v.count - u.count; const t = v.total_us - u.total_us;
      if (n <= 0) continue;
      out[k] = { per_msg: +(n / confirmed).toFixed(2), mean_us: Math.round(t / n), us_per_msg: Math.round(t / confirmed) };
    }
    return out;
  }

  console.error(`spawning ${P} process(es), msg=${KIND}`);
  const pids = [];
  for (let i = 0; i < P; i++) pids.push(await spawn());
  const ws = pids.map((pid) => { buffers.set(pid, []); return watcher(pid); });
  // Warm: one message each, sequential.
  for (let i = 0; i < P; i++) await one(pids[i], ws[i]);
  const report = { node, procs: P, clients: C, msg: KIND, pids };

  if (IDLE > 0) {
    const ms = []; const sc = [];
    const end = now() + IDLE * 1000;
    const p0 = await phases(); const u0 = cpuUs();
    let n = 0;
    while (now() < end && n < 400) { const r = await one(pids[0], ws[0]); ms.push(r.rtt); sc.push(r.sched); n++; }
    const p1 = await phases(); const u1 = cpuUs();
    report.idle = { n, rtt_p50_ms: pct(ms, 0.5), rtt_p95_ms: pct(ms, 0.95), sched_p50_ms: pct(sc, 0.5),
      cpu_ms_per_msg: u0 === null ? null : +((u1 - u0) / 1000 / n).toFixed(2),
      kreds_per_msg: p1.reductions == null ? null : Math.round((p1.reductions - p0.reductions) / 1000 / n),
      phases: phaseDelta(p0, p1, n) };
    console.error('idle', JSON.stringify({ n, rtt_p50_ms: report.idle.rtt_p50_ms, rtt_p95_ms: report.idle.rtt_p95_ms }));
  }

  {
    // Saturation: C closed-loop clients per process.
    const counts0 = ws.map((w) => w.count);
    const rtts = []; const scs = [];
    let measuring = false; let stop = false; let errors = 0; let firstErr = null;
    const loops = [];
    for (let i = 0; i < P; i++) for (let c = 0; c < C; c++) loops.push((async () => {
      while (!stop) {
        try { const r = await one(pids[i], ws[i]); if (measuring) { rtts.push(r.rtt); scs.push(r.sched); } }
        catch (e) { errors++; firstErr ??= e.message; await new Promise((r) => setTimeout(r, 50)); }
      }
    })());
    await new Promise((r) => setTimeout(r, 5000)); // ramp
    const c0 = ws.map((w) => w.count); const p0 = await phases(); const u0 = cpuUs(); const t0 = now();
    measuring = true;
    await new Promise((r) => setTimeout(r, SECONDS * 1000));
    measuring = false;
    const c1 = ws.map((w) => w.count); const t1 = now(); const u1 = cpuUs(); const p1 = await phases();
    stop = true;
    await Promise.all(loops);
    const per = c1.map((c, i) => (c - c0[i]) / ((t1 - t0) / 1000));
    const confirmed = c1.reduce((s, c, i) => s + c - c0[i], 0);
    report.sat = {
      seconds: SECONDS, confirmed,
      node_msg_s: +(confirmed / ((t1 - t0) / 1000)).toFixed(1),
      proc_msg_s: +(per.reduce((a, b) => a + b, 0) / P).toFixed(1),
      proc_msg_s_min: +Math.min(...per).toFixed(1),
      rtt_p50_ms: pct(rtts, 0.5), rtt_p95_ms: pct(rtts, 0.95), sched_p50_ms: pct(scs, 0.5), sched_p95_ms: pct(scs, 0.95),
      cpu_ms_per_msg: u0 === null ? null : +((u1 - u0) / 1000 / confirmed).toFixed(2),
      kreds_per_msg: p1.reductions == null ? null : Math.round((p1.reductions - p0.reductions) / 1000 / confirmed),
      cpu_cores: u0 === null ? null : +((u1 - u0) / 1000 / (t1 - t0)).toFixed(2),
      polls_per_msg: +(ws.reduce((a, w) => a + w.polls, 0) / Math.max(1, ws.reduce((a, w) => a + w.count, 0))).toFixed(2),
      errors, first_error: firstErr, run_queue: p1.run_queue,
      phases: phaseDelta(p0, p1, confirmed),
    };
    void counts0;
    console.error('sat', JSON.stringify({ node_msg_s: report.sat.node_msg_s, proc_msg_s: report.sat.proc_msg_s,
      rtt_p50_ms: report.sat.rtt_p50_ms, rtt_p95_ms: report.sat.rtt_p95_ms, errors }));
  }
  for (const w of ws) w.stop = true;
  await Promise.all(ws.map((w) => w.done));
  for (const w of workers) await w.terminate();
  const json = JSON.stringify(report, null, 1);
  if (OUT) fs.writeFileSync(OUT, json);
  console.log(json);
}
