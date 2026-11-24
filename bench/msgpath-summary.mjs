// node bench/msgpath-summary.mjs a.json [b.json]
// One line per run, then each window's phases by us per confirmed message
// (top 25); with two files, the second's delta against the first.
import fs from 'node:fs';
const [a, b] = process.argv.slice(2).map((f) => JSON.parse(fs.readFileSync(f, 'utf8')));
const line = (r) => `${r.msg} procs=${r.procs} clients=${r.clients}  sat: node ${r.sat.node_msg_s} msg/s, per-proc ${r.sat.proc_msg_s} (min ${r.sat.proc_msg_s_min}), rtt p50/p95 ${r.sat.rtt_p50_ms}/${r.sat.rtt_p95_ms} ms, sched p50 ${r.sat.sched_p50_ms}` +
  (r.idle ? `  |  idle rtt p50/p95 ${r.idle.rtt_p50_ms}/${r.idle.rtt_p95_ms} ms (sched ${r.idle.sched_p50_ms})` : '') + `  errors ${r.sat.errors}`;
console.log(line(a)); if (b) console.log(line(b));
for (const w of ['idle', 'sat']) {
  if (!a[w]) continue;
  const pa = a[w].phases, pb = b?.[w]?.phases;
  const keys = [...new Set([...Object.keys(pa), ...Object.keys(pb || {})])]
    .sort((x, y) => ((pb?.[y] || pa[y])?.us_per_msg || 0) - ((pb?.[x] || pa[x])?.us_per_msg || 0)).slice(0, 25);
  console.log(`\n${w}: phase  calls/msg  mean_us  us/msg` + (pb ? '   ->  calls/msg  mean_us  us/msg' : ''));
  for (const k of keys) {
    const f = (p) => p ? `${String(p.per_msg).padStart(6)} ${String(p.mean_us).padStart(8)} ${String(p.us_per_msg).padStart(7)}` : '     -        -       -';
    console.log(`  ${k.padEnd(28)} ${f(pa[k])}` + (pb ? `   -> ${f(pb[k])}` : ''));
  }
}
