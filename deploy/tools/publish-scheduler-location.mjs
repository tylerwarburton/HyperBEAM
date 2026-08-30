// Publish an ao Scheduler-Location record for a HyperBEAM node.
// Base-layer Arweave tx, signed by the node wallet, so ao-scheduler-utils can
// resolve the node's address (== scheduler-location on spawns) to its URL.
//
//   node tools/publish-scheduler-location.mjs <wallet.json> <url> [--dry-run]
//
// Tags copied verbatim from a known-good record (tWYnOtJo6nQRLK0I7dvMeW4CdIJqt6XPkRa8bJWSfqY).
import fs from 'node:fs';
import Arweave from 'arweave';

const [walletPath, rawUrl, ...rest] = process.argv.slice(2);
const dryRun = rest.includes('--dry-run');

if (!walletPath || !rawUrl) {
  console.error('usage: node publish-scheduler-location.mjs <wallet.json> <url> [--dry-run]');
  process.exit(2);
}
const url = rawUrl.replace(/\/+$/, ''); // no trailing slash

const arweave = Arweave.init({ host: 'arweave.net', port: 443, protocol: 'https' });
const jwk = JSON.parse(fs.readFileSync(walletPath, 'utf8'));
const address = await arweave.wallets.jwkToAddress(jwk);

const TAGS = [
  ['Content-Type', 'text/plain; charset=utf-8'],
  ['Data-Protocol', 'ao'],
  ['Type', 'Scheduler-Location'],
  ['Variant', 'ao.N.1'],
  ['Url', url],
  ['Time-To-Live', '60480000'],
  ['codec-device', 'ans104@1.0'],
  ['nonce', '1'],
  ['ao-types', 'nonce="integer", time-to-live="integer"'],
];

const tx = await arweave.createTransaction({ data: url }, jwk);
for (const [name, value] of TAGS) tx.addTag(name, value);
await arweave.transactions.sign(tx, jwk);

const feeAr = arweave.ar.winstonToAr(tx.reward);
const balW = await arweave.wallets.getBalance(address);
const balAr = arweave.ar.winstonToAr(balW);

console.log('signer      :', address);
console.log('url         :', url);
console.log('data bytes  :', Buffer.byteLength(url));
console.log('tx id       :', tx.id);
console.log('fee (AR)    :', feeAr);
console.log('wallet (AR) :', balAr);
console.log('tags        :');
for (const [n, v] of TAGS) console.log(`   ${n} = ${v}`);

if (dryRun) {
  console.log('\n[dry-run] built + priced OK; not posted.');
  process.exit(0);
}

const res = await arweave.transactions.post(tx);
console.log('\npost status :', res.status, res.statusText || '');
if (res.status === 200 || res.status === 208) {
  console.log('PUBLISHED', tx.id);
} else {
  console.error('POST FAILED', res.status, JSON.stringify(res.data || {}));
  process.exit(1);
}
