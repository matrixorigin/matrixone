const assert = require('node:assert/strict');
const { execFile } = require('node:child_process');
const { mkdtempSync, readFileSync, rmSync } = require('node:fs');
const { createServer } = require('node:http');
const { tmpdir } = require('node:os');
const { join } = require('node:path');
const test = require('node:test');
const { promisify } = require('node:util');

const workflow = readFileSync(join(__dirname, '../workflows/coverage-identity-contract.yml'), 'utf8');
const verify = workflow.split('\n  verify:\n')[1];
const script = verify.match(/^        run: \|\n((?:(?: {10}.*)?\n)+)/m)[1].replace(/^ {10}/gm, '');
const sha = 'a'.repeat(40);
const jobs = [[17, 2, 0, 'cs'], [17, 2, 1, 'idc'], [1, 2, 0, 'cs'], [17, 999999, 0, 'cs']].map(([parent, attempt, group, pool]) => ({
  name: 'Coverage standby / ' + parent + '/' + attempt + '/' + group + '/' + sha + '/' + pool + '/' + sha + '0'.repeat(24) + ' / Run identity',
  run_id: 17, run_attempt: 2, status: 'completed', conclusion: 'skipped',
}));
jobs.push({ name: 'Read identity markers while active', run_id: 17, run_attempt: 2,
  status: 'in_progress', conclusion: null });

// Run the workflow's shell and jq unchanged. Only the API and the wait are simulated.
for (const [name, responses, expectPass, calls] of [
  ['immediately visible', [200], true, 1],
  ['404 then visible', [404, 200], true, 2],
  ['persistent 404', [404], false, 6],
  ['403 after 404', [404, 403, 200], false, 2],
  ['server error', [500, 200], false, 1],
  ['connection error', ['disconnect', 200], false, 1],
  ['incomplete metadata then ready', ['missing', 200], true, 2],
  ['markers visible but observer not running', ['inactive', 200], true, 2],
  ['404 then wrong attempt', [404, 'stale'], false, 6],
]) {
  test(name, async t => {
    const dir = mkdtempSync(join(tmpdir(), 'coverage-identity-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const urls = [];
    const server = createServer((req, res) => {
      const reply = responses[Math.min(urls.length, responses.length - 1)];
      urls.push(req.url);
      if (reply === 'disconnect') return req.socket.destroy();
      res.writeHead(typeof reply === 'number' ? reply : 200);
      const result = reply === 'missing' ? jobs.slice(0, 1) :
        reply === 'inactive' ? jobs.slice(0, -1) :
        reply === 'stale' ? jobs.map(j => ({ ...j, run_attempt: 1 })) : jobs;
      res.end(JSON.stringify({ jobs: result }));
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    t.after(() => new Promise(resolve => server.close(resolve)));
    const env = {
      PATH: process.env.PATH, RUNNER_TEMP: dir, GH_TOKEN: 'test-only',
      GITHUB_API_URL: 'http://127.0.0.1:' + server.address().port,
      GITHUB_REPOSITORY: 'example/repo', GITHUB_RUN_ID: '17', GITHUB_RUN_ATTEMPT: '2', GITHUB_SHA: sha,
    };
    const run = promisify(execFile)('bash', ['-c', 'sleep() { :; }\n' + script], { env, timeout: 5000 });
    if (expectPass) await run;
    else await assert.rejects(run, err => Number.isInteger(err.code) && err.code > 0);
    assert.deepEqual(urls, Array(calls).fill('/repos/example/repo/actions/runs/17/attempts/2/jobs?per_page=100'));
  });
}
